use crate::KafkaOption;
use std::thread;
use std::time::{Duration, SystemTime};

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::veto_configuration_vc00::{
    Vetoes, VetoesArgs, finish_vetoes_buffer,
};

use crate::cli_utils::set_kafka_options;
use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::{
    RunStart, RunStartArgs, SpectraDetectorMapping, SpectraDetectorMappingArgs,
    finish_run_start_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::{
    RunStop, RunStopArgs, finish_run_stop_buffer,
};
use log::{debug, error, info, warn};
use rand::RngExt;
use rand::prelude::ThreadRng;
use rand_distr::{Distribution, Normal};
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use serde_json::json;
use uuid::Uuid;

pub const VETO_COUNT: usize = 32;

fn generate_run_start<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    det_max: i32,
    event_topic: &str,
    job_id: &str,
) -> &'a [u8] {
    fbb.reset();
    let args = SpectraDetectorMappingArgs {
        spectrum: Some(fbb.create_vector(&(0..=det_max).collect::<Vec<_>>())),
        detector_id: Some(fbb.create_vector(&(0..=det_max).collect::<Vec<_>>())),
        n_spectra: det_max,
    };

    let nexus_structure = json!( {
        "children": [
            {
                "type": "group",
                "name": "raw_data_1",
                "children": [
                    {
                        "type": "group",
                        "name": "events",
                        "children": [
                            {
                                "type": "stream",
                                "stream": {
                                    "topic": event_topic,
                                    "source": "saluki_howl",
                                    "writer_module": "ev44",
                                },
                            },
                        ],
                        "attributes": [{"name": "NX_class", "values": "NXentry"}],
                    },
                ],
                "attributes": [{"name": "NX_class", "values": "NXentry"}],
            }
        ]
    });

    let det_spec_map_buf = SpectraDetectorMapping::create(fbb, &args);
    let file_name = Uuid::new_v4().to_string();
    let run_name = format!("saluki-howl-{}", Uuid::new_v4());

    let start_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_millis();

    let run_start_args = RunStartArgs {
        start_time: start_time as u64,
        stop_time: 0, // TODO check this - it's optional so not necessarily 0
        run_name: Some(fbb.create_string(&run_name)),
        instrument_name: Some(fbb.create_string("saluki-howl")),
        nexus_structure: Some(fbb.create_string(&nexus_structure.to_string())),
        job_id: Some(fbb.create_string(job_id)),
        broker: None,
        service_id: None,
        filename: Some(fbb.create_string(&file_name)),
        n_periods: 1,
        detector_spectrum_map: Some(det_spec_map_buf),
        metadata: None,
        control_topic: None,
    };
    let run_start_buf = RunStart::create(fbb, &run_start_args);

    finish_run_start_buffer(fbb, run_start_buf);
    fbb.finished_data()
}

fn generate_run_stop<'a>(fbb: &'a mut FlatBufferBuilder<'_>, job_id: &str) -> &'a [u8] {
    fbb.reset();
    let stop_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_millis();

    let run_stop_args = RunStopArgs {
        stop_time: stop_time as u64,
        run_name: None,
        job_id: Some(fbb.create_string(job_id)),
        service_id: None,
        command_id: Some(fbb.create_string("")),
    };
    let run_stop_buf = RunStop::create(fbb, &run_stop_args);
    finish_run_stop_buffer(fbb, run_stop_buf);
    fbb.finished_data()
}

fn get_veto_names_fbb<'a>(
    veto_names: &[String],
    fbb: &mut FlatBufferBuilder<'a>,
    buf: &mut Vec<WIPOffset<&'a str>>,
) {
    buf.clear();

    for i in 0..VETO_COUNT {
        let name = veto_names
            .get(i)
            .cloned()
            .unwrap_or(format!("saluki_veto_{i}"));

        buf.push(fbb.create_string(&name.to_string()));
    }
}

fn get_enabled_vetoes(conf: &HowlConfig, rng: &mut ThreadRng) -> u32 {
    let mut vetoes = 0;
    let mut prob;

    for i in 0..VETO_COUNT {
        prob = conf.veto_probability.get(i).cloned().unwrap_or(0.0);
        vetoes = (vetoes << 1) | rng.random_bool(prob) as u32;
    }

    vetoes
}

fn get_active_vetoes(conf: &HowlConfig) -> u32 {
    let mut vetoes = 0;
    let mut active;

    for i in 0..VETO_COUNT {
        active = conf.enabled_vetoes.get(i).cloned().unwrap_or(false);
        vetoes = (vetoes << 1) | active as u32;
    }

    vetoes
}

fn produce_messages(
    producer: &ThreadedProducer<DefaultProducerContext>,
    fbb: &mut FlatBufferBuilder,
    rng: &mut ThreadRng,
    frame: u32,
    conf: &HowlConfig,
    current_job_id: &mut String,
    vetoes_mask: &u32,
) {
    // get current time
    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_nanos()
        .try_into()
        .expect("This will fail after April 11th, 2262");

    match producer.send(
        BaseRecord::to(conf.event_topic)
            .key("")
            .payload(generate_fake_metadata(vetoes_mask, fbb, now_nanos))
            .timestamp(now_nanos / 1_000_000),
    ) {
        Ok(_) => {}
        Err(err) => {
            error!("Failed to send messages: {}", err.0);
        }
    }

    let ev44 = generate_fake_events(fbb, rng, frame, conf.event_message_config, now_nanos).to_vec();

    for _ in 0..conf.messages_per_frame {
        match producer.send(
            BaseRecord::to(conf.event_topic)
                .key("")
                .payload(if conf.fast {
                    ev44.as_slice()
                } else {
                    generate_fake_events(fbb, rng, frame, conf.event_message_config, now_nanos)
                })
                .timestamp(now_nanos / 1_000_000),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send messages: {}", err.0);
            }
        }
    }

    if conf.frames_per_run > 0 && frame.is_multiple_of(conf.frames_per_run) {
        info!(
            "Starting new run after {} simulated frames",
            conf.frames_per_run
        );
        match producer.send(
            BaseRecord::to(conf.run_info_topic)
                .key("")
                .payload(generate_run_stop(fbb, current_job_id))
                .timestamp(now_nanos / 1_000_000),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send run stop: {}", err.0);
            }
        }
        *current_job_id = Uuid::new_v4().to_string();
        match producer.send(
            BaseRecord::to(conf.run_info_topic)
                .key("")
                .payload(generate_run_start(
                    fbb,
                    conf.event_message_config.det_max,
                    conf.event_topic,
                    current_job_id,
                ))
                .timestamp(now_nanos / 1_000_000),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send run start: {}", err.0);
            }
        }
    }
}

pub struct EventMessageConfig {
    pub events_per_message: i32,
    pub tof_peak: f32,
    pub tof_sigma: f32,
    pub det_min: i32,
    pub det_max: i32,
}

fn generate_fake_events<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    rng: &mut ThreadRng,
    msg_id: u32,
    conf: &EventMessageConfig,
    timestamp_ns: i64,
) -> &'a [u8] {
    fbb.reset();

    let det_ids: Vec<i32> = (0..conf.events_per_message)
        .map(|_| rng.random_range(conf.det_min..=conf.det_max))
        .collect();

    let normal =
        Normal::new(conf.tof_peak, conf.tof_sigma).expect("Failed to generate normal distribution");
    let tofs: Vec<i32> = (0..conf.events_per_message)
        .map(|_| normal.sample(rng) as i32)
        .collect();

    let args = Event44MessageArgs {
        source_name: Some(fbb.create_string("saluki")),
        message_id: msg_id as i64,
        reference_time: Some(fbb.create_vector(&[timestamp_ns])),
        reference_time_index: Some(fbb.create_vector(&[0])),
        time_of_flight: Some(fbb.create_vector(&tofs)),
        pixel_id: Some(fbb.create_vector(&det_ids)),
    };
    let ev44 = Event44Message::create(fbb, &args);
    finish_event_44_message_buffer(fbb, ev44);
    fbb.finished_data()
}

fn generate_fake_metadata<'a>(
    vetoes_mask: &u32,
    fbb: &'a mut FlatBufferBuilder<'_>,
    timestamp_ns: i64,
) -> &'a [u8] {
    fbb.reset();

    let args = Pu00MessageArgs {
        reference_time: timestamp_ns,
        message_id: 0,
        source_name: Some(fbb.create_string("saluki")),
        period_number: Some(0),
        vetos: Some(*vetoes_mask), // active
        proton_charge: Some(0.1),
    };
    let pu00 = Pu00Message::create(fbb, &args);
    finish_pu_00_message_buffer(fbb, pu00);

    fbb.finished_data()
}

fn generate_veto_config<'a>(
    veto_names: &[String],
    fbb: &'a mut FlatBufferBuilder<'_>,
    timestamp_ns: i64,
    vetoes_mask: &u32,
) -> &'a [u8] {
    fbb.reset();
    let mut veto_names_fbb: Vec<WIPOffset<&str>> = Vec::new();
    get_veto_names_fbb(veto_names, fbb, &mut veto_names_fbb);

    let args = VetoesArgs {
        timestamp: timestamp_ns,
        vetoes: *vetoes_mask, // enabled
        veto_names: Some(fbb.create_vector(&veto_names_fbb)),
    };
    let vc00 = Vetoes::create(fbb, &args);
    finish_vetoes_buffer(fbb, vc00);

    fbb.finished_data()
}

fn calculate_data_rate(
    fbb: &mut FlatBufferBuilder<'_>,
    rng: &mut ThreadRng,
    conf: &HowlConfig,
    timestamp_ns: i64,
    vetoes_mask: &u32,
) {
    let ev44_size =
        generate_fake_events(fbb, rng, 0, conf.event_message_config, timestamp_ns).len() as u32;
    debug!("ev44 size is {ev44_size} bytes");

    let pu00_size = generate_fake_metadata(vetoes_mask, fbb, timestamp_ns).len() as u32;
    debug!("pu00 size is {pu00_size} bytes");

    let rate_bytes_per_sec = ev44_size * conf.messages_per_frame * conf.frames_per_second
        + pu00_size * conf.frames_per_second;
    debug!("bytes per second: {rate_bytes_per_sec}");

    let rate_mbit_per_sec = (rate_bytes_per_sec as f64 / (1024. * 1024.)) * 8.0;
    let rate_mebibits_per_sec = rate_mbit_per_sec / 8.0;
    debug!("rate mbit per sec: {rate_mbit_per_sec}");
    println!(
        "Attempting to simulate data rate: {rate_mbit_per_sec:.3} Mbit/s ({rate_mebibits_per_sec:.3} MiB/s)"
    );
    println!("Each pu00 is {pu00_size} bytes");
    println!("Each ev44 is {ev44_size} bytes");
}

fn send_run_start(
    producer: &mut ThreadedProducer<DefaultProducerContext>,
    fbb: &mut FlatBufferBuilder<'_>,
    conf: &HowlConfig,
    current_job_id: &str,
    now_nanos: i64,
) {
    producer
        .send(
            BaseRecord::to(conf.run_info_topic)
                .key("")
                .payload(generate_run_start(
                    fbb,
                    conf.event_message_config.det_max,
                    conf.event_topic,
                    current_job_id,
                ))
                .timestamp(now_nanos / 1_000_000),
        )
        .expect("Failed to enqueue run start message");
}

fn send_veto_config(
    producer: &mut ThreadedProducer<DefaultProducerContext>,
    fbb: &mut FlatBufferBuilder<'_>,
    conf: &HowlConfig,
    vetoes_mask: &u32,
    now_nanos: i64,
) {
    producer
        .send(
            BaseRecord::to(conf.veto_config_topic)
                .key("")
                .payload(generate_veto_config(
                    &conf.veto_names,
                    fbb,
                    now_nanos,
                    vetoes_mask,
                ))
                .timestamp(now_nanos / 1_000_000),
        )
        .expect("Failed to enqueue run veto configuration message");
}

fn howl_begin(
    producer: &mut ThreadedProducer<DefaultProducerContext>,
    fbb: &mut FlatBufferBuilder<'_>,
    rng: &mut ThreadRng,
    conf: &HowlConfig,
    current_job_id: &mut String,
    vetoes_mask: &u32,
) {
    let target_frame_time = Duration::from_secs_f64(1.0 / conf.frames_per_second as f64);
    debug!("Target frame time: {target_frame_time:?}");

    let mut frames: u32 = 0;

    let mut target_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time");
    debug!("Target time: {target_time:?}");

    loop {
        target_time += target_frame_time;
        debug!("New target: {target_time:?}");
        frames += 1;
        debug!("current job id: {current_job_id}");
        produce_messages(
            producer,
            fbb,
            rng,
            frames,
            conf,
            current_job_id,
            vetoes_mask,
        );
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Failed to get system time");

        debug!("Current time: {now:?}");
        debug!("Target time: {target_time:?}");

        if target_time > now {
            let sleep_time = target_time - now;
            thread::sleep(sleep_time);
        } else {
            let behind = now - target_time;
            warn!(
                "saluki howl running {} ms behind schedule",
                behind.as_millis()
            )
        }
    }
}

pub struct HowlConfig<'a> {
    pub broker: &'a str,
    pub event_topic: &'a str,
    pub run_info_topic: &'a str,
    pub veto_config_topic: &'a str,
    pub messages_per_frame: u32,
    pub frames_per_second: u32,
    pub frames_per_run: u32,
    pub veto_probability: Vec<f64>,
    pub enabled_vetoes: Vec<bool>,
    pub veto_names: Vec<String>,
    pub event_message_config: &'a EventMessageConfig,
    pub fast: bool,
    pub kafka_config: Option<Vec<KafkaOption>>,
}

pub fn howl(conf: &HowlConfig) {
    let mut fbb = FlatBufferBuilder::new();
    let mut rng = rand::rng();

    let active_vetoes = get_active_vetoes(conf);
    let enabled_vetoes = get_enabled_vetoes(conf, &mut rng);

    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_nanos()
        .try_into()
        .expect("This will fail after April 11th, 2262");

    calculate_data_rate(&mut fbb, &mut rng, conf, now_nanos, &active_vetoes);

    let mut client_config: ClientConfig = ClientConfig::new();
    client_config.set("bootstrap.servers", conf.broker);
    set_kafka_options(&mut client_config, &conf.kafka_config);

    // create producer
    let mut producer: ThreadedProducer<DefaultProducerContext> =
        client_config.create().expect("Producer creation error");

    let mut current_job_id = Uuid::new_v4().to_string();

    send_run_start(&mut producer, &mut fbb, conf, &current_job_id, now_nanos);
    send_veto_config(&mut producer, &mut fbb, conf, &enabled_vetoes, now_nanos);
    howl_begin(
        &mut producer,
        &mut fbb,
        &mut rng,
        conf,
        &mut current_job_id,
        &active_vetoes,
    );
}
