use std::thread;
use std::time::{Duration, SystemTime};

use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
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

fn generate_run_start<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    det_max: i32,
    topic_prefix: &str,
    job_id: &str,
) -> &'a [u8] {
    fbb.reset();
    let args = SpectraDetectorMappingArgs {
        spectrum: Some(fbb.create_vector(&(0..=det_max).collect::<Vec<_>>())),
        detector_id: Some(fbb.create_vector(&(0..=det_max).collect::<Vec<_>>())),
        n_spectra: det_max,
    };
    let events_topic = format!("{topic_prefix}_rawEvents");

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
                                    "topic": events_topic,
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
        .unwrap()
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
        .unwrap()
        .as_millis();

    let run_stop_args = RunStopArgs {
        stop_time: stop_time as u64,
        run_name: None,
        job_id: Some(fbb.create_string(job_id)),
        service_id: None,
        command_id: None,
    };
    let run_stop_buf = RunStop::create(fbb, &run_stop_args);
    finish_run_stop_buffer(fbb, run_stop_buf);
    fbb.finished_data()
}

#[allow(clippy::too_many_arguments)]
fn produce_messages(
    producer: &ThreadedProducer<DefaultProducerContext>,
    fbb: &mut FlatBufferBuilder,
    rng: &mut ThreadRng,
    frame: u32,
    topic_prefix: &str,
    events_per_message: i32,
    messages_per_frame: u32,
    frames_per_run: u32,
    tof_peak: f32,
    tof_sigma: f32,
    det_min: i32,
    det_max: i32,
    mut current_job_id: String,
) -> String {
    // get current time
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f32;

    for _ in 0..messages_per_frame {
        match producer.send(
            BaseRecord::to(format!("{topic_prefix}_rawEvents").as_str())
                .key("")
                .payload(generate_fake_events(
                    fbb,
                    rng,
                    frame,
                    events_per_message,
                    tof_peak,
                    tof_sigma,
                    det_min,
                    det_max,
                    now,
                )),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send messages: {}", err.0);
            }
        }
    }

    if frames_per_run > 0 && frame.is_multiple_of(frames_per_run) {
        info!("Starting new run after {frames_per_run} simulated frames");
        match producer.send(
            BaseRecord::to(format!("{topic_prefix}_runInfo").as_str())
                .key("")
                .payload(generate_run_start(
                    fbb,
                    det_max,
                    topic_prefix,
                    &current_job_id,
                )),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send run start: {}", err.0);
            }
        }
        current_job_id = Uuid::new_v4().to_string();
        match producer.send(
            BaseRecord::to(format!("{topic_prefix}_runInfo").as_str())
                .key("")
                .payload(generate_run_stop(fbb, &current_job_id)),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send run stop: {}", err.0);
            }
        }
    }

    current_job_id
}

#[allow(clippy::too_many_arguments)]
fn generate_fake_events<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    rng: &mut ThreadRng,
    msg_id: u32,
    events_per_message: i32,
    tof_peak: f32,
    tof_sigma: f32,
    det_min: i32,
    det_max: i32,
    timestamp: f32,
) -> &'a [u8] {
    fbb.reset();

    let det_ids: Vec<i32> = (0..events_per_message)
        .map(|_| rng.random_range(det_min..=det_max))
        .collect();

    let normal = Normal::new(tof_peak, tof_sigma).unwrap();
    let tofs: Vec<i32> = (0..events_per_message)
        .map(|_| normal.sample(rng) as i32)
        .collect();

    let args = Event44MessageArgs {
        source_name: Some(fbb.create_string("saluki")),
        message_id: msg_id as i64,
        reference_time: Some(fbb.create_vector(&[(timestamp * 1_000_000_000.0) as i64])),
        reference_time_index: Some(fbb.create_vector(&[0])),
        time_of_flight: Some(fbb.create_vector(&tofs)),
        pixel_id: Some(fbb.create_vector(&det_ids)),
    };
    let ev44 = Event44Message::create(fbb, &args);
    finish_event_44_message_buffer(fbb, ev44);
    fbb.finished_data()
}

#[allow(clippy::too_many_arguments)]
pub fn howl(
    broker: &str,
    topic_prefix: &str,
    events_per_message: i32,
    messages_per_frame: u32,
    frames_per_second: u32,
    frames_per_run: u32,
    tof_peak: f32,
    tof_sigma: f32,
    det_min: i32,
    det_max: i32,
) {
    // create producer
    let mut fbb = FlatBufferBuilder::new();
    let mut rng = rand::rng();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f32;
    let ev44_size = generate_fake_events(
        &mut fbb,
        &mut rng,
        0,
        events_per_message,
        tof_peak,
        tof_sigma,
        det_min,
        det_max,
        now,
    )
    .len() as u32;

    debug!("ev44 size is {ev44_size} bytes");

    // calculate rate
    let rate_bytes_per_sec = ev44_size * messages_per_frame * frames_per_second;
    debug!("bytes per second: {rate_bytes_per_sec}");

    let rate_mbit_per_sec: f32 = (rate_bytes_per_sec as f32 / 1024f32.powf(2.0)) * 8.0;
    let rate_mebibits_per_sec = rate_mbit_per_sec / 8.0;
    debug!("rate mbit per sec: {rate_mbit_per_sec}");
    println!(
        "Attempting to simulate data rate: {rate_mbit_per_sec:.3} Mbit/s ({rate_mebibits_per_sec:.3} MiB/s)"
    );
    println!("Each ev44 is {ev44_size} bytes");

    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .create()
        .expect("Producer creation error");

    let mut current_job_id = Uuid::new_v4().to_string();

    let runinfo_topic = format!("{topic_prefix}_runInfo");

    producer
        .send(
            BaseRecord::to(runinfo_topic.as_str())
                .key("")
                .payload(generate_run_start(
                    &mut fbb,
                    det_max,
                    topic_prefix,
                    &current_job_id,
                )),
        )
        .expect("Failed to enqueue run start message");

    let target_frame_time = Duration::from_secs_f64(1.0 / frames_per_second as f64);
    debug!("Target frame time: {target_frame_time:?}");

    let mut frames: u32 = 0;

    let mut target_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    debug!("Target time: {target_time:?}");
    loop {
        target_time += target_frame_time;
        debug!("New target: {target_time:?}");
        frames += 1;

        current_job_id = produce_messages(
            &producer,
            &mut fbb,
            &mut rng,
            frames,
            topic_prefix,
            events_per_message,
            messages_per_frame,
            frames_per_run,
            tof_peak,
            tof_sigma,
            det_min,
            det_max,
            current_job_id,
        );
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

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
