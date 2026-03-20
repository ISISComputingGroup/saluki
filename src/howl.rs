use std::thread;
use std::time::{Duration, SystemTime};

use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::det_spec_map_df12::SPECTRA_DETECTOR_MAPPING_IDENTIFIER;
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
use log::{debug, warn};
use rand::{Rng, RngExt, rng};
use rand_distr::{Distribution, Normal};
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use serde_json::json;
use uuid::Uuid;

fn generate_run_start<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    det_max: i32,
    topic_prefix: String,
    job_id: String,
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
        stop_time: 0, // TODO check this
        run_name: Some(fbb.create_string(run_name.as_str())),
        instrument_name: Some(fbb.create_string("saluki-howl")),
        nexus_structure: Some(fbb.create_string(nexus_structure.to_string().as_str())), // TODO
        job_id: Some(fbb.create_string(job_id.as_str())),
        broker: None,
        service_id: None,
        filename: Some(fbb.create_string(file_name.as_str())),
        n_periods: 1,
        detector_spectrum_map: Some(det_spec_map_buf),
        metadata: None,
        control_topic: None,
    };
    let run_start_buf = RunStart::create(fbb, &run_start_args);

    finish_run_start_buffer(fbb, run_start_buf);
    fbb.finished_data()
}

fn generate_run_stop<'a>(fbb: &'a mut FlatBufferBuilder<'_>, job_id: String) -> &'a [u8] {
    fbb.reset();
    let stop_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let run_stop_args = RunStopArgs {
        stop_time: stop_time as u64,
        run_name: None,
        job_id: Some(fbb.create_string(job_id.as_str())),
        service_id: None,
        command_id: None,
    };
    let run_stop_buf = RunStop::create(fbb, &run_stop_args);
    finish_run_stop_buffer(fbb, run_stop_buf);
    fbb.finished_data()
}

fn produce_messages( producer: &BaseProducer, fbb: &mut FlatBufferBuilder, topic_prefix: String, events_per_message: i32, messages_per_frame: i32, frames_per_run: i32, tof_peak: i32, tof_sigma: i32, det_min: i32, det_max: i32, current_job_id: String) -> String {
    // get currnet time

    // generate fake events

    // for x in range(messages_per_frame)

    // poll producer

    // create run stop + start if frames_per_run > 0 and  frame % frames_per_run == 0

    // return current job id

    current_job_id
}

fn generate_fake_events<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    msg_id: i64,
    events_per_message: i32,
    tof_peak: f32,
    tof_sigma: f32,
    det_min: i32,
    det_max: i32,
    timestamp: f32,
) -> &'a [u8] {
    fbb.reset();
    let mut rng = rand::rng();

    let det_ids: Vec<i32> = (0..events_per_message)
        .map(|_| rng.random_range(det_min..=det_max))
        .collect();

    let normal = Normal::new(tof_peak, tof_sigma).unwrap();
    let tofs: Vec<i32> = (0..events_per_message)
        .map(|_| normal.sample(&mut rng) as i32)
        .collect();

    let args = Event44MessageArgs {
        source_name: Some(fbb.create_string("saluki")),
        message_id: msg_id,
        reference_time: Some(fbb.create_vector(&[(timestamp * 1_000_000_000.0) as i64])),
        reference_time_index: Some(fbb.create_vector(&vec![0])),
        time_of_flight: Some(fbb.create_vector(&tofs)),
        pixel_id: Some(fbb.create_vector(&det_ids)),
    };
    let ev44 = Event44Message::create(fbb, &args);
    finish_event_44_message_buffer(fbb, ev44);
    fbb.finished_data()
}

pub fn howl(
    broker: String,
    topic_prefix: String,
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

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f32;
    let ev44_size = generate_fake_events(
        &mut fbb,
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

    let producer: BaseProducer = ClientConfig::new()
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
                    current_job_id,
                )),
        )
        .expect("Failed to enqueue run start message");

    let target_frame_time = Duration::from_secs((1 / frames_per_second) as u64);

    let mut frames: u64 = 0;

    let mut target_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    loop {
        target_time += target_frame_time;
        frames += 1;

        current_job_id = produce_messages(&producer, &mut fbb, );

        let sleep_time = target_time
            - SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

        if sleep_time.as_millis() > 0 {
            thread::sleep(sleep_time);
        } else {
            warn!(
                "saluki howl running {} seconds behind schedule",
                sleep_time.as_secs()
            )
        }
    }
}
