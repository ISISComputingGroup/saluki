use std::time::SystemTime;

use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, Event44MessageBuilder, finish_event_44_message_buffer,
};
use log::debug;
use rand::{Rng, RngExt, rng};
use rand_distr::{Distribution, Normal};

fn generate_run_start() {}

fn generate_run_stop() {}

fn produce_messages() {}

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

    // TODO work this out properly when we have streaming-data-types
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

    let rate_mbit_per_sec: f32  = (rate_bytes_per_sec as f32 / 1024f32.powf(2.0)) * 8.0;
    let rate_mebibits_per_sec = rate_mbit_per_sec/8.0;
    debug!("rate mbit per sec: {rate_mbit_per_sec}");
    println!(
        "Attempting to simulate data rate: {rate_mbit_per_sec:.3} Mbit/s ({rate_mebibits_per_sec:.3} MiB/s)"
    );
    println!("Each ev44 is {ev44_size} bytes");

    // produce run start

    // loop:
    //  produce ev44s
    //  produce run stop and starts if frames-per-run specified
}
