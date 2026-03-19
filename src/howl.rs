use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{finish_event_44_message_buffer, Event44Message, Event44MessageArgs, Event44MessageBuilder};
use rand::{rng, Rng, RngExt};
use rand_distr::{Normal, Distribution};

fn generate_fake_events<'a>(fbb :&'a mut FlatBufferBuilder<'_>, msg_id: i64, events_per_message: i32, tof_peak: f32, tof_sigma: f32, det_min: i32, det_max: i32, timestamp: f32) -> &'a [u8] {
    let mut rng = rand::rng();

    let det_ids: Vec<i32> = (0..events_per_message)
        .map(|_| rng.random_range(det_min..=det_max))
        .collect();

    let normal = Normal::new(tof_peak, tof_sigma).unwrap();
    let tofs: Vec<i32> = (0..events_per_message).map(|_| normal.sample(&mut rng) as i32).collect();

    let args = Event44MessageArgs {
        source_name: Some(fbb.create_string("saluki")),
        message_id: msg_id,
        reference_time: Some(fbb.create_vector(&[(timestamp * 1_000_000_000.0) as i64])),
        reference_time_index: Some(fbb.create_vector(&vec!(0))),
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
    events_per_message: u32,
    messages_per_frame: u32,
    frames_per_second: u32,
    frames_per_run: u32,
    tof_peak: u32,
    tof_sigma: u32,
    det_min: u32,
    det_max: u32,
) {
    // create producer

    let mut fbb = FlatBufferBuilder::new();


    // TODO work this out properly when we have streaming-data-types
    let ev44_size = 60; // bytes
    let now = SystemTime::now();
    let ev44_size = generate_fake_events(fbb, 0, events_per_message, tof_peak, tof_sigma, det_min, det_max, now).len();


    // calculate rate
    let rate_bytes_per_sec = (ev44_size * messages_per_frame * frames_per_second) as u64;
    let rate_mbit_per_sec = (rate_bytes_per_sec * u64::pow(1024, 2)) * 8;
    println!(
        "Attempting to simulate data rate: {rate_mbit_per_sec} Mbit/s ({rate_bytes_per_sec} MiB/s)"
    );
    println!("Each ev44 is {ev44_size} bytes");

    // produce run start

    // loop:
    //  produce ev44s
    //  produce run stop and starts if frames-per-run specified
}
