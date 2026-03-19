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

    // TODO work this out properly when we have streaming-data-types
    let ev44_size = 60; // bytes

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
