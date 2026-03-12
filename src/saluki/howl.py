import json
import logging
import time
import uuid

import numpy as np
from confluent_kafka import Producer
from streaming_data_types import serialise_6s4t, serialise_ev44, serialise_pl72
from streaming_data_types.run_start_pl72 import DetectorSpectrumMap

logger = logging.getLogger("saluki")

RNG = np.random.default_rng()


def generate_fake_events(
    msg_id: int,
    events_per_message: int,
    tof_peak: float,
    tof_sigma: float,
    det_min: int,
    det_max: int,
    timestamp: float,
) -> bytes:
    detector_ids = RNG.integers(low=det_min, high=det_max, size=events_per_message)
    tofs = np.maximum(0.0, RNG.normal(loc=tof_peak, scale=tof_sigma, size=events_per_message))
    tofs.sort()

    return serialise_ev44(
        source_name="saluki",
        reference_time=[timestamp * 1_000_000_000],
        message_id=msg_id,
        reference_time_index=[0],
        time_of_flight=tofs,
        pixel_id=detector_ids,
    )


def generate_run_start(det_max: int) -> bytes:
    det_spec_map = DetectorSpectrumMap(
        detector_ids=np.arange(0, det_max, dtype=np.int32),
        spectrum_numbers=np.arange(0, det_max, dtype=np.int32),
        n_spectra=det_max,
    )
    return serialise_pl72(
        start_time=int(time.time() * 1000),
        stop_time=None,
        run_name=f"saluki-howl-{uuid.uuid4()}",
        instrument_name="saluki-howl",
        nexus_structure=json.dumps({}),
        job_id=str(uuid.uuid4()),
        filename=str(uuid.uuid4()),
        detector_spectrum_map=det_spec_map,
    )


def generate_run_stop() -> bytes:
    return serialise_6s4t(
        stop_time=int(time.time() * 1000),
        job_id=str(uuid.uuid4()),
    )


def make_producer(broker: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": broker,
            "queue.buffering.max.kbytes": 1024 * 1024,
            "queue.buffering.max.messages": 100000,
            "linger.ms": 10,
            "batch.num.messages": 10_000,
            "max.in.flight.requests.per.connection": 32,
            "acks": 1,
        },
    )


def produce_messages(
    producer: Producer,
    topic_prefix: str,
    frame: int,
    events_per_message: int,
    messages_per_frame: int,
    frames_per_run: int,
    tof_peak: float,
    tof_sigma: float,
    det_min: int,
    det_max: int,
) -> None:
    now = time.time()
    for _ in range(messages_per_frame):
        producer.produce(
            topic=f"{topic_prefix}_rawEvents",
            key=None,
            value=generate_fake_events(frame, events_per_message, tof_peak, tof_sigma, det_min, det_max, timestamp=now),
            timestamp=int(now * 1000),
        )
    producer.poll(0)

    if frames_per_run != 0 and frame % frames_per_run == 0:
        logger.info(f"Starting new run after {frames_per_run} simulated frames")
        producer.produce(
            topic=f"{topic_prefix}_runInfo",
            key=None,
            value=generate_run_stop(),
            timestamp=int(now * 1000),
        )
        producer.produce(
            topic=f"{topic_prefix}_runInfo",
            key=None,
            value=generate_run_start(det_max),
            timestamp=int(now * 1000),
        )


def howl(
    broker: str,
    topic_prefix: str,
    events_per_message: int,
    messages_per_frame: int,
    frames_per_second: int,
    frames_per_run: int,
    tof_peak: float,
    tof_sigma: float,
    det_min: int,
    det_max: int,
) -> None:  # pragma: no cover (infinite loop)
    """
    Send messages vaguely resembling a run to Kafka.
    """
    producer = make_producer(broker)

    target_frame_time = 1 / frames_per_second

    frames = 0

    ev44_size = len(
        generate_fake_events(0, events_per_message, tof_peak, tof_sigma, det_min, det_max, timestamp=time.time())
    )
    rate_bytes_per_sec = ev44_size * messages_per_frame * frames_per_second
    rate_mbit_per_sec = (rate_bytes_per_sec / 1024**2) * 8

    logger.info(
        f"Attempting to simulate data rate: {rate_mbit_per_sec:.3f} Mbit/s "
        f"({rate_mbit_per_sec / 8:.3f} MiB/s)"
    )
    logger.info(f"Each ev44 is {ev44_size} bytes")

    producer.produce(
        topic=f"{topic_prefix}_runInfo",
        key=None,
        value=generate_run_start(det_max),
    )

    target_time = time.time()

    while True:
        target_time += target_frame_time
        frames += 1

        produce_messages(
            producer,
            topic_prefix,
            frames,
            events_per_message,
            messages_per_frame,
            frames_per_run,
            tof_peak,
            tof_sigma,
            det_min,
            det_max,
        )

        sleep_time = target_time - time.time()

        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            logger.warning(f"saluki-howl running {abs(sleep_time):.3f} seconds behind schedule")
