import logging
import time

import numpy as np
from confluent_kafka import Producer
from streaming_data_types import serialise_ev44

logger = logging.getLogger("saluki")

RNG = np.random.default_rng()


def generate_fake_ev44(
    msg_id: int,
    events_per_frame: int,
    tof_peak: float,
    tof_sigma: float,
    det_min: int,
    det_max: int,
) -> bytes:
    detector_ids = np.random.randint(low=det_min, high=det_max, size=events_per_frame)
    tofs = np.maximum(0.0, RNG.normal(loc=tof_peak, scale=tof_sigma, size=events_per_frame))

    return serialise_ev44(
        source_name="saluki",
        reference_time=[time.time() * 1_000_000_000],
        message_id=msg_id,
        reference_time_index=[0],
        time_of_flight=tofs,
        pixel_id=detector_ids,
    )


def howl(
    broker: str,
    topic: str,
    events_per_frame: int,
    frames_per_second: int,
    tof_peak: float,
    tof_sigma: float,
    det_min: int,
    det_max: int,
) -> None:
    """
    Prints the broker and topic metadata for a given broker.
    If a topic is given, only this topic's partitions and watermarks will be printed.
    :param broker: The broker address including port number.
    :param topic: Optional topic to filter information to.
    """

    producer = Producer(
        {
            "bootstrap.servers": broker,
        }
    )

    target_frame_time = 1 / frames_per_second

    msg_id = 0

    ev44_size = len(generate_fake_ev44(0, events_per_frame, tof_peak, tof_sigma, det_min, det_max))
    rate_bytes_per_sec = ev44_size * frames_per_second
    rate_mbit_per_sec = (rate_bytes_per_sec / 1024**2) * 8
    logger.info(f"Attempting to simulate data rate: {rate_mbit_per_sec:.3f} MBit/s")

    while True:
        start_time = time.time()
        target_end_time = start_time + target_frame_time

        producer.produce(
            topic=topic,
            key=None,
            value=generate_fake_ev44(
                msg_id, events_per_frame, tof_peak, tof_sigma, det_min, det_max
            ),
        )
        msg_id += 1

        sleep_time = max(target_end_time - time.time(), 0)
        if sleep_time == 0:
            logger.warning("saluki-howl cannot keep up with target event/frame rate")
        time.sleep(sleep_time)
