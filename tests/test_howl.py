from unittest.mock import ANY, MagicMock, call

import numpy as np
from confluent_kafka.cimpl import Producer
from streaming_data_types import deserialise_6s4t, deserialise_ev44, deserialise_pl72

from saluki.howl import (
    generate_fake_events,
    generate_run_start,
    generate_run_stop,
    make_producer,
    produce_messages,
)


def test_generate_run_start():
    pl72 = deserialise_pl72(generate_run_start(50000, "test"))
    det_spec_map = pl72.detector_spectrum_map
    assert det_spec_map is not None
    assert det_spec_map.n_spectra == 50000


def test_generate_run_stop():
    deserialise_6s4t(generate_run_stop())


def test_generate_events():
    ev44 = deserialise_ev44(
        generate_fake_events(
            msg_id=123,
            events_per_message=1,
            tof_peak=12345,
            tof_sigma=0,
            det_min=5,
            det_max=6,
            timestamp=12345,
        )
    )
    assert ev44.message_id == 123
    assert ev44.pixel_id == np.array([5], dtype=np.int32)
    assert ev44.time_of_flight == np.array([12345], dtype=np.int32)


def test_make_producer():
    # Just test it doesn't crash - can't usefully test much more than that
    make_producer("127.0.0.1")


def test_produce_event_messages():
    producer = MagicMock(spec=Producer)

    produce_messages(
        producer,
        "some_prefix",
        frame=1,
        frames_per_run=10,
        messages_per_frame=1,
        events_per_message=1,
        tof_peak=12345,
        tof_sigma=0,
        det_min=5,
        det_max=6,
    )

    producer.produce.assert_called_once_with(
        topic="some_prefix_rawEvents", key=None, value=ANY, timestamp=ANY
    )


def test_produce_runinfo_messages():
    producer = MagicMock(spec=Producer)

    produce_messages(
        producer,
        "some_prefix",
        frame=10,
        frames_per_run=10,
        messages_per_frame=1,
        events_per_message=1,
        tof_peak=12345,
        tof_sigma=0,
        det_min=5,
        det_max=6,
    )

    # event followed by run stop and run start pair.
    producer.produce.assert_has_calls(
        [
            call(topic="some_prefix_rawEvents", key=None, value=ANY, timestamp=ANY),
            call(topic="some_prefix_runInfo", key=None, value=ANY, timestamp=ANY),
            call(topic="some_prefix_runInfo", key=None, value=ANY, timestamp=ANY),
        ]
    )
