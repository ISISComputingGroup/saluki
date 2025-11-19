from argparse import ArgumentTypeError
from unittest.mock import Mock, patch

import pytest
from confluent_kafka import Message
from streaming_data_types import serialise_f144
from streaming_data_types.forwarder_config_update_fc00 import (
    ConfigurationUpdate,
    StreamInfo,
    serialise_fc00,
)

from saluki.utils import (
    _parse_timestamp,
    _try_to_deserialise_message,
    deserialise_and_print_messages,
    parse_kafka_uri, dateutil_parsable_or_unix_timestamp,
)


@pytest.fixture
def mock_message():
    return Mock(spec=Message)


def test_deserialising_message_with_no_message_continues():
    with patch("saluki.utils._try_to_deserialise_message") as mock_deserialise_message:
        deserialise_and_print_messages([None], None)
        mock_deserialise_message.assert_not_called()


def test_deserialising_message_with_error_continues(mock_message):
    mock_message.error.return_value = "Some error"
    with patch("saluki.utils._try_to_deserialise_message") as mock_deserialise_message:
        deserialise_and_print_messages([mock_message], None)
        mock_deserialise_message.assert_not_called()


def test_deserialising_message_with_wrong_partition_continues(mock_message):
    noninteresting_partition = 123
    mock_message.error.return_value = False
    mock_message.partition.return_value = noninteresting_partition
    with patch("saluki.utils._try_to_deserialise_message") as mock_deserialise_message:
        deserialise_and_print_messages([mock_message], 234)
        mock_deserialise_message.assert_not_called()


def test_deserialising_message_with_correct_partition_calls_deserialise(mock_message):
    partition = 123
    mock_message.error.return_value = False
    mock_message.partition.return_value = partition
    with patch("saluki.utils._try_to_deserialise_message") as mock_deserialise_message:
        deserialise_and_print_messages([mock_message], partition)
        mock_deserialise_message.assert_called_once()


def test_deserialising_empty_message(mock_message):
    assert (None, "") == _try_to_deserialise_message(b"")


def test_deserialising_message_with_invalid_schema_falls_back_to_raw_bytes_decode():
    assert _try_to_deserialise_message(b"blah") == (None, "blah")


def test_deserialising_message_which_raises_does_not_stop_loop(mock_message):
    with patch("saluki.utils.logger") as logger:
        ok_message = Mock(spec=Message)
        ok_message.value.return_value = b""
        ok_message.error.return_value = False
        ok_message.timestamp.return_value = 2, 1

        mock_message.value.side_effect = Exception
        mock_message.error.return_value = False
        mock_message.timestamp.return_value = 2, 1

        deserialise_and_print_messages([mock_message, ok_message], None)
        assert logger.info.call_count == 1


def test_deserialising_with_schema_list_ignores_messages_with_schema_not_in_list(mock_message):
    with patch("saluki.utils.logger") as logger:
        ok_message = Mock(spec=Message)
        ok_message.value.return_value = serialise_fc00(config_change=1, streams=[])  # type: ignore
        ok_message.error.return_value = False
        ok_message.timestamp.return_value = 2, 1

        mock_message.value.return_value = serialise_f144(source_name="test", value=123)
        mock_message.error.return_value = False
        mock_message.timestamp.return_value = 2, 1

        deserialise_and_print_messages(
            [mock_message, ok_message], None, schemas_to_filter_to=["fc00"]
        )
        assert logger.info.call_count == 1


def test_message_that_has_valid_schema_but_empty_payload():
    with pytest.raises(Exception):
        # Empty fc00 message - valid schema but not valid payload
        _try_to_deserialise_message(b" 	  fc00")


def test_schema_that_isnt_in_deserialiser_list(mock_message):
    assert _try_to_deserialise_message(b" 	  blah123") == ("blah", " \t  blah123")


def test_message_that_has_valid_schema_but_invalid_payload(mock_message):
    with pytest.raises(Exception):
        _try_to_deserialise_message(b" 	  fc0012345")


def test_message_that_has_valid_schema_and_valid_payload(mock_message):
    assert _try_to_deserialise_message(
        b"\x10\x00\x00\x00\x66\x63\x30\x30\x08\x00\x0c\x00\x06\x00\x08\x00\x08\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x03\x00\x00\x00\x0c\x00\x00\x00\x2c\x00\x00\x00\x4c\x00\x00\x00\xea\xff\xff\xff\x00\x00\x00\x00\x7c\x00\x00\x00\x6c\x00\x00\x00\x50\x00\x00\x00\x01\x00\x0e\x00\x16\x00\x08\x00\x0c\x00\x10\x00\x14\x00\x04\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x9c\x00\x00\x00\x8c\x00\x00\x00\x70\x00\x00\x00\x01\x00\x0e\x00\x18\x00\x08\x00\x0c\x00\x10\x00\x16\x00\x04\x00\x0e\x00\x00\x00\x00\x00\x00\x00\xbc\x00\x00\x00\xac\x00\x00\x00\x90\x00\x00\x00\x00\x00\x01\x00\x11\x00\x00\x00\x4e\x44\x57\x32\x36\x37\x32\x5f\x73\x61\x6d\x70\x6c\x65\x45\x6e\x76\x00\x00\x00\x04\x00\x00\x00\x66\x31\x34\x34\x00\x00\x00\x00\x1b\x00\x00\x00\x54\x45\x3a\x4e\x44\x57\x32\x36\x37\x32\x3a\x43\x53\x3a\x53\x42\x3a\x4d\x42\x42\x49\x5f\x42\x4c\x4f\x43\x4b\x00\x11\x00\x00\x00\x4e\x44\x57\x32\x36\x37\x32\x5f\x73\x61\x6d\x70\x6c\x65\x45\x6e\x76\x00\x00\x00\x04\x00\x00\x00\x66\x31\x34\x34\x00\x00\x00\x00\x19\x00\x00\x00\x54\x45\x3a\x4e\x44\x57\x32\x36\x37\x32\x3a\x43\x53\x3a\x53\x42\x3a\x42\x49\x5f\x42\x4c\x4f\x43\x4b\x00\x00\x00\x11\x00\x00\x00\x4e\x44\x57\x32\x36\x37\x32\x5f\x73\x61\x6d\x70\x6c\x65\x45\x6e\x76\x00\x00\x00\x04\x00\x00\x00\x66\x31\x34\x34\x00\x00\x00\x00\x1c\x00\x00\x00\x54\x45\x3a\x4e\x44\x57\x32\x36\x37\x32\x3a\x43\x53\x3a\x53\x42\x3a\x46\x4c\x4f\x41\x54\x5f\x42\x4c\x4f\x43\x4b\x00\x00\x00\x00"
    ) == (
        "fc00",
        ConfigurationUpdate(
            config_change=1,
            streams=[
                StreamInfo(
                    channel="TE:NDW2672:CS:SB:MBBI_BLOCK",
                    schema="f144",
                    topic="NDW2672_sampleEnv",
                    protocol=1,
                    periodic=0,
                ),
                StreamInfo(
                    channel="TE:NDW2672:CS:SB:BI_BLOCK",
                    schema="f144",
                    topic="NDW2672_sampleEnv",
                    protocol=1,
                    periodic=0,
                ),
                StreamInfo(
                    channel="TE:NDW2672:CS:SB:FLOAT_BLOCK",
                    schema="f144",
                    topic="NDW2672_sampleEnv",
                    protocol=1,
                    periodic=0,
                ),
            ],
        ),
    )


def test_parse_timestamp_with_valid_timestamp(mock_message):
    mock_message.timestamp.return_value = (1, 1753434939336)
    assert _parse_timestamp(mock_message) == "2025-07-25 09:15:39.336000"


def test_parse_timestamp_with_timestamp_not_available(mock_message):
    mock_message.timestamp.return_value = (2, "blah")
    assert _parse_timestamp(mock_message) == "Unknown"


def test_uri_with_broker_name_and_topic_successfully_split():
    test_broker = "localhost"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic = parse_kafka_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_uri_with_port_after_broker_is_included_in_broker_output():
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic = parse_kafka_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_uri_with_no_topic():
    test_broker = "some_broker"
    with pytest.raises(RuntimeError):
        parse_kafka_uri(test_broker)

@pytest.mark.parametrize("timestamp",
["2025-11-19T15:27:11",
 "2025-11-19T15:27:11Z",
"2025-11-19T15:27:11+00:00"
 ]

)
def test_parses_datetime_properly_with_string(timestamp):
    assert dateutil_parsable_or_unix_timestamp(timestamp) == 1763566031000

@pytest.mark.parametrize("timestamp",
["1763566031000",
 "1763566031",
 "1763566031000000",
 ]
)
def test_parses_datetime_properly_and_leaves_unix_timestamp_alone(timestamp):
    assert dateutil_parsable_or_unix_timestamp(timestamp) == int(timestamp)


def test_invalid_timestamp_raises():
    with pytest.raises(ArgumentTypeError):
        dateutil_parsable_or_unix_timestamp("invalid")