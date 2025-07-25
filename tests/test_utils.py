from unittest.mock import Mock, patch

import pytest

from saluki.utils import (
    parse_kafka_uri,
    _parse_timestamp,
    _deserialise_and_print_messages,
    __try_to_deserialise_message,
)
from confluent_kafka import Message


@pytest.fixture
def mock_message():
    return Mock(spec=Message)


def test_deserialising_message_with_no_message_continues():
    with patch("saluki.utils.__try_to_deserialise_message") as mock_deserialise_message:
        _deserialise_and_print_messages([None], None)
        mock_deserialise_message.assert_not_called()


def test_deserialising_message_with_error_continues(mock_message):
    mock_message.error.return_value = "Some error"
    with patch("saluki.utils.__try_to_deserialise_message") as mock_deserialise_message:
        _deserialise_and_print_messages([mock_message], None)
        mock_deserialise_message.assert_not_called()


def test_deserialising_message_with_wrong_partition_continues(mock_message):
    noninteresting_partition = 123
    mock_message.error.return_value = False
    mock_message.partition.return_value = noninteresting_partition
    with patch("saluki.utils.__try_to_deserialise_message") as mock_deserialise_message:
        _deserialise_and_print_messages([mock_message], 234)
        mock_deserialise_message.assert_not_called()


def test_deserialising_message_with_correct_partition_calls_deserialise(mock_message):
    partition = 123
    mock_message.error.return_value = False
    mock_message.partition.return_value = partition
    with patch("saluki.utils.__try_to_deserialise_message") as mock_deserialise_message:
        _deserialise_and_print_messages([mock_message], partition)
        mock_deserialise_message.assert_called_once()


def test_deserialising_empty_message(mock_message):
    assert (None, "") == __try_to_deserialise_message(b"")


def test_deserialising_message_with_invalid_schema_falls_back_to_raw_bytes_decode(
    mock_message,
):
    pass


def test_deserialising_message_which_raises_does_not_stop_loop(mock_message):
    pass


def test_schema_that_isnt_in_deserialiser_list(mock_message):
    pass


def test_message_that_has_valid_schema_but_empty_payload(mock_message):
    pass


def test_message_that_has_valid_schema_but_invalid_payload(mock_message):
    pass


def test_message_that_has_valid_schema_and_valid_payload(mock_message):
    pass


def test_parse_timestamp_with_valid_timestamp(mock_message):
    mock_message.timestamp.return_value = (1, 1753434939336)
    assert _parse_timestamp(mock_message) == "2025-07-25 10:15:39.336000"


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
