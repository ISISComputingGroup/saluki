from unittest.mock import Mock

import pytest

from saluki.utils import parse_kafka_uri, _parse_timestamp, _deserialise_and_print_messages
from confluent_kafka import Message

@pytest.fixture
def mock_message():
    return Mock(spec=Message)

def test_deserialising_message_with_no_message_continues(mock_message):
    _deserialise_and_print_messages([mock_message])

def test_deserialising_message_with_error_continues(mock_message):
    pass

def test_deserialising_message_with_wrong_partition_continues(mock_message):
    pass



# test with normal payload

# test with fallback serialiser when schema not found (empty payload)

# test with schema that looks ok, but not in list of deserialisers

# test exception while deserialising


def test_parse_timestamp_with_valid_timestamp(mock_message):
    mock_message.timestamp.return_value = (1, 1753434939336)
    assert _parse_timestamp(mock_message) == '2025-07-25 10:15:39.336000'

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
