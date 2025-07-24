import pytest

from saluki.utils import parse_kafka_uri


# test with normal payload

# test with fallback serialiser when schema not found (empty payload)

# test with schema that looks ok, but not in list of deserialisers

# test exception while deserialising

# test _parse_timestamp

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
