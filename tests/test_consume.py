from unittest import mock
from unittest.mock import patch

import pytest
from confluent_kafka import TopicPartition

from saluki.consume import consume


@patch("saluki.consume.Consumer")
def test_go_forwards_with_no_offset_raises(_):
    with pytest.raises(ValueError):
        consume("broker", "topic", go_forwards=True, offset=None)


@patch("saluki.consume.Consumer")
def test_go_forwards_with_offset_assigns_at_offset(mock_consumer):
    expected_topic = "topic"
    expected_offset = 1234
    expected_partition = 1
    consume(
        "broker",
        expected_topic,
        go_forwards=True,
        offset=expected_offset,
        partition=expected_partition,
    )
    mock_assign = mock_consumer.return_value.assign

    mock_assign.assert_called_with(
        [TopicPartition(expected_topic, expected_partition, expected_offset)]
    )


@patch("saluki.consume.Consumer")
def test_consume_with_offset_and_num_of_messages_goes_back_offset_minus_messages(
    mock_consumer,
):
    expected_offset = 1234
    expected_topic = "sometopic"
    num_messages = 3
    expected_start_offset = expected_offset - num_messages + 1

    consume("broker", expected_topic, offset=expected_offset, num_messages=num_messages)

    mock_assign = mock_consumer.return_value.assign
    mock_assign.assert_called_once()

    mock_assign_call = mock_assign.call_args.args[0][0]
    assert mock_assign_call.topic == expected_topic
    assert mock_assign_call.offset == expected_start_offset


@patch("saluki.consume.Consumer")
def test_consume_with_no_offset_and_num_of_messages_goes_back_high_watermark_minus_messages(
    mock_consumer,
):
    expected_topic = "sometopic"
    num_messages = 3
    high_watermark_offset = 2345
    expected_start_offset = high_watermark_offset - num_messages

    mock_consumer.return_value.get_watermark_offsets.return_value = (
        None,
        high_watermark_offset,
    )

    consume("broker", topic=expected_topic, num_messages=num_messages)
    mock_assign = mock_consumer.return_value.assign
    mock_assign.assert_called_once()

    mock_assign_call = mock_assign.call_args.args[0][0]

    assert mock_assign_call.topic == expected_topic
    assert mock_assign_call.offset == expected_start_offset


def test_consume_but_exception_thrown_consumer_is_closed():
    with (
        mock.patch("saluki.consume.Consumer") as c,
    ):
        c.return_value.consume.side_effect = Exception
        consume("somebroker", "sometopic", num_messages=1)
        c.return_value.close.assert_called_once()


@patch("saluki.consume.Consumer")
def test_consume_with_timestamp(mock_consumer):
    expected_topic = "sometopic"
    partition = 0
    timestamp = 1234
    offset = 2345

    mock_consumer.offsets_for_times.return_value = [
        TopicPartition(expected_topic, partition, offset)
    ]
    consume("somebroker", topic=expected_topic, timestamp=timestamp, partition=partition)

    mock_consumer.return_value.assign.assert_called_with(
        [TopicPartition(expected_topic, partition, offset)]
    )
