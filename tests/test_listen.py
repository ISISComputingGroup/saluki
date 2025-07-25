from unittest.mock import MagicMock

from confluent_kafka import TopicPartition

from saluki.listen import listen
from unittest import mock


def test_listen_with_partition_assigns_to_partition():
    expected_partition = 123
    topic = "sometopic"
    with (
        mock.patch(
            "saluki.listen._deserialise_and_print_messages",
            side_effect=KeyboardInterrupt,
        ),
        mock.patch("saluki.listen.Consumer") as c,
    ):
        listen("somebroker", "sometopic", partition=expected_partition)
        assert c.return_value.assign.call_args[0][0][0].topic == topic
        assert c.return_value.assign.call_args[0][0][0].partition == expected_partition


def test_keyboard_interrupt_causes_consumer_to_close():
    with (
        mock.patch(
            "saluki.listen._deserialise_and_print_messages",
            side_effect=KeyboardInterrupt,
        ),
        mock.patch("saluki.listen.Consumer") as c,
    ):
        listen("somebroker", "sometopic")
        c.return_value.close.assert_called_once()
