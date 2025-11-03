from unittest import mock

from confluent_kafka import TopicPartition

from saluki.listen import listen


def test_listen_with_partition_assigns_to_partition():
    expected_partition = 123
    topic = "sometopic"
    with (
        mock.patch(
            "saluki.listen.deserialise_and_print_messages",
            side_effect=KeyboardInterrupt,
        ),
        mock.patch("saluki.listen.Consumer") as c,
    ):
        listen("somebroker", "sometopic", partition=expected_partition)
        c.return_value.assign.assert_called_with(
            [TopicPartition(topic, expected_partition)]
        )


def test_keyboard_interrupt_causes_consumer_to_close():
    with (
        mock.patch(
            "saluki.listen.deserialise_and_print_messages",
            side_effect=KeyboardInterrupt,
        ),
        mock.patch("saluki.listen.Consumer") as c,
    ):
        listen("somebroker", "sometopic")
        c.return_value.close.assert_called_once()
