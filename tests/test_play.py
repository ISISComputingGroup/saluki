from unittest.mock import Mock, patch

from confluent_kafka import Message, TopicPartition

from saluki.play import play


def test_play_with_offsets():
    src_broker = "broker1"
    src_topic = "topic1"
    dest_broker = "broker2"
    dest_topic = "topic2"
    offsets = [1, 2]

    message_1 = Mock(spec=Message)
    message_1_key = "msg1key"
    message_1.key.return_value = message_1_key
    message_1_val = "msg1"
    message_1.value.return_value = message_1_val

    message_2 = Mock(spec=Message)
    message_2_key = "msg2key"
    message_2.key.return_value = message_2_key
    message_2_val = "msg2"
    message_2.value.return_value = message_2_val

    with patch("saluki.play.Consumer") as c, patch("saluki.play.Producer") as p:
        consumer_obj = c()
        consumer_obj.consume.return_value = [message_1, message_2]

        play(src_broker, src_topic, dest_broker, dest_topic, offsets, None)

        assert consumer_obj.assign.call_args.args[0][0].topic == src_topic
        assert consumer_obj.assign.call_args.args[0][0].offset == offsets[0]

        consumer_obj.consume.assert_called_with(2)  # stop - start + 1

        p_obj = p()
        call_1 = p_obj.produce.call_args_list[0]
        assert call_1.args == (dest_topic, message_1_val, message_1_key)
        call_2 = p_obj.produce.call_args_list[1]
        assert call_2.args == (dest_topic, message_2_val, message_2_key)


def test_play_with_timestamps():
    src_broker = "broker1"
    src_topic = "topic1"
    dest_broker = "broker2"
    dest_topic = "topic2"
    timestamps = [1762444369, 1762444375]

    message_1 = Mock(spec=Message)
    message_1_key = "msg1key"
    message_1.key.return_value = message_1_key
    message_1_val = "msg1"
    message_1.value.return_value = message_1_val

    message_2 = Mock(spec=Message)
    message_2_key = "msg2key"
    message_2.key.return_value = message_2_key
    message_2_val = "msg2"
    message_2.value.return_value = message_2_val

    with patch("saluki.play.Consumer") as c, patch("saluki.play.Producer") as p:
        consumer_obj = c()
        consumer_obj.offsets_for_times.return_value = [
            TopicPartition(src_topic, partition=0, offset=2),
            TopicPartition(src_topic, partition=0, offset=3),
        ]
        consumer_obj.consume.return_value = [message_1, message_2]

        play(src_broker, src_topic, dest_broker, dest_topic, None, timestamps)

        assert consumer_obj.assign.call_args.args[0][0].topic == src_topic
        assert consumer_obj.assign.call_args.args[0][0].offset == 2

        consumer_obj.consume.assert_called_with(2)  # stop - start + 1

        p_obj = p()
        call_1 = p_obj.produce.call_args_list[0]
        assert call_1.args == (dest_topic, message_1_val, message_1_key)
        call_2 = p_obj.produce.call_args_list[1]
        assert call_2.args == (dest_topic, message_2_val, message_2_key)


def test_play_with_exception_when_consuming_consumer_still_closed():
    with (
        patch("saluki.play.Consumer") as mock_consumer,
        patch("saluki.play.Producer"),
        patch("saluki.play.logger") as mock_logger,
    ):
        mock_consumer().consume.side_effect = Exception("blah")
        play("", "", "", "", [1, 2], None)

        mock_logger.exception.assert_called_once()

        mock_consumer().close.assert_called_once()
