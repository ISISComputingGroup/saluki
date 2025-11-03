import logging
from confluent_kafka import Consumer, Producer, TopicPartition

logger = logging.getLogger("saluki")


def play(
    src_broker: str,
    src_topic: str,
    dest_broker: str,
    dest_topic: str,
    offsets: list[int] | None,
    timestamps: list[int] | None,
) -> None:
    """
    Replay data from src_topic to dest_topic between the offsets OR timestamps specified.
    This currently assumes contiguous data in a topic (ie. no log compaction) and only uses partition 0.

    :param src_broker: The source broker, including port.
    :param src_topic: The topic to replay data from.
    :param dest_broker: The destination broker, including port.
    :param dest_topic: The topic to replay data to.
    :param offsets: The start and finish offsets to replay data from.
    :param timestamps: The start and finish timestamps to replay data from.
    """

    print(f"ARGS: {src_broker}, {src_topic}, {dest_broker}, {dest_topic}, {offsets}, {timestamps}")

    consumer = Consumer(
        {
            "bootstrap.servers": src_broker,
            "group.id": "saluki-play",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": dest_broker,
        }
    )
    src_partition = 0

    if timestamps is not None:
        start_offset, stop_offset = consumer.offsets_for_times([
            TopicPartition(src_topic, src_partition, timestamps[0]),
            TopicPartition(src_topic, src_partition, timestamps[1]),
        ])
    else:
        start_offset = TopicPartition(src_topic, src_partition, offsets[0])
        stop_offset = TopicPartition(src_topic, src_partition, offsets[1])
    consumer.assign([start_offset])

    num_messages = stop_offset.offset - start_offset.offset + 1

    try:
        msgs = consumer.consume(num_messages)
        [producer.produce(dest_topic, message.value(), message.key()) for message in msgs]
        producer.flush()
    except Exception:
        logger.exception("Got exception while consuming:")
    finally:
        logger.debug(f"Closing consumer {consumer}")
        consumer.close()

