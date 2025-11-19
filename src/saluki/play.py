import logging
import uuid
from time import sleep
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
    This currently assumes contiguous data in a topic (ie. no log compaction) and uses partition 0.

    :param src_broker: The source broker, including port.
    :param src_topic: The topic to replay data from.
    :param dest_broker: The destination broker, including port.
    :param dest_topic: The topic to replay data to.
    :param offsets: The start and finish offsets to replay data from.
    :param timestamps: The start and finish timestamps to replay data from.
    """

    consumer = Consumer(
        {
            "bootstrap.servers": src_broker,
            "group.id": f"saluki-play-{uuid.uuid4()}",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": dest_broker,
        }
    )
    src_partition = 0

    if timestamps is not None:
        logger.debug(f"getting offsets for times: {timestamps[0]} and {timestamps[1]}")
        start_offset = consumer.offsets_for_times(
            [
                TopicPartition(src_topic, src_partition, timestamps[0]),

            ]
        )[0]
        # See https://github.com/confluentinc/confluent-kafka-python/issues/1178 as to why offsets_for_times is called twice.
        stop_offset = consumer.offsets_for_times([TopicPartition(src_topic, src_partition, timestamps[1])])[0]
    elif offsets is not None:
        start_offset = TopicPartition(src_topic, src_partition, offsets[0])
        stop_offset = TopicPartition(src_topic, src_partition, offsets[1])
    else:
        raise ValueError("offsets and timestamps cannot both be None")

    logger.debug(f"start_offset: {start_offset.offset}, stop_offset: {stop_offset.offset}")

    logger.debug(f"assigning to offset {start_offset.offset}")
    consumer.assign([start_offset])

    num_messages = stop_offset.offset - start_offset.offset + 1

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush()."""
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
        else:
            logger.debug('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    try:
        msgs = consumer.consume(num_messages)
        logger.debug(f"finished consuming {num_messages} messages")
        consumer.close()
        # logger.debug(f"{msgs}")
        for message in msgs:
            producer.poll(0)
            producer.produce(dest_topic, message.value(), message.key(), callback=delivery_report)
        # producer.produce_batch(dest_topic, [{'key': message.key(), 'value': message.value()} for message in msgs])
        # producer.poll()
        logger.debug(f"flushing producer. len(p): {len(producer)}")
        # while len(producer): producer.flush()

        producer.flush(timeout=10)

        logger.debug(f"length after flushing: {len(producer)}")

    except Exception:
        logger.exception("Got exception while replaying:")
    finally:
        logger.debug(f"Closing consumer {consumer}")
        consumer.close()
