import logging

from confluent_kafka import Consumer, TopicPartition
from saluki import try_to_deserialise_message

logger = logging.getLogger("saluki")


def consume(
    broker: str,
    topic: str,
    partition: int = 0,
    num_messages: int = 1,
    offset: int | None = None,
    go_forwards: bool = False,
) -> None:
    """
    consume from a topic and deserialise each message

    :param broker: the broker address, including the port
    :param topic: the topic to use
    :param partition: the partition to listen to (default is all partitions in a given topic)
    :param num_messages: number of messages to consume
    :param offset: offset to consume from/to
    :param go_forwards: whether to consume forwards or backwards
    :return: None
    """
    c = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "saluki",
        }
    )

    if go_forwards:
        if offset is None:
            logger.error("Can't go forwards without an offset")
            return
        start = offset
    else:
        if offset is not None:
            start = offset - num_messages
        else:
            start = (
                c.get_watermark_offsets(TopicPartition(topic, partition), cached=False)[
                    1
                ]
                - num_messages
            )

    logger.info(f"starting at {start}")
    c.assign([TopicPartition(topic, partition, start)])

    try:
        logger.info(f"consuming {num_messages} messages")
        msgs = c.consume(num_messages)
        for msg in msgs:
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: {}".format(msg.error()))
                continue
            if partition is not None and msg.partition() != partition:
                continue
            deserialised = try_to_deserialise_message(msg.value())
            logger.info(f"{msg.offset()}: {deserialised}")
    except Exception as e:
        logger.error(e)
    finally:
        logger.debug(f"closing consumer {c}")
        c.close()
