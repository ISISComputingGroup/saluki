import logging

from confluent_kafka import Consumer, TopicPartition
from saluki import _deserialise_and_print_messages

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
            "session.timeout.ms": 6000,
            "auto.offset.reset": "latest",
            "enable.auto.offset.store": False,
            "enable.auto.commit": False,
            "metadata.max.age.ms": 6000,
        }
    )

    if go_forwards:
        if offset is None:
            logger.error("Can't go forwards without an offset")
            return
        start = offset
    else:
        if offset is not None:
            start = offset - num_messages + 1
        else:
            start = (
                c.get_watermark_offsets(TopicPartition(topic, partition), cached=False)[
                    1
                ]
                - num_messages
            )

    logger.info(f"starting at offset {start}")
    c.assign([TopicPartition(topic, partition, start)])

    try:
        logger.info(f"consuming {num_messages} messages")
        msgs = c.consume(num_messages)
        _deserialise_and_print_messages(msgs, partition)
    except Exception as e:
        logger.error(e)
    finally:
        logger.debug(f"closing consumer {c}")
        c.close()
