import logging

from confluent_kafka import Consumer, TopicPartition
from saluki import _deserialise_and_print_messages

logger = logging.getLogger("saluki")


def listen(broker: str, topic: str, partition: int | None = None) -> None:
    """
    Listen to a topic and deserialise each message
    :param broker: the broker address, including the port
    :param topic: the topic to use
    :param partition: the partition to listen to (default is all partitions in a given topic)
    :return: None
    """
    c = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "saluki",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    if partition is not None:
        c.assign([TopicPartition(topic, partition)])
    try:
        logger.info(f"listening to {broker}/{topic}")
        while True:
            msg = c.poll(1.0)
            _deserialise_and_print_messages([msg], partition)
    except KeyboardInterrupt:
        logger.debug("finished listening")
    finally:
        logger.debug(f"closing consumer {c}")
        c.close()
