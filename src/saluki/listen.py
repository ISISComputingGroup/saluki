import logging

from confluent_kafka import Consumer, TopicPartition
from saluki import try_to_deserialise_message

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
        }
    )
    c.subscribe([topic])
    if partition is not None:
        c.assign([TopicPartition(topic, partition)])
    try:
        logger.info(f"listening to {broker}/{topic}")
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: {}".format(msg.error()))
                continue
            if partition is not None and msg.partition() != partition:
                continue
            deserialised = try_to_deserialise_message(msg.value())
            logger.info(f"{msg.offset()}: {deserialised}")
    except KeyboardInterrupt:
        logger.debug("finished listening")
    finally:
        logger.debug(f"closing consumer {c}")
        c.close()
