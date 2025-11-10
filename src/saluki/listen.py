import logging
import uuid

from confluent_kafka import Consumer, TopicPartition

from saluki.utils import deserialise_and_print_messages

logger = logging.getLogger("saluki")


def listen(
    broker: str,
    topic: str,
    partition: int | None = None,
    schemas_to_filter_to: list[str] | None = None,
) -> None:
    """
    Listen to a topic and deserialise each message
    :param broker: the broker address, including the port
    :param topic: the topic to use
    :param partition: the partition to listen to (default is all partitions in a given topic)
    :param schemas_to_filter_to: schemas to filter when listening to messages
    :return: None
    """
    c = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"saluki-listen-{uuid.uuid4()}",
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
            deserialise_and_print_messages(
                [msg], partition, schemas_to_filter_to=schemas_to_filter_to
            )
    except KeyboardInterrupt:
        logger.debug("finished listening")
    finally:
        logger.debug(f"closing consumer {c}")
        c.close()
