import logging

from confluent_kafka import Consumer
from saluki import try_to_deserialise_message

logger = logging.getLogger("saluki")


def listen(broker: str, topic: str) -> None:
    """
    Listen to a topic and deserialise each message
    :param broker: the broker address, including the port
    :param topic: the topic to use
    :return: None
    """
    c = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "saluki",
        }
    )
    c.subscribe([topic])
    try:
        logger.info(f"listening to {broker}/{topic}")
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: {}".format(msg.error()))
                continue
            deserialised = try_to_deserialise_message(msg.value())
            logger.info(f"{msg.offset()}: {deserialised}")
    except KeyboardInterrupt:
        logging.debug("finished listening")
    finally:
        logging.debug(f"closing consumer {c}")
        c.close()
