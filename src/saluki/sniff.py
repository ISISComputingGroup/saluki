import logging
import uuid

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

logger = logging.getLogger("saluki")


def sniff(broker: str) -> None:
    """
    Prints the broker and topic metadata for a given broker.
    :param broker: The broker address including port number.
    """
    a = AdminClient({"bootstrap.servers": broker})
    c = Consumer({"bootstrap.servers": broker, "group.id": f"saluki-sniff-{uuid.uuid4()}"})
    t = a.list_topics(timeout=5)
    logger.info(f"Cluster ID: {t.cluster_id}")
    logger.info("Brokers:")
    for value in t.brokers.values():
        logger.info(f"\t{value}")

    logger.info("Topics:")

    for k, v in t.topics.items():
        partitions = v.partitions.keys()
        logger.info(f"\t{k}:")
        for p in partitions:
            tp = TopicPartition(k, p)
            low, high = c.get_watermark_offsets(tp)
            logger.info(f"\t\t{tp.partition} - low:{low}, high:{high}, num_messages:{high - low}")
