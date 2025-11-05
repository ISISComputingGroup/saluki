from unittest.mock import patch

from confluent_kafka.admin import ClusterMetadata, BrokerMetadata, TopicMetadata

from saluki.sniff import sniff

def test_sniff_with_two_partitions_in_a_topic():
    with patch("saluki.sniff.AdminClient") as a, patch("saluki.sniff.Consumer") as c, patch("saluki.sniff.logger") as logger:
        fake_cluster_md = ClusterMetadata()
        broker1 = BrokerMetadata()
        broker2 = BrokerMetadata()
        fake_cluster_md.brokers = {0: broker1, 1: broker2}

        topic1 = TopicMetadata()
        topic2 = TopicMetadata()

        fake_cluster_md.topics = {
            "topic1": topic1,
            "topic2": topic2
        }
        a.list_topics.return_value = fake_cluster_md
        sniff("whatever")

        # TODO 