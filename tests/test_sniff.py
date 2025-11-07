from unittest.mock import patch

from confluent_kafka.admin import BrokerMetadata, ClusterMetadata, TopicMetadata

from saluki.sniff import sniff


def test_sniff_with_two_partitions_in_a_topic():
    with (
        patch("saluki.sniff.AdminClient") as a,
        patch("saluki.sniff.Consumer") as c,
        patch("saluki.sniff.logger") as logger,
    ):
        fake_cluster_md = ClusterMetadata()
        broker1 = BrokerMetadata()
        broker1.id = "id1"  # type: ignore
        broker1.host = "mybroker"  # type: ignore
        broker1.port = 9093
        fake_cluster_md.brokers = {0: broker1}

        topic1 = TopicMetadata()
        topic1.partitions = {0: {}}
        topic2 = TopicMetadata()
        topic2.partitions = {0: {}, 1: {}}

        fake_cluster_md.topics = {"topic1": topic1, "topic2": topic2}
        a().list_topics.return_value = fake_cluster_md
        c().get_watermark_offsets.return_value = 1, 2
        sniff("whatever")

        brokers_call = logger.info.call_args_list[2]

        assert "mybroker:9093/id1" in brokers_call.args[0]

        topic1_call = logger.info.call_args_list[5]
        assert "0 - low:1, high:2, num_messages:1" in topic1_call.args[0]

        topic2_call1 = logger.info.call_args_list[7]
        assert "0 - low:1, high:2, num_messages:1" in topic2_call1.args[0]

        topic2_call2 = logger.info.call_args_list[8]
        assert "1 - low:1, high:2, num_messages:1" in topic2_call2.args[0]
