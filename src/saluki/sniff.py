from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

def sniff(broker: str):
    a = AdminClient({'bootstrap.servers': broker})
    c = Consumer({'bootstrap.servers': broker, 'group.id': 'saluki-sniff'})
    t = a.list_topics(timeout=5)
    print(f"Cluster ID: {t.cluster_id}")
    print(f"Brokers:")
    [print(f"\t{value}") for value in t.brokers.values()]

    print(f"Topics:")

    for k,v in t.topics.items():
        partitions = v.partitions.keys()
        print(f"\t{k}:")
        for p in partitions:
            tp = TopicPartition(k, p)
            low, high = c.get_watermark_offsets(tp)
            print(f"\t\tlow:{low}, high:{high}, num_messages:{high-low}")
