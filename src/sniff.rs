use crate::cli_utils::BrokerAndOptionalTopic;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;

pub fn sniff(broker: &BrokerAndOptionalTopic) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker.broker())
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(broker.topic.as_deref(), Duration::from_secs(1))
        .expect("Failed to fetch metadata");

    if broker.topic.is_none() {
        println!("Brokers ({}):", metadata.brokers().len());
        for broker in metadata.brokers() {
            println!("\t{}: {}:{}  ", broker.id(), broker.host(), broker.port());
        }
        println!("\nTopics:");
    }

    for topic in metadata.topics() {
        println!("\tTopic: {}  Error: {:?}", topic.name(), topic.error());
        for partition in topic.partitions() {
            println!(
                "\t\tPartition: {}  Leader: {}  Replicas: {:?}(in sync: {:?})  Err: {:?}",
                partition.id(),
                partition.leader(),
                partition.replicas(),
                partition.isr(),
                partition.error()
            );
            let (low, high) = consumer
                .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                .unwrap_or((-1, -1));
            println!(
                "\t\t\tLow watermark: {}  High watermark: {} (messages: {})",
                low,
                high,
                high - low
            );
        }
    }
}
