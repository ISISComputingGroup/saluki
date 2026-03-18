use crate::cli_utils::BrokerAndTopic;
use log::{debug, error};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use uuid::Uuid;

pub fn consume(
    topic: BrokerAndTopic,
    partition: Option<i32>,
    filter: Option<String>,
    num_messages: Option<u32>,
    offset: Option<i64>,
    go_forwards: Option<bool>,
    timestamp: Option<u64>,
) {
    debug!(
        "Listening to topic: {} partition {} on broker {}:{}, filtering {}",
        topic.topic,
        partition.unwrap_or(1),
        topic.host,
        topic.port,
        filter.unwrap_or("none".to_string())
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", Uuid::new_v4().to_string())
        .set("bootstrap.servers", topic.broker())
        .create()
        .expect("Consumer creation failed");

    // TODO offset, go_forwards, timestamp
    consumer
        .subscribe(&[&*topic.topic])
        .expect(&format!("Failed to subscribe to topic {}", topic.topic));

    let mut counter = 0;
    for message in consumer.iter() {
        match message {
            Ok(message) => {
                if partition.is_some() && (message.partition() != partition.unwrap()) {
                    continue;
                }
                // TODO get file id
                // TODO filter if filtering
                // TODO deserialise
                println!(
                    "Received message: {:?}",
                    String::from_utf8_lossy(message.payload().unwrap())
                );
            }
            Err(e) => {
                error!("Consumer error: {:?}", e);
                break;
            }
        }
        if (num_messages.is_some()) {
            counter += 1;
            if (counter == num_messages.unwrap()) {
                debug!("Reached {} messages, exiting", num_messages.unwrap());
                break;
            }
        }
    }
}
