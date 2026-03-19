use crate::cli_utils::BrokerAndTopic;
use isis_streaming_data_types::{deserialize_message, get_schema_id};
use log::{debug, error};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::ToBytes;
use rdkafka::{ClientConfig, Message};
use uuid::Uuid;

pub fn consume(
    topic: &BrokerAndTopic,
    partition: Option<i32>,
    filter: &Option<String>,
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
        filter.as_deref().unwrap_or("none")
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
                if partition.is_some() && message.partition() != partition.unwrap() {
                    continue;
                }

                match message.payload() {
                    Some(p) => {
                        if let Some(f) = filter {
                            if let Some(schema_id) = get_schema_id(p) {
                                if schema_id != f.to_bytes() {
                                    continue;
                                }
                            }
                            debug!("Message has no schema id, ignoring filter")
                        }

                        match deserialize_message(p) {
                            Ok(d) => println!("{:?}", d),
                            Err(e) => println!("Failed to deserialize message: {:?}", e),
                        }
                    }
                    None => {
                        error!("No payload in message");
                        continue;
                    }
                }
            }
            Err(e) => {
                error!("Consumer error: {:?}", e);
                break;
            }
        }
        if num_messages.is_some() {
            counter += 1;
            if counter == num_messages.unwrap() {
                debug!("Reached {} messages, exiting", num_messages.unwrap());
                break;
            }
        }
    }
}
