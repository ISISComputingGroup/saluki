use crate::KafkaOption;
use crate::cli_utils::BrokerAndTopic;
use futures::stream::StreamExt;
use log::error;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use tokio::time::{self, Duration};
use uuid::Uuid;

pub async fn count(
    topic: BrokerAndTopic,
    message_interval: u64,
    kafka_config: Option<Vec<KafkaOption>>,
) {
    let mut config = ClientConfig::new();
    config.set("group.id", Uuid::new_v4().to_string());
    config.set("bootstrap.servers", topic.broker());

    if let Some(kafka_options) = kafka_config {
        for option in kafka_options {
            println!(
                "Setting Kafka config option {}={}",
                option.key, option.value
            );
            config.set(&option.key, &option.value);
        }
    }

    let consumer: StreamConsumer<DefaultConsumerContext> =
        config.create().expect("Consumer creation failed");

    consumer
        .subscribe(&[&topic.topic])
        .unwrap_or_else(|e| panic!("Failed to subscribe to topic {}: {}", topic.topic, e));

    let start = std::time::Instant::now();
    let mut bytes_this_second: usize = 0;
    let mut total_bytes: usize = 0;
    let mut stream = consumer.stream();
    let mut interval = time::interval(Duration::from_secs(message_interval));

    loop {
        tokio::select! {
            _msg = stream.next() => {
                match _msg {
                    Some(Ok(msg)) if msg.payload().is_some() => {
                        bytes_this_second += msg.payload_len();
                        total_bytes += msg.payload_len();
                    },
                    Some(Err(e)) => error!("Error reading from stream {:?}", e),
                    _ => {}
                }
            }
            _ = interval.tick() => {
                println!("{:.5} Mbit/s (since program start: average {:.5} Mbit/s, {:.5} MB total)",
                    bytes_this_second as f64/125000.0 /  interval.period().as_secs_f64(),
                    total_bytes as f64 / 125000.0 / start.elapsed().as_secs_f64(),
                    total_bytes as f64 / 1_000_000.0
                );
                bytes_this_second = 0;
            }
        }
    }
}
