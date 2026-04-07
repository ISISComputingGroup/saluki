use crate::cli_utils::BrokerAndTopic;
use futures::stream::StreamExt;
use log::{error};
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use tokio::time::{self, Duration};
use uuid::Uuid;

pub async fn count(topic: BrokerAndTopic) {
    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("group.id", Uuid::new_v4().to_string())
        .set("bootstrap.servers", topic.broker())
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&*topic.topic])
        .unwrap_or_else(|_| panic!("Failed to subscribe to topic {}", topic.topic));

    let mut bytes_this_second: usize = 0;
    let mut total_bytes: usize = 0;
    let mut stream = consumer.stream();
    let mut interval = time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _msg = stream.next() => {
                match _msg {
                    Some(Ok(msg)) => {
                        if msg.payload().is_some() {
                        bytes_this_second += msg.payload_len();
                        total_bytes += msg.payload_len();
                        }

                    },
                    Some(Err(e)) => error!("Error reading from stream {:?}", e),
                    None => {}
                }
            }
            _ = interval.tick() => {
                println!("Megabits per second: {}, Total megabytes: {}", bytes_this_second as f32/125000.0, total_bytes as f32/1_000_000.0);
                bytes_this_second = 0;
            }
        }
    }
}
