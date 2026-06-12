use crate::KafkaOption;
use crate::cli_utils::BrokerAndTopic;

use isis_streaming_data_types::{deserialize_message, get_schema_id};
use log::{debug, error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::ToBytes;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::time::Duration;
use uuid::Uuid;

pub struct ConsumeConfig<'a> {
    pub topic: &'a BrokerAndTopic,
    pub partition: Option<i32>,
    pub filter: &'a Option<String>,
    pub num_messages: Option<i64>,
    pub offset: Option<i64>,
    pub last: Option<i64>,
    pub timestamp: Option<i64>,
    pub key: bool,
    pub terse: bool,
    pub kafka_config: Option<Vec<KafkaOption>>,
}

#[allow(clippy::too_many_arguments)]
pub fn consume(config: &ConsumeConfig) {
    debug!(
        "Listening to topic: {} partition {:?} on broker {}:{}, filtering {}",
        config.topic.topic,
        config.partition,
        config.topic.host,
        config.topic.port,
        config.filter.as_deref().unwrap_or("none")
    );
    let mut client_config = ClientConfig::new();
    client_config.set("group.id", Uuid::new_v4().to_string());
    client_config.set("bootstrap.servers", config.topic.broker());

    if let Some(kafka_options) = &config.kafka_config {
        for option in kafka_options {
            println!(
                "Setting Kafka config option {}={}",
                option.key, option.value
            );
            client_config.set(&option.key, &option.value);
        }
    }

    let consumer: BaseConsumer = client_config.create().expect("Base creation failed");

    let start: Option<Offset>;

    let (low_watermark, high_watermark) = consumer
        .fetch_watermarks(
            &config.topic.topic,
            config.partition.unwrap_or(0),
            Duration::from_secs(1),
        )
        .unwrap_or_else(|_| panic!("Failed to get watermarks for topic {}", config.topic.topic));
    let num_messages_on_topic = high_watermark - low_watermark;

    if let Some(_timestamp) = config.timestamp {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            &config.topic.topic,
            config.partition.unwrap_or(0),
            Offset::Offset(_timestamp),
        )
        .expect("Can't add partition to consumer with timestamp");
        start = Some(
            consumer
                .offsets_for_times(tpl, Duration::from_secs(1))
                .expect("Failed to get offset for time")
                .elements()
                .first()
                .expect("No topics found for timestamp")
                .offset(),
        );
    } else if let Some(_offset) = config.offset {
        assert!(
            _offset >= low_watermark && _offset <= high_watermark,
            "offset ({_offset:?}) must be between high ({high_watermark}) and low({low_watermark}) watermarks"
        );
        start = Some(Offset::Offset(_offset));
    } else if let Some(last_num_messages) = config.last {
        assert!(
            last_num_messages <= num_messages_on_topic,
            "Cannot consume {last_num_messages:?} messages from a topic which only has {num_messages_on_topic} messages"
        );
        start = Some(Offset::Offset(high_watermark - last_num_messages));
    } else {
        start = None;
    }

    if let Some(_start) = start {
        info!("Starting at {_start:?}");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&config.topic.topic, config.partition.unwrap_or(0), _start)
            .unwrap_or_else(|_| panic!("Failed to set partition offset to {:?}", _start));
        consumer.assign(&tpl).expect("Failed to assign to topic");
    }

    consumer
        .subscribe(&[&config.topic.topic])
        .unwrap_or_else(|_| panic!("Failed to subscribe to topic {}", config.topic.topic));

    let mut counter = 0;
    for message in consumer.iter() {
        match message {
            Ok(message) => {
                if config.partition.is_some() && Some(message.partition()) != config.partition {
                    continue;
                }

                match message.payload() {
                    Some(p) => {
                        if let Some(f) = config.filter {
                            if let Some(schema_id) = get_schema_id(p)
                                && schema_id != f.to_bytes()
                            {
                                continue;
                            }
                            debug!("Message has no schema id, ignoring filter")
                        }
                        print!("[partition={}", message.partition());
                        if config.key {
                            print!(" key={:?}", message.key().unwrap_or(b""));
                        }
                        print!("] ");
                        match deserialize_message(p) {
                            Ok(d) => {
                                if config.terse {
                                    let schema = get_schema_id(p)
                                        .and_then(|s| str::from_utf8(s).ok())
                                        .unwrap_or("invalid");
                                    println!("{schema} ({} bytes)", p.len())
                                } else {
                                    println!("{d:?}")
                                }
                            }
                            Err(e) => error!("Failed to deserialize message: {e:?}"),
                        }
                    }
                    None => {
                        error!("No payload in message");
                        continue;
                    }
                }
            }
            Err(e) => {
                error!("Consumer error: {e:?}");
                break;
            }
        }
        counter += 1;
        if Some(counter) == config.num_messages || Some(counter) == config.last {
            println!("Reached {} messages, exiting", counter);
            break;
        }
    }
}
