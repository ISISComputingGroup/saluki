use crate::cli_utils::BrokerAndTopic;
use isis_streaming_data_types::{deserialize_message, get_schema_id};
use log::{debug, error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::ToBytes;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::time::Duration;
use uuid::Uuid;

pub fn consume(
    topic: &BrokerAndTopic,
    partition: Option<i32>,
    filter: &Option<String>,
    num_messages: Option<i64>,
    offset: Option<i64>,
    last: Option<i64>,
    timestamp: Option<i64>,
) {
    debug!(
        "Listening to topic: {} partition {:?} on broker {}:{}, filtering {}",
        topic.topic,
        partition,
        topic.host,
        topic.port,
        filter.as_deref().unwrap_or("none")
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", Uuid::new_v4().to_string())
        .set("bootstrap.servers", topic.broker())
        .create()
        .expect("Consumer creation failed");

    let start: Option<Offset>;

    let (low_watermark, high_watermark) = consumer
        .fetch_watermarks(&topic.topic, partition.unwrap_or(0), Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("Failed to get watermarks for topic {}", topic.topic));
    let num_messages_on_topic = high_watermark - low_watermark;

    if let Some(_timestamp) = timestamp {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            &topic.topic,
            partition.unwrap_or(0),
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
    } else if let Some(_offset) = offset {
        assert!(
            _offset >= low_watermark && _offset <= high_watermark,
            "offset ({_offset:?}) must be between high ({high_watermark}) and low({low_watermark}) watermarks"
        );
        start = Some(Offset::Offset(_offset));
    } else if let Some(last_num_messages) = last {
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
        tpl.add_partition_offset(&topic.topic, partition.unwrap_or(0), _start)
            .unwrap_or_else(|_| panic!("Failed to set partition offset to {:?}", _start));
        consumer.assign(&tpl).expect("Failed to assign to topic");
    }

    consumer
        .subscribe(&[&*topic.topic])
        .unwrap_or_else(|_| panic!("Failed to subscribe to topic {}", topic.topic));

    let mut counter = 0;
    for message in consumer.iter() {
        match message {
            Ok(message) => {
                if partition.is_some() && Some(message.partition()) != partition {
                    continue;
                }

                match message.payload() {
                    Some(p) => {
                        if let Some(f) = filter {
                            if let Some(schema_id) = get_schema_id(p)
                                && schema_id != f.to_bytes()
                            {
                                continue;
                            }
                            debug!("Message has no schema id, ignoring filter")
                        }

                        match deserialize_message(p) {
                            Ok(d) => println!("{d:?}"),
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
        if Some(counter) == num_messages || Some(counter) == last {
            println!("Reached {} messages, exiting", counter);
            break;
        }
    }
}
