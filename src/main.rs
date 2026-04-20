mod cli_utils;
mod consume;
mod count;
mod howl;
mod sniff;

use crate::cli_utils::BrokerAndOptionalTopic;
use crate::count::count;
use crate::howl::{EventMessageConfig, HowlConfig, howl};
use crate::sniff::sniff;
use clap::{Parser, Subcommand};
use cli_utils::{BrokerAndTopic, parse_broker_spec, parse_broker_spec_optional_topic};
use std::str::FromStr;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

#[derive(Debug, Clone)]
pub struct KafkaOption {
    key: String,
    value: String,
}

impl FromStr for KafkaOption {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (key, value) = s
            .split_once('=')
            .ok_or_else(|| format!("expected KEY=VALUE, got '{}", s))?;

        Ok(KafkaOption {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(alias = "listen")] // for muscle memory's sake...
    /// Consume from a topic and deserialise messages. Optionally specify a number of messages to consume.
    Consume {
        /// topic name, including broker and port. format: broker:port/topic
        #[arg(value_parser = parse_broker_spec)]
        topic: BrokerAndTopic,
        /// Partition - default is all partitions or if using offset/timestamp default is 0.
        #[arg(short, long)]
        partition: Option<i32>,
        #[arg(short, long)]
        filter: Option<String>,
        /// Timestamp to consume from
        #[arg(short, long, conflicts_with = "offset")]
        timestamp: Option<i64>,
        /// Offset to consume from
        #[arg(short, long, conflicts_with = "timestamp")]
        offset: Option<i64>,
        /// Stop printing after x messages
        #[arg(short, long)]
        messages: Option<i64>,
        /// Print last x messages on topic
        #[arg(short, long, conflicts_with_all = ["offset","timestamp","messages","filter"])]
        last: Option<i64>,
        // Additonal command line arguments
        #[arg(short = 'X', long)]
        kafka_config: Option<Vec<KafkaOption>>,
    },
    /// Print broker metadata.
    Sniff {
        /// The broker to look at metadata. Optionally suffixed with a topic name to filter to that topic.
        #[arg(value_parser = parse_broker_spec_optional_topic)]
        broker: BrokerAndOptionalTopic,
        // Additonal command line arguments
        #[arg(short = 'X', long)]
        kafka_config: Option<Vec<KafkaOption>>,
    },
    Howl {
        /// Kafka Broker URL, including port
        broker: String,
        /// topic prefix to use eg. INSTNAME
        topic_prefix: String,
        /// Events per ev44 to simulate
        #[arg(short, long, default_value = "100")]
        events_per_message: i32,
        /// Number of ev44 per frame to simulate
        #[arg(short, long, default_value = "20")]
        messages_per_frame: u32,
        /// Frames per second to simulate
        #[arg(short, long, default_value = "1")]
        frames_per_second: u32,
        /// Frames to take before beginning new run (0 to run forever)
        #[arg(long, default_value = "0")]
        frames_per_run: u32,
        /// Time-of-flight peak (ns)
        #[arg(long, default_value = "10000000.0")]
        tof_peak: f32,
        /// Time-of-flight sigma (ns)
        #[arg(long, default_value = "2000000.0")]
        tof_sigma: f32,
        /// Minimum detector ID
        #[arg(long, default_value = "0")]
        det_min: i32,
        /// Maximum detector ID
        #[arg(long, default_value = "1000")]
        det_max: i32,
        // Additonal command line arguments
        #[arg(short = 'X', long)]
        kafka_config: Option<Vec<KafkaOption>>,
    },
    Count {
        /// topic name, including broker and port. format: broker:port/topic
        #[arg(value_parser = parse_broker_spec)]
        topic: BrokerAndTopic,
        /// Data information print intervals (s)
        #[arg[long, default_value = "1"]]
        message_interval: u64,
        /// Additonal command line arguments
        #[arg(short = 'X', long)]
        kafka_config: Option<Vec<KafkaOption>>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    env_logger::Builder::new()
        .filter_level(cli.verbosity.into())
        .format_timestamp_micros()
        .init();

    match cli.command {
        Commands::Consume {
            topic,
            partition,
            filter,
            messages,
            offset,
            last,
            timestamp,
            kafka_config,
        } => consume::consume(
            &topic,
            partition,
            &filter,
            messages,
            offset,
            last,
            timestamp,
            kafka_config,
        ),
        Commands::Sniff {
            broker,
            kafka_config,
        } => sniff(&broker, kafka_config),
        Commands::Howl {
            broker,
            topic_prefix,
            events_per_message,
            messages_per_frame,
            frames_per_second,
            frames_per_run,
            tof_peak,
            tof_sigma,
            det_min,
            det_max,
            kafka_config,
        } => howl(&HowlConfig {
            kafka_config,
            broker: &broker,
            event_topic: &format!("{topic_prefix}_rawEvents"),
            run_info_topic: &format!("{topic_prefix}_runInfo"),
            messages_per_frame,
            frames_per_second,
            frames_per_run,
            event_message_config: &EventMessageConfig {
                events_per_message,
                tof_peak,
                tof_sigma,
                det_min,
                det_max,
            },
        }),
        Commands::Count {
            topic,
            message_interval,
            kafka_config,
        } => {
            count(topic, message_interval, kafka_config).await;
        } // Commands::Play {} => {}
    }
}
