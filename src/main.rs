mod cli_utils;
mod consume;
mod howl;
mod sniff;

use crate::cli_utils::BrokerAndOptionalTopic;
use crate::howl::howl;
use crate::sniff::sniff;
use clap::{Parser, Subcommand};
use cli_utils::{BrokerAndTopic, parse_broker_spec, parse_broker_spec_optional_topic};
use log::debug;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(alias = "listen")] // for muscle memory's sake...
    /// Consume from a topic and deserialise messages. Optionally specify a number of messages to consume.
    Consume {
        /// topic name, including broker and port. format: broker:port/topic
        #[arg(value_parser = parse_broker_spec)]
        topic: BrokerAndTopic,
        #[arg(short, long)]
        partition: Option<i32>,
        #[arg(short, long)]
        filter: Option<String>,
        #[arg(short, long)]
        timestamp: Option<u64>,
        #[arg(short, long)]
        offset: Option<i64>,
        #[arg(short, long)]
        messages: Option<u32>,
        #[arg(short, long, requires = "messages", default_value = "false")]
        go_forwards: Option<bool>,
    },
    /// Print broker metadata.
    Sniff {
        /// The broker to look at metadata. Optionally suffixed with a topic name to filter to that topic.
        #[arg(value_parser = parse_broker_spec_optional_topic)]
        broker: BrokerAndOptionalTopic,
    },
    Howl {
        /// Kafka Broker URL, including port
        broker: String, // TODO and optionally port
        /// topic prefix to use eg. INSTNAME
        topic_prefix: String,
        /// Events per ev44 to simulate
        #[arg(short, long, default_value = "100")]
        events_per_message: u32,
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
        #[arg(long, default_value = "10000000")]
        tof_peak: u32,
        /// Time-of-flight sigma (ns)
        #[arg(long, default_value = "2000000")]
        tof_sigma: u32,
        /// Minimum detector ID
        #[arg(long, default_value = "0")]
        det_min: u32,
        /// Maximum detector ID
        #[arg(long, default_value = "1000")]
        det_max: u32,
    }, // TODO Play {},
}

fn main() {
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
            go_forwards,
            timestamp,
        } => {
            debug!(
                "Consuming from broker {}, port {}, topic {}",
                topic.host, topic.port, topic.topic
            );
            consume::consume(
                topic,
                partition,
                filter,
                messages,
                offset,
                go_forwards,
                timestamp,
            )
        }
        Commands::Sniff { broker } => sniff(&broker),
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
        } => howl(
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
        ),
        // Commands::Play {} => {}
    }
}
