mod cli_utils;
mod consume;
mod sniff;

use crate::sniff::sniff;
use clap::{Parser, Subcommand};
use cli_utils::{BrokerAndTopic, parse_broker_spec};
use log::debug;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
    Sniff { broker: String },
    // TODO Howl {},
    // TODO Play {},
}

fn main() {
    let cli = Cli::parse();

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
        Commands::Sniff { broker } => sniff(&broker), // Commands::Howl {} => {}
                                                      // Commands::Play {} => {}
    }
}
