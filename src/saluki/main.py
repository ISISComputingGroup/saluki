import argparse
import sys


import logging
from logging import FileHandler
from typing import Tuple

from saluki.consume import consume
from saluki.listen import listen

logger = logging.getLogger("saluki")
logging.basicConfig(level=logging.INFO)

_LISTEN = "listen"
_PRODUCE = "produce"
_CONSUME = "consume"


def parse_kafka_uri(uri: str) -> Tuple[str, str]:
    """Parse Kafka connection URI.

    A broker hostname/ip must be present.
    If username is provided, a SASL mechanism must also be provided.
    Any other validation must be performed in the calling code.
    """
    security_protocol, tail = uri.split("+") if "+" in uri else ("", uri)
    sasl_mechanism, tail = tail.split("\\") if "\\" in tail else ("", tail)
    username, tail = tail.split("@") if "@" in tail else ("", tail)
    broker, topic = tail.split("/") if "/" in tail else (tail, "")
    if not broker:
        raise RuntimeError(
            f"Unable to parse URI {uri}, broker not defined. URI should be of form [PROTOCOL+SASL_MECHANISM\\username@]broker:9092"
        )
    if username and not (security_protocol and sasl_mechanism):
        raise RuntimeError(
            f"Unable to parse URI {uri}, PROTOCOL or SASL_MECHANISM not defined. URI should be of form [PROTOCOL+SASL_MECHANISM\\username@]broker:9092"
        )
    return broker, topic


def main():
    parser = argparse.ArgumentParser(
        prog="saluki",
        description="serialise/de-serialise flatbuffers and consume/produce from/to kafka",
    )

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "topic", type=str, help="Kafka topic. format is broker<:port>/topic"
    )

    parent_parser.add_argument(
        "-X",
        "--kafka-config",
        help="kafka options to pass through to librdkafka",
        required=False,
        default="",
    )
    parent_parser.add_argument(
        "-l",
        "--log-file",
        help="filename to output all data to",
        required=False,
        default=None,
        type=argparse.FileType("a"),
    )

    sub_parsers = parser.add_subparsers(
        help="sub-command help", required=True, dest="command"
    )

    consumer_parser = argparse.ArgumentParser(add_help=False)
    consumer_parser.add_argument(
        "-e",
        "--entire",
        help="show all elements of an array in a message (truncated by default)",
        default=False,
        required=False,
    )

    consumer_mode_parser = sub_parsers.add_parser(
        _CONSUME, help="consumer mode", parents=[parent_parser, consumer_parser]
    )
    consumer_mode_parser.add_argument(
        "-m",
        "--messages",
        help="How many messages to go back",
        type=int,
        required=False,
        default=1,
    )
    consumer_mode_parser.add_argument(
        "-o", "--offset", help="offset to consume from", type=int, required=False
    )
    consumer_mode_parser.add_argument(
        "-s", "--schema", required=False, default="auto", type=str
    )
    consumer_mode_parser.add_argument(
        "-g", "--go-forwards", required=False, action="store_true"
    )
    consumer_mode_parser.add_argument(
        "-p", "--partition", required=False, type=int, default=0
    )

    listen_parser = sub_parsers.add_parser(
        _LISTEN,
        help="listen mode - listen until KeyboardInterrupt",
        parents=[parent_parser, consumer_parser],
    )
    listen_parser.add_argument(
        "-p", "--partition", required=False, type=int, default=None
    )

    # Producer mode - add this later
    producer_parser = sub_parsers.add_parser(
        _PRODUCE, help="producer mode", parents=[parent_parser]
    )
    producer_parser.add_argument(
        "-f",
        "--filename",
        help="JSON file to produce",
        required=True,
        type=argparse.FileType("r"),
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()

    broker, topic = parse_kafka_uri(args.topic)

    if args.log_file:
        logger.addHandler(FileHandler(args.log_file.name))

    if args.command == _LISTEN:
        listen(broker, topic, args.partition)
    elif args.command == _CONSUME:
        consume(
            broker,
            topic,
            args.partition,
            args.messages,
            args.offset,
            args.go_forwards,
        )
    elif args.command == _PRODUCE:
        raise NotImplementedError


if __name__ == "__main__":
    main()
