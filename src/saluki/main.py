import argparse
import logging
import sys

from saluki.consume import consume
from saluki.listen import listen
from saluki.play import play
from saluki.sniff import sniff
from saluki.utils import parse_kafka_uri, dateutil_parsable_or_unix_timestamp

logger = logging.getLogger("saluki")
logging.basicConfig(level=logging.INFO)

_LISTEN = "listen"
_CONSUME = "consume"
_PLAY = "play"
_SNIFF = "sniff"


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="saluki",
        description="serialise/de-serialise flatbuffers and consume/produce from/to kafka",
    )
    common_options = argparse.ArgumentParser(add_help=False)
    common_options.add_argument("-v", "--verbose", help="show DEBUG logs", action='store_true')
    common_options.add_argument(
        "-l",
        "--log-file",
        help="filename to output all data to",
        required=False,
        default=None,
        type=argparse.FileType("a"),
    )

    topic_parser = argparse.ArgumentParser(add_help=False)
    topic_parser.add_argument("topic", type=str, help="Kafka topic. format is broker<:port>/topic")

    topic_parser.add_argument(
        "-X",
        "--kafka-config",
        help="kafka options to pass through to librdkafka",
        required=False,
        default=None,
    )
    topic_parser.add_argument("-p", "--partition", required=False, type=int, default=0)
    topic_parser.add_argument("-f", "--filter", required=False, action="append")

    sub_parsers = parser.add_subparsers(help="sub-command help", required=True, dest="command")

    sniff_parser = sub_parsers.add_parser(_SNIFF, help="sniff - broker metadata", parents=[common_options])
    sniff_parser.add_argument("broker", type=str, help="broker, optionally suffixed with a topic name to filter to")

    consumer_parser = argparse.ArgumentParser(add_help=False)
    consumer_parser.add_argument(
        "-e",
        "--entire",
        help="show all elements of an array in a message (truncated by default)",
        default=False,
        required=False,
    )

    consumer_mode_parser = sub_parsers.add_parser(
        _CONSUME, help="consumer mode", parents=[topic_parser, consumer_parser, common_options]
    )
    consumer_mode_parser.add_argument(
        "-m",
        "--messages",
        help="How many messages to go back",
        type=int,
        required=False,
        default=1,
    )

    consumer_mode_parser.add_argument("-g", "--go-forwards", required=False, action="store_true")
    cg = consumer_mode_parser.add_mutually_exclusive_group(required=False)
    cg.add_argument(
        "-o", "--offset", help="offset to consume from", type=int,
    )
    cg.add_argument("-t", "--timestamp", help="timestamp to consume from", type=dateutil_parsable_or_unix_timestamp)

    listen_parser = sub_parsers.add_parser(  # noqa: F841
        _LISTEN,
        help="listen mode - listen until KeyboardInterrupt",
        parents=[topic_parser, consumer_parser, common_options],
    )

    play_parser = sub_parsers.add_parser(
        _PLAY,
        help="replay mode - replay data into another topic",
        parents=[common_options],
    )
    play_parser.add_argument("topics", type=str, nargs=2, help="SRC topic DEST topic")
    g = play_parser.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "-o",
        "--offsets",
        help="offsets to replay between (inclusive)",
        type=int,
        nargs=2,
    )
    g.add_argument("-t", "--timestamps", help='timestamps to replay between in ISO8601 or RFC3339 format ie. "2025-11-17 07:00:00 or as a unix timestamp"  ', type=dateutil_parsable_or_unix_timestamp, nargs=2)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.log_file:
        logger.addHandler(logging.FileHandler(args.log_file.name))

    if "kafka_config" in args and args.kafka_config is not None:
        raise NotImplementedError("-X is not implemented yet.")

    if args.command == _LISTEN:
        broker, topic = parse_kafka_uri(args.topic)
        listen(broker, topic, args.partition, args.filter)
    elif args.command == _CONSUME:
        broker, topic = parse_kafka_uri(args.topic)
        consume(
            broker, topic, args.partition, args.messages, args.offset, args.go_forwards, args.filter, args.timestamp
        )
    elif args.command == _PLAY:
        src_broker, src_topic = parse_kafka_uri(args.topics[0])
        dest_broker, dest_topic = parse_kafka_uri(args.topics[1])

        play(
            src_broker,
            src_topic,
            dest_broker,
            dest_topic,
            args.offsets,
            args.timestamps,
        )
    elif args.command == _SNIFF:
        try:
            broker, topic = parse_kafka_uri(args.broker)
            logger.debug(f"Sniffing single topic {topic} on broker {broker}")
            sniff(broker, topic)
        except RuntimeError:
            logger.debug(f"Sniffing whole broker {args.broker}")
            sniff(args.broker)


if __name__ == "__main__":
    main()
