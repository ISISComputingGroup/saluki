import argparse
import logging
import sys

from saluki.consume import consume
from saluki.listen import listen
from saluki.utils import parse_kafka_uri

logger = logging.getLogger("saluki")
logging.basicConfig(level=logging.INFO)

_LISTEN = "listen"
_CONSUME = "consume"
_REPLAY = "replay"


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="saluki",
        description="serialise/de-serialise flatbuffers and consume/produce from/to kafka",
    )

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("topic", type=str, help="Kafka topic. format is broker<:port>/topic")

    parent_parser.add_argument(
        "-X",
        "--kafka-config",
        help="kafka options to pass through to librdkafka",
        required=False,
        default=None,
    )
    parent_parser.add_argument(
        "-l",
        "--log-file",
        help="filename to output all data to",
        required=False,
        default=None,
        type=argparse.FileType("a"),
    )

    sub_parsers = parser.add_subparsers(help="sub-command help", required=True, dest="command")

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
    consumer_mode_parser.add_argument("-s", "--schema", required=False, default="auto", type=str)
    consumer_mode_parser.add_argument("-g", "--go-forwards", required=False, action="store_true")
    consumer_mode_parser.add_argument("-p", "--partition", required=False, type=int, default=0)
    # TODO make this allow multiple comma-split args
    consumer_mode_parser.add_argument("-f", "--filter", required=False, type=str, nargs='+')

    listen_parser = sub_parsers.add_parser(
        _LISTEN,
        help="listen mode - listen until KeyboardInterrupt",
        parents=[parent_parser, consumer_parser],
    )
    listen_parser.add_argument("-p", "--partition", required=False, type=int, default=None)
    # TODO make filtering work for this as well

    #### NEW FEATURES HERE PLZ
    # replay from, to offset
    # saluki replay -o FROMOFFSET TOOFFSET srcbroker/srctopic destbroker/desttopic

    # replay from, to timestamp
    # saluki replay -t FROMTIMESTAMP TOTIMESTAMP srcbroker/srctopic destbroker/desttopic

    # saluki consume x messages of y schema
    # saluki consume -f pl72 mybroker:9092/XXX_runInfo -m 10 # get the last pl72 run starts

    # saluki consume x messages of y or z schema
    # saluki consume -f pl72,6s4t mybroker:9092/XXX_runInfo -m 10 # get the last pl72 run starts or 6s4t run stops

    replay_parser = sub_parsers.add_parser(
        _REPLAY,
        help="replay mode - replay data into another topic",
        parents=[parent_parser],
    )
    replay_parser.add_argument("-o", "--offset", help="replay between offsets", type=bool, required=False, default="store_false")
    replay_parser.add_argument("-t", "--timestamp", help="replay between timestamps", type=bool, required=False)


    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()

    if args.kafka_config is not None:
        raise NotImplementedError("-X is not implemented yet.")

    broker, topic = parse_kafka_uri(args.topic)

    if args.log_file:
        logger.addHandler(logging.FileHandler(args.log_file.name))

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
    elif args.command == _REPLAY:
        pass

if __name__ == "__main__":
    main()
