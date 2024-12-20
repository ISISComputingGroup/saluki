import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="saluki",
        description="serialise/de-serialise flatbuffers and consume/produce from/to kafka",
    )

    parser.add_argument("-s", "--schema", required=False, default="auto")

    sub_parsers = parser.add_subparsers(
        help="sub-command help", required=True, dest="command"
    )

    consumer_parser = sub_parsers.add_parser("c", help="consumer mode")
    consumer_parser.add_argument("-m", help="How many messages to go back", type=int)
    consumer_parser.add_argument("-o", help="offset to consume from", type=int)

    listen_parser = sub_parsers.add_parser(
        "l", help="listen mode - listen until KeyboardInterrupt"
    )

    # Producer mode - add this later
    producer_parser = sub_parsers.add_parser("p", help="producer mode")

    parser.add_argument("-b", "--broker", required=True, type=str)
    parser.add_argument("-t", "--topic", required=True, type=str)
    parser.add_argument(
        "-X",
        help="kafka options to pass through to librdkafka",
        required=False,
        default="",
    )
    parser.add_argument("-f", help="filename to output all data to", required=False)

    if len(sys.argv) == 1:
        parser.print_usage()
        sys.exit(1)

    args = parser.parse_args()
    if args.command == "l":
        while True:
            print("listening")
    elif args.command == "c":
        raise NotImplementedError
    elif args.command == "p":
        raise NotImplementedError


if __name__ == "__main__":
    main()
