import argparse
import sys
from confluent_kafka import Consumer
from streaming_data_types import DESERIALISERS
from streaming_data_types.exceptions import (
    WrongSchemaException,
    StreamingDataTypesException,
)


def main():
    parser = argparse.ArgumentParser(
        prog="saluki",
        description="serialise/de-serialise flatbuffers and consume/produce from/to kafka",
    )

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("-b", "--broker", required=True, type=str)
    parent_parser.add_argument("-t", "--topic", required=True, type=str)
    parent_parser.add_argument(
        "-X",
        help="kafka options to pass through to librdkafka",
        required=False,
        default="",
    )
    parent_parser.add_argument(
        "-f", help="filename to output all data to", required=False
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
        "c", help="consumer mode", parents=[parent_parser, consumer_parser]
    )
    consumer_mode_parser.add_argument(
        "-m", help="How many messages to go back", type=int
    )
    consumer_mode_parser.add_argument("-o", help="offset to consume from", type=int)
    consumer_mode_parser.add_argument(
        "-s", "--schema", required=False, default="auto", type=str
    )

    _ = sub_parsers.add_parser(
        "l",
        help="listen mode - listen until KeyboardInterrupt",
        parents=[parent_parser, consumer_parser],
    )

    # Producer mode - add this later
    _ = sub_parsers.add_parser("p", help="producer mode", parents=[parent_parser])

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    if args.command == "l":
        c = Consumer(
            {
                "bootstrap.servers": args.broker,
                "group.id": "saluki",
            }
        )

        c.subscribe([args.topic])
        try:
            print(f"listening to {args.broker}/{args.topic}")
            while True:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                deserialised = try_to_deserialise_message(msg.value())
                print(f"{msg.offset()}: {deserialised}")
        except KeyboardInterrupt:
            print("finished listening")
        finally:
            print(f"closing consumer {c}")
            c.close()
    elif args.command == "c":
        raise NotImplementedError  # TODO
    elif args.command == "p":
        raise NotImplementedError  # TODO


def try_to_deserialise_message(payload: bytes) -> str:
    print("got some data")
    file_id = payload[4:8]
    deserialiser = lambda x: x  # Fall back to this if we need to so data isn't lost
    try:
        deserialiser = DESERIALISERS[file_id.decode()]
    except WrongSchemaException as e:
        pass  # TODO
    except StreamingDataTypesException:
        pass  # TODO
    except KeyError:
        pass
    return deserialiser(payload)


if __name__ == "__main__":
    main()
