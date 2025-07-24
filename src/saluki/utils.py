import datetime
import logging
from typing import Tuple, List

from confluent_kafka import Message

from streaming_data_types import DESERIALISERS
from streaming_data_types.exceptions import ShortBufferException
from streaming_data_types.utils import get_schema


logger = logging.getLogger("saluki")


def __try_to_deserialise_message(payload: bytes) -> Tuple[str | None, str | None]:
    logger.debug(f"got some data: {payload}")
    try:
        schema = get_schema(payload)
    except ShortBufferException:
        schema = None

    logger.debug(f"schema: {schema}")

    try:
        deserialiser = DESERIALISERS[schema]
    except KeyError:
        logger.exception(f"Invalid schema: {schema}, falling back to raw bytes decode")

        def fallback_deserialiser(payload: bytes) -> str:
            return payload.decode()

        deserialiser = (
            fallback_deserialiser  # Fall back to this if we need to so data isn't lost
        )

    logger.debug(f"Deserialiser: {deserialiser}")

    try:
        ret = deserialiser(payload)
    except Exception as e:
        raise e

    return schema, ret


def _deserialise_and_print_messages(msgs: List[Message], partition: int | None) -> None:
    for msg in msgs:
        try:
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: {}".format(msg.error()))
                continue
            if partition is not None and msg.partition() != partition:
                continue
            schema, deserialised = __try_to_deserialise_message(msg.value())
            time = _parse_timestamp(msg)
            logger.info(f"{msg.offset()} ({time}):({schema}) {deserialised}")
        except Exception as e:
            logger.exception(f"Got error while deserialising: {e}")


def _parse_timestamp(msg: Message) -> str:
    """
    Parse a message timestamp.

    See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message.timestamp
    :param msg: the message to parse.
    :return: either the string-formatted timestamp or "Unknown" if not able to parse.
    """
    timestamp_type, timestamp_ms_from_epoch = msg.timestamp()
    if timestamp_type == 1:  # TIMESTAMP_CREATE_TIME
        return datetime.datetime.fromtimestamp(timestamp_ms_from_epoch / 1000).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
    else:
        # TIMESTAMP_NOT_AVAILABLE or TIMESTAMP_LOG_APPEND_TIME
        return "Unknown"


def parse_kafka_uri(uri: str) -> Tuple[str, str]:
    """Parse Kafka connection URI.

    A broker hostname/ip must be present.
    If username is provided, a SASL mechanism must also be provided.
    Any other validation must be performed in the calling code.
    """
    broker, topic = uri.split("/") if "/" in uri else (uri, "")
    if not topic:
        raise RuntimeError(
            f"Unable to parse URI {uri}, topic not defined. URI should be of form"
            f" broker[:port]/topic"
        )
    return (
        broker,
        topic,
    )
