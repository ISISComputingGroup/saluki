import logging
from typing import Tuple, List
import datetime
from confluent_kafka import Message
from streaming_data_types import DESERIALISERS
from streaming_data_types.exceptions import StreamingDataTypesException
from streaming_data_types.utils import get_schema

logger = logging.getLogger("saluki")


def _fallback_deserialiser(payload: bytes) -> str:
    return payload.decode()


def try_to_deserialise_message(payload: bytes) -> Tuple[str, str]:
    logger.debug(f"got some data: {payload}")
    schema = get_schema(payload)
    deserialiser = (
        _fallback_deserialiser  # Fall back to this if we need to so data isn't lost
    )
    try:
        deserialiser = DESERIALISERS[schema]
    except StreamingDataTypesException:
        pass  # TODO
    except KeyError:
        pass
    return schema, deserialiser(payload)


def _deserialise_and_print_messages(msgs: List[Message], partition: int | None) -> None:
    for msg in msgs:
        if msg is None:
            continue
        if msg.error():
            logger.error("Consumer error: {}".format(msg.error()))
            continue
        if partition is not None and msg.partition() != partition:
            continue
        schema, deserialised = try_to_deserialise_message(msg.value())
        time = _parse_timestamp(msg)
        logger.info(f"{msg.offset()} ({time}):({schema}) {deserialised}")


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
