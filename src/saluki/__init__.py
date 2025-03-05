import logging
from typing import Tuple, List

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
        logger.info(f"{msg.offset()}:({schema}) {deserialised}")
