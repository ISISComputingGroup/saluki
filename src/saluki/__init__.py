import logging

from streaming_data_types import DESERIALISERS
from streaming_data_types.exceptions import StreamingDataTypesException
from streaming_data_types.utils import get_schema

logger = logging.getLogger("saluki")


def _fallback_deserialiser(payload: bytes) -> str:
    return payload.decode()


def try_to_deserialise_message(payload: bytes) -> str:
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
    return deserialiser(payload)
