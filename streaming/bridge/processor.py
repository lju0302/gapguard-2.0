from collections.abc import Mapping
from typing import Any

from streaming.common.message import (
    kafka_key,
    message_type,
    validate_identity,
    verify_data_hash,
)


def process_message(message: Mapping[str, Any]) -> dict[str, Any]:
    copied = dict(message)
    validate_identity(copied)
    if not verify_data_hash(copied):
        raise ValueError("data hash mismatch")

    return {
        "message": copied,
        "message_type": message_type(copied),
        "kafka_key": kafka_key(copied),
    }
