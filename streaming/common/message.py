import hashlib
import json
from collections.abc import Mapping
from typing import Any


Message = Mapping[str, Any]


def canonical_json_without_hash(message: Message) -> bytes:
    unsigned = {key: value for key, value in message.items() if key != "dataHash"}
    return json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def compute_data_hash(message: Message) -> str:
    return hashlib.sha256(canonical_json_without_hash(message)).hexdigest()


def verify_data_hash(message: Message) -> bool:
    return message.get("dataHash") == compute_data_hash(message)


def message_type(message: Message) -> str:
    return "event" if "eventType" in message else "telemetry"


def kafka_key(message: Message) -> Any:
    return message["streamId"]


def validate_identity(message: Message) -> None:
    try:
        event_id = message["eventId"]
        test_run_id = message["testRunId"]
        stream_id = message["streamId"]
        schema_version = message["schemaVersion"]
        seq_no = message["seqNo"]
    except (KeyError, TypeError) as exc:
        raise ValueError("message identity fields are required") from exc

    if schema_version != 3:
        raise ValueError("schemaVersion must be 3")
    if isinstance(seq_no, bool) or not isinstance(seq_no, int) or seq_no < 0:
        raise ValueError("seqNo must be a non-negative integer")
    if event_id != f"{test_run_id}:{stream_id}:{seq_no}":
        raise ValueError("eventId does not match message identity")
