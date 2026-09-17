from copy import deepcopy

import pytest

from streaming.bridge import process_message
from streaming.common.message import compute_data_hash


def _message(**updates):
    message = {
        "siteId": "field-001",
        "sectionId": "section-001",
        "deviceId": "device-001",
        "sourceType": "simulator",
        "measuredAt": "2026-09-15T00:00:00Z",
        "seqNo": 1,
        "eventId": "run-001:stream-001:1",
        "streamId": "stream-001",
        "schemaVersion": 3,
        "producedAt": "2026-09-15T00:00:00.010Z",
        "testRunId": "run-001",
        "sensors": {"top": {"tiltX": 0.1}, "bottom": {"tiltX": 0.05}},
    }
    message.update(updates)
    message["dataHash"] = compute_data_hash(message)
    return message


def test_processes_telemetry_and_uses_stream_id_as_kafka_key():
    message = _message()

    result = process_message(message)

    assert result == {
        "message": message,
        "message_type": "telemetry",
        "kafka_key": "stream-001",
    }
    assert result["message"] is not message


def test_processes_event():
    message = _message(eventType="THRESHOLD_EXCEEDED")
    message["dataHash"] = compute_data_hash(message)

    result = process_message(message)

    assert result["message_type"] == "event"
    assert result["kafka_key"] == "stream-001"


def test_rejects_hash_mismatch():
    message = _message()
    message["sensors"]["top"]["tiltX"] = 0.2

    with pytest.raises(ValueError, match="data hash mismatch"):
        process_message(message)


def test_rejects_identity_mismatch():
    message = _message(eventId="run-001:other-stream:1")
    message["dataHash"] = compute_data_hash(message)

    with pytest.raises(ValueError, match="eventId does not match message identity"):
        process_message(message)


def test_does_not_modify_mapping_or_nested_payload():
    message = _message(eventType="SENSOR_ERROR")
    before = deepcopy(message)

    process_message(message)

    assert message == before
    assert message["sensors"] == before["sensors"]
