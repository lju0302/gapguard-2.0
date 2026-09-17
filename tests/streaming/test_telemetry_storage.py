from datetime import datetime, timezone

import pytest

from edge.edge_monitor.config import load_config
from simulators.vm_simulator import default_runtimes, run_once
from streaming.bridge.in_memory import InMemoryBridge, InMemoryTopic
from streaming.consumers.telemetry_storage import (
    SQLiteTelemetryStore,
    TelemetryStorageConsumer,
)


def _message(seq_no=1):
    return run_once(
        default_runtimes("run-001")[0],
        load_config(),
        seq_no,
        datetime(2026, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 9, 15, 0, 0, 0, 1000, tzinfo=timezone.utc),
    )


def test_consumer_stores_once_and_returns_next_offset_after_success():
    bridge = InMemoryBridge()
    bridge.publish(_message())
    bridge.publish(_message())

    with SQLiteTelemetryStore() as store:
        result = TelemetryStorageConsumer(store).consume(bridge.topics["gapguard.telemetry.v1"])

        assert result.next_offset == 2
        assert result.stored_count == 1
        assert result.duplicate_count == 1
        assert store.count == 1


def test_consumer_does_not_advance_when_message_validation_fails():
    bridge = InMemoryBridge()
    message = _message()
    message["dataHash"] = "0" * 64
    bridge.topics["gapguard.telemetry.v1"] = InMemoryTopic()
    bridge.topics["gapguard.telemetry.v1"].publish(message)

    with SQLiteTelemetryStore() as store:
        with pytest.raises(ValueError, match="data hash mismatch"):
            TelemetryStorageConsumer(store).consume(bridge.topics["gapguard.telemetry.v1"])
        assert store.count == 0
