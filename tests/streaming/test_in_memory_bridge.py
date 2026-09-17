from datetime import datetime, timezone

import pytest

from edge.edge_monitor.config import load_config
from simulators.vm_simulator import default_runtimes, run_once
from streaming.bridge.in_memory import InMemoryBridge


def _message():
    return run_once(
        default_runtimes("run-001")[0],
        load_config(),
        1,
        datetime(2026, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 9, 15, 0, 0, 0, 1000, tzinfo=timezone.utc),
    )


def test_bridge_appends_in_order_and_reports_duplicate():
    bridge = InMemoryBridge()
    first = bridge.publish(_message())
    second = bridge.publish(_message())

    assert first == {
        "topic": "gapguard.telemetry.v1",
        "offset": 0,
        "kafka_key": "site-001-wall-001",
        "event_id": "run-001:site-001-wall-001:1",
        "duplicate": False,
    }
    assert second["offset"] == 1
    assert second["duplicate"] is True
    assert len(bridge.topics["gapguard.telemetry.v1"].values) == 2


def test_bridge_rejects_bad_hash_without_append():
    bridge = InMemoryBridge()
    message = _message()
    message["dataHash"] = "sha256:" + "0" * 64

    with pytest.raises(ValueError, match="data hash mismatch"):
        bridge.publish(message)

    assert bridge.topics == {}
