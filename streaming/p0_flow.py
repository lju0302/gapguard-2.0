"""One-command local P0 flow: simulator, bridge, topic, and storage consumer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from edge.edge_monitor.config import load_config
from simulators.vm_simulator import default_runtimes, run_once
from streaming.bridge.in_memory import InMemoryBridge
from streaming.consumers.telemetry_storage import SQLiteTelemetryStore, TelemetryStorageConsumer


@dataclass(frozen=True)
class P0RunResult:
    test_run_id: str
    runtime_count: int
    published_count: int
    stored_count: int
    duplicate_count: int
    next_offset: int


def run_local_p0(
    test_run_id: str,
    storage_path: str | Path = ":memory:",
    measured_at: datetime | None = None,
    produced_at: datetime | None = None,
) -> P0RunResult:
    if not test_run_id:
        raise ValueError("test_run_id must not be empty")
    measured_at = measured_at or datetime.now(timezone.utc)
    produced_at = produced_at or measured_at
    config = load_config()
    runtimes = default_runtimes(test_run_id)
    bridge = InMemoryBridge()
    for runtime in runtimes:
        bridge.publish(run_once(runtime, config, 1, measured_at, produced_at))

    topic = bridge.topics["gapguard.telemetry.v1"]
    with SQLiteTelemetryStore(storage_path) as store:
        consumed = TelemetryStorageConsumer(store).consume(topic)
        return P0RunResult(
            test_run_id=test_run_id,
            runtime_count=len(runtimes),
            published_count=len(topic.values),
            stored_count=consumed.stored_count,
            duplicate_count=consumed.duplicate_count,
            next_offset=consumed.next_offset,
        )


if __name__ == "__main__":
    print(json.dumps(run_local_p0("local-run").__dict__, ensure_ascii=False))
