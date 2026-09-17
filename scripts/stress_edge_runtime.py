"""Run staged, hardware-free load tests for the three-worker Edge runtime."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import queue
import resource
import sys
import tempfile
import threading
import time
from typing import Any, Mapping

repo_root = Path(os.environ.get("GAPGUARD_REPO_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(repo_root))

from edge.edge_monitor.config import load_config
from edge.edge_monitor.processing import WindowSummary, SensorSummary
from edge.edge_monitor.runtime import EdgeRuntime
from edge.edge_monitor.storage.blackbox import SQLiteBlackbox


def _summary(measured_at: datetime, index: int) -> WindowSummary:
    value = 0.1 + (index % 10) * 0.001
    sensor = SensorSummary(value, 0.0, 0.1, 0.2)
    return WindowSummary(measured_at, 100, sensor, sensor)


class SyntheticCollector:
    """Emit completed one-second summaries at a controlled rate."""

    max_schedule_lateness_ms = 0.0

    def __init__(self, count: int, interval_s: float, *, drop_oldest: bool = False) -> None:
        self.count = count
        self.interval_s = interval_s
        self.drop_oldest = drop_oldest
        self.blocked_puts = 0
        self.total_put_wait_s = 0.0
        self.max_put_wait_s = 0.0
        self.dropped_windows = 0

    def collect_to_queue(self, target_queue, *, stop_event=None, count=None, on_overflow=None):
        total = self.count if count is None else count
        start = datetime.now(timezone.utc) - timedelta(seconds=total * self.interval_s)
        full_count = 0
        for index in range(total):
            if stop_event is not None and stop_event.is_set():
                break
            if self.interval_s:
                time.sleep(self.interval_s)
            item = (_summary(start + timedelta(seconds=index * self.interval_s), index), 0.0)
            if self.drop_oldest:
                try:
                    target_queue.put_nowait(item)
                except queue.Full:
                    full_count += 1
                    self.dropped_windows += 1
                    if on_overflow is not None:
                        on_overflow(target_queue.qsize(), full_count)
                    try:
                        target_queue.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        target_queue.put_nowait(item)
                    except queue.Full:
                        self.dropped_windows += 1
                continue
            while True:
                put_started = time.monotonic()
                try:
                    target_queue.put(item, timeout=0.05)
                    put_wait_s = time.monotonic() - put_started
                    self.total_put_wait_s += put_wait_s
                    self.max_put_wait_s = max(self.max_put_wait_s, put_wait_s)
                    if put_wait_s >= 0.001:
                        self.blocked_puts += 1
                    break
                except queue.Full:
                    full_count += 1
                    if on_overflow is not None:
                        on_overflow(target_queue.qsize(), full_count)
                    if stop_event is not None and stop_event.is_set():
                        raise RuntimeError("synthetic collector stopped while queue was full")


class CountingBlackbox(SQLiteBlackbox):
    def __init__(self, path: str | Path, delay_s: float = 0.0) -> None:
        super().__init__(path)
        self.delay_s = delay_s
        self.telemetry_enqueued = 0

    def enqueue_many(self, messages):
        if self.delay_s:
            time.sleep(self.delay_s)
        inserted = super().enqueue_many(messages)
        self.telemetry_enqueued += sum(
            1 for message_type, _ in messages if message_type == "telemetry"
        )
        return inserted


class RecordingSender:
    def __init__(self, delay_s: float = 0.0, online: bool = True) -> None:
        self.delay_s = delay_s
        self.online = online
        self.sent: list[tuple[str, Mapping[str, Any], float]] = []

    def send_json(self, message_type, payload, properties=None):
        del properties
        if self.delay_s:
            time.sleep(self.delay_s)
        if not self.online:
            raise ConnectionError("synthetic network outage")
        self.sent.append((message_type, dict(payload), time.time()))


@dataclass(frozen=True)
class Stage:
    name: str
    windows: int
    collector_interval_s: float
    spool_delay_s: float
    sender_delay_s: float
    queue_size: int
    start_online: bool = True
    recover_after_spool: bool = False


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * percentile))
    return ordered[index]


def run_stage(stage: Stage) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="gapguard-stress-") as temp_dir:
        config = replace(
            load_config(),
            source_type="simulator",
            blackbox_path=str(Path(temp_dir) / "blackbox.db"),
            snapshot_dir=str(Path(temp_dir) / "snapshots"),
        )
        sender = RecordingSender(stage.sender_delay_s, stage.start_online)
        collector = SyntheticCollector(
            stage.windows,
            stage.collector_interval_s,
            drop_oldest=os.environ.get("GAPGUARD_STRESS_QUEUE_POLICY") == "drop_oldest",
        )
        with CountingBlackbox(Path(temp_dir) / "blackbox.db", stage.spool_delay_s) as blackbox:
            runtime = EdgeRuntime(
                config,
                collector,
                blackbox=blackbox,
                transport=sender,
                queue_maxsize=stage.queue_size,
            )
            max_queue = 0
            max_outbox = 0
            monitor_stop = threading.Event()

            def monitor() -> None:
                nonlocal max_queue, max_outbox
                while not monitor_stop.is_set():
                    diagnostics = runtime.get_diagnostics()
                    max_queue = max(max_queue, diagnostics["queueDepth"])
                    max_outbox = max(max_outbox, diagnostics["outboxDepth"])
                    time.sleep(0.005)

            monitor_thread = threading.Thread(target=monitor, daemon=True)
            started = time.monotonic()
            monitor_thread.start()
            runtime.start(window_count=stage.windows)
            runtime.join_collector()
            runtime.join_spooler()

            if stage.recover_after_spool:
                sender.online = True
                _trigger_flush(runtime)
            idle = _wait_for_idle(runtime, timeout=30.0) if sender.online else False
            runtime.raise_if_worker_failed()
            elapsed_s = time.monotonic() - started
            diagnostics = runtime.get_diagnostics()
            monitor_stop.set()
            monitor_thread.join(timeout=1.0)

            sent_telemetry = [item for item in sender.sent if item[0] == "telemetry"]
            telemetry_pending = sum(
                1 for item in blackbox.pending() if item.message_type == "telemetry"
            )
            latency_ms = [
                (sent_at - datetime.fromisoformat(payload["measuredAt"]).timestamp()) * 1000
                for message_type, payload, sent_at in sent_telemetry
            ]
            result = {
                "stage": stage.name,
                "requested": stage.windows,
                "spooled": blackbox.telemetry_enqueued,
                "sent": len(sent_telemetry),
                "pending": telemetry_pending,
                "loss": stage.windows - len(sent_telemetry) - telemetry_pending,
                "loss_pct": round(
                    (stage.windows - len(sent_telemetry) - telemetry_pending)
                    / stage.windows
                    * 100,
                    3,
                ),
                "throughput_rows_s": round(stage.windows / elapsed_s, 1),
                "elapsed_s": round(elapsed_s, 3),
                "queue_full": diagnostics["queueFullCount"],
                "dropped_windows": collector.dropped_windows,
                "blocked_puts": collector.blocked_puts,
                "total_put_wait_ms": round(collector.total_put_wait_s * 1000, 2),
                "max_put_wait_ms": round(collector.max_put_wait_s * 1000, 2),
                "max_queue": max_queue,
                "max_outbox": max_outbox,
                "outbox_end": diagnostics["outboxDepth"],
                "p50_send_ms": round(_percentile(latency_ms, 0.50), 2),
                "p95_send_ms": round(_percentile(latency_ms, 0.95), 2),
                "p99_send_ms": round(_percentile(latency_ms, 0.99), 2),
                "idle": idle,
                "quick_check": blackbox.quick_check(),
            }
            runtime.stop()
            return result


def _trigger_flush(runtime: EdgeRuntime) -> None:
    flush = getattr(runtime, "flush", None) or getattr(runtime, "trigger_flush")
    flush()


def _wait_for_idle(runtime: EdgeRuntime, timeout: float) -> bool:
    wait_for_idle = getattr(runtime, "wait_for_idle", None)
    if wait_for_idle is not None:
        return bool(wait_for_idle(timeout=timeout))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if runtime.blackbox.count == 0:
            return True
        _trigger_flush(runtime)
        time.sleep(0.005)
    return runtime.blackbox.count == 0


def main() -> None:
    stages = [
        Stage("01-normal", 100, 0.001, 0.0, 0.0, 30),
        Stage("02-spooler-pressure", 500, 0.0, 0.005, 0.0, 30),
        Stage("03-sender-pressure", 500, 0.0, 0.0, 0.005, 30),
        Stage("04-small-queue-heavy", 500, 0.0, 0.008, 0.008, 5),
        Stage("05-network-recovery", 200, 0.0, 0.0, 0.0, 30, False, True),
    ]
    results = [run_stage(stage) for stage in stages]
    print(json.dumps(results, ensure_ascii=False, indent=2))
    print("\n| 단계 | 요청 | 보존 | 전송 | 유실률 | 버린 윈도우 | 처리량 | put 대기 | 최대 큐 | 최대 outbox | p99 전송지연 |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for result in results:
        print(
            f"| {result['stage']} | {result['requested']} | {result['spooled']} | "
            f"{result['sent']} | {result['loss_pct']}% | {result['dropped_windows']} | "
            f"{result['throughput_rows_s']} rows/s | {result['blocked_puts']}회 | "
            f"{result['max_queue']} | {result['max_outbox']} | "
            f"{result['p99_send_ms']} ms |"
        )


if __name__ == "__main__":
    main()
