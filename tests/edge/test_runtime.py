import queue
import threading
import time
from dataclasses import replace

import pytest

from edge.edge_monitor.config import load_config
from edge.edge_monitor.processing import FixedRateCollector, SensorSample
from edge.edge_monitor.runtime import EdgeRuntime
from edge.edge_monitor.storage.blackbox import SQLiteBlackbox
from streaming.bridge.in_memory import InMemoryBridge
from streaming.consumers.telemetry_storage import SQLiteTelemetryStore, TelemetryStorageConsumer


def _config(tmp_path):
    return replace(
        load_config(),
        source_type="simulator",
        blackbox_path=str(tmp_path / "blackbox.db"),
    )


def _collector(*, tilt_x=0.1):
    now = [0.0]

    def clock():
        now[0] += 0.01
        return now[0]

    def reader():
        top = SensorSample(tilt_x, 0.0, 0.1, 0.1, 0.1)
        bottom = SensorSample(0.0, 0.0, 0.1, 0.1, 0.1)
        return top, bottom

    return FixedRateCollector(
        reader,
        sample_rate_hz=100,
        telemetry_rate_hz=1,
        clock=clock,
        sleeper=lambda _: None,
    )


class RecordingSender:
    def __init__(self, *, online=True, delay=0.0):
        self.online = online
        self.delay = delay
        self.sent = []

    def send_json(self, message_type, payload, properties=None):
        del properties
        if self.delay:
            time.sleep(self.delay)
        if not self.online:
            raise ConnectionError("offline")
        self.sent.append((message_type, dict(payload)))


class BridgeSender:
    def __init__(self, bridge):
        self.bridge = bridge

    def send_json(self, message_type, payload, properties=None):
        del message_type, properties
        self.bridge.publish(payload)


def test_three_workers_keep_collection_independent_from_sender(tmp_path):
    sender = RecordingSender(delay=0.01)
    with SQLiteBlackbox(tmp_path / "blackbox.db") as blackbox:
        runtime = EdgeRuntime(
            _config(tmp_path),
            _collector(),
            blackbox=blackbox,
            transport=sender,
        )
        started = time.monotonic()
        runtime.start(window_count=3)
        runtime.join_collector()
        collection_elapsed = time.monotonic() - started

        assert collection_elapsed < 1.0
        runtime.join_spooler()
        assert runtime.wait_for_idle()
        assert [item[1]["seqNo"] for item in sender.sent] == [1, 2, 3]
        assert runtime.get_diagnostics()["outboxDepth"] == 0
        runtime.stop()


def test_network_failure_buffers_and_replays_fifo(tmp_path):
    sender = RecordingSender(online=False)
    with SQLiteBlackbox(tmp_path / "blackbox.db") as blackbox:
        runtime = EdgeRuntime(
            _config(tmp_path),
            _collector(),
            blackbox=blackbox,
            transport=sender,
        )
        runtime.start(window_count=3)
        runtime.join_spooler()
        pending = blackbox.pending()
        assert [item.payload["seqNo"] for item in pending if item.message_type == "telemetry"] == [1, 2, 3]

        sender.online = True
        runtime.flush()
        assert runtime.wait_for_idle()
        assert [item[1]["seqNo"] for item in sender.sent if item[0] == "telemetry"] == [1, 2, 3]
        runtime.stop()


def test_threshold_event_follows_telemetry_in_outbox(tmp_path):
    with SQLiteBlackbox(tmp_path / "blackbox.db") as blackbox:
        runtime = EdgeRuntime(
            replace(_config(tmp_path), warning_relative_tilt_deg=2.0),
            _collector(tilt_x=3.0),
            blackbox=blackbox,
            transport=None,
        )
        runtime.start(window_count=1)
        runtime.join_spooler()

        pending = blackbox.pending()
        assert [item.message_type for item in pending] == ["telemetry", "event"]
        assert pending[0].payload["seqNo"] == 1
        assert pending[1].payload["seqNo"] == 2
        assert pending[1].payload["details"]["relatedSeqNo"] == 1
        runtime.stop()


def test_restart_replays_existing_outbox(tmp_path):
    path = tmp_path / "blackbox.db"
    config = _config(tmp_path)
    with SQLiteBlackbox(path) as blackbox:
        first = EdgeRuntime(
            config,
            _collector(),
            blackbox=blackbox,
            transport=RecordingSender(online=False),
        )
        first.start(window_count=2)
        first.join_spooler()
        assert blackbox.count == 2
        first.stop()

    sender = RecordingSender()
    with SQLiteBlackbox(path) as blackbox:
        second = EdgeRuntime(
            config,
            _collector(),
            blackbox=blackbox,
            transport=sender,
        )
        second.start(window_count=0)
        second.join_spooler()
        second.flush()
        assert second.wait_for_idle()
        assert [item[1]["seqNo"] for item in sender.sent] == [1, 2]
        second.stop()


def test_worker_failure_is_exposed(tmp_path):
    class FailingCollector:
        max_schedule_lateness_ms = 0.0

        def collect_to_queue(self, *args, **kwargs):
            raise RuntimeError("collector_boom")

    runtime = EdgeRuntime(_config(tmp_path), FailingCollector(), transport=None)
    runtime.start(window_count=1)
    runtime.join_collector()
    with pytest.raises(RuntimeError, match="collector worker failed: collector_boom"):
        runtime.raise_if_worker_failed()
    runtime.close()


def test_three_workers_feed_the_existing_bridge_and_storage(tmp_path):
    bridge = InMemoryBridge()
    with SQLiteBlackbox(tmp_path / "blackbox.db") as blackbox:
        runtime = EdgeRuntime(
            _config(tmp_path),
            _collector(),
            blackbox=blackbox,
            transport=BridgeSender(bridge),
        )
        runtime.start(window_count=1)
        runtime.join_spooler()
        assert runtime.wait_for_idle()
        runtime.stop()

    topic = bridge.topics["gapguard.telemetry.v1"]
    with SQLiteTelemetryStore(tmp_path / "telemetry.db") as store:
        result = TelemetryStorageConsumer(store).consume(topic)
        assert result.stored_count == 1
        assert store.count == 1


def test_full_summary_queue_waits_instead_of_dropping_completed_window():
    target_queue = queue.Queue(maxsize=1)
    overflow_seen = threading.Event()

    def reader():
        sample = SensorSample(0.1, 0.0, 0.1, 0.1, 0.1)
        return sample, sample

    collector = FixedRateCollector(
        reader,
        sample_rate_hz=100,
        telemetry_rate_hz=100,
        clock=lambda: 0.0,
        sleeper=lambda _: None,
    )

    worker = threading.Thread(
        target=collector.collect_to_queue,
        args=(target_queue,),
        kwargs={
            "count": 2,
            "on_overflow": lambda *_: overflow_seen.set(),
        },
    )
    worker.start()
    assert overflow_seen.wait(timeout=1.0)
    first = target_queue.get(timeout=1.0)
    target_queue.task_done()
    worker.join(timeout=1.0)

    assert not worker.is_alive()
    second = target_queue.get(timeout=1.0)
    assert first[0].measured_at <= second[0].measured_at


def test_slow_spooler_preserves_all_windows_with_bounded_queue(tmp_path):
    class SlowBlackbox(SQLiteBlackbox):
        def enqueue_many(self, messages):
            time.sleep(0.1)
            return super().enqueue_many(messages)

    with SlowBlackbox(tmp_path / "blackbox.db") as blackbox:
        runtime = EdgeRuntime(
            _config(tmp_path),
            _collector(),
            blackbox=blackbox,
            transport=None,
            queue_maxsize=1,
        )
        runtime.start(window_count=5)
        runtime.join_collector()
        runtime.join_spooler()

        assert blackbox.count == 5
        assert [item.payload["seqNo"] for item in blackbox.pending()] == [1, 2, 3, 4, 5]
        assert runtime.get_diagnostics()["queueFullCount"] > 0
        runtime.stop()
