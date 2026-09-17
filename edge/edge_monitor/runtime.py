"""Three-worker Edge runtime: collect, decide/spool, then send."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import queue
import threading
import time
from typing import Any, Callable, Mapping, Protocol

from .config import EdgeConfig
from .pipeline import EdgePipeline, PipelineOutput
from .processing import FixedRateCollector, IncompleteWindowError, WindowSummary
from .rules import RuleEngine, Thresholds
from .storage.blackbox import SQLiteBlackbox

LOGGER = logging.getLogger(__name__)


class JsonSender(Protocol):
    def send_json(self, message_type: str, payload: Mapping[str, Any], properties: Any = None) -> object: ...


@dataclass(frozen=True)
class ConnectionTransition:
    event_type: str
    severity: str
    reason: str


class ConnectionHealthMonitor:
    def __init__(
        self,
        *,
        initially_connected: bool = True,
        failure_threshold: int = 3,
        recovery_threshold: int = 2,
    ) -> None:
        if failure_threshold <= 0 or recovery_threshold <= 0:
            raise ValueError("connection thresholds must be positive")
        self.is_connected = initially_connected
        self.failure_threshold = failure_threshold
        self.recovery_threshold = recovery_threshold
        self._failures = 0
        self._successes = 0

    def observe(self, success: bool) -> ConnectionTransition | None:
        if success:
            self._failures = 0
            self._successes += 1
            if not self.is_connected and self._successes >= self.recovery_threshold:
                self.is_connected = True
                self._successes = 0
                return ConnectionTransition(
                    "CONNECTION_RECOVERED", "INFO", "edge_transport_recovered"
                )
            return None

        self._successes = 0
        self._failures += 1
        if self.is_connected and self._failures >= self.failure_threshold:
            self.is_connected = False
            self._failures = 0
            return ConnectionTransition(
                "CONNECTION_LOST", "ERROR", "edge_transport_unavailable"
            )
        return None


@dataclass
class RuntimeMetrics:
    queue_depth: int = 0
    queue_full_count: int = 0
    outbox_depth: int = 0
    collection_ms: float = 0.0
    spool_ms: float = 0.0
    delivery_ms: float = 0.0
    freshness_ms: float = 0.0


class EdgeRuntime:
    """Keep sensor timing independent from rules, SQLite, and transport."""

    def __init__(
        self,
        config: EdgeConfig,
        collector: FixedRateCollector,
        *,
        blackbox: SQLiteBlackbox | None = None,
        transport: JsonSender | None = None,
        health_monitor: ConnectionHealthMonitor | None = None,
        test_run_id: str = "edge-live",
        stream_id: str | None = None,
        queue_maxsize: int = 30,
        output_callback: Callable[[PipelineOutput], None] | None = None,
    ) -> None:
        if queue_maxsize <= 0:
            raise ValueError("queue_maxsize must be positive")
        self.config = config
        self.collector = collector
        self.blackbox = blackbox or SQLiteBlackbox(config.blackbox_path)
        self._owns_blackbox = blackbox is None
        self.transport = transport
        self.output_callback = output_callback
        self.summary_queue: queue.Queue[tuple[WindowSummary | IncompleteWindowError, float] | None] = queue.Queue(maxsize=queue_maxsize)
        self.transition_queue: queue.Queue[tuple[ConnectionTransition, str]] = queue.Queue()
        self.stop_collector_event = threading.Event()
        self.stop_spooler_event = threading.Event()
        self.stop_sender_event = threading.Event()
        self.drain_event = threading.Event()
        self.metrics = RuntimeMetrics()
        self._metrics_lock = threading.Lock()
        self._worker_failure: RuntimeError | None = None
        self._worker_failure_lock = threading.Lock()
        self._is_running = False
        self.health_monitor = health_monitor or ConnectionHealthMonitor(
            initially_connected=self.blackbox.count == 0,
            failure_threshold=config.connection_failure_threshold,
            recovery_threshold=config.connection_recovery_threshold,
        )
        self.pipeline = EdgePipeline(
            config,
            RuleEngine(Thresholds.from_config(config)),
            test_run_id=test_run_id,
            stream_id=stream_id,
            sequence_provider=lambda: self.blackbox.next_sequence(config.device_id),
        )
        self._collector_thread: threading.Thread | None = None
        self._spooler_thread: threading.Thread | None = None
        self._sender_thread: threading.Thread | None = None

    def _record_worker_failure(self, worker_name: str, exc: Exception) -> None:
        failure = RuntimeError(f"{worker_name} worker failed: {exc}")
        with self._worker_failure_lock:
            if self._worker_failure is None:
                self._worker_failure = failure
        LOGGER.exception("%s", failure)
        self.stop_collector_event.set()
        self.stop_spooler_event.set()
        self.stop_sender_event.set()
        self.drain_event.set()

    def raise_if_worker_failed(self) -> None:
        with self._worker_failure_lock:
            if self._worker_failure is not None:
                raise self._worker_failure

    def _run_worker(self, name: str, target: Callable[..., None], *args: Any) -> None:
        try:
            target(*args)
        except Exception as exc:
            self._record_worker_failure(name, exc)

    def _update_metrics(self, **values: float | int) -> None:
        with self._metrics_lock:
            for key, value in values.items():
                setattr(self.metrics, key, value)
            self.metrics.queue_depth = self.summary_queue.qsize()
            self.metrics.outbox_depth = self.blackbox.count

    def get_diagnostics(self) -> dict[str, Any]:
        with self._metrics_lock:
            return {
                "queueDepth": self.summary_queue.qsize(),
                "queueFullCount": self.metrics.queue_full_count,
                "outboxDepth": self.blackbox.count,
                "collectionMs": round(self.metrics.collection_ms, 2),
                "spoolMs": round(self.metrics.spool_ms, 2),
                "deliveryMs": round(self.metrics.delivery_ms, 2),
                "freshnessMs": round(self.metrics.freshness_ms, 2),
                "maxScheduleLatenessMs": round(self.collector.max_schedule_lateness_ms, 2),
                "connected": self.health_monitor.is_connected,
            }

    def _on_queue_overflow(self, queue_depth: int, queue_full_count: int) -> None:
        self._update_metrics(queue_depth=queue_depth, queue_full_count=queue_full_count)

    def _collector_worker(self, window_count: int | None) -> None:
        try:
            self.collector.collect_to_queue(
                self.summary_queue,
                stop_event=self.stop_collector_event,
                count=window_count,
                on_overflow=self._on_queue_overflow,
            )
        finally:
            while not self.stop_spooler_event.is_set():
                try:
                    self.summary_queue.put(None, timeout=0.1)
                    break
                except queue.Full:
                    continue

    def _enqueue_transition(self, transition: ConnectionTransition) -> None:
        self.transition_queue.put((transition, datetime.now(timezone.utc).isoformat()))

    def _drain_transitions(self) -> None:
        while True:
            try:
                transition, measured_at = self.transition_queue.get_nowait()
            except queue.Empty:
                return
            event = self.pipeline.build_connection_event(
                transition, measured_at=measured_at
            )
            self.blackbox.enqueue("event", event)
            self.drain_event.set()
            self.transition_queue.task_done()

    def _spooler_worker(self) -> None:
        while not self.stop_spooler_event.is_set() or not self.summary_queue.empty():
            self._drain_transitions()
            try:
                item = self.summary_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if item is None:
                self.summary_queue.task_done()
                break

            started = time.monotonic()
            raw_summary, collection_ms = item
            freshness_ms = 0.0
            if isinstance(raw_summary, IncompleteWindowError):
                event = self.pipeline.build_sensor_error(
                    raw_summary, measured_at=datetime.now(timezone.utc)
                )
                self.blackbox.enqueue("event", event)
                self.drain_event.set()
            else:
                output = self.pipeline.process(raw_summary)
                freshness_ms = max(
                    0.0,
                    (datetime.now(timezone.utc) - raw_summary.measured_at).total_seconds() * 1000.0,
                )
                messages: list[tuple[str, Mapping[str, Any]]] = [("telemetry", output.telemetry)]
                messages.extend(("event", event) for event in output.events)
                self.blackbox.enqueue_many(messages)
                self.drain_event.set()
                if self.output_callback is not None:
                    self.output_callback(output)
            self._update_metrics(
                collection_ms=collection_ms,
                spool_ms=(time.monotonic() - started) * 1000.0,
                freshness_ms=freshness_ms,
            )
            self.summary_queue.task_done()

    def _sender_worker(self) -> None:
        if self.transport is None:
            return
        backoff = 1.0
        while not self.stop_sender_event.is_set():
            self.drain_event.wait(timeout=backoff)
            self.drain_event.clear()
            if self.stop_sender_event.is_set():
                break
            while self.blackbox.count and not self.stop_sender_event.is_set():
                started = time.monotonic()
                failed = False
                for message in self.blackbox.pending(self.config.replay_batch_size):
                    try:
                        self.transport.send_json(message.message_type, message.payload)
                    except Exception as exc:
                        self.blackbox.record_failure(message.row_id, str(exc))
                        transition = self.health_monitor.observe(False)
                        if transition is not None:
                            self._enqueue_transition(transition)
                        backoff = min(30.0, backoff * 2.0)
                        failed = True
                        break
                    self.blackbox.acknowledge(message.row_id)
                    transition = self.health_monitor.observe(True)
                    if transition is not None:
                        self._enqueue_transition(transition)
                    backoff = 1.0
                self._update_metrics(delivery_ms=(time.monotonic() - started) * 1000.0)
                if failed:
                    break

    def start(self, window_count: int | None = None) -> None:
        if self._is_running:
            return
        self._is_running = True
        self.stop_collector_event.clear()
        self.stop_spooler_event.clear()
        self.stop_sender_event.clear()
        if self.transport is not None:
            self._sender_thread = threading.Thread(
                target=self._run_worker, args=("sender", self._sender_worker), name="SenderWorker", daemon=True
            )
            self._sender_thread.start()
            if self.blackbox.count:
                self.drain_event.set()
        self._spooler_thread = threading.Thread(
            target=self._run_worker, args=("spooler", self._spooler_worker), name="SpoolerWorker", daemon=True
        )
        self._spooler_thread.start()
        self._collector_thread = threading.Thread(
            target=self._run_worker, args=("collector", self._collector_worker, window_count), name="CollectorWorker", daemon=True
        )
        self._collector_thread.start()

    def flush(self) -> int:
        self.drain_event.set()
        return self.blackbox.count

    def wait_for_idle(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.blackbox.count == 0:
                return True
            self.drain_event.set()
            time.sleep(0.005)
        return self.blackbox.count == 0

    def join_collector(self) -> None:
        if self._collector_thread is not None:
            self._collector_thread.join(timeout=5.0)

    def join_spooler(self) -> None:
        if self._spooler_thread is not None:
            self._spooler_thread.join(timeout=5.0)

    def stop(self) -> None:
        if not self._is_running:
            return
        self.stop_collector_event.set()
        self.join_collector()
        self.stop_spooler_event.set()
        self.join_spooler()
        self.stop_sender_event.set()
        self.drain_event.set()
        if self._sender_thread is not None:
            self._sender_thread.join(timeout=2.0)
        self._is_running = False

    def close(self) -> None:
        self.stop()
        if self._owns_blackbox:
            self.blackbox.close()

    def build_connection_event(self, transition: ConnectionTransition, measured_at: str) -> dict[str, Any]:
        return self.pipeline.build_connection_event(transition, measured_at=measured_at)
