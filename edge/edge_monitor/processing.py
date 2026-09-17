"""100 Hz sample collection primitives and one-second aggregation."""

from __future__ import annotations

import math
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterator, Sequence


EXPECTED_WINDOW_SAMPLE_COUNT = 100


class IncompleteWindowError(ValueError):
    """Raised when a one-second window does not contain 100 paired samples."""

    def __init__(
        self,
        *,
        expected_count: int,
        actual_count: int,
        failure_reason: str,
    ) -> None:
        self.expected_count = expected_count
        self.actual_count = actual_count
        self.failure_reason = failure_reason
        super().__init__(
            f"incomplete sensor window: expected {expected_count} paired samples, "
            f"got {actual_count} ({failure_reason})"
        )


@dataclass(frozen=True)
class SensorSample:
    """One calibrated sensor reading; acceleration is dynamic acceleration."""

    tilt_x: float
    tilt_y: float
    acceleration_x: float
    acceleration_y: float
    acceleration_z: float
    raw_acceleration: tuple[int, int, int] | None = None
    calibrated_acceleration: tuple[float, float, float] | None = None

    @property
    def acceleration_magnitude(self) -> float:
        return math.sqrt(
            self.acceleration_x**2
            + self.acceleration_y**2
            + self.acceleration_z**2
        )


@dataclass(frozen=True)
class PairedSample:
    measured_at: datetime
    top: SensorSample
    bottom: SensorSample


@dataclass(frozen=True)
class SensorSummary:
    tilt_x: float
    tilt_y: float
    vibration_rms: float
    peak_acceleration: float


@dataclass(frozen=True)
class WindowSummary:
    measured_at: datetime
    sample_count: int
    top: SensorSummary
    bottom: SensorSummary


def _summarize(samples: Sequence[SensorSample]) -> SensorSummary:
    if not samples:
        raise ValueError("cannot aggregate an empty sensor window")
    magnitudes = [sample.acceleration_magnitude for sample in samples]
    return SensorSummary(
        tilt_x=sum(sample.tilt_x for sample in samples) / len(samples),
        tilt_y=sum(sample.tilt_y for sample in samples) / len(samples),
        vibration_rms=math.sqrt(sum(value**2 for value in magnitudes) / len(samples)),
        peak_acceleration=max(magnitudes),
    )


def aggregate_window(
    samples: Sequence[PairedSample],
    *,
    expected_count: int = EXPECTED_WINDOW_SAMPLE_COUNT,
) -> WindowSummary:
    """Aggregate one complete, paired raw-sample window."""

    if len(samples) != expected_count:
        raise IncompleteWindowError(
            expected_count=expected_count,
            actual_count=len(samples),
            failure_reason="sample_count_mismatch",
        )
    return WindowSummary(
        measured_at=samples[-1].measured_at,
        sample_count=len(samples),
        top=_summarize([sample.top for sample in samples]),
        bottom=_summarize([sample.bottom for sample in samples]),
    )


class FixedRateCollector:
    """Read paired sensors at a monotonic fixed rate and emit complete windows."""

    def __init__(
        self,
        reader: Callable[[], tuple[SensorSample, SensorSample]],
        *,
        sample_rate_hz: int = 100,
        telemetry_rate_hz: int = 1,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        on_sample: Callable[[PairedSample], None] | None = None,
    ) -> None:
        if sample_rate_hz <= 0 or telemetry_rate_hz <= 0:
            raise ValueError("rates must be positive")
        if sample_rate_hz % telemetry_rate_hz:
            raise ValueError("sample rate must be divisible by telemetry rate")
        self.reader = reader
        self.sample_rate_hz = sample_rate_hz
        self.window_size = sample_rate_hz // telemetry_rate_hz
        self.clock = clock
        self.sleeper = sleeper
        self.on_sample = on_sample
        self._max_schedule_lateness_ms = 0.0
        self._sample_count = 0

    @property
    def max_schedule_lateness_ms(self) -> float:
        return self._max_schedule_lateness_ms

    def windows(self, count: int | None = None) -> Iterator[WindowSummary]:
        period = 1.0 / self.sample_rate_hz
        deadline = self.clock()
        produced = 0
        while count is None or produced < count:
            window: list[PairedSample] = []
            for _ in range(self.window_size):
                self._record_lateness(deadline)
                try:
                    top, bottom = self.reader()
                except Exception as exc:
                    raise IncompleteWindowError(
                        expected_count=self.window_size,
                        actual_count=len(window),
                        failure_reason=f"sensor_read_failed:{type(exc).__name__}",
                    ) from exc
                paired = PairedSample(datetime.now(timezone.utc), top, bottom)
                window.append(paired)
                if self.on_sample is not None:
                    self.on_sample(paired)
                deadline += period
                remaining = deadline - self.clock()
                if remaining > 0:
                    self.sleeper(remaining)
                elif remaining < -period:
                    deadline = self.clock()
            yield aggregate_window(window, expected_count=self.window_size)
            produced += 1

    def collect_to_queue(
        self,
        target_queue: queue.Queue,
        *,
        stop_event: threading.Event | None = None,
        count: int | None = None,
        on_overflow: Callable[[int, int], None] | None = None,
    ) -> None:
        """Collect windows through a bounded, lossless handoff queue.

        When the queue is full, apply backpressure until the spooler makes room.
        This keeps memory bounded and avoids silently dropping a completed window.
        """
        period = 1.0 / self.sample_rate_hz
        deadline = self.clock()
        produced = 0
        queue_full_count = 0

        while count is None or produced < count:
            if stop_event is not None and stop_event.is_set():
                break

            window_start = self.clock()
            window: list[PairedSample] = []
            error: IncompleteWindowError | None = None

            for _ in range(self.window_size):
                if stop_event is not None and stop_event.is_set():
                    break
                self._record_lateness(deadline)
                try:
                    top, bottom = self.reader()
                except Exception as exc:
                    error = IncompleteWindowError(
                        expected_count=self.window_size,
                        actual_count=len(window),
                        failure_reason=f"sensor_read_failed:{type(exc).__name__}",
                    )
                    break
                paired = PairedSample(datetime.now(timezone.utc), top, bottom)
                window.append(paired)
                if self.on_sample is not None:
                    self.on_sample(paired)
                deadline += period
                remaining = deadline - self.clock()
                if remaining > 0:
                    self.sleeper(remaining)
                elif remaining < -period:
                    deadline = self.clock()

            collection_ms = (self.clock() - window_start) * 1000.0

            if (
                stop_event is not None
                and stop_event.is_set()
                and len(window) < self.window_size
                and error is None
            ):
                break

            if error is not None:
                item: tuple[WindowSummary | IncompleteWindowError, float] = (
                    error,
                    collection_ms,
                )
            else:
                summary = aggregate_window(window, expected_count=self.window_size)
                item = (summary, collection_ms)

            while True:
                try:
                    target_queue.put(item, timeout=0.1)
                    break
                except queue.Full:
                    queue_full_count += 1
                    if on_overflow is not None:
                        on_overflow(target_queue.qsize(), queue_full_count)
                    if stop_event is not None and stop_event.is_set():
                        raise RuntimeError(
                            "collector stopped while waiting for the spooler queue"
                        )

            produced += 1
            if error is not None:
                deadline = self.clock() + period * self.window_size

    def _record_lateness(self, deadline: float) -> None:
        self._sample_count += 1
        self._max_schedule_lateness_ms = max(
            self._max_schedule_lateness_ms,
            max(0.0, (self.clock() - deadline) * 1000.0),
        )
