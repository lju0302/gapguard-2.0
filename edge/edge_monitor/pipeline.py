"""Turn one-second summaries into hashed GAPGUARD V3 messages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import threading
from typing import Any, Callable

from .config import EdgeConfig
from .contracts.message_builder import build_event_message, build_telemetry_message
from .processing import IncompleteWindowError, WindowSummary
from .rules import RuleDecision, RuleEngine


@dataclass(frozen=True)
class PipelineOutput:
    telemetry: dict[str, Any]
    events: tuple[dict[str, Any], ...]
    decision: RuleDecision


class EdgePipeline:
    """Own rule evaluation and the device-wide V3 sequence."""

    def __init__(
        self,
        config: EdgeConfig,
        rule_engine: RuleEngine,
        *,
        test_run_id: str = "edge-live",
        stream_id: str | None = None,
        initial_seq_no: int = 0,
        sequence_provider: Callable[[], int] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not test_run_id:
            raise ValueError("test_run_id must not be empty")
        self.config = config
        self.rule_engine = rule_engine
        self.test_run_id = test_run_id
        self.stream_id = stream_id or config.section_id
        self._seq_no = initial_seq_no
        self._sequence_provider = sequence_provider
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._latest_state: dict[str, Any] = {
            "status": "INITIALIZING",
            "lastMeasuredAt": None,
            "relativeTiltX": 0.0,
            "relativeTiltY": 0.0,
        }

    @property
    def latest_state(self) -> dict[str, Any]:
        with self._state_lock:
            return dict(self._latest_state)

    def _next_sequence(self) -> int:
        if self._sequence_provider is not None:
            return self._sequence_provider()
        self._seq_no += 1
        return self._seq_no

    def process(self, summary: WindowSummary) -> PipelineOutput:
        with self._lock:
            decision = self.rule_engine.evaluate(summary)
            measured_at = summary.measured_at
            produced_at = self._clock()
            telemetry_seq_no = self._next_sequence()
            telemetry = build_telemetry_message(
                self.config,
                summary,
                telemetry_seq_no,
                self.test_run_id,
                self.stream_id,
                produced_at,
                status=decision.status,
            )
            events: list[dict[str, Any]] = []
            if decision.emit_threshold_event:
                events.append(
                    build_event_message(
                        self.config,
                        measured_at,
                        self._next_sequence(),
                        self.test_run_id,
                        self.stream_id,
                        "THRESHOLD_EXCEEDED",
                        decision.status,
                        "edge_threshold_exceeded",
                        {
                            "relatedSeqNo": telemetry_seq_no,
                            "ruleCode": self.rule_engine.thresholds.rule_code,
                            "ruleVersion": self.rule_engine.thresholds.rule_version,
                            "violations": [
                                {
                                    "metricName": violation.metric_name,
                                    "metricValue": violation.metric_value,
                                    "thresholdValue": violation.threshold_value,
                                    "severity": violation.severity,
                                }
                                for violation in decision.violations
                            ],
                        },
                        produced_at,
                    )
                )

            with self._state_lock:
                self._latest_state = {
                    "status": decision.status,
                    "lastMeasuredAt": measured_at.isoformat(),
                    "relativeTiltX": summary.top.tilt_x - summary.bottom.tilt_x,
                    "relativeTiltY": summary.top.tilt_y - summary.bottom.tilt_y,
                }
            return PipelineOutput(telemetry, tuple(events), decision)

    def build_sensor_error(
        self, error: IncompleteWindowError, *, measured_at: datetime | str
    ) -> dict[str, Any]:
        return build_event_message(
            self.config,
            measured_at,
            self._next_sequence(),
            self.test_run_id,
            self.stream_id,
            "SENSOR_ERROR",
            "ERROR",
            "incomplete_sensor_window",
            {
                "expectedSampleCount": error.expected_count,
                "actualSampleCount": error.actual_count,
                "interpolatedSampleCount": 0,
                "failureReason": error.failure_reason,
            },
            self._clock(),
        )

    def build_connection_event(
        self, transition: Any, *, measured_at: datetime | str
    ) -> dict[str, Any]:
        return build_event_message(
            self.config,
            measured_at,
            self._next_sequence(),
            self.test_run_id,
            self.stream_id,
            transition.event_type,
            transition.severity,
            transition.reason,
            {},
            self._clock(),
        )
