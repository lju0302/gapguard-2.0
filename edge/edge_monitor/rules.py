"""Deterministic first-pass Edge status and threshold-event rules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .config import EdgeConfig
from .processing import WindowSummary


STATUS_RANK = {"NORMAL": 0, "WARNING": 1, "DANGER": 2}


@dataclass(frozen=True)
class Thresholds:
    warning_relative_tilt_deg: float
    danger_relative_tilt_deg: float
    warning_vibration_rms: float
    danger_vibration_rms: float
    warning_peak_acceleration: float
    danger_peak_acceleration: float
    rule_code: str = "EDGE_STRUCTURAL_V2"
    rule_version: str = "2.0"

    @classmethod
    def from_config(cls, config: EdgeConfig) -> "Thresholds":
        return cls(
            config.warning_relative_tilt_deg,
            config.danger_relative_tilt_deg,
            config.warning_vibration_rms,
            config.danger_vibration_rms,
            config.warning_peak_acceleration,
            config.danger_peak_acceleration,
            config.rule_code,
            config.rule_version,
        )

    def __post_init__(self) -> None:
        pairs = (
            (self.warning_relative_tilt_deg, self.danger_relative_tilt_deg),
            (self.warning_vibration_rms, self.danger_vibration_rms),
            (self.warning_peak_acceleration, self.danger_peak_acceleration),
        )
        if any(warning < 0 or danger <= warning for warning, danger in pairs):
            raise ValueError(
                "each danger threshold must exceed its non-negative warning threshold"
            )


@dataclass(frozen=True)
class RuleViolation:
    metric_name: str
    metric_value: float
    threshold_value: float
    severity: str


@dataclass(frozen=True)
class RuleDecision:
    status: str
    violations: tuple[RuleViolation, ...]
    emit_threshold_event: bool


class RuleEngine:
    """Evaluate windows and emit incidents only on upward transitions."""

    def __init__(self, thresholds: Thresholds, *, previous_status: str = "NORMAL") -> None:
        if previous_status not in STATUS_RANK:
            raise ValueError("previous_status must be NORMAL, WARNING, or DANGER")
        self.thresholds = thresholds
        self._previous_status = previous_status

    @property
    def previous_status(self) -> str:
        return self._previous_status

    @staticmethod
    def _violation(
        name: str, value: float, warning: float, danger: float
    ) -> RuleViolation | None:
        absolute_value = abs(value)
        if absolute_value >= danger:
            return RuleViolation(name, value, danger, "DANGER")
        if absolute_value >= warning:
            return RuleViolation(name, value, warning, "WARNING")
        return None

    def evaluate(self, summary: WindowSummary) -> RuleDecision:
        relative_x = summary.top.tilt_x - summary.bottom.tilt_x
        relative_y = summary.top.tilt_y - summary.bottom.tilt_y
        candidates: Iterable[RuleViolation | None] = (
            self._violation(
                "relativeTiltX",
                relative_x,
                self.thresholds.warning_relative_tilt_deg,
                self.thresholds.danger_relative_tilt_deg,
            ),
            self._violation(
                "relativeTiltY",
                relative_y,
                self.thresholds.warning_relative_tilt_deg,
                self.thresholds.danger_relative_tilt_deg,
            ),
            self._violation(
                "top.vibrationRms",
                summary.top.vibration_rms,
                self.thresholds.warning_vibration_rms,
                self.thresholds.danger_vibration_rms,
            ),
            self._violation(
                "bottom.vibrationRms",
                summary.bottom.vibration_rms,
                self.thresholds.warning_vibration_rms,
                self.thresholds.danger_vibration_rms,
            ),
            self._violation(
                "top.peakAcceleration",
                summary.top.peak_acceleration,
                self.thresholds.warning_peak_acceleration,
                self.thresholds.danger_peak_acceleration,
            ),
            self._violation(
                "bottom.peakAcceleration",
                summary.bottom.peak_acceleration,
                self.thresholds.warning_peak_acceleration,
                self.thresholds.danger_peak_acceleration,
            ),
        )
        violations = tuple(item for item in candidates if item is not None)
        status = max(
            (item.severity for item in violations),
            key=STATUS_RANK.get,
            default="NORMAL",
        )
        emit = STATUS_RANK[status] > STATUS_RANK[self._previous_status]
        self._previous_status = status
        return RuleDecision(status, violations, emit)
