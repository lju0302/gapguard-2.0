"""Pure builders for GAPGUARD Edge V3 telemetry and event messages."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from ..processing import WindowSummary
from .hashing import attach_data_hash


EVENT_TYPES = frozenset(
    {
        "DEVICE_STARTED",
        "DEVICE_STOPPED",
        "CALIBRATION_COMPLETED",
        "CALIBRATION_FAILED",
        "THRESHOLD_EXCEEDED",
        "CONNECTION_LOST",
        "CONNECTION_RECOVERED",
        "LOCAL_BUFFERING_STARTED",
        "LOCAL_BUFFERING_STOPPED",
        "RETRANSMISSION_STARTED",
        "RETRANSMISSION_COMPLETED",
        "RETRANSMISSION_FAILED",
        "SENSOR_ERROR",
    }
)
SEVERITIES = frozenset({"INFO", "WARNING", "DANGER", "ERROR"})
STATUSES = frozenset({"NORMAL", "WARNING", "DANGER"})


def _timestamp(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be a datetime or ISO-8601 string")
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _common_fields(
    config: Any,
    measured_at: datetime | str,
    seq_no: int,
    test_run_id: str,
    stream_id: str,
    produced_at: datetime | str | None,
) -> dict[str, Any]:
    if isinstance(seq_no, bool) or not isinstance(seq_no, int) or seq_no < 0:
        raise ValueError("seq_no must be a non-negative integer")
    if not test_run_id or not stream_id:
        raise ValueError("test_run_id and stream_id must not be empty")

    return {
        "siteId": config.site_id,
        "sectionId": config.section_id,
        "deviceId": config.device_id,
        "sourceType": config.source_type,
        "measuredAt": _timestamp(measured_at),
        "seqNo": seq_no,
        "eventId": f"{test_run_id}:{stream_id}:{seq_no}",
        "streamId": stream_id,
        "schemaVersion": 3,
        "producedAt": _timestamp(produced_at or datetime.now(timezone.utc)),
        "testRunId": test_run_id,
    }


def build_telemetry_message(
    config: Any,
    summary: WindowSummary,
    seq_no: int,
    test_run_id: str,
    stream_id: str,
    produced_at: datetime | str | None = None,
    status: str = "NORMAL",
) -> dict[str, Any]:
    """Build a hashed V3 telemetry message without mutating ``summary``."""

    if status not in STATUSES:
        raise ValueError(f"invalid telemetry status: {status}")

    top = summary.top
    bottom = summary.bottom
    payload = _common_fields(
        config,
        summary.measured_at,
        seq_no,
        test_run_id,
        stream_id,
        produced_at,
    )
    payload.update(
        {
            "sensors": {
                "top": {
                    "i2cAddress": "0x69",
                    "tiltX": top.tilt_x,
                    "tiltY": top.tilt_y,
                    "vibrationRms": top.vibration_rms,
                    "peakAcceleration": top.peak_acceleration,
                },
                "bottom": {
                    "i2cAddress": "0x68",
                    "tiltX": bottom.tilt_x,
                    "tiltY": bottom.tilt_y,
                    "vibrationRms": bottom.vibration_rms,
                    "peakAcceleration": bottom.peak_acceleration,
                },
            },
            # Keep derived values stable across JSON serialization and runtimes.
            "relativeTiltX": round(top.tilt_x - bottom.tilt_x, 6),
            "relativeTiltY": round(top.tilt_y - bottom.tilt_y, 6),
            "status": status,
        }
    )
    return attach_data_hash(payload)


def build_event_message(
    config: Any,
    measured_at: datetime | str,
    seq_no: int,
    test_run_id: str,
    stream_id: str,
    event_type: str,
    severity: str,
    reason: str,
    details: Mapping[str, Any] | None = None,
    produced_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Build a hashed V3 event message without mutating ``details``."""

    if event_type not in EVENT_TYPES:
        raise ValueError(f"invalid event_type: {event_type}")
    if severity not in SEVERITIES:
        raise ValueError(f"invalid severity: {severity}")

    payload = _common_fields(
        config, measured_at, seq_no, test_run_id, stream_id, produced_at
    )
    payload.update(
        {
            "eventType": event_type,
            "severity": severity,
            "reason": reason,
            "details": dict(details) if details is not None else {},
        }
    )
    return attach_data_hash(payload)
