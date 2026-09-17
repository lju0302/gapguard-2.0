"""Environment-based configuration for the GAPGUARD Edge service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_CALIBRATION_PATH = (
    Path(__file__).resolve().parents[1] / "calibration" / "rpi-site-a-01.json"
)


def _get_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw, 0)


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class EdgeConfig:
    site_id: str
    section_id: str
    device_id: str
    source_type: str
    azure_iot_hub_connection_string: str
    sample_rate_hz: int
    telemetry_rate_hz: int
    top_sensor_address: int
    bottom_sensor_address: int
    i2c_bus: int
    calibration_path: str
    warning_relative_tilt_deg: float
    danger_relative_tilt_deg: float
    warning_vibration_rms: float
    danger_vibration_rms: float
    warning_peak_acceleration: float
    danger_peak_acceleration: float
    rule_code: str
    rule_version: str
    blackbox_path: str
    replay_batch_size: int
    connection_failure_threshold: int
    connection_recovery_threshold: int
    snapshot_dir: str
    snapshot_max_duration_sec: int
    snapshot_max_local_bytes: int
    snapshot_retention_count: int
    snapshot_blob_sas_url: str
    snapshot_container: str
    monitor_socket_path: str

    def __post_init__(self) -> None:
        if not self.site_id or not self.section_id or not self.device_id:
            raise ValueError("Edge identity values must not be empty")
        if self.source_type not in {"physical", "simulator"}:
            raise ValueError("GAPGUARD_SOURCE_TYPE must be physical or simulator")
        if self.sample_rate_hz != 100:
            raise ValueError("Edge V2 requires GAPGUARD_SAMPLE_RATE_HZ=100")
        if self.telemetry_rate_hz != 1:
            raise ValueError("Edge V2 requires GAPGUARD_TELEMETRY_RATE_HZ=1")
        if self.i2c_bus < 0:
            raise ValueError("GAPGUARD_I2C_BUS must not be negative")
        if self.top_sensor_address != 0x69 or self.bottom_sensor_address != 0x68:
            raise ValueError(
                "Edge V2 fixes the top sensor at 0x69 and the bottom sensor at 0x68"
            )
        if not self.calibration_path:
            raise ValueError("GAPGUARD_CALIBRATION_PATH must not be empty")
        if not self.rule_code or not self.rule_version:
            raise ValueError("Edge rule code and version must not be empty")
        if self.replay_batch_size <= 0:
            raise ValueError("GAPGUARD_REPLAY_BATCH_SIZE must be positive")
        if self.connection_failure_threshold <= 0:
            raise ValueError("GAPGUARD_CONNECTION_FAILURE_THRESHOLD must be positive")
        if self.connection_recovery_threshold <= 0:
            raise ValueError("GAPGUARD_CONNECTION_RECOVERY_THRESHOLD must be positive")
        if self.snapshot_max_duration_sec < 1 or self.snapshot_max_duration_sec > 60:
            raise ValueError("GAPGUARD_SNAPSHOT_MAX_DURATION_SEC must be between 1 and 60")
        if self.snapshot_max_local_bytes <= 0:
            raise ValueError("GAPGUARD_SNAPSHOT_MAX_LOCAL_BYTES must be positive")
        if self.snapshot_retention_count < 0:
            raise ValueError("GAPGUARD_SNAPSHOT_RETENTION_COUNT must not be negative")
        if self.snapshot_blob_sas_url and not self.snapshot_container:
            raise ValueError(
                "GAPGUARD_SNAPSHOT_CONTAINER is required with GAPGUARD_SNAPSHOT_BLOB_SAS_URL"
            )


def load_config() -> EdgeConfig:
    """Load the Edge runtime configuration."""

    device_id = _get_str("GAPGUARD_DEVICE_ID", "edge-pi-001")
    return EdgeConfig(
        site_id=_get_str("GAPGUARD_SITE_ID", "local-site"),
        section_id=_get_str("GAPGUARD_SECTION_ID", "local-section"),
        device_id=device_id,
        source_type=_get_str("GAPGUARD_SOURCE_TYPE", "physical"),
        azure_iot_hub_connection_string=_get_str("AZURE_IOT_HUB_CONNECTION_STRING"),
        sample_rate_hz=_get_int("GAPGUARD_SAMPLE_RATE_HZ", 100),
        telemetry_rate_hz=_get_int("GAPGUARD_TELEMETRY_RATE_HZ", 1),
        top_sensor_address=_get_int("GAPGUARD_TOP_SENSOR_ADDRESS", 0x69),
        bottom_sensor_address=_get_int("GAPGUARD_BOTTOM_SENSOR_ADDRESS", 0x68),
        i2c_bus=_get_int("GAPGUARD_I2C_BUS", 1),
        calibration_path=_get_str(
            "GAPGUARD_CALIBRATION_PATH", str(DEFAULT_CALIBRATION_PATH)
        ),
        warning_relative_tilt_deg=_get_float(
            "GAPGUARD_WARNING_RELATIVE_TILT_DEG", 2.0
        ),
        danger_relative_tilt_deg=_get_float(
            "GAPGUARD_DANGER_RELATIVE_TILT_DEG", 5.0
        ),
        warning_vibration_rms=_get_float("GAPGUARD_WARNING_VIBRATION_RMS", 0.5),
        danger_vibration_rms=_get_float("GAPGUARD_DANGER_VIBRATION_RMS", 1.0),
        warning_peak_acceleration=_get_float(
            "GAPGUARD_WARNING_PEAK_ACCELERATION", 2.0
        ),
        danger_peak_acceleration=_get_float(
            "GAPGUARD_DANGER_PEAK_ACCELERATION", 4.0
        ),
        rule_code=_get_str("GAPGUARD_RULE_CODE", "EDGE_STRUCTURAL_V2"),
        rule_version=_get_str("GAPGUARD_RULE_VERSION", "2.0"),
        blackbox_path=_get_str("GAPGUARD_BLACKBOX_PATH", "gapguard-blackbox.db"),
        replay_batch_size=_get_int("GAPGUARD_REPLAY_BATCH_SIZE", 100),
        connection_failure_threshold=_get_int(
            "GAPGUARD_CONNECTION_FAILURE_THRESHOLD", 3
        ),
        connection_recovery_threshold=_get_int(
            "GAPGUARD_CONNECTION_RECOVERY_THRESHOLD", 2
        ),
        snapshot_dir=_get_str("GAPGUARD_SNAPSHOT_DIR", "gapguard-snapshots"),
        snapshot_max_duration_sec=_get_int("GAPGUARD_SNAPSHOT_MAX_DURATION_SEC", 60),
        snapshot_max_local_bytes=_get_int(
            "GAPGUARD_SNAPSHOT_MAX_LOCAL_BYTES", 1_073_741_824
        ),
        snapshot_retention_count=_get_int("GAPGUARD_SNAPSHOT_RETENTION_COUNT", 10),
        snapshot_blob_sas_url=_get_str("GAPGUARD_SNAPSHOT_BLOB_SAS_URL"),
        snapshot_container=_get_str("GAPGUARD_SNAPSHOT_CONTAINER"),
        monitor_socket_path=_get_str("GAPGUARD_MONITOR_SOCKET_PATH"),
    )
