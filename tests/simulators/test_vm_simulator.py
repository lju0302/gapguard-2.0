from datetime import datetime, timezone

import pytest
from edge.edge_monitor.config import load_config
from edge.edge_monitor.contracts.hashing import verify_data_hash
from simulators.vm_simulator import (
    default_runtimes,
    deterministic_summary,
    emit_telemetry,
    run_once,
)


def test_default_runtimes_create_eight_stable_sites():
    runtimes = default_runtimes("run-001")

    assert len(runtimes) == 8
    assert runtimes[0].section_id == "site-001-wall-001"
    assert runtimes[-1].generator_id == "generator-008"
    assert all(runtime.test_run_id == "run-001" for runtime in runtimes)


def test_run_once_builds_deterministic_v3_telemetry():
    config = load_config()
    measured_at = datetime(2026, 9, 15, tzinfo=timezone.utc)
    produced_at = datetime(2026, 9, 15, 0, 0, 0, 1000, tzinfo=timezone.utc)
    runtime = default_runtimes("run-001")[0]

    first = run_once(runtime, config, 1, measured_at, produced_at)
    second = run_once(runtime, config, 1, measured_at, produced_at)

    assert first == second
    assert first["schemaVersion"] == 3
    assert first["siteId"] == "site-001"
    assert first["sectionId"] == "site-001-wall-001"
    assert first["deviceId"] == "generator-001"
    assert first["eventId"] == "run-001:site-001-wall-001:1"
    assert first["status"] == "NORMAL"
    assert first["relativeTiltX"] == 0.04
    assert first["relativeTiltY"] == -0.02
    assert verify_data_hash(first)


def test_summary_and_emit_helpers_cover_window_and_hash_contract():
    config = load_config()
    runtime = default_runtimes("run-001")[0]
    measured_at = datetime(2026, 9, 15, tzinfo=timezone.utc)
    produced_at = datetime(2026, 9, 15, 0, 0, 0, 1000, tzinfo=timezone.utc)

    summary = deterministic_summary(runtime, measured_at)
    message = emit_telemetry(runtime, config, 1, measured_at, produced_at)

    assert summary.sample_count == 100
    assert message["relativeTiltX"] == pytest.approx(
        summary.top.tilt_x - summary.bottom.tilt_x
    )
    assert message["relativeTiltY"] == pytest.approx(
        summary.top.tilt_y - summary.bottom.tilt_y
    )
    assert verify_data_hash(message)
