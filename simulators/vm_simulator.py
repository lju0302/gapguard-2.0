"""Deterministic simulator output for the local GAPGUARD P0 flow."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from edge.edge_monitor.config import EdgeConfig
from edge.edge_monitor.contracts.message_builder import build_telemetry_message
from edge.edge_monitor.processing import SensorSummary, WindowSummary


@dataclass(frozen=True)
class SimulatorRuntime:
    site_id: str
    section_id: str
    generator_id: str
    test_run_id: str


def default_runtimes(test_run_id: str) -> list[SimulatorRuntime]:
    if not test_run_id:
        raise ValueError("test_run_id must not be empty")
    return [
        SimulatorRuntime(
            site_id=f"site-{index:03d}",
            section_id=f"site-{index:03d}-wall-001",
            generator_id=f"generator-{index:03d}",
            test_run_id=test_run_id,
        )
        for index in range(1, 9)
    ]


def deterministic_summary(
    runtime: SimulatorRuntime,
    measured_at: datetime,
) -> WindowSummary:
    """Return one stable NORMAL window for a simulator runtime."""

    if measured_at.tzinfo is None:
        raise ValueError("measured_at must be timezone-aware")
    top = SensorSummary(0.12, -0.04, 0.18, 0.42)
    bottom = SensorSummary(0.08, -0.02, 0.16, 0.37)
    return WindowSummary(measured_at, 100, top, bottom)


def emit_telemetry(
    runtime: SimulatorRuntime,
    config: EdgeConfig,
    seq_no: int,
    measured_at: datetime,
    produced_at: datetime,
) -> dict:
    runtime_config = replace(
        config,
        site_id=runtime.site_id,
        section_id=runtime.section_id,
        device_id=runtime.generator_id,
        source_type="simulator",
    )
    summary = deterministic_summary(runtime, measured_at)
    return build_telemetry_message(
        runtime_config,
        summary,
        seq_no,
        runtime.test_run_id,
        runtime.section_id,
        produced_at,
        status="NORMAL",
    )


def run_once(
    runtime: SimulatorRuntime,
    config: EdgeConfig,
    seq_no: int,
    measured_at: datetime,
    produced_at: datetime,
) -> dict:
    return emit_telemetry(runtime, config, seq_no, measured_at, produced_at)
