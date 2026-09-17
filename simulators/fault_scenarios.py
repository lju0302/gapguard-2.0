"""Deterministic Runtime fault and recovery events for local P0 tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from edge.edge_monitor.config import EdgeConfig
from edge.edge_monitor.contracts.message_builder import build_event_message
from .vm_simulator import SimulatorRuntime


def connection_loss_scenario(
    runtime: SimulatorRuntime,
    config: EdgeConfig,
    lost_at: datetime,
    recovered_at: datetime,
) -> tuple[dict, dict]:
    if lost_at.tzinfo is None or recovered_at.tzinfo is None:
        raise ValueError("fault timestamps must be timezone-aware")
    runtime_config = replace(
        config,
        site_id=runtime.site_id,
        section_id=runtime.section_id,
        device_id=runtime.generator_id,
        source_type="simulator",
    )
    lost = build_event_message(
        runtime_config,
        lost_at,
        1,
        runtime.test_run_id,
        runtime.section_id,
        "CONNECTION_LOST",
        "ERROR",
        "simulated network fault",
        produced_at=lost_at,
    )
    recovered = build_event_message(
        runtime_config,
        recovered_at,
        2,
        runtime.test_run_id,
        runtime.section_id,
        "CONNECTION_RECOVERED",
        "INFO",
        "simulated network recovery",
        produced_at=recovered_at,
    )
    return lost, recovered
