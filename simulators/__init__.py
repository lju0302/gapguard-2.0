"""Deterministic local simulator primitives for GAPGUARD P0."""

from .vm_simulator import (
    SimulatorRuntime,
    default_runtimes,
    deterministic_summary,
    emit_telemetry,
    run_once,
)

__all__ = [
    "SimulatorRuntime",
    "default_runtimes",
    "deterministic_summary",
    "emit_telemetry",
    "run_once",
]
