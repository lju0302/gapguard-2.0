from datetime import datetime, timezone

from edge.edge_monitor.config import load_config
from simulators.vm_simulator import default_runtimes, run_once
from streaming.bridge.live import prepare_publish


def test_prepare_publish_routes_full_v3_telemetry():
    message = run_once(
        default_runtimes("run-001")[0],
        load_config(),
        1,
        datetime(2026, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 9, 15, 0, 0, 0, 1000, tzinfo=timezone.utc),
    )

    topic, key, value = prepare_publish(message)

    assert topic == "gapguard.telemetry.v1"
    assert key == "site-001-wall-001"
    assert value == message
