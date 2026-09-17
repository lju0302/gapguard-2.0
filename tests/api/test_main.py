from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.main import create_app
from edge.edge_monitor.config import load_config
from edge.edge_monitor.contracts.message_builder import build_event_message
from simulators.vm_simulator import default_runtimes, run_once
from streaming.consumers.alert_dispatch import SQLiteAlertStore
from streaming.consumers.telemetry_storage import SQLiteTelemetryStore


def test_query_api_exposes_health_and_filtered_data(tmp_path):
    db_path = tmp_path / "gapguard.db"
    runtime = default_runtimes("api-run")[0]
    measured_at = datetime(2026, 9, 16, tzinfo=timezone.utc)
    telemetry = run_once(runtime, load_config(), 1, measured_at, measured_at)
    alert = build_event_message(
        load_config(), measured_at, 2, "api-run", runtime.section_id,
        "THRESHOLD_EXCEEDED", "WARNING", "test", produced_at=measured_at,
    )
    with SQLiteTelemetryStore(db_path) as store:
        store.insert(telemetry)
    with SQLiteAlertStore(db_path) as store:
        store.insert(alert)

    client = TestClient(create_app(db_path))
    assert client.get("/health").json() == {"status": "ok"}
    assert len(client.get("/api/telemetry", params={"site_id": "site-001"}).json()) == 1
    assert len(client.get("/api/alerts", params={"site_id": "local-site"}).json()) == 1
    assert client.get("/api/summary").json() == {"telemetry_count": 1, "alert_count": 1}
