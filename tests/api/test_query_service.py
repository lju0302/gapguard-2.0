from datetime import datetime, timezone

from api.query_service import QueryService
from edge.edge_monitor.config import load_config
from edge.edge_monitor.contracts.message_builder import build_event_message
from simulators.vm_simulator import default_runtimes, run_once
from streaming.consumers.alert_dispatch import SQLiteAlertStore
from streaming.consumers.telemetry_storage import SQLiteTelemetryStore


def test_query_service_reads_and_filters_stored_payloads(tmp_path):
    path = tmp_path / "gapguard.db"
    runtime = default_runtimes("query-run")[0]
    now = datetime(2026, 9, 16, tzinfo=timezone.utc)
    telemetry = run_once(runtime, load_config(), 1, now, now)
    alert = build_event_message(
        load_config(), now, 2, "query-run", runtime.section_id,
        "THRESHOLD_EXCEEDED", "WARNING", "test", produced_at=now,
    )
    with SQLiteTelemetryStore(path) as store:
        store.insert(telemetry)
    with SQLiteAlertStore(path) as store:
        store.insert(alert)

    with QueryService(path) as service:
        assert service.list_telemetry(site_id="site-001")[0]["eventId"] == telemetry["eventId"]
        assert service.list_alerts(site_id="site-999") == []
        assert service.summary() == {"telemetry_count": 1, "alert_count": 1}
