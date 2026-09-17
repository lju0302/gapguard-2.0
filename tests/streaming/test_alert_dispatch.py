from datetime import datetime, timezone

from edge.edge_monitor.config import load_config
from edge.edge_monitor.contracts.message_builder import build_event_message
from streaming.consumers.alert_dispatch import SQLiteAlertStore


def _event():
    return build_event_message(
        load_config(),
        datetime(2026, 9, 16, tzinfo=timezone.utc),
        1,
        "run-001",
        "site-001-wall-001",
        "THRESHOLD_EXCEEDED",
        "WARNING",
        "tilt threshold exceeded",
        produced_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )


def test_alert_store_is_idempotent_by_event_id():
    with SQLiteAlertStore() as store:
        assert store.insert(_event()) is True
        assert store.insert(_event()) is False
        assert store.count == 1


def test_alert_store_rejects_telemetry():
    message = _event()
    message.pop("eventType")
    with SQLiteAlertStore() as store:
        try:
            store.insert(message)
        except ValueError as exc:
            assert str(exc) == "alert store requires an event message"
        else:
            raise AssertionError("telemetry must be rejected")
