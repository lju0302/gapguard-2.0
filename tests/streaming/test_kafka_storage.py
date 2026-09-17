from pathlib import Path

import pytest

from streaming.consumers.kafka_storage import KafkaTelemetryConsumer


def test_kafka_consumer_requires_transport_dependency_when_constructed(monkeypatch, tmp_path: Path):
    monkeypatch.setitem(__import__("sys").modules, "kafka", None)

    with pytest.raises(RuntimeError, match="requirements-transport"):
        KafkaTelemetryConsumer(storage_path=tmp_path / "telemetry.db")
