"""Kafka adapter that commits offsets only after SQLite storage succeeds."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from streaming.common.retry import process_with_retry
from streaming.consumers.kafka_dlq import KafkaDlqPublisher, build_dlq_metadata
from .telemetry_storage import SQLiteTelemetryStore, TelemetryStorageConsumer


class KafkaTelemetryConsumer:
    def __init__(
        self,
        bootstrap_servers: str | list[str] = "localhost:9092",
        topic: str = "gapguard.telemetry.v1",
        group_id: str = "telemetry-storage-v1",
        storage_path: str | Path = ":memory:",
        dlq_publisher: KafkaDlqPublisher | None = None,
        max_attempts: int = 3,
    ) -> None:
        try:
            from kafka import KafkaConsumer
        except ImportError as exc:
            raise RuntimeError("install streaming/requirements-transport.txt") from exc
        servers = [bootstrap_servers] if isinstance(bootstrap_servers, str) else bootstrap_servers
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        self._store = SQLiteTelemetryStore(storage_path)
        self._storage_consumer = TelemetryStorageConsumer(self._store)
        self._dlq_publisher = dlq_publisher
        self._max_attempts = max_attempts

    def poll_once(self, max_records: int = 100) -> dict[str, int]:
        if max_records <= 0:
            raise ValueError("max_records must be positive")
        records = self._consumer.poll(timeout_ms=1000, max_records=max_records)
        stored_count = 0
        duplicate_count = 0
        dlq_count = 0
        for messages in records.values():
            for record in messages:
                def handle(message: dict[str, Any]) -> None:
                    from streaming.bridge.processor import process_message

                    processed = process_message(message)
                    if processed["message_type"] != "telemetry":
                        raise ValueError("Kafka telemetry consumer received a non-telemetry message")
                    if self._storage_consumer.store.insert(processed["message"]):
                        nonlocal stored_count
                        stored_count += 1
                    else:
                        nonlocal duplicate_count
                        duplicate_count += 1

                def publish_dlq(record_value: dict[str, Any]) -> None:
                    if self._dlq_publisher is None:
                        raise RuntimeError("DLQ publisher is required after retries are exhausted")
                    self._dlq_publisher.publish(record_value)

                result = process_with_retry(
                    record.value,
                    handle,
                    publish_dlq,
                    metadata=build_dlq_metadata(
                        record.topic,
                        record.partition,
                        record.offset,
                        record.value.get("eventId"),
                        self._max_attempts,
                        "consumer processing failed",
                    ),
                    max_attempts=self._max_attempts,
                )
                if result.status == "dlq":
                    dlq_count += 1
        if records:
            self._consumer.commit()
        return {
            "stored_count": stored_count,
            "duplicate_count": duplicate_count,
            "dlq_count": dlq_count,
        }

    @property
    def stored_count(self) -> int:
        return self._store.count

    def close(self) -> None:
        self._consumer.close()
        self._store.close()
