"""Idempotent alert storage and Kafka adapter."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from streaming.bridge.processor import process_message
from streaming.common.retry import process_with_retry
from streaming.consumers.kafka_dlq import KafkaDlqPublisher, build_dlq_metadata


class SQLiteAlertStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                event_id TEXT PRIMARY KEY,
                site_id TEXT NOT NULL,
                section_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                measured_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    def insert(self, message: dict[str, Any]) -> bool:
        if "eventType" not in message:
            raise ValueError("alert store requires an event message")
        payload = json.dumps(message, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        cursor = self._connection.execute(
            """
            INSERT OR IGNORE INTO alerts
                (event_id, site_id, section_id, event_type, severity, measured_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message["eventId"],
                message["siteId"],
                message["sectionId"],
                message["eventType"],
                message["severity"],
                message["measuredAt"],
                payload,
            ),
        )
        self._connection.commit()
        return cursor.rowcount == 1

    @property
    def count(self) -> int:
        return int(self._connection.execute("SELECT COUNT(*) FROM alerts").fetchone()[0])

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "SQLiteAlertStore":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class KafkaAlertConsumer:
    def __init__(
        self,
        bootstrap_servers: str | list[str] = "localhost:9092",
        topic: str = "gapguard.alert.v1",
        group_id: str = "alert-dispatch-v1",
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
        self._store = SQLiteAlertStore(storage_path)
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
                    processed = process_message(message)
                    if processed["message_type"] != "event":
                        raise ValueError("Kafka alert consumer received a non-event message")
                    if self._store.insert(processed["message"]):
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
