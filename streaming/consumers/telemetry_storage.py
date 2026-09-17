"""Minimal idempotent telemetry storage consumer for local P0 tests."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from streaming.bridge.in_memory import InMemoryTopic
from streaming.bridge.processor import process_message


@dataclass(frozen=True)
class ConsumeResult:
    next_offset: int
    stored_count: int
    duplicate_count: int


class SQLiteTelemetryStore:
    """Small SQLite sink whose event ID is the idempotency key."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry (
                event_id TEXT PRIMARY KEY,
                site_id TEXT NOT NULL,
                section_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                seq_no INTEGER NOT NULL,
                measured_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    def insert(self, message: dict[str, Any]) -> bool:
        if message.get("eventType") is not None:
            raise ValueError("telemetry consumer cannot store event messages")
        payload = json.dumps(
            message,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        cursor = self._connection.execute(
            """
            INSERT OR IGNORE INTO telemetry
                (event_id, site_id, section_id, device_id, seq_no, measured_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message["eventId"],
                message["siteId"],
                message["sectionId"],
                message["deviceId"],
                message["seqNo"],
                message["measuredAt"],
                payload,
            ),
        )
        self._connection.commit()
        return cursor.rowcount == 1

    @property
    def count(self) -> int:
        return int(self._connection.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0])

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "SQLiteTelemetryStore":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class TelemetryStorageConsumer:
    def __init__(self, store: SQLiteTelemetryStore) -> None:
        self.store = store

    def consume(
        self, topic: InMemoryTopic, offset: int = 0, limit: int = 100
    ) -> ConsumeResult:
        if offset < 0 or limit <= 0:
            raise ValueError("offset must be non-negative and limit must be positive")
        next_offset = offset
        stored_count = 0
        duplicate_count = 0
        for message in topic.values[offset : offset + limit]:
            processed = process_message(message)
            if processed["message_type"] != "telemetry":
                raise ValueError("telemetry consumer received a non-telemetry message")
            if self.store.insert(processed["message"]):
                stored_count += 1
            else:
                duplicate_count += 1
            # The caller may persist this only after consume returns successfully.
            next_offset += 1
        return ConsumeResult(next_offset, stored_count, duplicate_count)
