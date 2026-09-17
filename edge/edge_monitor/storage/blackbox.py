"""SQLite-backed durable outbox for locally buffered messages."""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..contracts.hashing import verify_data_hash


@dataclass(frozen=True)
class BufferedMessage:
    row_id: int
    message_type: str
    payload: dict[str, Any]
    attempts: int


class SQLiteBlackbox:
    """Append-only-until-acknowledged local message outbox."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA busy_timeout=30000")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT NOT NULL,
                    message_type TEXT NOT NULL CHECK(message_type IN ('telemetry', 'event')),
                    seq_no INTEGER NOT NULL,
                    measured_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    enqueued_at TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    UNIQUE(device_id, message_type, seq_no)
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS device_sequence (
                    device_id TEXT PRIMARY KEY,
                    last_seq_no INTEGER NOT NULL
                )
                """
            )
            self._connection.commit()

    def next_sequence(self, device_id: str) -> int:
        """Atomically reserve the next device-wide sequence number."""

        if not device_id:
            raise ValueError("device_id must not be empty")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT last_seq_no FROM device_sequence WHERE device_id = ?",
                    (device_id,),
                ).fetchone()
                next_value = 1 if row is None else int(row[0]) + 1
                self._connection.execute(
                    """
                    INSERT INTO device_sequence(device_id, last_seq_no) VALUES (?, ?)
                    ON CONFLICT(device_id) DO UPDATE SET last_seq_no = excluded.last_seq_no
                    """,
                    (device_id, next_value),
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
            return next_value

    def _prepare_insert(
        self, message_type: str, payload: Mapping[str, Any]
    ) -> tuple[str, str, int, str, str, str]:
        if message_type not in {"telemetry", "event"}:
            raise ValueError("message_type must be telemetry or event")
        if not verify_data_hash(payload):
            raise ValueError("refusing to buffer a payload with an invalid dataHash")
        required = ("deviceId", "seqNo", "measuredAt")
        if any(key not in payload for key in required):
            raise ValueError("payload is missing blackbox identity fields")
        seq_no = payload["seqNo"]
        if isinstance(seq_no, bool) or not isinstance(seq_no, int) or seq_no < 0:
            raise ValueError("payload seqNo must be a non-negative integer")
        device_id = payload["deviceId"]
        if not isinstance(device_id, str) or not device_id:
            raise ValueError("payload deviceId must be a non-empty string")
        measured_at = payload["measuredAt"]
        if not isinstance(measured_at, str) or not measured_at:
            raise ValueError("payload measuredAt must be a non-empty string")
        body = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return (
            device_id,
            message_type,
            seq_no,
            measured_at,
            body,
            datetime.now(timezone.utc).isoformat(),
        )

    def enqueue(self, message_type: str, payload: Mapping[str, Any]) -> bool:
        params = self._prepare_insert(message_type, payload)
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO outbox
                    (device_id, message_type, seq_no, measured_at, payload_json, enqueued_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            self._connection.commit()
            return cursor.rowcount == 1

    def enqueue_many(
        self, messages: Sequence[tuple[str, Mapping[str, Any]]]
    ) -> int:
        """Atomically enqueue messages in the provided order."""

        if not messages:
            return 0
        rows = [self._prepare_insert(m_type, payload) for m_type, payload in messages]
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._connection.executemany(
                    """
                    INSERT OR IGNORE INTO outbox
                        (device_id, message_type, seq_no, measured_at, payload_json, enqueued_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                self._connection.commit()
                return cursor.rowcount
            except Exception:
                self._connection.rollback()
                raise

    def pending(self, limit: int = 100) -> list[BufferedMessage]:
        if limit <= 0:
            return []
        with self._lock:
            rows = self._connection.execute(
                "SELECT id, message_type, payload_json, attempts "
                "FROM outbox ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                BufferedMessage(
                    row_id=row["id"],
                    message_type=row["message_type"],
                    payload=json.loads(row["payload_json"]),
                    attempts=row["attempts"],
                )
                for row in rows
            ]

    def acknowledge(self, row_id: int) -> None:
        with self._lock:
            self._connection.execute("DELETE FROM outbox WHERE id = ?", (row_id,))
            self._connection.commit()

    def record_failure(self, row_id: int, error: str) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                (error[:1000], row_id),
            )
            self._connection.commit()

    @property
    def count(self) -> int:
        with self._lock:
            row = self._connection.execute("SELECT COUNT(*) FROM outbox").fetchone()
            return int(row[0])

    def quick_check(self) -> bool:
        """Return whether SQLite's non-destructive integrity check succeeds."""

        with self._lock:
            rows = self._connection.execute("PRAGMA quick_check").fetchall()
        return len(rows) == 1 and str(rows[0][0]).lower() == "ok"

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "SQLiteBlackbox":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
