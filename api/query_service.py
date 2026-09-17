"""Read-only SQLite query service used by the local and future Cloud SQL API."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class QueryService:
    def __init__(self, db_path: str | Path) -> None:
        self._lock = threading.RLock()
        path = Path(db_path)
        self._connection = (
            sqlite3.connect(":memory:", check_same_thread=False)
            if str(db_path) == ":memory:" or not path.exists()
            else sqlite3.connect(
                f"file:{path.absolute()}?mode=ro", uri=True, check_same_thread=False
            )
        )
        self._connection.row_factory = sqlite3.Row

    def _list(self, table: str, site_id: str | None, limit: int) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        columns = {
            "telemetry": "event_id, site_id, section_id, device_id, seq_no, measured_at, payload_json",
            "alerts": "event_id, site_id, section_id, event_type, severity, measured_at, payload_json",
        }[table]
        try:
            with self._lock:
                if site_id is None:
                    rows = self._connection.execute(
                        f"SELECT {columns} FROM {table} ORDER BY rowid DESC LIMIT ?", (limit,)
                    ).fetchall()
                else:
                    rows = self._connection.execute(
                        f"SELECT {columns} FROM {table} WHERE site_id = ? ORDER BY rowid DESC LIMIT ?",
                        (site_id, limit),
                    ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise
        return [json.loads(row["payload_json"]) for row in rows]

    def list_telemetry(self, site_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self._list("telemetry", site_id, limit)

    def list_alerts(self, site_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self._list("alerts", site_id, limit)

    def summary(self) -> dict[str, int]:
        result = {"telemetry_count": 0, "alert_count": 0}
        with self._lock:
            for key, table in (("telemetry_count", "telemetry"), ("alert_count", "alerts")):
                try:
                    result[key] = int(self._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                except sqlite3.OperationalError as exc:
                    if "no such table" not in str(exc):
                        raise
        return result

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "QueryService":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
