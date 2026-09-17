"""Read-only Cloud SQL query adapter.

The adapter accepts an already-open DB-API connection for tests and uses
psycopg only when a real DSN is requested. It deliberately does not create a
connection from environment variables or perform migrations implicitly.
"""

from __future__ import annotations

import json
from typing import Any


class CloudSqlQueryService:
    """Small read-only query surface matching the local QueryService methods."""

    _COLUMNS = {
        "telemetry": "payload_json",
        "alerts": "payload_json",
    }

    def __init__(self, connection: Any, *, owns_connection: bool = False) -> None:
        self._connection = connection
        self._owns_connection = owns_connection

    @classmethod
    def from_dsn(cls, dsn: str) -> "CloudSqlQueryService":
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("install api/requirements-cloud.txt") from exc
        return cls(psycopg.connect(dsn, autocommit=True), owns_connection=True)

    @staticmethod
    def _payload(row: Any) -> dict[str, Any]:
        value = row.get("payload_json") if isinstance(row, dict) else row[0]
        return json.loads(value) if isinstance(value, str) else value

    def _list(self, table: str, site_id: str | None, limit: int) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        where = " WHERE site_id = %s" if site_id is not None else ""
        params: tuple[Any, ...] = (site_id, limit) if site_id is not None else (limit,)
        cursor = self._connection.cursor()
        try:
            cursor.execute(
                f"SELECT {self._COLUMNS[table]} FROM {table}{where} "
                "ORDER BY measured_at DESC, event_id DESC LIMIT %s",
                params,
            )
            return [self._payload(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def list_telemetry(self, site_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self._list("telemetry", site_id, limit)

    def list_alerts(self, site_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self._list("alerts", site_id, limit)

    def summary(self) -> dict[str, int]:
        result: dict[str, int] = {}
        cursor = self._connection.cursor()
        try:
            for key, table in (("telemetry_count", "telemetry"), ("alert_count", "alerts")):
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                result[key] = int(cursor.fetchone()[0])
        finally:
            cursor.close()
        return result

    def close(self) -> None:
        if self._owns_connection:
            self._connection.close()

    def __enter__(self) -> "CloudSqlQueryService":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
