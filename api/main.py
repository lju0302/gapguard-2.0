"""FastAPI read surface for locally stored telemetry and alerts."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query

from .query_service import QueryService


def create_app(
    db_path: str | Path | None = None,
    query_service: Any | None = None,
) -> FastAPI:
    service = query_service or QueryService(
        db_path or os.getenv("GAPGUARD_STORAGE_DB", "/tmp/gapguard.sqlite3")
    )
    app = FastAPI(title="GAPGUARD Query API", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/telemetry")
    def telemetry(
        site_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict]:
        return service.list_telemetry(site_id=site_id, limit=limit)

    @app.get("/api/alerts")
    def alerts(
        site_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict]:
        return service.list_alerts(site_id=site_id, limit=limit)

    @app.get("/api/summary")
    def summary() -> dict[str, int]:
        return service.summary()

    return app


app = create_app()
