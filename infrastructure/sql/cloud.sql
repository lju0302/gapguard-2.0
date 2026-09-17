-- Apply this schema explicitly after Cloud SQL is provisioned.
-- The application never runs migrations implicitly.

CREATE TABLE IF NOT EXISTS telemetry (
    event_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    section_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    seq_no BIGINT NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL,
    payload_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS telemetry_site_measured_idx
    ON telemetry (site_id, measured_at DESC);

CREATE TABLE IF NOT EXISTS alerts (
    event_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    section_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL,
    payload_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS alerts_site_measured_idx
    ON alerts (site_id, measured_at DESC);
