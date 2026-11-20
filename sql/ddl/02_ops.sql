-- Schema ops: pipeline observability (platform on-call engineer).
-- Re-runnable; applied by scripts/apply_sql.py after 01_sg.sql.

CREATE SCHEMA IF NOT EXISTS ops;

-- One row per Spark micro-batch, written by the monitor from StreamingQueryProgress.
-- Upserted on batch_id, so a re-run batch overwrites its own row.
CREATE TABLE IF NOT EXISTS ops.stream_metrics (
    batch_id                bigint        PRIMARY KEY,
    recorded_at             timestamptz   NOT NULL DEFAULT now(),
    input_rows              bigint        NOT NULL CHECK (input_rows >= 0),
    processed_rows_per_sec  numeric(12,2),
    consumer_lag            bigint        CHECK (consumer_lag >= 0),
    p95_latency_seconds     numeric(10,3) CHECK (p95_latency_seconds >= 0),
    dlq_count               bigint        NOT NULL DEFAULT 0 CHECK (dlq_count >= 0),
    alert_count             bigint        NOT NULL DEFAULT 0 CHECK (alert_count >= 0),
    -- LATE_EVENT rows (D-04): valid, kept in enriched_transactions, counted here.
    late_count              bigint        NOT NULL DEFAULT 0 CHECK (late_count >= 0)
);

CREATE INDEX IF NOT EXISTS stream_metrics_recorded_at_idx ON ops.stream_metrics (recorded_at);

-- Operational alerts raised by the monitor (lag, latency, DLQ rate), not fraud alerts.
CREATE TABLE IF NOT EXISTS ops.alerts (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raised_at  timestamptz NOT NULL DEFAULT now(),
    severity   text        NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH')),
    source     text        NOT NULL,
    message    text        NOT NULL
);

CREATE INDEX IF NOT EXISTS ops_alerts_raised_at_idx ON ops.alerts (raised_at);

-- Health thresholds read by v_pipeline_health and v_alert_backlog (never hard-coded in SQL).
-- Seeded by apply_sql.py from config/dq.yaml, overridden by LAG_ALERT_THRESHOLD /
-- LATENCY_ALERT_SECONDS from .env when set.
CREATE TABLE IF NOT EXISTS ops.health_config (
    key         text        PRIMARY KEY,
    value       jsonb       NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_ro') THEN
        GRANT USAGE ON SCHEMA ops TO superset_ro;
        GRANT SELECT ON ALL TABLES IN SCHEMA ops TO superset_ro;
        ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO superset_ro;
    ELSE
        RAISE WARNING 'role superset_ro does not exist: ops grants skipped';
    END IF;
END;
$$;
