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

-- Lag moved to ops.lag_samples (the monitor samples it; a batch row cannot exist while the
-- processor is down). CASCADE drops v_pipeline_health, which sql/views re-creates.
ALTER TABLE ops.stream_metrics DROP COLUMN IF EXISTS consumer_lag CASCADE;

-- Last Kafka offset the processor finished, per partition (ADR-02). Spark keeps offsets in its
-- checkpoint and never commits them to Kafka, so this is how a monitor on any machine can
-- compute lag. Written in the same transaction as each batch; overwritten (not GREATEST) so a
-- replay from earlier offsets shows up as lag.
CREATE TABLE IF NOT EXISTS ops.stream_offsets (
    topic                  text        NOT NULL,
    partition              integer     NOT NULL,
    last_processed_offset  bigint      NOT NULL,
    updated_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

-- Consumer lag sampled by the monitor every 15 s: end offsets - (last_processed_offset + 1).
-- consumer_lag is NULL when no partition has a processed offset yet (e.g. after a reset).
CREATE TABLE IF NOT EXISTS ops.lag_samples (
    topic               text        NOT NULL,
    sampled_at          timestamptz NOT NULL DEFAULT now(),
    consumer_lag        bigint      CHECK (consumer_lag >= 0),
    lag_by_partition    jsonb       NOT NULL,
    unknown_partitions  integer     NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, sampled_at)
);

-- Processor progress per streaming query (D-04, ADR-01). Written in the same transaction as
-- each batch's data, so a re-run batch (same batch_id) is detected and only re-published.
-- `watermark` only moves forward, to min(newest event_time, wall clock) - watermark_seconds;
-- `watermark_before` is the value the last batch judged lateness against.
-- `python -m streamguard.processor --reset` deletes the row together with the checkpoint.
CREATE TABLE IF NOT EXISTS ops.processor_state (
    query_name        text        PRIMARY KEY,
    last_batch_id     bigint      NOT NULL,
    watermark         timestamptz,
    watermark_before  timestamptz,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

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
