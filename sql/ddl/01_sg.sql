-- Schema sg: StreamGuard business tables.
-- Re-runnable: every statement is IF NOT EXISTS / CREATE OR REPLACE. Applied by
-- scripts/apply_sql.py inside one transaction, as POSTGRES_USER (the owner).
-- Thresholds are NOT defined here: they are rows in sg.rule_config / sg.dq_config,
-- loaded from config/*.yaml (CLAUDE.md "Rules and thresholds").

CREATE SCHEMA IF NOT EXISTS sg;

-- ---------------------------------------------------------------------------
-- Configuration (re-read by the processor every micro-batch, D-09)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sg.rule_config (
    rule_id     text        PRIMARY KEY CHECK (rule_id ~ '^[A-Z][A-Z_]*$'),
    name        text        NOT NULL,
    enabled     boolean     NOT NULL DEFAULT true,
    severity    text        NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH')),
    params      jsonb       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(params) = 'object'),
    version     integer     NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Any real change to a rule bumps its version, so each alert's rule_version says which
-- thresholds produced it (D-09, ADR-01 replay caveat). A no-op UPDATE does not bump.
CREATE OR REPLACE FUNCTION sg.rule_config_bump_version() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.name, NEW.enabled, NEW.severity, NEW.params)
       IS DISTINCT FROM (OLD.name, OLD.enabled, OLD.severity, OLD.params) THEN
        NEW.version    := OLD.version + 1;
        NEW.updated_at := now();
    ELSE
        NEW.version    := OLD.version;
        NEW.updated_at := OLD.updated_at;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER rule_config_bump_version
    BEFORE UPDATE ON sg.rule_config
    FOR EACH ROW EXECUTE FUNCTION sg.rule_config_bump_version();

CREATE TABLE IF NOT EXISTS sg.dq_config (
    key         text        PRIMARY KEY,
    value       jsonb       NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sg.merchant_blacklist (
    merchant_id text        PRIMARY KEY CHECK (merchant_id ~ '^mer_[0-9a-f]{8}$'),
    reason      text        NOT NULL,
    added_at    timestamptz NOT NULL DEFAULT now()
);

-- 30-day spending profile per card (D-03). Refreshed by a batch job, never by the stream.
CREATE TABLE IF NOT EXISTS sg.card_baseline (
    card_token      text          PRIMARY KEY CHECK (card_token ~ '^tok_[0-9a-f]{16}$'),
    avg_amount_30d  numeric(18,2) NOT NULL CHECK (avg_amount_30d >= 0),
    tx_count_30d    integer       NOT NULL CHECK (tx_count_30d >= 0),
    updated_at      timestamptz   NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Stream output
-- ---------------------------------------------------------------------------

-- One row per valid payment event. Natural key event_id, written ON CONFLICT DO NOTHING (D-08).
CREATE TABLE IF NOT EXISTS sg.enriched_transactions (
    event_id          uuid          PRIMARY KEY,
    card_token        text          NOT NULL CHECK (card_token ~ '^tok_[0-9a-f]{16}$'),
    merchant_id       text          NOT NULL CHECK (merchant_id ~ '^mer_[0-9a-f]{8}$'),
    merchant_category char(4)       NOT NULL CHECK (merchant_category ~ '^[0-9]{4}$'),
    amount            numeric(18,2) NOT NULL CHECK (amount > 0),
    currency          char(3)       NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    country           char(2)       NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
    channel           text          NOT NULL CHECK (channel IN ('POS', 'ECOM', 'ATM')),
    event_time        timestamptz   NOT NULL,
    status            text          NOT NULL CHECK (status IN ('APPROVED', 'DECLINED', 'PENDING')),
    schema_version    smallint      NOT NULL,
    kafka_partition   integer       NOT NULL,
    kafka_offset      bigint        NOT NULL,
    ingested_at       timestamptz   NOT NULL,
    processed_at      timestamptz   NOT NULL DEFAULT now(),
    is_late           boolean       NOT NULL DEFAULT false
);

-- Rule SQL looks up each card's recent events (velocity, travel).
CREATE INDEX IF NOT EXISTS enriched_transactions_card_time_idx
    ON sg.enriched_transactions (card_token, event_time);

-- agg_1min recompute scans one minute of events at a time.
CREATE INDEX IF NOT EXISTS enriched_transactions_event_time_idx
    ON sg.enriched_transactions (event_time);

-- Natural key alert_id = sha256(rule_id || dedup key) (D-07), written ON CONFLICT DO NOTHING
-- so a replay never resets analyst state (ADR-01).
CREATE TABLE IF NOT EXISTS sg.alerts (
    alert_id      char(64)    PRIMARY KEY CHECK (alert_id ~ '^[0-9a-f]{64}$'),
    rule_id       text        NOT NULL REFERENCES sg.rule_config (rule_id),
    rule_version  integer     NOT NULL,
    card_token    text        NOT NULL CHECK (card_token ~ '^tok_[0-9a-f]{16}$'),
    event_ids     text[]      NOT NULL CHECK (cardinality(event_ids) >= 1),
    severity      text        NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH')),
    details       jsonb       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
    -- Payment status of the triggering event (D-10); not the analyst status below.
    txn_status    text        CHECK (txn_status IN ('APPROVED', 'DECLINED', 'PENDING')),
    event_time    timestamptz NOT NULL,
    alert_time    timestamptz NOT NULL DEFAULT now(),
    status        text        NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED'))
);

CREATE INDEX IF NOT EXISTS alerts_alert_time_idx ON sg.alerts (alert_time);
CREATE INDEX IF NOT EXISTS alerts_rule_id_idx ON sg.alerts (rule_id);
CREATE INDEX IF NOT EXISTS alerts_event_time_idx ON sg.alerts (event_time);
CREATE INDEX IF NOT EXISTS alerts_open_idx ON sg.alerts (alert_time) WHERE status = 'OPEN';

-- 1-minute aggregates. Written with absolute values, never count = count + n (ADR-01).
CREATE TABLE IF NOT EXISTS sg.agg_1min (
    window_start  timestamptz   NOT NULL,
    channel       text          NOT NULL CHECK (channel IN ('POS', 'ECOM', 'ATM')),
    country       char(2)       NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
    tx_count      bigint        NOT NULL CHECK (tx_count >= 0),
    amount_sum    numeric(20,2) NOT NULL,
    alert_count   bigint        NOT NULL DEFAULT 0 CHECK (alert_count >= 0),
    updated_at    timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, channel, country)
);

-- Analyst verdict per alert (D-05, D-12). Entered from Superset SQL Lab as feedback_writer.
CREATE TABLE IF NOT EXISTS sg.alert_feedback (
    alert_id  char(64)    PRIMARY KEY REFERENCES sg.alerts (alert_id),
    label     text        NOT NULL CHECK (label IN ('TRUE_POSITIVE', 'FALSE_POSITIVE')),
    analyst   text        NOT NULL,
    noted_at  timestamptz NOT NULL DEFAULT now(),
    comment   text
);

-- Feedback closes the alert, so "OPEN" == "no feedback yet" (D-12 backlog).
-- SECURITY DEFINER: feedback_writer has no write access to sg.alerts itself.
CREATE OR REPLACE FUNCTION sg.alert_feedback_close_alert() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, sg AS $$
BEGIN
    UPDATE sg.alerts SET status = 'CLOSED'
     WHERE alert_id = NEW.alert_id AND status <> 'CLOSED';
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION sg.alert_feedback_close_alert() FROM PUBLIC;

CREATE OR REPLACE TRIGGER alert_feedback_close_alert
    AFTER INSERT OR UPDATE ON sg.alert_feedback
    FOR EACH ROW EXECUTE FUNCTION sg.alert_feedback_close_alert();

-- Invalid events also published to payments.dlq. Natural key = source Kafka position.
CREATE TABLE IF NOT EXISTS sg.dlq_events (
    id               bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reason_code      text        NOT NULL CHECK (reason_code IN (
                         'MALFORMED_JSON', 'MISSING_FIELD', 'AMOUNT_OUT_OF_RANGE',
                         'CURRENCY_NOT_ALLOWED', 'FUTURE_EVENT_TIME')),
    error_detail     text,
    raw_value        text        NOT NULL,
    source_topic     text        NOT NULL,
    kafka_partition  integer     NOT NULL,
    kafka_offset     bigint      NOT NULL,
    failed_at        timestamptz NOT NULL,
    CONSTRAINT dlq_events_source_position_key
        UNIQUE (source_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS dlq_events_failed_at_idx ON sg.dlq_events (failed_at);

-- ---------------------------------------------------------------------------
-- Grants. Login roles are created by deploy/postgres/init/02_roles.sql on first start;
-- skip with a WARNING if they do not exist (apply_sql.py prints notices).
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_ro') THEN
        GRANT USAGE ON SCHEMA sg TO superset_ro;
        GRANT SELECT ON ALL TABLES IN SCHEMA sg TO superset_ro;
        -- Covers tables and views created later by this role (e.g. sql/views re-created).
        ALTER DEFAULT PRIVILEGES IN SCHEMA sg GRANT SELECT ON TABLES TO superset_ro;
    ELSE
        RAISE WARNING 'role superset_ro does not exist: sg grants skipped';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'feedback_writer') THEN
        -- Access to alert_feedback only; nothing else in sg, not even SELECT on alerts.
        -- SELECT is required by PostgreSQL for the upsert: ON CONFLICT needs it on the
        -- conflict column and each EXCLUDED.col needs it on that column (verified on PG 16).
        GRANT USAGE ON SCHEMA sg TO feedback_writer;
        GRANT SELECT, INSERT, UPDATE ON sg.alert_feedback TO feedback_writer;
    ELSE
        RAISE WARNING 'role feedback_writer does not exist: sg grants skipped';
    END IF;
END;
$$;
