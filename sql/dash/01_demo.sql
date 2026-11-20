-- Demo data for the dashboard layer (scripts/build_dash_layer.py). Schema demo only; never sg/ops.
-- Synthetic tokens only (tok_dec0de..., mer_dec0...), the last 2 hours, re-seeded on every run
-- so timestamps stay current. Columns match the real tables, so dash.* views look the same.

CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.enriched_transactions (
    event_id uuid, card_token text, merchant_id text, merchant_category text, amount numeric(18,2),
    currency text, country text, channel text, event_time timestamptz, status text,
    kafka_partition integer, kafka_offset bigint, ingested_at timestamptz, processed_at timestamptz,
    is_late boolean);
CREATE TABLE IF NOT EXISTS demo.alerts (
    alert_id text, rule_id text, rule_version integer, card_token text, event_ids text[],
    severity text, details jsonb, txn_status text, event_time timestamptz, alert_time timestamptz,
    status text);
CREATE TABLE IF NOT EXISTS demo.alert_feedback (
    alert_id text, label text, analyst text, noted_at timestamptz, comment text);
-- Key for the feedback CLI's upsert when labelling demo alerts.
CREATE UNIQUE INDEX IF NOT EXISTS alert_feedback_alert_id_key ON demo.alert_feedback (alert_id);
CREATE TABLE IF NOT EXISTS demo.dlq_events (
    id bigint, reason_code text, error_detail text, raw_value text, source_topic text,
    kafka_partition integer, kafka_offset bigint, failed_at timestamptz);
CREATE TABLE IF NOT EXISTS demo.stream_metrics (
    batch_id bigint, recorded_at timestamptz, input_rows bigint, processed_rows_per_sec numeric,
    p95_latency_seconds numeric, dlq_count bigint, alert_count bigint, late_count bigint);
CREATE TABLE IF NOT EXISTS demo.lag_samples (
    topic text, sampled_at timestamptz, consumer_lag bigint, unknown_partitions integer);
CREATE TABLE IF NOT EXISTS demo.ops_alerts (
    id bigint, raised_at timestamptz, severity text, source text, message text);
CREATE TABLE IF NOT EXISTS demo.rule_config (
    rule_id text, name text, enabled boolean, severity text, params jsonb, version integer,
    updated_at timestamptz);
CREATE TABLE IF NOT EXISTS demo.health_config (key text, value jsonb);

TRUNCATE demo.enriched_transactions, demo.alerts, demo.alert_feedback, demo.dlq_events,
         demo.stream_metrics, demo.lag_samples, demo.ops_alerts, demo.rule_config,
         demo.health_config;

-- 10 events/s for 2 hours (72,000 rows) over 5,000 cards and 2,000 merchants.
INSERT INTO demo.enriched_transactions
SELECT gen_random_uuid(),
       'tok_dec0de' || lpad(to_hex((i * 7919) % 5000), 10, 'a'),
       'mer_dec0' || lpad(to_hex(i % 2000), 4, 'a'),
       (ARRAY['5411','5812','5912','5999','4111','5732','7011','6011'])[1 + i % 8],
       (1 + ((i * 7919) % 300000) / 100.0)::numeric(18,2),
       c.currency, c.country,
       CASE WHEN i % 20 < 12 THEN 'POS' WHEN i % 20 < 19 THEN 'ECOM' ELSE 'ATM' END,
       t,
       CASE WHEN i % 100 < 93 THEN 'APPROVED' WHEN i % 100 < 98 THEN 'DECLINED' ELSE 'PENDING' END,
       i % 6, i / 6,
       t + interval '400 milliseconds',
       t + (1 + i % 5) * interval '1 second',
       i % 2000 = 0
  FROM generate_series(0, 71999) AS i
 CROSS JOIN LATERAL (SELECT now() - interval '2 hours' + i * interval '100 milliseconds' AS t) tt
 CROSS JOIN LATERAL (
       SELECT (ARRAY['INR','INR','INR','INR','INR','INR','USD','GBP','SGD','AED'])[1 + i % 10]
                  AS currency,
              (ARRAY['IN','IN','IN','IN','IN','IN','US','GB','SG','AE'])[1 + i % 10] AS country) c;

-- 100 alerts: velocity 40, travel 15, spike 25, blacklist 20, spread over 2 hours.
INSERT INTO demo.alerts
SELECT encode(sha256(convert_to('DEMO|' || r.rule_id || '|' || k, 'UTF8')), 'hex'),
       r.rule_id, 1,
       'tok_dec0de' || lpad(to_hex((k * 131 + r.n) % 5000), 10, 'a'),
       ARRAY(SELECT gen_random_uuid()::text FROM generate_series(1, r.n_events)),
       r.severity,
       jsonb_build_object('demo', true),
       'APPROVED',
       now() - interval '2 hours' + (k + 0.5) * (interval '2 hours' / r.n),
       now() - interval '2 hours' + (k + 0.5) * (interval '2 hours' / r.n) + interval '6 seconds',
       'OPEN'
  FROM (VALUES ('VELOCITY', 40, 6, 'HIGH'), ('IMPOSSIBLE_TRAVEL', 15, 2, 'HIGH'),
               ('AMOUNT_SPIKE', 25, 1, 'MEDIUM'), ('MERCHANT_BLACKLIST', 20, 1, 'HIGH'))
       AS r(rule_id, n, n_events, severity)
 CROSS JOIN LATERAL generate_series(0, r.n - 1) AS k;

-- Analysts reviewed ~60% of alerts older than 20 minutes; precision differs per rule.
INSERT INTO demo.alert_feedback
SELECT alert_id,
       CASE WHEN (abs(hashtext(alert_id)) % 100) <
                 CASE rule_id WHEN 'MERCHANT_BLACKLIST' THEN 90 WHEN 'VELOCITY' THEN 75
                              WHEN 'AMOUNT_SPIKE' THEN 55 ELSE 40 END
            THEN 'TRUE_POSITIVE' ELSE 'FALSE_POSITIVE' END,
       'demo.analyst', alert_time + interval '9 minutes', 'demo review'
  FROM demo.alerts
 WHERE alert_time < now() - interval '20 minutes' AND abs(hashtext(alert_id)) % 10 < 6;
UPDATE demo.alerts a SET status = 'CLOSED' FROM demo.alert_feedback f WHERE f.alert_id = a.alert_id;

-- Dead letters for every reason code.
INSERT INTO demo.dlq_events
SELECT row_number() OVER (), d.reason_code, 'demo ' || lower(d.reason_code),
       '{"demo": true}', 'payments.raw', k % 6, 900000 + k,
       now() - interval '2 hours' + (k + 0.5) * (interval '2 hours' / d.n)
  FROM (VALUES ('MALFORMED_JSON', 30), ('MISSING_FIELD', 45), ('AMOUNT_OUT_OF_RANGE', 25),
               ('CURRENCY_NOT_ALLOWED', 20), ('FUTURE_EVENT_TIME', 10)) AS d(reason_code, n)
 CROSS JOIN LATERAL generate_series(0, d.n - 1) AS k;

-- One batch every 5 s; a slow spell 45 minutes ago (latency and lag spike, then recovery).
INSERT INTO demo.stream_metrics
SELECT b, t, 50 + (b % 12) * 4, (400 + (b % 9) * 25)::numeric,
       CASE WHEN t BETWEEN now() - interval '47 minutes' AND now() - interval '44 minutes'
            THEN 41.5 ELSE 2 + (b % 7) * 0.6 END,
       CASE WHEN b % 48 = 0 THEN 1 ELSE 0 END,
       CASE WHEN b % 72 = 0 THEN 1 ELSE 0 END,
       CASE WHEN b % 400 = 0 THEN 1 ELSE 0 END
  FROM generate_series(0, 1439) AS b
 CROSS JOIN LATERAL (SELECT now() - interval '2 hours' + b * interval '5 seconds' AS t) tt;

INSERT INTO demo.lag_samples
SELECT 'payments.raw', t,
       CASE WHEN t BETWEEN now() - interval '47 minutes' AND now() - interval '44 minutes'
            THEN 7200 - (s % 4) * 300 ELSE 250 + (s % 11) * 60 END,
       0
  FROM generate_series(0, 479) AS s
 CROSS JOIN LATERAL (SELECT now() - interval '2 hours' + s * interval '15 seconds' AS t) tt;

INSERT INTO demo.ops_alerts VALUES
    (1, now() - interval '46 minutes 30 seconds', 'HIGH', 'monitor:lag',
     'ALERT: consumer lag 7,200 > 5,000 on payments.raw (demo)'),
    (2, now() - interval '46 minutes', 'HIGH', 'monitor:latency',
     'ALERT: p95 processing latency 41.5s > 30s (demo)'),
    (3, now() - interval '43 minutes 45 seconds', 'LOW', 'monitor:lag',
     'RESOLVED: consumer lag back to 310 (<= 5,000) on payments.raw (demo)'),
    (4, now() - interval '43 minutes 30 seconds', 'LOW', 'monitor:latency',
     'RESOLVED: p95 processing latency back to 2.6s (<= 30s) (demo)');

INSERT INTO demo.rule_config VALUES
    ('VELOCITY', 'More than max_tx transactions per card in a sliding window', true, 'HIGH',
     '{"max_tx": 5, "window_seconds": 60}', 1, now()),
    ('IMPOSSIBLE_TRAVEL', 'Same card used in two countries within the window', true, 'HIGH',
     '{"window_minutes": 30}', 1, now()),
    ('AMOUNT_SPIKE', 'Amount above multiplier x the card''s 30-day average', true, 'MEDIUM',
     '{"multiplier": 5, "lookback_days": 30}', 1, now()),
    ('MERCHANT_BLACKLIST', 'Payment to a blacklisted merchant', true, 'HIGH', '{}', 1, now());

INSERT INTO demo.health_config VALUES
    ('lag_alert_threshold', '5000'), ('latency_alert_seconds', '30'),
    ('backlog_overdue_minutes', '15');
