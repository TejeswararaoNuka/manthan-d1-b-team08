-- Dashboard dataset views (Superset datasets point only at these). Built from the dash base
-- views, never from sql/views, so they work whether the base is real, derived or demo.
-- Thresholds come from dash."healthConfig" (ops.health_config or its demo copy).

CREATE VIEW dash."alertsPerMinute" AS
SELECT date_trunc('minute', alert_time) AS minute, rule_id, severity, count(*) AS alerts
  FROM dash."alerts"
 GROUP BY 1, 2, 3;

CREATE VIEW dash."topRules" AS
WITH latest AS (SELECT max(alert_time) AS t FROM dash."alerts")
SELECT r.rule_id,
       r.name,
       r.enabled,
       r.severity,
       count(a.alert_id)                                                  AS alerts_total,
       count(a.alert_id) FILTER (WHERE a.alert_time > latest.t - interval '1 hour')
                                                                          AS alerts_last_hour,
       count(a.alert_id) FILTER (WHERE a.status = 'OPEN')                 AS open_alerts
  FROM dash."ruleConfig" r
 CROSS JOIN latest
  LEFT JOIN dash."alerts" a ON a.rule_id = r.rule_id
 GROUP BY r.rule_id, r.name, r.enabled, r.severity;

CREATE VIEW dash."alertBacklog" AS
WITH cfg AS (SELECT (value #>> '{}')::numeric AS overdue
               FROM dash."healthConfig" WHERE key = 'backlog_overdue_minutes')
SELECT a.alert_id, a.rule_id, a.severity, a.card_token, a.event_time, a.alert_time,
       round(extract(epoch FROM now() - a.alert_time) / 60.0, 1)         AS age_minutes,
       coalesce(extract(epoch FROM now() - a.alert_time) / 60.0 > (SELECT overdue FROM cfg),
                false)                                                   AS is_overdue
  FROM dash."alerts" a
  LEFT JOIN dash."alertFeedback" f ON f.alert_id = a.alert_id
 WHERE f.alert_id IS NULL;

CREATE VIEW dash."rulePrecisionDaily" AS
SELECT (a.alert_time AT TIME ZONE 'UTC')::date                             AS alert_day,
       a.rule_id,
       count(*)                                                          AS alerts,
       count(f.alert_id)                                                 AS labelled,
       count(*) FILTER (WHERE f.label = 'TRUE_POSITIVE')                 AS true_positives,
       count(*) FILTER (WHERE f.label = 'FALSE_POSITIVE')                AS false_positives,
       count(*) FILTER (WHERE f.alert_id IS NULL)                        AS unreviewed,
       round(count(*) FILTER (WHERE f.label = 'TRUE_POSITIVE')::numeric
             / nullif(count(f.alert_id), 0), 4)                          AS precision
  FROM dash."alerts" a
  LEFT JOIN dash."alertFeedback" f ON f.alert_id = a.alert_id
 GROUP BY 1, 2;

CREATE VIEW dash."recentAlerts" AS
SELECT a.alert_time, a.rule_id, a.severity, a.card_token, a.status,
       coalesce(f.label, 'UNREVIEWED') AS label, a.alert_id
  FROM dash."alerts" a
  LEFT JOIN dash."alertFeedback" f ON f.alert_id = a.alert_id
 ORDER BY a.alert_time DESC
 LIMIT 500;

CREATE VIEW dash."dlqByReason" AS
SELECT reason_code, count(*) AS dead_letters, max(failed_at) AS last_seen
  FROM dash."dlqEvents"
 GROUP BY reason_code;

CREATE VIEW dash."latencyOverTime" AS
SELECT recorded_at, p95_latency_seconds, processed_rows_per_sec, input_rows, dlq_count
  FROM dash."streamMetrics";

CREATE VIEW dash."lagOverTime" AS
SELECT sampled_at, consumer_lag FROM dash."lagSamples";

-- One row: the Overview numbers. "Last 5 minutes / last hour" are measured back from the newest
-- data point, so a paused pipeline shows its last known rate rather than zero.
CREATE VIEW dash."headline" AS
WITH tx AS (SELECT max(ingested_at) AS t FROM dash."enrichedTransactions"),
     al AS (SELECT max(alert_time) AS t FROM dash."alerts"),
     lag AS (SELECT consumer_lag FROM dash."lagSamples" ORDER BY sampled_at DESC LIMIT 1)
SELECT (SELECT count(*) FROM dash."alertBacklog")                              AS open_alerts,
       (SELECT count(*) FROM dash."alerts", al WHERE alert_time > al.t - interval '1 hour')
                                                                               AS alerts_last_hour,
       (SELECT round(count(*) / 300.0, 1) FROM dash."enrichedTransactions", tx
         WHERE ingested_at > tx.t - interval '5 minutes')                     AS events_per_sec,
       (SELECT consumer_lag FROM lag)                                          AS consumer_lag,
       coalesce((SELECT to_char(consumer_lag, 'FM999,999,999') FROM lag), 'Not Available')
                                                                               AS consumer_lag_text;

-- One row: pipeline status against the thresholds.
CREATE VIEW dash."pipelineHealth" AS
WITH cfg AS (
    SELECT (SELECT (value #>> '{}')::numeric FROM dash."healthConfig"
             WHERE key = 'lag_alert_threshold')   AS lag_limit,
           (SELECT (value #>> '{}')::numeric FROM dash."healthConfig"
             WHERE key = 'latency_alert_seconds') AS latency_limit),
m AS (SELECT * FROM dash."streamMetrics" ORDER BY recorded_at DESC LIMIT 1),
w AS (SELECT sum(input_rows) AS rows_in, sum(dlq_count) AS dlq
        FROM dash."streamMetrics"
       WHERE recorded_at > (SELECT max(recorded_at) FROM dash."streamMetrics")
                           - interval '15 minutes'),
g AS (SELECT consumer_lag FROM dash."lagSamples" ORDER BY sampled_at DESC LIMIT 1)
SELECT coalesce((SELECT to_char(consumer_lag, 'FM999,999,999') FROM g), 'Not Available')
                                                                          AS consumer_lag,
       CASE WHEN (SELECT consumer_lag FROM g) IS NULL THEN 'Not Available'
            WHEN (SELECT consumer_lag FROM g) > cfg.lag_limit THEN 'ALERT' ELSE 'OK' END
                                                                          AS lag_status,
       m.p95_latency_seconds                                              AS p95_latency_seconds,
       CASE WHEN m.p95_latency_seconds IS NULL THEN 'Not Available'
            WHEN m.p95_latency_seconds > cfg.latency_limit THEN 'ALERT' ELSE 'OK' END
                                                                          AS latency_status,
       round(100.0 * w.dlq / nullif(w.rows_in, 0), 2)                     AS dlq_rate_pct_15min,
       m.recorded_at                                                      AS last_batch_at,
       round(extract(epoch FROM now() - m.recorded_at)::numeric, 0)       AS seconds_since_batch
  FROM cfg
  LEFT JOIN m ON true
 CROSS JOIN w;
