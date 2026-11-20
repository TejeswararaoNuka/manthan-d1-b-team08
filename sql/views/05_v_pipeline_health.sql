-- Latest micro-batch metrics with lag / latency status against ops.health_config.
-- Always returns exactly one row; statuses are UNKNOWN until metrics or thresholds exist.
DROP VIEW IF EXISTS ops.v_pipeline_health;

CREATE VIEW ops.v_pipeline_health AS
WITH cfg AS (
    SELECT (SELECT (value #>> '{}')::numeric FROM ops.health_config
             WHERE key = 'lag_alert_threshold')   AS lag_threshold,
           (SELECT (value #>> '{}')::numeric FROM ops.health_config
             WHERE key = 'latency_alert_seconds') AS latency_threshold
),
latest AS (
    SELECT *
      FROM ops.stream_metrics
     ORDER BY recorded_at DESC, batch_id DESC
     LIMIT 1
),
status AS (
    SELECT l.*,
           cfg.lag_threshold,
           cfg.latency_threshold,
           CASE WHEN l.consumer_lag IS NULL OR cfg.lag_threshold IS NULL THEN 'UNKNOWN'
                WHEN l.consumer_lag > cfg.lag_threshold THEN 'ALERT'
                ELSE 'OK' END                                              AS lag_status,
           CASE WHEN l.p95_latency_seconds IS NULL OR cfg.latency_threshold IS NULL
                     THEN 'UNKNOWN'
                WHEN l.p95_latency_seconds > cfg.latency_threshold THEN 'ALERT'
                ELSE 'OK' END                                              AS latency_status
      FROM cfg
      LEFT JOIN latest l ON true
)
SELECT batch_id,
       recorded_at,
       round(extract(epoch FROM now() - recorded_at)::numeric, 1) AS seconds_since_last_batch,
       input_rows,
       processed_rows_per_sec,
       consumer_lag,
       lag_threshold,
       lag_status,
       p95_latency_seconds,
       latency_threshold,
       latency_status,
       dlq_count,
       alert_count,
       late_count,
       CASE WHEN 'ALERT' IN (lag_status, latency_status) THEN 'ALERT'
            WHEN 'UNKNOWN' IN (lag_status, latency_status) THEN 'UNKNOWN'
            ELSE 'OK' END                                         AS overall_status
  FROM status;
