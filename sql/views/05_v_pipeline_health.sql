-- Pipeline health: latest batch metrics + latest monitor lag sample, with status against
-- ops.health_config. Always one row; statuses are UNKNOWN until data or thresholds exist.
DROP VIEW IF EXISTS ops.v_pipeline_health;

CREATE VIEW ops.v_pipeline_health AS
WITH cfg AS (
    SELECT (SELECT (value #>> '{}')::numeric FROM ops.health_config
             WHERE key = 'lag_alert_threshold')   AS lag_threshold,
           (SELECT (value #>> '{}')::numeric FROM ops.health_config
             WHERE key = 'latency_alert_seconds') AS latency_threshold
),
latest AS (
    SELECT * FROM ops.stream_metrics ORDER BY recorded_at DESC, batch_id DESC LIMIT 1
),
lag AS (
    SELECT sampled_at, consumer_lag, unknown_partitions
      FROM ops.lag_samples
     WHERE topic = 'payments.raw'
     ORDER BY sampled_at DESC
     LIMIT 1
),
status AS (
    SELECT l.*,
           g.sampled_at AS lag_sampled_at,
           g.consumer_lag,
           g.unknown_partitions,
           cfg.lag_threshold,
           cfg.latency_threshold,
           CASE WHEN g.consumer_lag IS NULL OR cfg.lag_threshold IS NULL THEN 'UNKNOWN'
                WHEN g.consumer_lag > cfg.lag_threshold THEN 'ALERT'
                ELSE 'OK' END                                              AS lag_status,
           CASE WHEN l.p95_latency_seconds IS NULL OR cfg.latency_threshold IS NULL
                     THEN 'UNKNOWN'
                WHEN l.p95_latency_seconds > cfg.latency_threshold THEN 'ALERT'
                ELSE 'OK' END                                              AS latency_status
      FROM cfg
      LEFT JOIN latest l ON true
      LEFT JOIN lag g ON true
)
SELECT batch_id,
       recorded_at,
       round(extract(epoch FROM now() - recorded_at)::numeric, 1) AS seconds_since_last_batch,
       input_rows,
       processed_rows_per_sec,
       consumer_lag,
       lag_sampled_at,
       unknown_partitions,
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
