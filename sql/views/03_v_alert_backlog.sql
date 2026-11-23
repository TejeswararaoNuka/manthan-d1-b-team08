-- Open alerts (= alerts with no analyst feedback, D-12) with their age.
-- is_overdue uses ops.health_config.backlog_overdue_minutes (no threshold hard-coded here).
DROP VIEW IF EXISTS sg.v_alert_backlog;

CREATE VIEW sg.v_alert_backlog AS
WITH cfg AS (
    SELECT (SELECT (value #>> '{}')::numeric
              FROM ops.health_config
             WHERE key = 'backlog_overdue_minutes') AS overdue_minutes
)
SELECT a.alert_id,
       a.rule_id,
       a.rule_version,
       a.severity,
       a.card_token,
       a.event_ids,
       a.txn_status,
       a.event_time,
       a.alert_time,
       now() - a.alert_time                                          AS age,
       round(extract(epoch FROM now() - a.alert_time) / 60.0, 1)     AS age_minutes,
       coalesce(extract(epoch FROM now() - a.alert_time) / 60.0 > cfg.overdue_minutes, false)
                                                                     AS is_overdue
  FROM sg.alerts a
 CROSS JOIN cfg
 WHERE a.status = 'OPEN';
