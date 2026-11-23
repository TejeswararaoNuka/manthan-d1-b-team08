-- Alerts per minute (UTC), split by rule and severity. Superset sums over the split it needs.
DROP VIEW IF EXISTS sg.v_alerts_per_minute;

CREATE VIEW sg.v_alerts_per_minute AS
SELECT date_trunc('minute', a.alert_time) AS minute,
       a.rule_id,
       a.severity,
       count(*)                           AS alert_count
  FROM sg.alerts a
 GROUP BY 1, 2, 3;
