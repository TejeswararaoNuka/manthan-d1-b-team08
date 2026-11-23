-- Daily precision per rule (D-12): TP / (TP + FP), by the UTC day the alert was created.
-- Unreviewed alerts are reported separately and never counted as FP; precision is NULL
-- until at least one alert of that day and rule is labelled.
DROP VIEW IF EXISTS sg.v_rule_precision_daily;

CREATE VIEW sg.v_rule_precision_daily AS
SELECT (a.alert_time AT TIME ZONE 'UTC')::date                         AS alert_day,
       a.rule_id,
       count(*)                                                        AS alerts,
       count(f.alert_id)                                               AS labelled,
       count(*) FILTER (WHERE f.label = 'TRUE_POSITIVE')               AS true_positives,
       count(*) FILTER (WHERE f.label = 'FALSE_POSITIVE')              AS false_positives,
       count(*) FILTER (WHERE f.alert_id IS NULL)                      AS unreviewed,
       round(count(*) FILTER (WHERE f.label = 'TRUE_POSITIVE')::numeric
             / nullif(count(f.alert_id), 0), 4)                        AS precision
  FROM sg.alerts a
  LEFT JOIN sg.alert_feedback f ON f.alert_id = a.alert_id
 GROUP BY 1, 2;
