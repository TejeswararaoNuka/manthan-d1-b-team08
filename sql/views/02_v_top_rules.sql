-- Alert volume per rule. Every configured rule appears, including rules with no alerts.
DROP VIEW IF EXISTS sg.v_top_rules;

CREATE VIEW sg.v_top_rules AS
SELECT r.rule_id,
       r.name,
       r.enabled,
       r.severity,
       r.version                                                            AS rule_version,
       count(a.alert_id)                                                    AS alerts_total,
       count(a.alert_id) FILTER (WHERE a.alert_time >= now() - interval '24 hours')
                                                                            AS alerts_last_24h,
       count(a.alert_id) FILTER (WHERE a.alert_time >= now() - interval '1 hour')
                                                                            AS alerts_last_1h,
       count(a.alert_id) FILTER (WHERE a.status = 'OPEN')                   AS open_alerts,
       max(a.alert_time)                                                    AS last_alert_time
  FROM sg.rule_config r
  LEFT JOIN sg.alerts a ON a.rule_id = r.rule_id
 GROUP BY r.rule_id, r.name, r.enabled, r.severity, r.version;
