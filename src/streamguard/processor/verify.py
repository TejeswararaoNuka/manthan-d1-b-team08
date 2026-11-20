"""Compare PostgreSQL with a data/expected/<scenario>.json file from the producer.

Only rows for the scenario's card tokens are compared; other traffic is ignored.
Returns a list of human-readable problems (empty = everything matches).
"""

from __future__ import annotations

from typing import Any


def compare(conn, doc: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    cards = doc["card_tokens"]
    with conn.cursor() as cur:
        # enriched rows and lateness
        cur.execute(
            "SELECT event_id::text, is_late FROM sg.enriched_transactions "
            "WHERE card_token = ANY (%s)",
            (cards,),
        )
        actual = dict(cur.fetchall())
        expected = {e["event_id"]: e["is_late"] for e in doc["enriched"]}
        for event_id, late in expected.items():
            if event_id not in actual:
                problems.append(f"enriched: missing event {event_id}")
            elif actual[event_id] != late:
                problems.append(
                    f"enriched: event {event_id} is_late={actual[event_id]}, " f"expected {late}"
                )
        for event_id in actual.keys() - expected.keys():
            problems.append(f"enriched: unexpected event {event_id}")

        # alerts: exactly the expected ones for these cards
        cur.execute(
            """SELECT alert_id, rule_id, rule_version, severity, card_token, event_ids,
                      details ->> 'trigger_event_id'
                 FROM sg.alerts WHERE card_token = ANY (%s)""",
            (cards,),
        )
        alerts = {r[0]: r for r in cur.fetchall()}
        for a in doc["alerts"]:
            row = alerts.get(a["alert_id"])
            if row is None:
                problems.append(f"alerts: missing {a['rule_id']} alert for case {a['case']}")
                continue
            got = {
                "rule_id": row[1],
                "rule_version": row[2],
                "severity": row[3],
                "card_token": row[4],
                "event_ids": list(row[5]),
                "trigger_event_id": row[6],
            }
            for field, value in got.items():
                if value != a[field]:
                    problems.append(
                        f"alerts: case {a['case']} {field}={value!r}, " f"expected {a[field]!r}"
                    )
        expected_ids = {a["alert_id"] for a in doc["alerts"]}
        for alert_id, row in alerts.items():
            if alert_id not in expected_ids:
                problems.append(
                    f"alerts: unexpected {row[1]} alert {alert_id} for {row[4]} "
                    f"(events {list(row[5])})"
                )

        # dead letters at their source positions
        for d in doc["dead_letters"]:
            cur.execute(
                """SELECT reason_code FROM sg.dlq_events
                    WHERE source_topic = %s AND kafka_partition = %s AND kafka_offset = %s""",
                (d["source_topic"], d["partition"], d["offset"]),
            )
            row = cur.fetchone()
            if row is None:
                problems.append(
                    f"dlq: missing {d['reason_code']} for case {d['case']} at "
                    f"p{d['partition']} o{d['offset']}"
                )
            elif row[0] != d["reason_code"]:
                problems.append(
                    f"dlq: case {d['case']} reason {row[0]}, " f"expected {d['reason_code']}"
                )
    return problems
