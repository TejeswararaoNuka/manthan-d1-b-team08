"""Run the rule SQL in sql/rules/ for the events a batch inserted, and build alert candidates.

Thresholds come from sg.rule_config (re-read every batch by the caller), never from code.
alert_id is computed here from each rule's dedup key (D-07, streamguard.common.alert_id).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from functools import cache
from typing import Any

from psycopg2.extras import Json, execute_values

from streamguard.common import alert_id as aid
from streamguard.common.settings import REPO_ROOT

RULES_DIR = REPO_ROOT / "sql" / "rules"


@dataclass(frozen=True)
class Candidate:
    rule_id: str
    card_token: str
    trigger_event_id: str
    event_time: datetime
    txn_status: str
    event_ids: list[str]
    details: dict[str, Any]
    alert_id: str


@dataclass(frozen=True)
class RuleSpec:
    sql_file: str
    params: Callable[[dict[str, Any]], dict[str, Any]]
    key: Callable[[dict[str, Any]], str]


RULES: dict[str, RuleSpec] = {
    "VELOCITY": RuleSpec(
        "01_velocity.sql",
        lambda p: {"max_tx": int(p["max_tx"]), "window_seconds": int(p["window_seconds"])},
        lambda r: aid.velocity_alert_id(r["card_token"], r["event_time"]),
    ),
    "IMPOSSIBLE_TRAVEL": RuleSpec(
        "02_impossible_travel.sql",
        lambda p: {"window_minutes": int(p["window_minutes"])},
        lambda r: aid.impossible_travel_alert_id(r["card_token"], r["trigger_event_id"]),
    ),
    "AMOUNT_SPIKE": RuleSpec(
        "03_amount_spike.sql",
        lambda p: {"multiplier": Decimal(str(p["multiplier"]))},
        lambda r: aid.amount_spike_alert_id(r["trigger_event_id"]),
    ),
    "MERCHANT_BLACKLIST": RuleSpec(
        "04_merchant_blacklist.sql",
        lambda p: {},
        lambda r: aid.merchant_blacklist_alert_id(r["trigger_event_id"]),
    ),
}


@cache
def rule_sql(sql_file: str) -> str:
    return (RULES_DIR / sql_file).read_text(encoding="utf-8")


def run_rule(cur, rule_id: str, params: dict[str, Any], new_event_ids: list[str]):
    """Rows (as dicts) the rule's SQL returns for these newly inserted events."""
    if not new_event_ids:
        return []
    spec = RULES[rule_id]
    cur.execute(rule_sql(spec.sql_file), {**spec.params(params), "new_event_ids": new_event_ids})
    columns = [c.name for c in cur.description]
    return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def run_rules(
    cur, rule_config: dict[str, dict[str, Any]], new_event_ids: list[str]
) -> list[Candidate]:
    """Alert candidates from every enabled rule, one per alert_id (the earliest trigger wins:
    e.g. several velocity triggers in one minute are one alert)."""
    by_id: dict[str, Candidate] = {}
    for rule_id, spec in RULES.items():
        cfg = rule_config.get(rule_id)
        if not cfg or not cfg["enabled"]:
            continue
        for row in run_rule(cur, rule_id, cfg["params"], new_event_ids):
            alert = spec.key(row)
            if alert in by_id:
                continue
            by_id[alert] = Candidate(
                rule_id=rule_id,
                card_token=row["card_token"],
                trigger_event_id=row["trigger_event_id"],
                event_time=row["event_time"],
                txn_status=row["txn_status"],
                event_ids=list(row["event_ids"]),
                details={**row["details"], "trigger_event_id": row["trigger_event_id"]},
                alert_id=alert,
            )
    return list(by_id.values())


def insert_alerts(
    cur, candidates: list[Candidate], rule_config: dict[str, dict[str, Any]]
) -> list[str]:
    """Insert candidates into sg.alerts (ON CONFLICT DO NOTHING keeps the first alert and any
    analyst state, ADR-01). Returns the alert_ids that were actually new."""
    if not candidates:
        return []
    returned = execute_values(
        cur,
        """INSERT INTO sg.alerts (alert_id, rule_id, rule_version, card_token, event_ids,
                                  severity, details, txn_status, event_time)
           VALUES %s
           ON CONFLICT (alert_id) DO NOTHING
           RETURNING alert_id""",
        [
            (
                c.alert_id,
                c.rule_id,
                rule_config[c.rule_id]["version"],
                c.card_token,
                c.event_ids,
                rule_config[c.rule_id]["severity"],
                Json(c.details),
                c.txn_status,
                c.event_time,
            )
            for c in candidates
        ],
        fetch=True,
    )
    return [r[0] for r in returned]
