"""PostgreSQL reads/writes the producer needs: config, blacklist, card baselines."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any

from psycopg2.extras import execute_values

from streamguard.common.config_defaults import load_dq_config, load_rules


def read_rule_config(conn) -> dict[str, dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT rule_id, name, enabled, severity, params, version FROM sg.rule_config")
        return {
            r[0]: {"name": r[1], "enabled": r[2], "severity": r[3], "params": r[4], "version": r[5]}
            for r in cur.fetchall()
        }


def read_dq_config(conn) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT key, value FROM sg.dq_config")
        return dict(cur.fetchall())


def read_blacklist(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT merchant_id FROM sg.merchant_blacklist ORDER BY merchant_id")
        return [r[0] for r in cur.fetchall()]


def config_differences(
    rules: dict[str, dict[str, Any]], dq: dict[str, Any], config_dir: Path
) -> list[str]:
    """Where the database config differs from config/*.yaml (scenario expectations need the
    defaults). Empty list means they match."""
    diffs = []
    for r in load_rules(config_dir / "rules.yaml"):
        db = rules.get(r.rule_id)
        if db is None:
            diffs.append(f"rule {r.rule_id} is missing")
            continue
        want = {"enabled": r.enabled, "params": r.params}
        have = {"enabled": db["enabled"], "params": db["params"]}
        if want != have:
            diffs.append(f"rule {r.rule_id}: database {have} != default {want}")
    for key, want in load_dq_config(config_dir / "dq.yaml").items():
        have = dq.get(key)
        if key == "amount_ceiling" and have is not None:
            same = Decimal(str(have)) == Decimal(want)
        elif isinstance(want, list) and isinstance(have, list):
            same = sorted(have) == sorted(want)
        else:
            same = have == want
        if not same:
            diffs.append(f"dq_config {key}: database {have!r} != default {want!r}")
    return diffs


def upsert_baselines(conn, rows: Iterable[tuple[str, Decimal, int]]) -> int:
    """Write absolute 30-day values (idempotent). rows: (card_token, avg_amount_30d, count)."""
    rows = list(rows)
    with conn, conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO sg.card_baseline (card_token, avg_amount_30d, tx_count_30d)
               VALUES %s
               ON CONFLICT (card_token) DO UPDATE
               SET avg_amount_30d = EXCLUDED.avg_amount_30d,
                   tx_count_30d = EXCLUDED.tx_count_30d,
                   updated_at = now()""",
            rows,
            page_size=1000,
        )
    return len(rows)
