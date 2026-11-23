"""Build the dashboard layer: schema dash, whose views read real sg/ops data or demo data.

    python scripts/build_dash_layer.py            # database = POSTGRES_DB (never POSTGRES_TEST_DB)

For each source: if the sg/ops table exists and has rows, its dash view reads it ("Real").
Otherwise the view reads a seeded demo.* table ("Demo"). Special cases:
  - streamMetrics: if ops.stream_metrics is missing/empty but enriched data is real, it is
    derived per minute from enriched/dlq rows ("Derived").
  - lagSamples: with real metrics but no ops.lag_samples, lag is "Not Available" (no rows).
  - alertFeedback / opsAlerts follow alerts / streamMetrics: empty is normal for live data
    (no reviews yet, no incidents), so they never fall back to demo when their parent is real.
  - alertFeedback with real alerts but no sg.alert_feedback table: "Fallback" to
    dash_feedback.alert_feedback (same columns and key). When sg.alert_feedback appears,
    `--migrate-feedback` upserts those rows into it and switches back (docs/feedback_loop.md).
Superset datasets use only dash.* (sql/dash/02_datasets.sql), whose names and columns never
change, so rerun this script when real tables arrive. dash."dataSource" says Live/Demo/Mixed.
Never writes to sg.* or ops.*. Runs in one transaction.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from psycopg2 import sql  # noqa: E402

from streamguard.common.settings import load_env, pg_connect  # noqa: E402

DASH_SQL = REPO_ROOT / "sql" / "dash"

# dash view -> (real relation, demo relation, column expressions shared by both)
SOURCES: dict[str, tuple[str, str, list[str]]] = {
    "enrichedTransactions": (
        "sg.enriched_transactions",
        "demo.enriched_transactions",
        [
            "event_id",
            "card_token",
            "merchant_id",
            "merchant_category::text AS merchant_category",
            "amount",
            "currency::text AS currency",
            "country::text AS country",
            "channel",
            "event_time",
            "status",
            "kafka_partition",
            "kafka_offset",
            "ingested_at",
            "processed_at",
            "is_late",
        ],
    ),
    "alerts": (
        "sg.alerts",
        "demo.alerts",
        [
            "alert_id::text AS alert_id",
            "rule_id",
            "rule_version",
            "card_token",
            "event_ids",
            "severity",
            "details",
            "txn_status",
            "event_time",
            "alert_time",
            "status",
        ],
    ),
    "dlqEvents": (
        "sg.dlq_events",
        "demo.dlq_events",
        [
            "id",
            "reason_code",
            "error_detail",
            "raw_value",
            "source_topic",
            "kafka_partition",
            "kafka_offset",
            "failed_at",
        ],
    ),
    "streamMetrics": (
        "ops.stream_metrics",
        "demo.stream_metrics",
        [
            "batch_id",
            "recorded_at",
            "input_rows",
            "processed_rows_per_sec::numeric AS " "processed_rows_per_sec",
            "p95_latency_seconds::numeric AS p95_latency_seconds",
            "dlq_count",
            "alert_count",
            "late_count",
        ],
    ),
    "lagSamples": (
        "ops.lag_samples",
        "demo.lag_samples",
        ["topic", "sampled_at", "consumer_lag", "unknown_partitions"],
    ),
    "ruleConfig": (
        "sg.rule_config",
        "demo.rule_config",
        ["rule_id", "name", "enabled", "severity", "params", "version", "updated_at"],
    ),
    "alertFeedback": (
        "sg.alert_feedback",
        "demo.alert_feedback",
        ["alert_id::text AS alert_id", "label", "analyst", "noted_at", "comment"],
    ),
    "opsAlerts": (
        "ops.alerts",
        "demo.ops_alerts",
        ["id", "raised_at", "severity", "source", "message"],
    ),
    "healthConfig": ("ops.health_config", "demo.health_config", ["key", "value"]),
}
FOLLOWS = {"alertFeedback": "alerts", "opsAlerts": "streamMetrics"}
FALLBACK_FEEDBACK = "dash_feedback.alert_feedback"
# Where the feedback CLI writes, by alertFeedback mode.
FEEDBACK_TARGET = {
    "Real": "sg.alert_feedback",
    "Fallback": FALLBACK_FEEDBACK,
    "Demo": "demo.alert_feedback",
}
FALLBACK_FEEDBACK_DDL = """
CREATE SCHEMA IF NOT EXISTS dash_feedback;
CREATE TABLE IF NOT EXISTS dash_feedback.alert_feedback (
    alert_id  text        PRIMARY KEY CHECK (alert_id ~ '^[0-9a-f]{64}$'),
    label     text        NOT NULL CHECK (label IN ('TRUE_POSITIVE', 'FALSE_POSITIVE')),
    analyst   text        NOT NULL,
    noted_at  timestamptz NOT NULL DEFAULT now(),
    comment   text
);
"""

DERIVED_METRICS = """
SELECT extract(epoch FROM m.minute)::bigint AS batch_id, m.minute AS recorded_at,
       m.input_rows, round(m.input_rows / 60.0, 2) AS processed_rows_per_sec,
       m.p95_latency_seconds, coalesce(d.dlq_count, 0) AS dlq_count,
       coalesce(a.alert_count, 0) AS alert_count, m.late_count
  FROM (SELECT date_trunc('minute', ingested_at) AS minute, count(*) AS input_rows,
               round(percentile_cont(0.95) WITHIN GROUP (
                   ORDER BY extract(epoch FROM processed_at - event_time))::numeric, 3)
                   AS p95_latency_seconds,
               count(*) FILTER (WHERE is_late) AS late_count
          FROM sg.enriched_transactions GROUP BY 1) m
  LEFT JOIN (SELECT date_trunc('minute', failed_at) AS minute, count(*) AS dlq_count
               FROM sg.dlq_events GROUP BY 1) d ON d.minute = m.minute
  LEFT JOIN (SELECT date_trunc('minute', alert_time) AS minute, count(*) AS alert_count
               FROM sg.alerts GROUP BY 1) a ON a.minute = m.minute
"""


def _exists(cur, relation: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
    return cur.fetchone()[0]


def _has_rows(cur, relation: str) -> bool:
    if not _exists(cur, relation):
        return False
    schema, table = relation.split(".")
    cur.execute(
        sql.SQL("SELECT EXISTS (SELECT 1 FROM {}.{})").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
    )
    return cur.fetchone()[0]


def decide_modes(cur) -> dict[str, str]:
    modes: dict[str, str] = {}
    for name, (real, _, _) in SOURCES.items():
        if name in FOLLOWS:
            continue
        modes[name] = "Real" if _has_rows(cur, real) else "Demo"
    if modes["streamMetrics"] == "Demo" and modes["enrichedTransactions"] == "Real":
        modes["streamMetrics"] = "Derived"
    if modes["lagSamples"] == "Demo" and modes["streamMetrics"] != "Demo":
        modes["lagSamples"] = "Not Available"
    for name, parent in FOLLOWS.items():
        real = SOURCES[name][0]
        if modes[parent] == "Demo":
            modes[name] = "Demo"
        elif _exists(cur, real):
            modes[name] = "Real"
        else:
            modes[name] = "Fallback" if name == "alertFeedback" else "Demo"
    return modes


def view_sql(name: str, mode: str) -> sql.Composed:
    real, demo, columns = SOURCES[name]
    target = sql.SQL("CREATE VIEW dash.{} AS ").format(sql.Identifier(name))
    if mode == "Derived":
        return target + sql.SQL(DERIVED_METRICS)
    relation = {"Demo": demo, "Not Available": demo, "Fallback": FALLBACK_FEEDBACK}.get(mode, real)
    schema, table = relation.split(".")
    body = sql.SQL("SELECT {} FROM {}.{}").format(
        sql.SQL(", ").join(sql.SQL(c) for c in columns),  # fixed expressions from this file
        sql.Identifier(schema),
        sql.Identifier(table),
    )
    if mode == "Not Available":
        body += sql.SQL(" WHERE false")  # same columns, no rows
    return target + body


def build(conn) -> list[tuple[str, str, int]]:
    with conn, conn.cursor() as cur:
        cur.execute("SET LOCAL client_min_messages = warning")
        cur.execute((DASH_SQL / "01_demo.sql").read_text(encoding="utf-8"))
        modes = decide_modes(cur)
        if modes["alertFeedback"] == "Fallback":
            cur.execute(FALLBACK_FEEDBACK_DDL)
        cur.execute("DROP SCHEMA IF EXISTS dash CASCADE")
        cur.execute("CREATE SCHEMA dash")
        for name in SOURCES:
            cur.execute(view_sql(name, modes[name]))
        cur.execute((DASH_SQL / "02_datasets.sql").read_text(encoding="utf-8"))
        cur.execute("""CREATE TABLE dash.source_status (
                   source text PRIMARY KEY, mode text NOT NULL, row_count bigint NOT NULL,
                   checked_at timestamptz NOT NULL DEFAULT now())""")
        status = []
        for name in SOURCES:
            cur.execute(sql.SQL("SELECT count(*) FROM dash.{}").format(sql.Identifier(name)))
            rows = cur.fetchone()[0]
            status.append((name, modes[name], rows))
            cur.execute(
                "INSERT INTO dash.source_status (source, mode, row_count) VALUES (%s, %s, %s)",
                (name, modes[name], rows),
            )
        cur.execute("""CREATE VIEW dash."dataSource" AS
               SELECT CASE
                        WHEN count(*) FILTER (WHERE mode = 'Demo') = 0
                          THEN 'Data Source: Live'
                        WHEN count(*) FILTER (WHERE mode <> 'Demo') = 0
                          THEN 'Data Source: Demo (synthetic numbers, not real)'
                        ELSE 'Data Source: Mixed (demo: '
                             || string_agg(source, ', ') FILTER (WHERE mode = 'Demo') || ')'
                      END AS data_source,
                      max(checked_at) AS checked_at
                 FROM dash.source_status""")
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_ro')")
        if cur.fetchone()[0]:
            cur.execute("GRANT USAGE ON SCHEMA dash TO superset_ro")
            cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA dash TO superset_ro")
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'feedback_writer')")
        if cur.fetchone()[0]:
            # The feedback CLI writes labels to the demo/fallback tables in those modes.
            cur.execute("GRANT USAGE ON SCHEMA demo TO feedback_writer")
            cur.execute("GRANT SELECT, INSERT, UPDATE ON demo.alert_feedback TO feedback_writer")
            if _exists(cur, FALLBACK_FEEDBACK):
                cur.execute("GRANT USAGE ON SCHEMA dash_feedback TO feedback_writer")
                cur.execute(
                    "GRANT SELECT, INSERT, UPDATE ON dash_feedback.alert_feedback "
                    "TO feedback_writer"
                )
        else:
            print(
                "WARNING: role superset_ro missing; Superset must use the .env user "
                "(see docs/dashboard_design.md)"
            )
    return status


def migrate_feedback(conn) -> tuple[int, int]:
    """Upsert dash_feedback.alert_feedback into sg.alert_feedback (rows whose alert exists).
    Returns (copied, skipped). The fallback table is kept, not dropped."""
    with conn, conn.cursor() as cur:
        if not (_exists(cur, FALLBACK_FEEDBACK) and _exists(cur, "sg.alert_feedback")):
            raise SystemExit(
                f"ERROR: --migrate-feedback needs both sg.alert_feedback and {FALLBACK_FEEDBACK}"
            )
        cur.execute("""INSERT INTO sg.alert_feedback (alert_id, label, analyst, noted_at, comment)
               SELECT f.alert_id, f.label, f.analyst, f.noted_at, f.comment
                 FROM dash_feedback.alert_feedback f
                 JOIN sg.alerts a ON a.alert_id = f.alert_id
               ON CONFLICT (alert_id) DO UPDATE SET
                   label = EXCLUDED.label, analyst = EXCLUDED.analyst,
                   noted_at = EXCLUDED.noted_at, comment = EXCLUDED.comment""")
        copied = cur.rowcount
        cur.execute("SELECT count(*) FROM dash_feedback.alert_feedback")
        return copied, cur.fetchone()[0] - copied


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", help="database (default POSTGRES_DB)")
    ap.add_argument(
        "--migrate-feedback",
        action="store_true",
        help="copy dash_feedback.alert_feedback into sg.alert_feedback, then rebuild",
    )
    args = ap.parse_args(argv)
    env = load_env()
    dbname = args.db or env.get("POSTGRES_DB")
    if not dbname or dbname == env.get("POSTGRES_TEST_DB"):
        print("ERROR: refusing to build the dashboard layer in the test database", file=sys.stderr)
        return 2
    conn = pg_connect(env, dbname, app="streamguard-dash-layer")
    try:
        if args.migrate_feedback:
            copied, skipped = migrate_feedback(conn)
            print(
                f"Migrated feedback: {copied} rows upserted into sg.alert_feedback"
                + (f", {skipped} skipped (alert not in sg.alerts)" if skipped else "")
            )
        status = build(conn)
    finally:
        conn.close()
    print(f"Dashboard layer built in {dbname}.dash\n")
    print(f"{'Source':<22}| {'Real or Demo':<14}| Rows")
    print(f"{'-' * 22}+{'-' * 15}+{'-' * 10}")
    for name, mode, rows in status:
        print(f"{name:<22}| {mode:<14}| {rows:,}")
    demo = [n for n, m, _ in status if m == "Demo"]
    verdict = "Live" if not demo else ("Demo" if len(demo) == len(status) else "Mixed")
    print(f"\nData Source: {verdict}" + (f" (demo: {', '.join(demo)})" if demo else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
