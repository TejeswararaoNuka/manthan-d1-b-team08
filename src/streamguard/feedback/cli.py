"""Analyst feedback CLI: list open alerts, label them TP/FP, report daily precision per rule.

    python -m streamguard.feedback open [--rule VELOCITY] [--limit 20]
    python -m streamguard.feedback label <alert_id or prefix> TP|FP --analyst NAME [--comment ..]
    python -m streamguard.feedback report [--days 7]

Reads go through the dash layer (scripts/build_dash_layer.py) as superset_ro; labels are
written as feedback_writer (D-05) to the table dash.source_status says is current:
sg.alert_feedback (Real), dash_feedback.alert_feedback (Fallback) or demo.alert_feedback (Demo,
when sg.alerts is empty; every output then says "(demo data)"). If a role cannot log in, the
.env user is used and a warning is printed (docs/feedback_loop.md).
"""

from __future__ import annotations

import argparse
import re
import sys

import psycopg2
from psycopg2 import sql

from streamguard.common.settings import load_env, pg_connect

FEEDBACK_TARGET = {
    "Real": ("sg", "alert_feedback"),
    "Fallback": ("dash_feedback", "alert_feedback"),
    "Demo": ("demo", "alert_feedback"),
}
LABELS = {"TP": "TRUE_POSITIVE", "FP": "FALSE_POSITIVE"}
PREFIX = re.compile(r"^[0-9a-f]{8,64}$")


class CliError(Exception):
    pass


def connect_as(env, dbname: str, role: str, password_key: str):
    """Log in as a least-privilege role; fall back to the .env user with a warning."""
    try:
        conn = psycopg2.connect(
            host=env["POSTGRES_HOST"],
            port=int(env["POSTGRES_PORT"]),
            user=role,
            password=env.get(password_key, ""),
            dbname=dbname,
            connect_timeout=10,
            application_name="streamguard-feedback",
        )
        return conn
    except psycopg2.OperationalError as exc:
        reason = str(exc).strip().splitlines()[-1] if str(exc).strip() else "login failed"
        print(
            f"WARNING: cannot log in as {role} ({reason}); using the .env user instead "
            "(see docs/feedback_loop.md)",
            file=sys.stderr,
        )
        return pg_connect(env, dbname, app="streamguard-feedback")


def _modes(cur) -> dict[str, str]:
    try:
        cur.execute("SELECT source, mode FROM dash.source_status")
    except psycopg2.errors.UndefinedTable:
        raise CliError(
            "the dash layer is missing: run python scripts/build_dash_layer.py"
        ) from None
    return dict(cur.fetchall())


def _tag(modes: dict[str, str]) -> str:
    return " (demo data)" if modes.get("alerts") == "Demo" else ""


def _fmt(value) -> str:
    if value is None:
        return "-"
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S") if hasattr(value, "hour") else str(value)
    return str(value)


def _print_table(headers: list[str], rows: list[tuple]) -> None:
    cells = [[_fmt(v) for v in r] for r in rows]
    widths = [max([len(h)] + [len(c[i]) for c in cells]) for i, h in enumerate(headers)]
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
    print("  ".join("-" * w for w in widths))
    for c in cells:
        print("  ".join(v.ljust(w) for v, w in zip(c, widths, strict=True)))


def cmd_open(reader, args) -> None:
    with reader.cursor() as cur:
        modes = _modes(cur)
        query = """SELECT alert_id, rule_id, severity, alert_time, age_minutes, is_overdue,
                          card_token
                     FROM dash."alertBacklog" {where} ORDER BY alert_time LIMIT %s"""
        params: list = [args.limit]
        where = ""
        if args.rule:
            where, params = "WHERE rule_id = %s", [args.rule, args.limit]
        cur.execute(query.format(where=where), params)
        rows = [(r[0][:12], *r[1:]) for r in cur.fetchall()]
        cur.execute(
            'SELECT count(*) FROM dash."alertBacklog"'
            + (" WHERE rule_id = %s" if args.rule else ""),
            [args.rule] if args.rule else [],
        )
        total = cur.fetchone()[0]
    print(f"Open alerts{_tag(modes)}: {total:,} (oldest first, showing {len(rows)})")
    _print_table(["alert_id", "rule", "severity", "alert_time", "age_min", "overdue", "card"], rows)


def cmd_label(reader, writer, args) -> None:
    prefix = args.alert_id.lower()
    if not PREFIX.match(prefix):
        raise CliError("alert_id must be 8-64 lowercase hex characters")
    with reader.cursor() as cur:
        modes = _modes(cur)
        cur.execute(
            'SELECT alert_id, rule_id FROM dash."alerts" WHERE alert_id LIKE %s LIMIT 2',
            (prefix + "%",),
        )
        found = cur.fetchall()
    if not found:
        raise CliError(f"no alert starts with {prefix}")
    if len(found) > 1:
        raise CliError(f"{prefix} matches more than one alert; give more characters")
    alert_id, rule_id = found[0]
    mode = modes.get("alertFeedback", "Real")
    schema, table = FEEDBACK_TARGET[mode]
    label = LABELS[args.label]
    with writer, writer.cursor() as cur:
        cur.execute(
            sql.SQL("""INSERT INTO {}.{} (alert_id, label, analyst, noted_at, comment)
                   VALUES (%s, %s, %s, now(), %s)
                   ON CONFLICT (alert_id) DO UPDATE SET
                       label = EXCLUDED.label, analyst = EXCLUDED.analyst,
                       noted_at = EXCLUDED.noted_at, comment = EXCLUDED.comment""").format(
                sql.Identifier(schema), sql.Identifier(table)
            ),
            (alert_id, label, args.analyst, args.comment),
        )
    print(
        f"Labelled {alert_id[:12]} ({rule_id}) as {label} by {args.analyst} "
        f"in {schema}.{table}{_tag(modes)}"
    )


def cmd_report(reader, args) -> None:
    with reader.cursor() as cur:
        modes = _modes(cur)
        cur.execute(
            """SELECT alert_day, rule_id, alerts, labelled, true_positives, false_positives,
                      unreviewed, precision
                 FROM dash."rulePrecisionDaily"
                WHERE alert_day >= (now() AT TIME ZONE 'UTC')::date - %s
                ORDER BY alert_day DESC, rule_id""",
            (args.days,),
        )
        daily = cur.fetchall()
        cur.execute("""SELECT rule_id, labelled, unreviewed, precision
                 FROM dash."labelCoverage" ORDER BY rule_id""")
        totals = cur.fetchall()
    print(f"Daily precision per rule, last {args.days} days{_tag(modes)}")
    print("precision = true positives / labelled; unreviewed alerts are not counted (D-12)\n")
    _print_table(
        ["day", "rule", "alerts", "labelled", "TP", "FP", "unreviewed", "precision"], daily
    )
    print(f"\nAll time{_tag(modes)}")
    _print_table(["rule", "labelled", "unreviewed", "precision"], totals)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m streamguard.feedback", description=__doc__.splitlines()[0]
    )
    ap.add_argument("--db", help="database (default POSTGRES_DB)")
    sub = ap.add_subparsers(dest="command", required=True)
    p_open = sub.add_parser("open", help="list open alerts, oldest first")
    p_open.add_argument("--rule")
    p_open.add_argument("--limit", type=int, default=20)
    p_label = sub.add_parser("label", help="label an alert TP or FP (relabelling updates it)")
    p_label.add_argument("alert_id", help="full alert_id or a unique prefix of 8+ characters")
    p_label.add_argument("label", choices=sorted(LABELS))
    p_label.add_argument("--analyst", required=True)
    p_label.add_argument("--comment")
    p_report = sub.add_parser("report", help="daily precision per rule")
    p_report.add_argument("--days", type=int, default=7)
    args = ap.parse_args(argv)

    env = load_env()
    dbname = args.db or env.get("POSTGRES_DB")
    reader = connect_as(env, dbname, "superset_ro", "SUPERSET_RO_PASSWORD")
    writer = None
    try:
        if args.command == "open":
            cmd_open(reader, args)
        elif args.command == "report":
            cmd_report(reader, args)
        else:
            writer = connect_as(env, dbname, "feedback_writer", "FEEDBACK_WRITER_PASSWORD")
            cmd_label(reader, writer, args)
    except CliError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        reader.close()
        if writer is not None:
            writer.close()
    return 0
