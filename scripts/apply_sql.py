"""Apply StreamGuard SQL to a PostgreSQL database: DDL, then views, then seed, then config.

Runs from any laptop (Windows included) against PC 1: the connection comes from .env
(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD). Needs only
requirements.txt; no psql, no Docker.

    python scripts/apply_sql.py                          # database = POSTGRES_DB
    python scripts/apply_sql.py --db streamguard_test    # tests use POSTGRES_TEST_DB
    python scripts/apply_sql.py --reset-config           # rewrite config/*.yaml over runtime edits

Everything runs in one transaction: on any error nothing is applied. Safe to re-run.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

import psycopg2  # noqa: E402
from dotenv import dotenv_values  # noqa: E402
from psycopg2.extras import Json  # noqa: E402

from streamguard.common.config_defaults import (  # noqa: E402
    ConfigError,
    load_dq_config,
    load_health_config,
    load_rules,
)

SQL_PHASES = ("ddl", "views", "seed")
CONFIG_TABLES = ("sg.rule_config", "sg.dq_config", "sg.merchant_blacklist", "ops.health_config")


def sql_files(sql_root: Path) -> list[Path]:
    """All files to run, in order: each phase in SQL_PHASES, files by name within a phase."""
    return [path for phase in SQL_PHASES for path in sorted((sql_root / phase).glob("*.sql"))]


def load_env(env_file: Path) -> dict[str, str]:
    """Values from env_file, overridden by variables already set in the process environment."""
    values = {k: v for k, v in dotenv_values(env_file).items() if v is not None}
    values.update(os.environ)
    return values


def connect(env: Mapping[str, str], dbname: str):
    missing = [
        k
        for k in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD")
        if not env.get(k)
    ]
    if missing:
        raise SystemExit(f"Missing in .env / environment: {', '.join(missing)}")
    return psycopg2.connect(
        host=env["POSTGRES_HOST"],
        port=int(env["POSTGRES_PORT"]),
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
        dbname=dbname,
        connect_timeout=10,
        application_name="streamguard-apply-sql",
    )


def _upsert_clause(reset: bool, columns: tuple[str, ...]) -> str:
    if not reset:
        return "DO NOTHING"
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
    return f"DO UPDATE SET {sets}"


def apply_config(cur, config_dir: Path, env: Mapping[str, str], reset: bool) -> None:
    """Insert config/*.yaml defaults. Existing rows are kept unless reset is True."""
    rules = load_rules(config_dir / "rules.yaml")
    dq = load_dq_config(config_dir / "dq.yaml")
    health = load_health_config(config_dir / "dq.yaml", dict(env))

    # The rule_config trigger bumps version / updated_at only when a value really changes.
    rule_sql = (
        "INSERT INTO sg.rule_config (rule_id, name, enabled, severity, params) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (rule_id) "
        + _upsert_clause(reset, ("name", "enabled", "severity", "params"))
    )
    for r in rules:
        cur.execute(rule_sql, (r.rule_id, r.name, r.enabled, r.severity, Json(r.params)))

    for table, values in (("sg.dq_config", dq), ("ops.health_config", health)):
        kv_sql = f"INSERT INTO {table} (key, value) VALUES (%s, %s) ON CONFLICT (key) "
        if reset:
            kv_sql += (
                "DO UPDATE SET value = EXCLUDED.value, updated_at = now() "
                f"WHERE {table}.value IS DISTINCT FROM EXCLUDED.value"
            )
        else:
            kv_sql += "DO NOTHING"
        for key, value in values.items():
            cur.execute(kv_sql, (key, Json(value)))


def apply_all(conn, sql_root: Path, config_dir: Path, env: Mapping[str, str], reset: bool):
    """Run every SQL file and load config in one transaction. Returns config table counts."""
    with conn:  # commit on success, rollback on any exception
        with conn.cursor() as cur:
            # Hide "already exists, skipping" notices; missing-role warnings still show.
            cur.execute("SET LOCAL client_min_messages = warning")
            for path in sql_files(sql_root):
                print(f"  apply {path.relative_to(sql_root.parent).as_posix()}")
                cur.execute(path.read_text(encoding="utf-8-sig"))
            print(f"  load  config/*.yaml ({'reset' if reset else 'keep existing values'})")
            apply_config(cur, config_dir, env, reset)
            counts = {}
            for table in CONFIG_TABLES:
                cur.execute(f"SELECT count(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", help="database name (default: POSTGRES_DB from .env)")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--sql-dir", type=Path, default=REPO_ROOT / "sql")
    parser.add_argument("--config-dir", type=Path, default=REPO_ROOT / "config")
    parser.add_argument(
        "--reset-config",
        action="store_true",
        help="overwrite config rows with config/*.yaml (default: keep existing rows)",
    )
    args = parser.parse_args(argv)

    env = load_env(args.env_file)
    dbname = args.db or env.get("POSTGRES_DB")
    if not dbname:
        parser.error("no --db given and POSTGRES_DB is not set")

    print(f"Applying SQL to {dbname} on {env.get('POSTGRES_HOST')}:{env.get('POSTGRES_PORT')}")
    try:
        conn = connect(env, dbname)
    except psycopg2.OperationalError as exc:
        print(f"ERROR: cannot connect: {exc}".rstrip(), file=sys.stderr)
        return 1
    try:
        counts = apply_all(conn, args.sql_dir, args.config_dir, env, args.reset_config)
    except (psycopg2.Error, ConfigError, OSError) as exc:
        print(f"ERROR (rolled back, nothing applied): {exc}".rstrip(), file=sys.stderr)
        return 1
    finally:
        for notice in conn.notices:  # e.g. "WARNING: role superset_ro does not exist"
            print(f"  {notice.strip()}")
        conn.close()

    print("Done. Config table row counts:")
    for table, count in counts.items():
        print(f"  {table:<24} {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
