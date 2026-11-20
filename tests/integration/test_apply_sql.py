"""scripts/apply_sql.py against a real PostgreSQL (POSTGRES_TEST_DB only, never POSTGRES_DB).

Run on PC 1 or any laptop with .env pointing at PC 1:  pytest -m integration
"""

import hashlib
import importlib.util
import uuid
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import errors
from psycopg2.extras import Json

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_apply_sql():
    spec = importlib.util.spec_from_file_location("apply_sql", REPO_ROOT / "scripts/apply_sql.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


apply_sql = _load_apply_sql()


@pytest.fixture(scope="module")
def env():
    values = apply_sql.load_env(REPO_ROOT / ".env")
    test_db = values.get("POSTGRES_TEST_DB")
    if not test_db:
        pytest.skip("POSTGRES_TEST_DB is not set")
    assert test_db != values.get("POSTGRES_DB"), "tests must never use POSTGRES_DB"
    return values


@pytest.fixture(scope="module")
def applied(env):
    """Fresh sg/ops schemas in the test database, then one apply_sql run."""
    conn = apply_sql.connect(env, env["POSTGRES_TEST_DB"])
    with conn, conn.cursor() as cur:
        cur.execute("SET client_min_messages = warning")
        cur.execute("DROP SCHEMA IF EXISTS sg, ops CASCADE")
    conn.close()
    assert apply_sql.main(["--db", env["POSTGRES_TEST_DB"]]) == 0
    return env


@pytest.fixture
def cur(applied):
    """A cursor in a transaction that is always rolled back."""
    conn = apply_sql.connect(applied, applied["POSTGRES_TEST_DB"])
    try:
        with conn.cursor() as c:
            yield c
    finally:
        conn.rollback()
        conn.close()


@contextmanager
def savepoint(cur):
    cur.execute("SAVEPOINT sp")
    try:
        yield
    finally:
        cur.execute("ROLLBACK TO SAVEPOINT sp")


def _counts(cur):
    return {t: _count(cur, t) for t in apply_sql.CONFIG_TABLES}


def _count(cur, table):
    cur.execute(f"SELECT count(*) FROM {table}")
    return cur.fetchone()[0]


def _insert_alert(cur, rule_id="VELOCITY", alert_time="now()"):
    event_id = str(uuid.uuid4())
    alert_id = hashlib.sha256(f"{rule_id}{event_id}".encode()).hexdigest()
    cur.execute(
        f"""INSERT INTO sg.alerts (alert_id, rule_id, rule_version, card_token, event_ids,
                                   severity, details, txn_status, event_time, alert_time)
            VALUES (%s, %s, 1, 'tok_00112233aabbccdd', %s, 'HIGH', %s, 'APPROVED',
                    now(), {alert_time})""",
        (alert_id, rule_id, [event_id], Json({"count": 6})),
    )
    return alert_id


def test_config_tables_are_seeded_from_yaml(cur):
    assert _counts(cur) == {
        "sg.rule_config": 4,
        "sg.dq_config": 4,
        "sg.merchant_blacklist": 20,
        "ops.health_config": 3,
    }
    cur.execute("SELECT value FROM sg.dq_config WHERE key = 'amount_ceiling'")
    assert cur.fetchone()[0] == "500000.00"
    cur.execute("SELECT params FROM sg.rule_config WHERE rule_id = 'VELOCITY'")
    assert cur.fetchone()[0] == {"max_tx": 5, "window_seconds": 60}


def test_second_run_succeeds_and_keeps_runtime_threshold_changes(applied):
    db = applied["POSTGRES_TEST_DB"]
    conn = apply_sql.connect(applied, db)
    try:
        with conn, conn.cursor() as c:
            c.execute(
                'UPDATE sg.rule_config SET params = \'{"max_tx": 9, "window_seconds": 60}\' '
                "WHERE rule_id = 'VELOCITY'"
            )
        assert apply_sql.main(["--db", db]) == 0
        with conn, conn.cursor() as c:
            assert _counts(c)["sg.merchant_blacklist"] == 20
            c.execute(
                "SELECT params->>'max_tx', version FROM sg.rule_config "
                "WHERE rule_id = 'VELOCITY'"
            )
            assert c.fetchone() == ("9", 2)

        assert apply_sql.main(["--db", db, "--reset-config"]) == 0
        with conn, conn.cursor() as c:
            c.execute(
                "SELECT params->>'max_tx', version FROM sg.rule_config "
                "WHERE rule_id = 'VELOCITY'"
            )
            assert c.fetchone() == ("5", 3)
            assert _counts(c)["sg.rule_config"] == 4
    finally:
        conn.close()


def test_noop_rule_update_does_not_bump_version(cur):
    cur.execute(
        "UPDATE sg.rule_config SET enabled = enabled WHERE rule_id = 'AMOUNT_SPIKE' "
        "RETURNING version"
    )
    assert cur.fetchone()[0] == 1


def test_superset_ro_can_read_sg_and_ops_but_not_write(cur):
    cur.execute("SET LOCAL ROLE superset_ro")
    for relation in (
        "sg.alerts",
        "sg.v_top_rules",
        "sg.v_alert_backlog",
        "sg.v_rule_precision_daily",
        "sg.v_alerts_per_minute",
        "ops.v_pipeline_health",
        "ops.stream_metrics",
    ):
        cur.execute(f"SELECT count(*) FROM {relation}")
    with savepoint(cur), pytest.raises(errors.InsufficientPrivilege):
        cur.execute(
            "INSERT INTO sg.merchant_blacklist (merchant_id, reason) "
            "VALUES ('mer_0000abcd', 'x')"
        )


def test_feedback_writer_can_upsert_feedback_and_nothing_else(cur):
    alert_id = _insert_alert(cur)
    cur.execute("SET LOCAL ROLE feedback_writer")
    upsert = """INSERT INTO sg.alert_feedback (alert_id, label, analyst, comment)
                VALUES (%s, %s, 'analyst1', 'checked with cardholder')
                ON CONFLICT (alert_id) DO UPDATE
                SET label = EXCLUDED.label, analyst = EXCLUDED.analyst,
                    noted_at = EXCLUDED.noted_at, comment = EXCLUDED.comment"""
    cur.execute(upsert, (alert_id, "FALSE_POSITIVE"))
    cur.execute(upsert, (alert_id, "TRUE_POSITIVE"))

    with savepoint(cur), pytest.raises(errors.InsufficientPrivilege):
        cur.execute("UPDATE sg.alerts SET status = 'CLOSED'")
    with savepoint(cur), pytest.raises(errors.InsufficientPrivilege):
        cur.execute("SELECT 1 FROM sg.alerts")

    cur.execute("RESET ROLE")
    cur.execute("SELECT label FROM sg.alert_feedback WHERE alert_id = %s", (alert_id,))
    assert cur.fetchone()[0] == "TRUE_POSITIVE"


def test_feedback_closes_alert_and_removes_it_from_backlog(cur):
    alert_id = _insert_alert(cur)
    cur.execute("SELECT count(*) FROM sg.v_alert_backlog WHERE alert_id = %s", (alert_id,))
    assert cur.fetchone()[0] == 1

    cur.execute(
        "INSERT INTO sg.alert_feedback (alert_id, label, analyst) "
        "VALUES (%s, 'TRUE_POSITIVE', 'analyst1')",
        (alert_id,),
    )

    cur.execute("SELECT status FROM sg.alerts WHERE alert_id = %s", (alert_id,))
    assert cur.fetchone()[0] == "CLOSED"
    cur.execute("SELECT count(*) FROM sg.v_alert_backlog WHERE alert_id = %s", (alert_id,))
    assert cur.fetchone()[0] == 0


def test_backlog_marks_alert_overdue_after_configured_minutes(cur):
    old = _insert_alert(cur, alert_time="now() - interval '16 minutes'")
    new = _insert_alert(cur, alert_time="now() - interval '14 minutes'")
    cur.execute("SELECT alert_id, is_overdue FROM sg.v_alert_backlog")
    overdue = dict(cur.fetchall())
    assert overdue[old] is True
    assert overdue[new] is False


def test_precision_counts_unreviewed_alerts_separately_not_as_false_positives(cur):
    tp, fp, _unreviewed = (_insert_alert(cur, "AMOUNT_SPIKE") for _ in range(3))
    cur.execute(
        "INSERT INTO sg.alert_feedback (alert_id, label, analyst) VALUES "
        "(%s, 'TRUE_POSITIVE', 'a'), (%s, 'FALSE_POSITIVE', 'a')",
        (tp, fp),
    )

    cur.execute("""SELECT alerts, labelled, true_positives, false_positives, unreviewed, precision
                     FROM sg.v_rule_precision_daily
                    WHERE rule_id = 'AMOUNT_SPIKE'
                      AND alert_day = (now() AT TIME ZONE 'UTC')::date""")
    alerts, labelled, tps, fps, unreviewed, precision = cur.fetchone()
    assert (alerts, labelled, tps, fps, unreviewed) == (3, 2, 1, 1, 1)
    assert precision == pytest.approx(0.5)


def test_pipeline_health_flags_lag_above_configured_threshold(cur):
    cur.execute("SELECT overall_status FROM ops.v_pipeline_health")
    assert cur.fetchall() == [("UNKNOWN",)]

    cur.execute("UPDATE ops.health_config SET value = '100' WHERE key = 'lag_alert_threshold'")
    cur.execute("""INSERT INTO ops.stream_metrics
                       (batch_id, input_rows, consumer_lag, p95_latency_seconds)
                   VALUES (1, 10, 50, 1.0), (2, 10, 150, 1.0)""")
    cur.execute(
        "SELECT batch_id, lag_status, latency_status, overall_status " "FROM ops.v_pipeline_health"
    )
    assert cur.fetchall() == [(2, "ALERT", "OK", "ALERT")]


def test_enriched_transactions_replay_does_not_duplicate_rows(cur):
    row = (
        str(uuid.uuid4()),
        "tok_00112233aabbccdd",
        "mer_0a1b2c3d",
        "5411",
        "120.50",
        "INR",
        "IN",
        "POS",
        "2026-11-20T10:15:30.123Z",
        "APPROVED",
        1,
        0,
        42,
    )
    sql = """INSERT INTO sg.enriched_transactions
                 (event_id, card_token, merchant_id, merchant_category, amount, currency,
                  country, channel, event_time, status, schema_version, kafka_partition,
                  kafka_offset, ingested_at)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
             ON CONFLICT (event_id) DO NOTHING"""
    cur.execute(sql, row)
    cur.execute(sql, row)
    cur.execute("SELECT count(*) FROM sg.enriched_transactions WHERE event_id = %s", (row[0],))
    assert cur.fetchone()[0] == 1


def test_dlq_rejects_second_row_for_same_kafka_position(cur):
    sql = """INSERT INTO sg.dlq_events (reason_code, raw_value, source_topic, kafka_partition,
                                        kafka_offset, failed_at)
             VALUES ('MALFORMED_JSON', '{bad', 'payments.raw', 3, 77, now())"""
    cur.execute(sql)
    with savepoint(cur), pytest.raises(psycopg2.errors.UniqueViolation):
        cur.execute(sql)
