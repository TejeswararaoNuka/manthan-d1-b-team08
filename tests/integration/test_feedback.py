"""Feedback CLI and precision through the dash layer, on POSTGRES_TEST_DB only.

The dash layer is built here with build() (its command line refuses the test database, so the
real dashboard can never be pointed at test data)."""

import hashlib
import importlib.util
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from streamguard.feedback import cli

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("dash", REPO_ROOT / "scripts/build_dash_layer.py")
dash = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dash)


@pytest.fixture
def fb(db, test_env):
    """Test DB with role logins allowed (CONNECT is revoked from PUBLIC on it)."""
    with db, db.cursor() as cur:
        cur.execute(
            f'GRANT CONNECT ON DATABASE "{test_env["POSTGRES_TEST_DB"]}" '
            "TO superset_ro, feedback_writer"
        )
    return db


def add_alert(db, rule="VELOCITY", alert_id=None):
    alert_id = alert_id or hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    with db, db.cursor() as cur:
        cur.execute(
            """INSERT INTO sg.alerts (alert_id, rule_id, rule_version, card_token, event_ids,
                                      severity, event_time)
               VALUES (%s, %s, 1, 'tok_00112233aabbccdd', '{x}', 'HIGH', now())""",
            (alert_id, rule),
        )
    return alert_id


def run(test_env, *args):
    return cli.main(["--db", test_env["POSTGRES_TEST_DB"], *args])


def one(db, query, params=()):
    with db.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    db.rollback()
    return row


def test_label_tp_closes_the_alert_and_relabelling_updates_it(fb, test_env, capsys):
    alert = add_alert(fb)
    dash.build(fb)

    assert run(test_env, "label", alert[:10], "TP", "--analyst", "asha") == 0
    assert one(fb, "SELECT status FROM sg.alerts WHERE alert_id = %s", (alert,)) == ("CLOSED",)
    assert run(test_env, "label", alert, "FP", "--analyst", "ravi", "--comment", "known") == 0

    assert one(fb, "SELECT label, analyst, comment FROM sg.alert_feedback") == (
        "FALSE_POSITIVE",
        "ravi",
        "known",
    )
    captured = capsys.readouterr()
    assert "WARNING" not in captured.err  # wrote as feedback_writer, read as superset_ro
    assert "(demo data)" not in captured.out


def test_label_rejects_unknown_ambiguous_and_short_prefixes(fb, test_env, capsys):
    add_alert(fb, alert_id="abcdef12" + "0" * 56)
    add_alert(fb, alert_id="abcdef12" + "1" * 56)
    dash.build(fb)

    assert run(test_env, "label", "abcdef12", "TP", "--analyst", "a") == 1
    assert run(test_env, "label", "ffffffff", "TP", "--analyst", "a") == 1
    assert run(test_env, "label", "abc", "TP", "--analyst", "a") == 1
    err = capsys.readouterr().err
    assert "more than one alert" in err and "no alert starts with" in err and "8-64" in err
    assert one(fb, "SELECT count(*) FROM sg.alert_feedback") == (0,)


def test_report_precision_is_true_positives_over_labelled_and_null_without_labels(
    fb, test_env, capsys
):
    alerts = [add_alert(fb, "MERCHANT_BLACKLIST") for _ in range(4)]
    add_alert(fb, "AMOUNT_SPIKE")
    dash.build(fb)
    for alert, label in zip(alerts[:3], ["TP", "TP", "FP"], strict=True):
        run(test_env, "label", alert, label, "--analyst", "a")
    capsys.readouterr()

    assert run(test_env, "report", "--days", "1") == 0
    out = capsys.readouterr().out
    assert (
        one(
            fb,
            """SELECT labelled, unreviewed, precision FROM dash."rulePrecisionDaily"
            WHERE rule_id = 'MERCHANT_BLACKLIST'""",
        )
        == (3, 1, Decimal("0.6667"))
    )
    assert one(
        fb, """SELECT precision FROM dash."rulePrecisionDaily" WHERE rule_id = 'AMOUNT_SPIKE'"""
    ) == (None,)
    assert "0.6667" in out and "(demo data)" not in out


def test_without_real_alerts_cli_labels_demo_alerts_and_says_demo_data(fb, test_env, capsys):
    dash.build(fb)  # sg.alerts is empty -> Demo
    demo_alert = one(fb, 'SELECT alert_id FROM dash."alertBacklog" LIMIT 1')[0]

    assert run(test_env, "label", demo_alert[:16], "TP", "--analyst", "a") == 0
    assert run(test_env, "report") == 0
    assert run(test_env, "open", "--limit", "1") == 0

    out = capsys.readouterr().out
    assert out.count("(demo data)") >= 3
    assert one(fb, "SELECT label FROM demo.alert_feedback WHERE alert_id = %s", (demo_alert,)) == (
        "TRUE_POSITIVE",
    )
    assert one(fb, "SELECT count(*) FROM sg.alert_feedback") == (0,)


def test_missing_feedback_table_uses_fallback_then_migrates_back(fb, test_env):
    alert = add_alert(fb)
    with fb, fb.cursor() as cur:
        cur.execute("ALTER TABLE sg.alert_feedback RENAME TO alert_feedback_hidden")
    try:
        status = dict((s, m) for s, m, _ in dash.build(fb))
        assert status["alertFeedback"] == "Fallback"
        assert run(test_env, "label", alert, "TP", "--analyst", "a") == 0
        assert one(fb, "SELECT label FROM dash_feedback.alert_feedback") == ("TRUE_POSITIVE",)
    finally:
        with fb, fb.cursor() as cur:
            cur.execute("ALTER TABLE sg.alert_feedback_hidden RENAME TO alert_feedback")

    copied, skipped = dash.migrate_feedback(fb)
    status = dict((s, m) for s, m, _ in dash.build(fb))

    assert (copied, skipped) == (1, 0)
    assert status["alertFeedback"] == "Real"
    assert one(fb, "SELECT label FROM sg.alert_feedback WHERE alert_id = %s", (alert,)) == (
        "TRUE_POSITIVE",
    )
    with fb, fb.cursor() as cur:
        cur.execute("DROP SCHEMA dash_feedback CASCADE")
