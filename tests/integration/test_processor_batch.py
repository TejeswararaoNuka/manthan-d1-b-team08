"""process_records end to end against POSTGRES_TEST_DB, using the producer's scenario as the
acceptance test (no Spark, no Kafka: a fake publisher records what would be sent)."""

import json
import zlib
from datetime import UTC, datetime, timedelta

import pytest

from streamguard.common.validation import format_event_time
from streamguard.processor.batch import (
    ALERTS_TOPIC,
    DLQ_TOPIC,
    BatchContext,
    Record,
    process_records,
)
from streamguard.processor.verify import compare
from streamguard.producer import db as producer_db
from streamguard.producer.catalog import Merchant
from streamguard.producer.scenarios import build_scenario, default_anchor, expected_document

pytestmark = pytest.mark.integration

NOW = datetime(2026, 11, 20, 10, 41, 17, tzinfo=UTC)
TOPIC = "payments.raw"


def scenario_batches(db, phase1_split=None):
    """Scenario messages as Kafka records (partition = hash of key, offsets per partition) in
    batches: phase 1 (optionally split in two), then phase 2. Returns (batches, expected doc)."""
    sc = build_scenario("all", default_anchor(NOW), NOW, Merchant("mer_bad00001", "5999"))
    producer_db.upsert_baselines(db, ((c, a, n) for c, (a, n) in sc.baselines.items()))
    next_offset: dict[int, int] = {}
    records, deliveries = [], {}
    for i, m in enumerate(sc.messages):
        p = zlib.crc32(m.key.encode()) % 6
        off = next_offset.get(p, 0)
        next_offset[p] = off + 1
        deliveries[i] = (p, off)
        records.append((m.phase, Record(TOPIC, p, off, m.key.encode(), m.value)))
    phase1 = [r for ph, r in records if ph == 1]
    phase2 = [r for ph, r in records if ph == 2]
    batches = [phase1] if phase1_split is None else [phase1[:phase1_split], phase1[phase1_split:]]
    batches.append(phase2)
    with db.cursor() as cur:
        cur.execute("SELECT rule_id, version, severity FROM sg.rule_config")
        rules = {r: {"version": v, "severity": s} for r, v, s in cur.fetchall()}
    db.commit()
    doc = expected_document(
        sc, topic=TOPIC, rules=rules, deliveries=deliveries, phase_gap=0, generated_at=NOW
    )
    return batches, doc


def run(db, publisher, batches, first_batch_id=0):
    ctx = BatchContext(conn=db, publisher=publisher, now=lambda: NOW, lag=lambda r: 0)
    return [process_records(b, first_batch_id + i, ctx) for i, b in enumerate(batches)]


def count(db, sql):
    with db.cursor() as cur:
        cur.execute(sql)
        value = cur.fetchone()[0]
    db.rollback()
    return value


@pytest.mark.parametrize("split", [None, 7, 20], ids=["one-batch", "split-at-7", "split-at-20"])
def test_scenario_produces_exactly_the_expected_alerts_dead_letters_and_rows(db, publisher, split):
    batches, doc = scenario_batches(db, split)
    run(db, publisher, batches)
    assert compare(db, doc) == []


def test_alerts_and_dead_letters_are_published_to_kafka_topics(db, publisher):
    batches, doc = scenario_batches(db)
    run(db, publisher, batches)

    alert_msgs = [json.loads(v) for _, v in publisher.on(ALERTS_TOPIC)]
    assert {m["alert_id"] for m in alert_msgs} == {a["alert_id"] for a in doc["alerts"]}
    assert all(k == json.loads(v)["alert_id"] for k, v in publisher.on(ALERTS_TOPIC))
    assert set(alert_msgs[0]) >= {
        "alert_id",
        "rule_id",
        "rule_version",
        "card_token",
        "event_ids",
        "severity",
        "details",
        "event_time",
        "alert_time",
    }
    dlq = [json.loads(v) for _, v in publisher.on(DLQ_TOPIC)]
    assert sorted(m["reason_code"] for m in dlq) == sorted(
        d["reason_code"] for d in doc["dead_letters"]
    )
    malformed = next(m for m in dlq if m["reason_code"] == "MALFORMED_JSON")
    assert malformed["raw_value"].startswith("{") and malformed["source_topic"] == TOPIC


def test_replay_of_the_same_input_creates_no_new_rows_and_the_same_alert_ids(db, publisher):
    batches, doc = scenario_batches(db)
    run(db, publisher, batches)
    tables = ("sg.enriched_transactions", "sg.alerts", "sg.dlq_events", "sg.agg_1min")
    before = {t: count(db, f"SELECT count(*) FROM {t}") for t in tables}
    alert_ids = count(db, "SELECT array_agg(alert_id ORDER BY alert_id) FROM sg.alerts")

    # Replay = offset reset / new checkpoint: the stored watermark is reset too.
    with db, db.cursor() as cur:
        cur.execute("DELETE FROM ops.processor_state")
    replay = run(db, publisher, scenario_batches(db, 10)[0])

    assert {t: count(db, f"SELECT count(*) FROM {t}") for t in tables} == before
    assert count(db, "SELECT array_agg(alert_id ORDER BY alert_id) FROM sg.alerts") == alert_ids
    assert sum(r.new_alerts for r in replay) == 0
    assert compare(db, doc) == []


def test_rerun_of_a_committed_batch_writes_nothing_and_republishes(db, publisher):
    batches, _ = scenario_batches(db)
    first = run(db, publisher, batches[:1])[0]
    sent = len(publisher.on(ALERTS_TOPIC))
    rows = count(db, "SELECT count(*) FROM sg.enriched_transactions")

    again = run(db, publisher, batches[:1])[0]  # same batch_id 0, e.g. crash before Kafka

    assert again.rerun and again.inserted == 0
    assert count(db, "SELECT count(*) FROM sg.enriched_transactions") == rows
    assert sorted(again.alerts) == sorted(first.alerts)
    assert len(publisher.on(ALERTS_TOPIC)) == 2 * sent


def test_late_event_is_stored_with_is_late_and_counted_in_metrics(db, publisher):
    batches, doc = scenario_batches(db)
    results = run(db, publisher, batches)
    late = [e for e in doc["enriched"] if e["is_late"]]
    assert len(late) == 1 and results[-1].late == 1
    assert count(db, "SELECT late_count FROM ops.stream_metrics WHERE batch_id = 1") == 1


def test_future_dated_valid_event_cannot_push_the_watermark_past_wall_clock(db, publisher):
    def event(seconds, n):
        e = {
            "event_id": f"00000000-0000-4000-8000-{n:012x}",
            "card_token": "tok_00112233aabbccdd",
            "merchant_id": "mer_0a1b2c3d",
            "merchant_category": "5411",
            "amount": "10.00",
            "currency": "INR",
            "country": "IN",
            "channel": "POS",
            "event_time": format_event_time(NOW + timedelta(seconds=seconds)),
            "status": "APPROVED",
            "schema_version": 1,
        }
        return Record(TOPIC, 0, n, b"tok_00112233aabbccdd", json.dumps(e).encode())

    results = run(db, publisher, [[event(23 * 3600, 1)], [event(-30, 2)]])
    assert results[0].watermark == NOW - timedelta(seconds=120)
    assert results[1].late == 0


def test_agg_1min_holds_absolute_counts_that_survive_a_replay(db, publisher):
    batches, _ = scenario_batches(db)
    run(db, publisher, batches)
    totals = "SELECT sum(tx_count), sum(alert_count) FROM sg.agg_1min"
    with db.cursor() as cur:
        cur.execute(totals)
        tx, alerts = cur.fetchone()
    db.rollback()
    assert tx == count(db, "SELECT count(*) FROM sg.enriched_transactions")
    assert alerts == count(db, "SELECT count(*) FROM sg.alerts")

    with db, db.cursor() as cur:
        cur.execute("DELETE FROM ops.processor_state")
    run(db, publisher, batches)
    with db.cursor() as cur:
        cur.execute(totals)
        assert cur.fetchone() == (tx, alerts)


def test_each_batch_writes_metrics_and_state(db, publisher):
    batches, doc = scenario_batches(db)
    run(db, publisher, batches)
    assert count(db, "SELECT count(*) FROM ops.stream_metrics") == 2
    assert count(db, "SELECT sum(dlq_count) FROM ops.stream_metrics") == len(doc["dead_letters"])
    assert count(db, "SELECT sum(alert_count) FROM ops.stream_metrics") == len(doc["alerts"])
    assert count(db, "SELECT last_batch_id FROM ops.processor_state") == 1
    # Scenario events are ~40 min old at NOW, so p95 latency is far above the 30 s threshold.
    with db.cursor() as cur:
        cur.execute(
            "SELECT batch_id, lag_status, latency_status, overall_status FROM ops.v_pipeline_health"
        )
        assert cur.fetchone() == (1, "OK", "ALERT", "ALERT")
