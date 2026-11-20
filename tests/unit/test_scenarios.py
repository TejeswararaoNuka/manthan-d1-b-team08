import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from streamguard.common import alert_id as aid
from streamguard.common.config_defaults import load_dq_config
from streamguard.common.validation import DQConfig, InvalidEvent, validate_event
from streamguard.producer.catalog import Merchant
from streamguard.producer.scenarios import (
    DLQ,
    DUPLICATE,
    ENRICHED_LATE,
    GROUPS,
    SCENARIOS,
    build_scenario,
    default_anchor,
    expected_document,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 11, 20, 10, 41, 17, 500000, tzinfo=UTC)
ANCHOR = default_anchor(NOW)
BLACKLISTED = Merchant("mer_bad00001", "5999")
DQ = DQConfig.from_values(load_dq_config(REPO_ROOT / "config" / "dq.yaml"))
WATERMARK = timedelta(seconds=120)


def _scenario(name="all", anchor=ANCHOR):
    return build_scenario(name, anchor, NOW, BLACKLISTED)


def reference_alerts(sc):
    """Independent implementation of the four rules with the D-07/D-14 semantics: sliding
    (t-60s, t] count > 5; another country within (t-30min, t]; amount > 5x baseline;
    blacklisted merchant. Duplicates dropped (first wins); late events are excluded from
    the windowed rules; watermark = newest event time seen in earlier phases - 120s."""
    seen, rows = set(), []
    newest_by_phase = {}
    for m in sc.messages:
        try:
            e = validate_event(m.value, DQ, NOW)
        except InvalidEvent:
            continue
        if e["event_id"] in seen:
            continue
        seen.add(e["event_id"])
        earlier = [t for p, t in newest_by_phase.items() if p < m.phase]
        late = bool(earlier) and e["event_time"] < max(earlier) - WATERMARK
        e["is_late"] = late
        rows.append(e)
        newest_by_phase[m.phase] = max(
            newest_by_phase.get(m.phase, e["event_time"]), e["event_time"]
        )
    found = set()
    for e in rows:
        card = [r for r in rows if r["card_token"] == e["card_token"] and not r["is_late"]]
        if not e["is_late"]:
            window = [
                r
                for r in card
                if e["event_time"] - timedelta(seconds=60) < r["event_time"] <= e["event_time"]
            ]
            if len(window) > 5:
                found.add(aid.velocity_alert_id(e["card_token"], e["event_time"]))
            earlier = [r for r in card if r["event_time"] < e["event_time"]]
            prev = max(earlier, key=lambda r: r["event_time"]) if earlier else None
            if (
                prev is not None
                and prev["country"] != e["country"]
                and e["event_time"] - timedelta(minutes=30) < prev["event_time"]
            ):
                found.add(aid.impossible_travel_alert_id(e["card_token"], e["event_id"]))
        base = sc.baselines.get(e["card_token"])
        if base and base[1] > 0 and e["amount"] > 5 * base[0]:
            found.add(aid.amount_spike_alert_id(e["event_id"]))
        if e["merchant_id"] == BLACKLISTED.merchant_id:
            found.add(aid.merchant_blacklist_alert_id(e["event_id"]))
    return found, rows


@pytest.mark.parametrize("name", SCENARIOS)
def test_expected_alerts_match_an_independent_rule_implementation(name):
    sc = _scenario(name)
    found, _ = reference_alerts(sc)
    assert found == {a.alert_id for a in sc.alerts}


def test_all_scenario_expects_one_alert_per_rule_and_seven_boundary_non_alerts():
    sc = _scenario()
    assert sorted(a.rule_id for a in sc.alerts) == [
        "AMOUNT_SPIKE",
        "IMPOSSIBLE_TRAVEL",
        "MERCHANT_BLACKLIST",
        "VELOCITY",
    ]
    assert {n.case for n in sc.no_alerts} == {
        "velocity_exactly_5",
        "velocity_6th_at_60s",
        "travel_30min_exact",
        "travel_31min",
        "spike_4_99x",
        "spike_5x_exact",
        "late_event",
    }


def test_every_message_gets_its_expected_validation_outcome():
    for m in _scenario().messages:
        if m.expect == DLQ:
            with pytest.raises(InvalidEvent) as err:
                validate_event(m.value, DQ, NOW)
            assert err.value.reason_code == m.reason_code, m.case
        else:
            validate_event(m.value, DQ, NOW)


def test_dead_letters_cover_every_dlq_reason_code():
    reasons = {m.reason_code for m in _scenario().messages if m.expect == DLQ}
    assert reasons == {
        "MALFORMED_JSON",
        "MISSING_FIELD",
        "AMOUNT_OUT_OF_RANGE",
        "CURRENCY_NOT_ALLOWED",
        "FUTURE_EVENT_TIME",
    }


def test_velocity_burst_is_six_events_within_fifty_seconds():
    burst = [a for a in _scenario("velocity").alerts if a.rule_id == "VELOCITY"][0]
    assert len(burst.event_ids) == 6
    assert burst.event_time - ANCHOR == timedelta(seconds=110)  # first at +60s


def test_late_event_is_five_minutes_behind_the_watermark_and_flagged_late():
    sc = _scenario()
    _, rows = reference_alerts(sc)
    late = [m for m in sc.messages if m.expect == ENRICHED_LATE]
    assert len(late) == 1
    newest_phase1 = max(m.event_time for m in sc.messages if m.phase == 1 and m.event_time)
    assert (newest_phase1 - WATERMARK) - late[0].event_time == timedelta(minutes=5)
    assert [r["is_late"] for r in rows if r["event_id"] == late[0].event_id] == [True]


def test_out_of_order_event_within_two_minutes_is_not_late():
    sc = _scenario("ordering")
    _, rows = reference_alerts(sc)
    newer, older = [m for m in sc.messages if m.case == "out_of_order_90s"]
    assert newer.event_time - older.event_time == timedelta(seconds=90)
    assert not any(r["is_late"] for r in rows)


def test_duplicates_are_sent_once_in_the_same_batch_and_once_in_the_next_phase():
    dups = [m for m in _scenario("dedup").messages if m.expect == DUPLICATE]
    assert sorted(m.phase for m in dups) == [1, 2]


def test_same_anchor_gives_identical_messages():
    a, b = _scenario(), _scenario()
    assert [m.value for m in a.messages] == [m.value for m in b.messages]


def test_new_anchor_gives_new_cards_and_event_ids():
    a, b = _scenario(), _scenario(anchor=ANCHOR + timedelta(minutes=1))
    assert not {m.key for m in a.messages} & {m.key for m in b.messages}
    assert not {m.event_id for m in a.messages} & {m.event_id for m in b.messages} - {None}


def test_scenario_events_never_lie_in_the_future_except_the_future_dlq_case():
    for m in _scenario().messages:
        if m.event_time and m.case != "future_event_time":
            assert m.event_time <= NOW


def test_expected_document_lists_dead_letter_positions_and_exact_counts():
    sc = _scenario()
    deliveries = {i: (i % 6, 1000 + i) for i in range(len(sc.messages))}
    rules = {
        r: {"version": 1, "severity": "HIGH"}
        for r in ("VELOCITY", "IMPOSSIBLE_TRAVEL", "AMOUNT_SPIKE", "MERCHANT_BLACKLIST")
    }
    doc = expected_document(
        sc, topic="payments.raw", rules=rules, deliveries=deliveries, phase_gap=15, generated_at=NOW
    )

    assert doc["counts"] == {
        "messages_sent": 43,
        "enriched_rows": 35,
        "late_rows": 1,
        "duplicates_dropped": 2,
        "dead_letters": 6,
        "alerts": 4,
    }
    assert all(d["partition"] is not None and d["offset"] >= 1000 for d in doc["dead_letters"])
    assert doc["baselines"] == {
        next(iter(sc.baselines)): {"avg_amount_30d": "1000.00", "tx_count_30d": 20}
    }
    json.dumps(doc)  # serialisable


def test_scenario_rejects_unknown_name_and_naive_times():
    with pytest.raises(ValueError):
        build_scenario("nope", ANCHOR, NOW, BLACKLISTED)
    with pytest.raises(ValueError):
        build_scenario("all", ANCHOR.replace(tzinfo=None), NOW, BLACKLISTED)


def test_groups_and_scenarios_line_up():
    assert set(SCENARIOS) == {"all", *GROUPS}
