"""Each rule in sql/rules/ against PostgreSQL (POSTGRES_TEST_DB), without Spark."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from streamguard.common import alert_id as aid
from streamguard.processor.batch import _load_config
from streamguard.rules.engine import run_rules

pytestmark = pytest.mark.integration

T0 = datetime(2026, 11, 20, 9, 0, tzinfo=UTC)
CARD = "tok_00112233aabbccdd"


def add(
    cur, seconds, *, card=CARD, country="IN", late=False, amount="250.00", merchant="mer_0a1b2c3d"
):
    event_id = str(uuid.uuid4())
    cur.execute(
        """INSERT INTO sg.enriched_transactions
               (event_id, card_token, merchant_id, merchant_category, amount, currency, country,
                channel, event_time, status, schema_version, kafka_partition, kafka_offset,
                ingested_at, is_late)
           VALUES (%s, %s, %s, '5411', %s, 'INR', %s, 'POS', %s, 'APPROVED', 1, 0, 0, now(), %s)""",
        (event_id, card, merchant, amount, country, T0 + timedelta(seconds=seconds), late),
    )
    return event_id


def alerts(cur, new_ids, rule=None):
    rules, _ = _load_config(cur)
    found = run_rules(cur, rules, new_ids)
    return [a for a in found if rule is None or a.rule_id == rule]


@pytest.fixture
def cur(db):
    with db.cursor() as c:
        yield c


# -- velocity ---------------------------------------------------------------------------------


def test_velocity_six_events_in_fifty_seconds_alert_once_with_all_six(cur):
    ids = [add(cur, 10 * i) for i in range(6)]
    (a,) = alerts(cur, ids, "VELOCITY")
    assert a.event_ids == ids
    assert a.trigger_event_id == ids[-1]
    assert a.alert_id == aid.velocity_alert_id(CARD, T0 + timedelta(seconds=50))
    assert a.details["tx_count"] == 6


def test_velocity_exactly_five_in_a_minute_does_not_alert(cur):
    ids = [add(cur, s) for s in (0, 14, 28, 42, 59)]
    assert alerts(cur, ids, "VELOCITY") == []


def test_velocity_sixth_event_exactly_sixty_seconds_after_first_does_not_alert(cur):
    ids = [add(cur, 12 * i) for i in range(6)]
    assert alerts(cur, ids, "VELOCITY") == []


def test_velocity_ignores_late_events(cur):
    ids = [add(cur, 10 * i, late=(i == 0)) for i in range(6)]
    assert alerts(cur, ids, "VELOCITY") == []


def test_velocity_out_of_order_arrival_completes_a_burst_ending_at_an_older_event(cur):
    existing = [add(cur, 10 * i) for i in range(1, 6)]  # +10..+50, already processed
    newcomer = add(cur, 5)  # arrives later, belongs before them
    (a,) = alerts(cur, [newcomer], "VELOCITY")
    assert a.trigger_event_id == existing[-1]
    assert len(a.event_ids) == 6


def test_velocity_burst_spanning_two_minutes_dedups_per_minute(cur):
    ids = [add(cur, 30 + 5 * i) for i in range(8)]  # +30..+65s: triggers at +55, +60, +65
    found = alerts(cur, ids, "VELOCITY")
    assert {a.alert_id for a in found} == {
        aid.velocity_alert_id(CARD, T0 + timedelta(seconds=55)),
        aid.velocity_alert_id(CARD, T0 + timedelta(seconds=60)),
    }


# -- impossible travel ------------------------------------------------------------------------


def test_travel_two_countries_twenty_minutes_apart_alerts(cur):
    first, second = add(cur, 0, country="IN"), add(cur, 1200, country="GB")
    (a,) = alerts(cur, [first, second], "IMPOSSIBLE_TRAVEL")
    assert a.event_ids == [first, second]
    assert a.details["from_country"] == "IN" and a.details["to_country"] == "GB"
    assert a.alert_id == aid.impossible_travel_alert_id(CARD, second)


@pytest.mark.parametrize("gap_minutes", [30, 31])
def test_travel_thirty_or_more_minutes_apart_does_not_alert(cur, gap_minutes):
    ids = [add(cur, 0, country="IN"), add(cur, 60 * gap_minutes, country="GB")]
    assert alerts(cur, ids, "IMPOSSIBLE_TRAVEL") == []


def test_travel_same_country_does_not_alert(cur):
    ids = [add(cur, 0), add(cur, 60)]
    assert alerts(cur, ids, "IMPOSSIBLE_TRAVEL") == []


def test_travel_ignores_late_events(cur):
    ids = [add(cur, 0, country="IN"), add(cur, 300, country="GB", late=True)]
    assert alerts(cur, ids, "IMPOSSIBLE_TRAVEL") == []


def test_travel_out_of_order_arrival_alerts_on_the_later_event(cur):
    later = add(cur, 600, country="GB")
    earlier = add(cur, 0, country="IN")  # arrives second
    (a,) = alerts(cur, [earlier], "IMPOSSIBLE_TRAVEL")
    assert a.trigger_event_id == later and a.event_ids == [earlier, later]


# -- amount spike -----------------------------------------------------------------------------


def set_baseline(cur, avg="1000.00", count=20, card=CARD):
    cur.execute(
        "INSERT INTO sg.card_baseline (card_token, avg_amount_30d, tx_count_30d) "
        "VALUES (%s, %s, %s)",
        (card, avg, count),
    )


def test_spike_six_times_average_alerts(cur):
    set_baseline(cur)
    e = add(cur, 0, amount="6000.00")
    (a,) = alerts(cur, [e], "AMOUNT_SPIKE")
    assert a.alert_id == aid.amount_spike_alert_id(e)
    assert a.details["ratio"] == "6.00"


@pytest.mark.parametrize("amount", ["4990.00", "5000.00"])
def test_spike_at_or_below_five_times_does_not_alert(cur, amount):
    set_baseline(cur)
    assert alerts(cur, [add(cur, 0, amount=amount)], "AMOUNT_SPIKE") == []


def test_spike_skipped_without_history(cur):
    set_baseline(cur, avg="0.00", count=0)
    assert alerts(cur, [add(cur, 0, amount="9000.00")], "AMOUNT_SPIKE") == []
    other = "tok_99887766554433aa"
    assert alerts(cur, [add(cur, 0, card=other, amount="9000.00")], "AMOUNT_SPIKE") == []


def test_spike_still_evaluates_late_events(cur):
    set_baseline(cur)
    assert len(alerts(cur, [add(cur, 0, amount="6000.00", late=True)], "AMOUNT_SPIKE")) == 1


# -- merchant blacklist -----------------------------------------------------------------------


def test_blacklisted_merchant_alerts_even_when_late(cur):
    e = add(cur, 0, merchant="mer_bad00001", late=True)
    (a,) = alerts(cur, [e], "MERCHANT_BLACKLIST")
    assert a.details["merchant_id"] == "mer_bad00001"
    assert a.alert_id == aid.merchant_blacklist_alert_id(e)


def test_normal_merchant_does_not_alert(cur):
    assert alerts(cur, [add(cur, 0)], "MERCHANT_BLACKLIST") == []


# -- config -----------------------------------------------------------------------------------


def test_disabled_rule_produces_no_alerts(cur):
    cur.execute("UPDATE sg.rule_config SET enabled = false WHERE rule_id = 'MERCHANT_BLACKLIST'")
    assert alerts(cur, [add(cur, 0, merchant="mer_bad00001")]) == []


def test_threshold_change_in_rule_config_applies_without_code_change(cur):
    cur.execute(
        "UPDATE sg.rule_config SET params = jsonb_set(params, '{max_tx}', '3') "
        "WHERE rule_id = 'VELOCITY'"
    )
    ids = [add(cur, 10 * i) for i in range(4)]
    assert len(alerts(cur, ids, "VELOCITY")) == 1
