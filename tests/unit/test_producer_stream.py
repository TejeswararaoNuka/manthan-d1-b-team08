import json
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from streamguard.common.validation import DQConfig, validate_event
from streamguard.producer.catalog import build_cards, build_merchants
from streamguard.producer.seed_baseline import baseline_for
from streamguard.producer.stream import EventStream, StreamConfig, target_rate

CEILING = Decimal("500000.00")
DQ = DQConfig(CEILING, frozenset({"INR", "USD", "EUR", "GBP", "SGD", "AED"}), 24)
START = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
BLACKLIST = [f"mer_bad{n:05d}" for n in range(1, 21)]


def make_stream(seed=42, cards=200):
    cfg = StreamConfig(
        amount_ceiling=CEILING,
        velocity_max_tx=5,
        velocity_window_seconds=60,
        blacklist_rate=0.01,
        travel_rate=0.01,
    )
    return EventStream(build_cards(seed, cards), build_merchants(seed, 100), BLACKLIST, cfg, seed)


def run(stream, n, per_second):
    events = []
    for i in range(n):
        ev = stream.next_event(START + timedelta(seconds=i / per_second))
        if ev:
            events.append(ev)
    return events


def test_catalog_has_requested_counts_and_valid_unique_ids():
    cards, merchants = build_cards(42, 5000), build_merchants(42, 2000)
    assert len({c.token for c in cards}) == 5000
    assert len({m.merchant_id for m in merchants}) == 2000
    assert all(re.fullmatch(r"tok_[0-9a-f]{16}", c.token) for c in cards)
    assert all(re.fullmatch(r"mer_[0-9a-f]{8}", m.merchant_id) for m in merchants)
    assert not any(m.merchant_id.startswith("mer_bad") for m in merchants)


def test_no_card_number_like_digit_runs_in_catalog_or_events():
    blob = " ".join(c.token for c in build_cards(42, 5000))
    blob += json.dumps(run(make_stream(), 2000, 200))
    assert not re.search(r"(?<!\d)\d{13,19}(?!\d)", blob)


def test_every_generated_event_is_schema_valid():
    now = START + timedelta(minutes=10)
    for ev in run(make_stream(), 2000, 200):
        validate_event(json.dumps(ev).encode(), DQ, now)


def test_same_seed_gives_same_stream_and_different_seed_does_not():
    assert run(make_stream(7), 300, 200) == run(make_stream(7), 300, 200)
    assert run(make_stream(7), 300, 200) != run(make_stream(8), 300, 200)


def test_no_card_exceeds_velocity_threshold_in_any_sliding_window():
    events = run(make_stream(cards=200), 6000, 1000)  # far above the cap: 200*5/60 = 16/s
    by_card = {}
    for ev in events:
        by_card.setdefault(ev["card_token"], []).append(datetime.fromisoformat(ev["event_time"]))
    for times in by_card.values():
        for t in times:
            assert sum(1 for s in times if t - timedelta(seconds=60) < s <= t) <= 5


def test_capped_stream_reports_skipped_events():
    stream = make_stream(cards=10)
    events = run(stream, 500, 1000)
    assert len(events) < 500
    assert stream.capped == 500 - len(events)


def test_amounts_are_two_decimal_strings_within_ceiling():
    for ev in run(make_stream(), 1000, 200):
        amount = Decimal(ev["amount"])
        assert amount == amount.quantize(Decimal("0.01"))
        assert 0 < amount <= CEILING


def test_some_events_hit_blacklisted_merchants_and_foreign_countries():
    events = run(make_stream(), 3000, 50)
    assert any(ev["merchant_id"] in BLACKLIST for ev in events)
    homes = {c.token: c.home_country for c in build_cards(42, 200)}
    assert any(ev["country"] != homes[ev["card_token"]] for ev in events)


def test_burst_schedule_switches_rate_at_the_configured_times():
    args = dict(rate=200, burst_rate=1000, burst_every=60, burst_seconds=5)
    assert target_rate(0, **args) == (200, False)
    assert target_rate(59.9, **args) == (200, False)
    assert target_rate(60, **args) == (1000, True)
    assert target_rate(64.9, **args) == (1000, True)
    assert target_rate(65, **args) == (200, False)
    assert target_rate(120.5, **args) == (1000, True)
    assert target_rate(60, 200, 1000, 0, 5) == (200, False)


def test_baselines_are_deterministic_and_zero_for_cards_without_history():
    cards = build_cards(42, 5000)
    first = [baseline_for(c, 42, CEILING) for c in cards]
    assert first == [baseline_for(c, 42, CEILING) for c in cards]
    for card, (_, avg, count) in zip(cards, first, strict=True):
        assert count == card.history_count
        assert (avg == 0) == (count == 0)
    assert any(count == 0 for _, _, count in first)
