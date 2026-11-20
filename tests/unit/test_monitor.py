from datetime import UTC, datetime, timedelta

from streamguard.metrics.monitor import AlertTracker, compute_lag

T = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)


def test_lag_is_end_offset_minus_next_offset_to_process_per_partition():
    total, per = compute_lag({0: 100, 1: 50, 2: 10}, {0: 89, 1: 49, 2: 9})
    assert per == {"0": 10, "1": 0, "2": 0}
    assert total == 10


def test_partition_without_processed_offset_is_unknown_not_counted():
    total, per = compute_lag({0: 100, 1: 50}, {0: 99})
    assert per == {"0": 0, "1": None}
    assert total == 0


def test_lag_is_unknown_when_no_partition_has_been_processed():
    assert compute_lag({0: 5}, {}) == (None, {"0": None})


def test_replay_from_earlier_offsets_shows_as_lag():
    assert compute_lag({0: 1000}, {0: 99})[0] == 900


def test_alert_raises_once_reminds_after_repeat_and_resolves():
    tracker = AlertTracker(repeat=timedelta(minutes=10))
    assert tracker.evaluate("lag", 6000, 5000, T) == "RAISE"
    assert tracker.evaluate("lag", 7000, 5000, T + timedelta(seconds=15)) is None
    assert tracker.evaluate("lag", 7000, 5000, T + timedelta(minutes=10)) == "REMIND"
    assert tracker.evaluate("lag", 100, 5000, T + timedelta(minutes=11)) == "RESOLVE"
    assert tracker.evaluate("lag", 100, 5000, T + timedelta(minutes=12)) is None


def test_value_equal_to_threshold_or_unknown_does_not_alert():
    tracker = AlertTracker(repeat=timedelta(minutes=10))
    assert tracker.evaluate("lag", 5000, 5000, T) is None
    assert tracker.evaluate("lag", None, 5000, T) is None
    assert tracker.evaluate("latency", 99, None, T) is None
