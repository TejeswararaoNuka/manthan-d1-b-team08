import hashlib
from datetime import UTC, datetime, timedelta, timezone

import pytest

from streamguard.common import alert_id as aid

CARD = "tok_00112233aabbccdd"
EVENT = "1b4e28ba-2fa1-4d3b-a3f5-ef19b5a7633b"


def test_alert_id_is_sha256_of_pipe_joined_rule_and_key():
    expected = hashlib.sha256(f"VELOCITY|{CARD}|2026-11-20T10:15:00Z".encode()).hexdigest()
    ts = datetime(2026, 11, 20, 10, 15, 42, 123000, tzinfo=UTC)
    assert aid.velocity_alert_id(CARD, ts) == expected


def test_velocity_key_floors_to_the_utc_minute_from_any_timezone():
    ist = timezone(timedelta(hours=5, minutes=30))
    ts = datetime(2026, 11, 20, 15, 45, 59, tzinfo=ist)  # 10:15:59Z
    assert aid.minute_floor_iso(ts) == "2026-11-20T10:15:00Z"


def test_rule_specific_keys_follow_d07():
    assert aid.impossible_travel_alert_id(CARD, EVENT) == aid.alert_id(
        "IMPOSSIBLE_TRAVEL", CARD, EVENT
    )
    assert (
        aid.amount_spike_alert_id(EVENT)
        == hashlib.sha256(f"AMOUNT_SPIKE|{EVENT}".encode()).hexdigest()
    )
    assert aid.merchant_blacklist_alert_id(EVENT) != aid.amount_spike_alert_id(EVENT)


def test_naive_timestamp_and_empty_key_are_rejected():
    with pytest.raises(ValueError):
        aid.minute_floor_iso(datetime(2026, 11, 20, 10, 15))
    with pytest.raises(ValueError):
        aid.alert_id("VELOCITY")
