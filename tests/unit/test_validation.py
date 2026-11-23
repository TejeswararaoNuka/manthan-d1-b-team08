import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from streamguard.common import validation as v

NOW = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
CFG = v.DQConfig(Decimal("500000.00"), frozenset({"INR", "USD", "EUR", "GBP", "SGD", "AED"}), 24)
DROP = object()


def event(**overrides):
    e = {
        "event_id": "1b4e28ba-2fa1-4d3b-a3f5-ef19b5a7633b",
        "card_token": "tok_00112233aabbccdd",
        "merchant_id": "mer_0a1b2c3d",
        "merchant_category": "5411",
        "amount": "120.50",
        "currency": "INR",
        "country": "IN",
        "channel": "POS",
        "event_time": "2026-11-20T09:59:30.123Z",
        "status": "APPROVED",
        "schema_version": 1,
    }
    e.update(overrides)
    return json.dumps({k: val for k, val in e.items() if val is not DROP})


def reason(raw):
    with pytest.raises(v.InvalidEvent) as err:
        v.validate_event(raw, CFG, NOW)
    return err.value.reason_code


def test_valid_event_is_parsed_with_decimal_amount_and_aware_time():
    out = v.validate_event(event(), CFG, NOW)
    assert out["amount"] == Decimal("120.50")
    assert out["event_time"] == datetime(2026, 11, 20, 9, 59, 30, 123000, tzinfo=UTC)


def test_amount_as_json_number_is_read_exactly_without_float():
    raw = event().replace('"120.50"', "120.10")
    assert v.validate_event(raw, CFG, NOW)["amount"] == Decimal("120.10")


@pytest.mark.parametrize("raw", [b"{not json", b"\xff\xfe", b"[1, 2]", None])
def test_unparseable_payload_is_malformed_json(raw):
    assert reason(raw) == v.MALFORMED_JSON


@pytest.mark.parametrize(
    "overrides",
    [
        {"currency": DROP},
        {"amount": None},
        {"card_token": "tok_123"},
        {"card_token": "tok_ABCDEF0123456789"},
        {"merchant_id": "m1"},
        {"merchant_category": 5411},
        {"channel": "MAIL"},
        {"status": "VOID"},
        {"schema_version": 2},
        {"event_time": "2026-11-20 10:00:00"},
        {"event_time": "2026-11-20T10:00:00+05:30"},
        {"amount": "12.345"},
        {"amount": True},
        {"event_id": "not-a-uuid"},
        {"event_id": "6fa459ea-ee8a-3ca4-894e-db77e160355e"},  # UUID version 3
    ],
)
def test_absent_null_or_badly_formatted_field_is_missing_field(overrides):
    assert reason(event(**overrides)) == v.MISSING_FIELD


@pytest.mark.parametrize("amount", ["0.00", "-10.00", "500000.01"])
def test_amount_outside_zero_to_ceiling_is_out_of_range(amount):
    assert reason(event(amount=amount)) == v.AMOUNT_OUT_OF_RANGE


def test_amount_exactly_at_ceiling_is_accepted():
    assert v.validate_event(event(amount="500000.00"), CFG, NOW)["amount"] == CFG.amount_ceiling


def test_currency_outside_allowed_list_is_rejected():
    assert reason(event(currency="JPY")) == v.CURRENCY_NOT_ALLOWED


def test_event_more_than_24h_in_future_is_rejected_but_exactly_24h_is_not():
    future = v.format_event_time(NOW + timedelta(hours=24, milliseconds=1))
    assert reason(event(event_time=future)) == v.FUTURE_EVENT_TIME
    v.validate_event(event(event_time=v.format_event_time(NOW + timedelta(hours=24))), CFG, NOW)


def test_first_failing_rule_in_table_order_wins():
    assert reason(event(amount="-1.00", currency="JPY")) == v.AMOUNT_OUT_OF_RANGE
    assert reason(event(currency="JPY", channel="MAIL")) == v.MISSING_FIELD


def test_format_event_time_writes_milliseconds_and_z():
    ts = datetime(2026, 11, 20, 10, 15, 30, 123456, tzinfo=UTC)
    assert v.format_event_time(ts) == "2026-11-20T10:15:30.123Z"
