"""Payment-event validation: the data-quality rules and reason codes from CLAUDE.md.

Pure Python, shared by the producer's scenario tests and the processor. Checks run in the
order of the reason-code table, so an event with several problems gets the first code:
MALFORMED_JSON -> MISSING_FIELD -> AMOUNT_OUT_OF_RANGE -> CURRENCY_NOT_ALLOWED ->
FUTURE_EVENT_TIME. LATE_EVENT is not decided here (it depends on the watermark, D-04).
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

MALFORMED_JSON = "MALFORMED_JSON"
MISSING_FIELD = "MISSING_FIELD"
AMOUNT_OUT_OF_RANGE = "AMOUNT_OUT_OF_RANGE"
CURRENCY_NOT_ALLOWED = "CURRENCY_NOT_ALLOWED"
FUTURE_EVENT_TIME = "FUTURE_EVENT_TIME"

SCHEMA_VERSION = 1
CHANNELS = frozenset({"POS", "ECOM", "ATM"})
STATUSES = frozenset({"APPROVED", "DECLINED", "PENDING"})

_PATTERNS = {
    "card_token": re.compile(r"^tok_[0-9a-f]{16}$"),
    "merchant_id": re.compile(r"^mer_[0-9a-f]{8}$"),
    "merchant_category": re.compile(r"^[0-9]{4}$"),
    "currency": re.compile(r"^[A-Z]{3}$"),
    "country": re.compile(r"^[A-Z]{2}$"),
}
_EVENT_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$")
_AMOUNT_STR = re.compile(r"^-?\d+(\.\d{1,2})?$")

REQUIRED_FIELDS = (
    "event_id",
    "card_token",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "country",
    "channel",
    "event_time",
    "status",
    "schema_version",
)


class InvalidEvent(ValueError):
    def __init__(self, reason_code: str, detail: str):
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


@dataclass(frozen=True)
class DQConfig:
    amount_ceiling: Decimal
    allowed_currencies: frozenset[str]
    max_future_hours: int

    @classmethod
    def from_values(cls, values: Mapping[str, Any]) -> DQConfig:
        """Build from sg.dq_config rows ({key: jsonb value})."""
        return cls(
            amount_ceiling=Decimal(str(values["amount_ceiling"])),
            allowed_currencies=frozenset(values["allowed_currencies"]),
            max_future_hours=int(values["max_future_hours"]),
        )


def format_event_time(ts: datetime) -> str:
    """ISO 8601 UTC with milliseconds and Z, e.g. 2026-11-20T10:15:30.123Z."""
    if ts.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    ts = ts.astimezone(UTC)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def parse_event_time(value: str) -> datetime:
    if not isinstance(value, str) or not _EVENT_TIME.match(value):
        raise ValueError(f"event_time must be ISO 8601 UTC ending in Z: {value!r}")
    return datetime.fromisoformat(value)


def _parse_amount(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, str)):
        raise ValueError(f"amount must be a number or numeric string: {value!r}")
    if isinstance(value, str) and not _AMOUNT_STR.match(value):
        raise ValueError(f"amount must have at most 2 decimal places: {value!r}")
    try:
        amount = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"amount is not a number: {value!r}") from None
    if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")):
        raise ValueError(f"amount must have at most 2 decimal places: {value!r}")
    return amount.quantize(Decimal("0.01"))


def _check_format(event: Mapping[str, Any]) -> dict[str, Any]:
    for field in REQUIRED_FIELDS:
        if event.get(field) is None:
            raise InvalidEvent(MISSING_FIELD, f"{field} is missing or null")
    out = dict(event)
    try:
        parsed = uuid.UUID(str(event["event_id"]))
        if parsed.version != 4:
            raise ValueError
        out["event_id"] = str(parsed)
    except ValueError:
        raise InvalidEvent(MISSING_FIELD, "event_id is not a UUID4") from None
    for field, pattern in _PATTERNS.items():
        if not isinstance(event[field], str) or not pattern.match(event[field]):
            raise InvalidEvent(MISSING_FIELD, f"{field} has the wrong format")
    if event["channel"] not in CHANNELS:
        raise InvalidEvent(MISSING_FIELD, "channel must be POS, ECOM or ATM")
    if event["status"] not in STATUSES:
        raise InvalidEvent(MISSING_FIELD, "status must be APPROVED, DECLINED or PENDING")
    version = event["schema_version"]
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        raise InvalidEvent(MISSING_FIELD, f"schema_version must be {SCHEMA_VERSION}")
    try:
        out["amount"] = _parse_amount(event["amount"])
        out["event_time"] = parse_event_time(event["event_time"])
    except ValueError as exc:
        raise InvalidEvent(MISSING_FIELD, str(exc)) from None
    return out


def validate_event(raw: bytes | str | None, cfg: DQConfig, now: datetime) -> dict[str, Any]:
    """Parse and validate one raw Kafka value.

    Returns the event with `amount` as Decimal and `event_time` as aware datetime.
    Raises InvalidEvent with the reason code on the first rule that fails.
    """
    if raw is None:
        raise InvalidEvent(MALFORMED_JSON, "empty payload")
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        event = json.loads(text, parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEvent(MALFORMED_JSON, str(exc)) from None
    if not isinstance(event, dict):
        raise InvalidEvent(MALFORMED_JSON, "payload is not a JSON object")

    out = _check_format(event)
    if out["amount"] <= 0 or out["amount"] > cfg.amount_ceiling:
        raise InvalidEvent(
            AMOUNT_OUT_OF_RANGE, f"amount {out['amount']} not in (0, {cfg.amount_ceiling}]"
        )
    if out["currency"] not in cfg.allowed_currencies:
        raise InvalidEvent(CURRENCY_NOT_ALLOWED, f"currency {out['currency']} is not allowed")
    if out["event_time"] > now + timedelta(hours=cfg.max_future_hours):
        raise InvalidEvent(
            FUTURE_EVENT_TIME, f"event_time more than {cfg.max_future_hours}h in the future"
        )
    return out
