"""Deterministic alert IDs (D-07).

alert_id = sha256("<rule_id>|<key part 1>|<key part 2>...") as lowercase hex, UTF-8 encoded.
Timestamps in keys are UTC minute floors written as YYYY-MM-DDTHH:MM:00Z.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime


def alert_id(rule_id: str, *key_parts: str) -> str:
    if not key_parts:
        raise ValueError("an alert key needs at least one part")
    return hashlib.sha256("|".join((rule_id, *key_parts)).encode("utf-8")).hexdigest()


def minute_floor_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:00Z")


def velocity_alert_id(card_token: str, trigger_event_time: datetime) -> str:
    return alert_id("VELOCITY", card_token, minute_floor_iso(trigger_event_time))


def impossible_travel_alert_id(card_token: str, trigger_event_id: str) -> str:
    return alert_id("IMPOSSIBLE_TRAVEL", card_token, trigger_event_id)


def amount_spike_alert_id(event_id: str) -> str:
    return alert_id("AMOUNT_SPIKE", event_id)


def merchant_blacklist_alert_id(event_id: str) -> str:
    return alert_id("MERCHANT_BLACKLIST", event_id)
