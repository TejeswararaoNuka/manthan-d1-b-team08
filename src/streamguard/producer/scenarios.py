"""Fixed, known event sequences with the exact alerts and dead letters they must produce.

build_scenario() is pure: everything is derived from the anchor time, so the same anchor
gives byte-identical events (replay test), and a new anchor gives new event IDs and card
tokens (runs never interfere with each other in PostgreSQL).

Preconditions (D-14): the processor starts from a fresh checkpoint, or runs the bounded
replay test (ADR-01); thresholds are the config/*.yaml defaults. Event times lie in
[anchor, anchor + 2200s]; the default anchor is 40 minutes ago, floored to the minute.

Phase 1 sends valid events in event-time order. After a pause (so the watermark moves),
phase 2 sends the late event, a cross-batch duplicate and the data-quality failures.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from streamguard.common import alert_id as aid
from streamguard.common import validation as v
from streamguard.producer.catalog import Merchant
from streamguard.producer.stream import make_event

GROUPS = ("velocity", "travel", "spike", "blacklist", "dedup", "ordering", "late", "dq")
SCENARIOS = ("all", *GROUPS)

NORMAL_MERCHANT = Merchant("mer_5ce0a001", "5411")
SPIKE_BASELINE = (Decimal("1000.00"), 20)  # avg_amount_30d, tx_count_30d for the spike card

ENRICHED = "ENRICHED"
ENRICHED_LATE = "ENRICHED_LATE"
DUPLICATE = "DUPLICATE"
DLQ = "DLQ"

_CARD_NUMBER_LIKE = re.compile(r"\d{13}")


@dataclass
class Message:
    case: str
    group: str
    phase: int
    key: str
    value: bytes
    expect: str  # ENRICHED | ENRICHED_LATE | DUPLICATE | DLQ
    event_id: str | None = None
    event_time: datetime | None = None
    reason_code: str | None = None  # for DLQ


@dataclass
class ExpectedAlert:
    case: str
    rule_id: str
    card_token: str
    event_ids: list[str]
    trigger_event_id: str
    event_time: datetime
    alert_id: str


@dataclass
class NoAlert:
    case: str
    rule_id: str
    card_token: str
    event_ids: list[str]
    why: str


@dataclass
class Scenario:
    name: str
    anchor: datetime
    messages: list[Message] = field(default_factory=list)
    alerts: list[ExpectedAlert] = field(default_factory=list)
    no_alerts: list[NoAlert] = field(default_factory=list)
    baselines: dict[str, tuple[Decimal, int]] = field(default_factory=dict)


def default_anchor(now: datetime) -> datetime:
    return (now - timedelta(minutes=40)).replace(second=0, microsecond=0)


class _Builder:
    def __init__(self, anchor: datetime, now: datetime, blacklisted: Merchant):
        self.anchor = anchor
        self.now = now
        self.blacklisted = blacklisted
        self.tag = anchor.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.sc = Scenario(name="", anchor=anchor)
        self._phase1: list[tuple[datetime, int, Message]] = []
        self._phase2: list[Message] = []  # dead letters
        self._phase2_dups: list[Message] = []  # sent first in phase 2
        self._phase2_late: list[Message] = []  # then the late event
        self._seq = 0

    # -- deterministic identifiers -------------------------------------------------------
    def _digest(self, text: str) -> str:
        return hashlib.sha256(f"streamguard-scenario|{self.tag}|{text}".encode()).hexdigest()

    def card(self, case: str) -> str:
        digest = self._digest(f"card|{case}")
        for start in range(0, len(digest) - 16):
            candidate = digest[start : start + 16]
            if not _CARD_NUMBER_LIKE.search(candidate):
                return "tok_" + candidate
        raise AssertionError("no usable token")  # pragma: no cover

    def event_id(self, case: str, n: int) -> str:
        return str(uuid.UUID(hex=self._digest(f"event|{case}|{n}")[:32], version=4))

    def at(self, seconds: float) -> datetime:
        return self.anchor + timedelta(seconds=seconds)

    # -- message builders ----------------------------------------------------------------
    def event(
        self,
        case: str,
        group: str,
        n: int,
        seconds: float,
        *,
        amount: str = "250.00",
        country: str = "IN",
        currency: str = "INR",
        merchant: Merchant = NORMAL_MERCHANT,
        card: str | None = None,
    ) -> dict[str, Any]:
        return make_event(
            event_id=self.event_id(case, n),
            card_token=card or self.card(case),
            merchant=merchant,
            amount=amount,
            currency=currency,
            country=country,
            channel="POS",
            event_time=self.at(seconds),
            status="APPROVED",
        )

    def send1(
        self,
        case: str,
        group: str,
        event: dict[str, Any],
        *,
        expect: str = ENRICHED,
        after: Message | None = None,
    ) -> Message:
        """Phase 1, in event-time order; `after` places it right after another message."""
        msg = Message(
            case=case,
            group=group,
            phase=1,
            key=event["card_token"],
            value=json.dumps(event).encode(),
            expect=expect,
            event_id=event["event_id"],
            event_time=v.parse_event_time(event["event_time"]),
        )
        sort_time = after.event_time if after else msg.event_time
        self._seq += 1
        self._phase1.append((sort_time, self._seq, msg))
        return msg

    def send2(self, msg: Message) -> Message:
        msg.phase = 2
        self._phase2.append(msg)
        return msg

    def dlq(self, case: str, key: str, value: bytes, reason: str) -> Message:
        return self.send2(
            Message(
                case=case, group="dq", phase=2, key=key, value=value, expect=DLQ, reason_code=reason
            )
        )

    def alert(self, case: str, rule_id: str, events: list[dict[str, Any]], alert: str) -> None:
        trigger = events[-1]
        self.sc.alerts.append(
            ExpectedAlert(
                case=case,
                rule_id=rule_id,
                card_token=trigger["card_token"],
                event_ids=[e["event_id"] for e in events],
                trigger_event_id=trigger["event_id"],
                event_time=v.parse_event_time(trigger["event_time"]),
                alert_id=alert,
            )
        )

    def no_alert(self, case: str, rule_id: str, events: list[dict[str, Any]], why: str) -> None:
        self.sc.no_alerts.append(
            NoAlert(case, rule_id, events[0]["card_token"], [e["event_id"] for e in events], why)
        )

    def finish(self) -> Scenario:
        self.sc.messages = [m for _, _, m in sorted(self._phase1, key=lambda x: x[:2])]
        self.sc.messages += self._phase2_dups + self._phase2_late + self._phase2
        return self.sc


# -- the cases -------------------------------------------------------------------------------


def _velocity(b: _Builder) -> None:
    g = "velocity"
    burst = [b.event("velocity_burst", g, i, 60 + 10 * i) for i in range(6)]  # 6 in 50s
    for e in burst:
        b.send1("velocity_burst", g, e)
    b.alert(
        "velocity_burst",
        "VELOCITY",
        burst,
        aid.velocity_alert_id(burst[-1]["card_token"], b.at(110)),
    )

    five = [b.event("velocity_exactly_5", g, i, s) for i, s in enumerate((60, 74, 88, 102, 119))]
    for e in five:
        b.send1("velocity_exactly_5", g, e)
    b.no_alert("velocity_exactly_5", "VELOCITY", five, "5 events in 59s: count 5 is not > 5")

    edge = [b.event("velocity_6th_at_60s", g, i, 60 + 12 * i) for i in range(6)]
    for e in edge:
        b.send1("velocity_6th_at_60s", g, e)
    b.no_alert(
        "velocity_6th_at_60s",
        "VELOCITY",
        edge,
        "6th event exactly 60s after the 1st: window (t-60s, t] holds 5",
    )


def _travel(b: _Builder) -> None:
    g = "travel"
    for case, gap, alerts in (
        ("travel_20min", 1200, True),
        ("travel_30min_exact", 1800, False),
        ("travel_31min", 1860, False),
    ):
        first = b.event(case, g, 0, 300, country="IN", currency="INR")
        second = b.event(case, g, 1, 300 + gap, country="GB", currency="GBP", amount="80.00")
        b.send1(case, g, first)
        b.send1(case, g, second)
        if alerts:
            b.alert(
                case,
                "IMPOSSIBLE_TRAVEL",
                [first, second],
                aid.impossible_travel_alert_id(second["card_token"], second["event_id"]),
            )
        else:
            b.no_alert(
                case,
                "IMPOSSIBLE_TRAVEL",
                [first, second],
                f"IN then GB {gap // 60} min apart: outside (t-30min, t]",
            )


def _spike(b: _Builder) -> None:
    g = "spike"
    card = b.card("spike")
    b.sc.baselines[card] = SPIKE_BASELINE
    for n, (case, amount, seconds) in enumerate(
        (
            ("spike_6x", "6000.00", 400),
            ("spike_4_99x", "4990.00", 520),
            ("spike_5x_exact", "5000.00", 640),
        )
    ):
        e = b.event(case, g, n, seconds, amount=amount, card=card)
        b.send1(case, g, e)
        if case == "spike_6x":
            b.alert(case, "AMOUNT_SPIKE", [e], aid.amount_spike_alert_id(e["event_id"]))
        else:
            b.no_alert(case, "AMOUNT_SPIKE", [e], f"{amount} vs 30-day average 1000.00: not > 5x")


def _blacklist(b: _Builder) -> None:
    e = b.event(
        "blacklisted_merchant", "blacklist", 0, 700, amount="300.00", merchant=b.blacklisted
    )
    b.send1("blacklisted_merchant", "blacklist", e)
    b.alert(
        "blacklisted_merchant",
        "MERCHANT_BLACKLIST",
        [e],
        aid.merchant_blacklist_alert_id(e["event_id"]),
    )


def _dedup(b: _Builder) -> None:
    g = "dedup"
    same = b.event("duplicate_same_batch", g, 0, 800)
    first = b.send1("duplicate_same_batch", g, same)
    b.send1("duplicate_same_batch", g, same, expect=DUPLICATE, after=first)

    later = b.event("duplicate_next_phase", g, 0, 820)
    b.send1("duplicate_next_phase", g, later)
    copy = Message(
        case="duplicate_next_phase",
        group=g,
        phase=2,
        key=later["card_token"],
        value=json.dumps(later).encode(),
        expect=DUPLICATE,
        event_id=later["event_id"],
        event_time=b.at(820),
    )
    b._phase2_dups.append(copy)


def _ordering(b: _Builder) -> None:
    g = "ordering"
    newer = b.event("out_of_order_90s", g, 0, 900)
    older = b.event("out_of_order_90s", g, 1, 810)  # sent after an event 90s newer
    first = b.send1("out_of_order_90s", g, newer)
    b.send1("out_of_order_90s", g, older, after=first)


def _late(b: _Builder) -> None:
    g = "late"
    on_time = b.event("late_event", g, 0, 2200, country="IN", currency="INR")
    late = b.event("late_event", g, 1, 2200 - 420, country="GB", currency="GBP", amount="80.00")
    b.send1("late_event", g, on_time)
    msg = Message(
        case="late_event",
        group=g,
        phase=2,
        key=late["card_token"],
        value=json.dumps(late).encode(),
        expect=ENRICHED_LATE,
        event_id=late["event_id"],
        event_time=b.at(1780),
    )
    b._phase2_late.append(msg)
    b.no_alert(
        "late_event",
        "IMPOSSIBLE_TRAVEL",
        [on_time, late],
        "GB event is 7 min behind the newest event (5 min past the 2-min watermark): "
        "late events are excluded from windowed rules (D-04)",
    )


def _dq(b: _Builder) -> None:
    g = "dq"
    # Must pass validation.
    as_number = b.event("amount_as_json_number", g, 0, 950, amount="499.99")
    msg = b.send1("amount_as_json_number", g, as_number)
    msg.value = msg.value.replace(b'"amount": "499.99"', b'"amount": 499.99')  # JSON number
    assert b'"amount": 499.99' in msg.value
    b.send1("amount_at_ceiling", g, b.event("amount_at_ceiling", g, 0, 1000, amount="500000.00"))

    # Must go to the DLQ.
    def bad(case: str, **overrides: Any) -> tuple[str, dict[str, Any]]:
        e = b.event(case, g, 0, 2000)
        e.update(overrides)
        return e["card_token"], e

    key, e = bad("malformed_json")
    b.dlq("malformed_json", key, json.dumps(e).encode()[:-20], v.MALFORMED_JSON)
    key, e = bad("missing_field")
    del e["currency"]
    b.dlq("missing_field", key, json.dumps(e).encode(), v.MISSING_FIELD)
    key, e = bad("negative_amount", amount="-10.00")
    b.dlq("negative_amount", key, json.dumps(e).encode(), v.AMOUNT_OUT_OF_RANGE)
    key, e = bad("amount_above_ceiling", amount="500000.01")
    b.dlq("amount_above_ceiling", key, json.dumps(e).encode(), v.AMOUNT_OUT_OF_RANGE)
    key, e = bad("currency_not_allowed", currency="JPY")
    b.dlq("currency_not_allowed", key, json.dumps(e).encode(), v.CURRENCY_NOT_ALLOWED)
    key, e = bad("future_event_time", event_time=v.format_event_time(b.now + timedelta(hours=25)))
    b.dlq("future_event_time", key, json.dumps(e).encode(), v.FUTURE_EVENT_TIME)


_CASES = {
    "velocity": _velocity,
    "travel": _travel,
    "spike": _spike,
    "blacklist": _blacklist,
    "dedup": _dedup,
    "ordering": _ordering,
    "late": _late,
    "dq": _dq,
}


def build_scenario(name: str, anchor: datetime, now: datetime, blacklisted: Merchant) -> Scenario:
    if name not in SCENARIOS:
        raise ValueError(f"unknown scenario {name!r}; choose from {', '.join(SCENARIOS)}")
    if anchor.tzinfo is None or now.tzinfo is None:
        raise ValueError("anchor and now must be timezone-aware")
    b = _Builder(anchor, now, blacklisted)
    for group in GROUPS if name == "all" else (name,):
        _CASES[group](b)
    sc = b.finish()
    sc.name = name
    return sc


def expected_document(
    sc: Scenario,
    *,
    topic: str,
    rules: dict[str, dict[str, Any]],
    deliveries: dict[int, tuple[int, int]],
    phase_gap: float,
    generated_at: datetime,
) -> dict[str, Any]:
    """The data/expected/<scenario>.json content.

    rules: rule_id -> {"version": int, "severity": str} from sg.rule_config.
    deliveries: message index -> (partition, offset) from Kafka delivery reports.
    """

    def ts(d: datetime) -> str:
        return v.format_event_time(d)

    def position(i: int) -> dict[str, Any]:
        part, off = deliveries.get(i, (None, None))
        return {"source_topic": topic, "partition": part, "offset": off}

    enriched, duplicates, dead = [], [], []
    for i, m in enumerate(sc.messages):
        if m.expect in (ENRICHED, ENRICHED_LATE):
            enriched.append(
                {
                    "case": m.case,
                    "event_id": m.event_id,
                    "card_token": m.key,
                    "event_time": ts(m.event_time),
                    "is_late": m.expect == ENRICHED_LATE,
                }
            )
        elif m.expect == DUPLICATE:
            duplicates.append(
                {"case": m.case, "event_id": m.event_id, "phase": m.phase, **position(i)}
            )
        else:
            dead.append({"case": m.case, "reason_code": m.reason_code, "key": m.key, **position(i)})
    alerts = [
        {
            "alert_id": a.alert_id,
            "rule_id": a.rule_id,
            "rule_version": rules[a.rule_id]["version"],
            "severity": rules[a.rule_id]["severity"],
            "card_token": a.card_token,
            "event_ids": a.event_ids,
            "trigger_event_id": a.trigger_event_id,
            "event_time": ts(a.event_time),
            "case": a.case,
        }
        for a in sc.alerts
    ]
    cards = sorted({m.key for m in sc.messages})
    return {
        "scenario": sc.name,
        "anchor": ts(sc.anchor),
        "generated_at": ts(generated_at),
        "topic": topic,
        "phase_gap_seconds": phase_gap,
        "preconditions": [
            "processor starts from a fresh checkpoint (or the bounded replay test, ADR-01)",
            "sg.rule_config and sg.dq_config hold the config/*.yaml defaults",
            "compare only rows for card_tokens listed here; other traffic is ignored",
        ],
        "card_tokens": cards,
        "baselines": {
            c: {"avg_amount_30d": str(a), "tx_count_30d": n} for c, (a, n) in sc.baselines.items()
        },
        "counts": {
            "messages_sent": len(sc.messages),
            "enriched_rows": len(enriched),
            "late_rows": sum(1 for e in enriched if e["is_late"]),
            "duplicates_dropped": len(duplicates),
            "dead_letters": len(dead),
            "alerts": len(alerts),
        },
        "alerts": alerts,
        "must_not_alert": [asdict(n) for n in sc.no_alerts],
        "dead_letters": dead,
        "enriched": enriched,
        "duplicates": duplicates,
    }
