"""Random payment-event stream and rate schedule. Pure logic: the caller passes the clock."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from streamguard.common.validation import SCHEMA_VERSION, format_event_time
from streamguard.producer.catalog import (
    COUNTRIES,
    Card,
    Merchant,
    draw_amount,
    random_uuid4,
)

CHANNEL_WEIGHTS = {"POS": 0.60, "ECOM": 0.35, "ATM": 0.05}
STATUS_WEIGHTS = {"APPROVED": 0.93, "DECLINED": 0.05, "PENDING": 0.02}
MAX_CARD_PICKS = 50  # attempts to find a card under the velocity cap before giving up


def target_rate(
    elapsed: float, rate: float, burst_rate: float, burst_every: float, burst_seconds: float
) -> tuple[float, bool]:
    """Events/second wanted at `elapsed` seconds, and whether a burst is on.

    Bursts start at burst_every, 2*burst_every, ... and last burst_seconds.
    """
    if burst_every > 0 and burst_seconds > 0 and elapsed >= burst_every:
        if elapsed % burst_every < burst_seconds:
            return burst_rate, True
    return rate, False


def make_event(
    *,
    event_id: str,
    card_token: str,
    merchant: Merchant,
    amount: Decimal | str,
    currency: str,
    country: str,
    channel: str,
    event_time: datetime,
    status: str,
) -> dict[str, Any]:
    """A schema-valid payment event (CLAUDE.md), amount sent as a 2-dp string."""
    return {
        "event_id": event_id,
        "card_token": card_token,
        "merchant_id": merchant.merchant_id,
        "merchant_category": merchant.mcc,
        "amount": str(amount),
        "currency": currency,
        "country": country,
        "channel": channel,
        "event_time": format_event_time(event_time),
        "status": status,
        "schema_version": SCHEMA_VERSION,
    }


@dataclass
class StreamConfig:
    amount_ceiling: Decimal
    velocity_max_tx: int  # from sg.rule_config VELOCITY params
    velocity_window_seconds: int
    blacklist_rate: float = 0.001
    travel_rate: float = 0.002
    max_skew_ms: int = 1500  # event_time up to this much before send time (natural disorder)


class EventStream:
    """Draws realistic events. Keeps each card at or under the velocity threshold so that
    velocity alerts come only from deliberate fraud (scenario mode), not from load."""

    def __init__(
        self,
        cards: list[Card],
        merchants: list[Merchant],
        blacklisted_ids: list[str],
        cfg: StreamConfig,
        seed: int,
    ):
        self.cards = cards
        self.merchants = merchants
        self.blacklisted = [Merchant(m, "5999") for m in sorted(blacklisted_ids)]
        self.cfg = cfg
        self.rng = random.Random(f"stream:{seed}")
        self.window = timedelta(seconds=cfg.velocity_window_seconds)
        self.recent: dict[str, list[datetime]] = {}
        self.capped = 0  # times no card was available under the cap

    def _pick_card(self, now: datetime, event_time: datetime) -> Card | None:
        # Later events have event_time >= now - max_skew, so older times can never share a
        # window with them.
        horizon = now - timedelta(milliseconds=self.cfg.max_skew_ms) - self.window
        for _ in range(MAX_CARD_PICKS):
            card = self.cards[self.rng.randrange(len(self.cards))]
            times = self.recent.setdefault(card.token, [])
            times[:] = [t for t in times if t > horizon]
            # Event times are not in send order (skew), so check conservatively: fewer than
            # max_tx other events within +-window means no window (t - w, t] containing this
            # event can exceed max_tx.
            near = sum(1 for t in times if abs(t - event_time) < self.window)
            if near < self.cfg.velocity_max_tx:
                times.append(event_time)
                return card
        self.capped += 1
        return None

    def next_event(self, now: datetime) -> dict[str, Any] | None:
        """One event with event_time slightly before `now`, or None if every card tried is
        at the velocity cap (the actual rate then falls below the target)."""
        skew = timedelta(milliseconds=self.rng.randint(0, self.cfg.max_skew_ms))
        event_time = now - skew
        card = self._pick_card(now, event_time)
        if card is None:
            return None
        rng = self.rng
        if self.blacklisted and rng.random() < self.cfg.blacklist_rate:
            merchant = rng.choice(self.blacklisted)
        else:
            merchant = self.merchants[rng.randrange(len(self.merchants))]
        country = card.home_country
        if rng.random() < self.cfg.travel_rate:
            country = rng.choice([c for c in COUNTRIES if c != card.home_country])
        return make_event(
            event_id=random_uuid4(rng),
            card_token=card.token,
            merchant=merchant,
            amount=draw_amount(card, rng, self.cfg.amount_ceiling),
            currency=card.currency,
            country=country,
            channel=rng.choices(list(CHANNEL_WEIGHTS), list(CHANNEL_WEIGHTS.values()))[0],
            event_time=event_time,
            status=rng.choices(list(STATUS_WEIGHTS), list(STATUS_WEIGHTS.values()))[0],
        )
