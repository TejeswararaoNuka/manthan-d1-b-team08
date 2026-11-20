"""Synthetic cards and merchants, fully determined by (seed, count). No clock, no I/O.

seed_baseline.py and the producer build the same catalog from the same seed, so the
30-day averages in sg.card_baseline match the amounts the producer draws (D-03).
"""

from __future__ import annotations

import random
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal

# Home country -> (weight, home currency, typical median ticket in minor units).
COUNTRIES: dict[str, tuple[float, str, int]] = {
    "IN": (0.60, "INR", 150_000),  # 1,500.00 INR
    "US": (0.10, "USD", 4_500),
    "GB": (0.10, "GBP", 3_500),
    "SG": (0.07, "SGD", 6_000),
    "AE": (0.07, "AED", 15_000),
    "DE": (0.03, "EUR", 4_000),
    "FR": (0.03, "EUR", 4_000),
}
MCCS = ("5411", "5812", "5814", "5912", "5999", "4111", "4121", "5732", "5651", "7011", "6011")
NEW_CARD_SHARE = 0.02  # cards with 0-2 transactions of history (D-03 edge cases)

_CARD_NUMBER_LIKE = re.compile(r"\d{13}")


@dataclass(frozen=True)
class Card:
    token: str
    home_country: str
    currency: str
    median_minor: int  # median ticket, minor units
    sigma: float  # log-normal spread of this card's amounts
    history_count: int  # transactions in the last 30 days (for seed_baseline)


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    mcc: str


def random_hex(rng: random.Random, digits: int) -> str:
    """Lowercase hex that never contains a card-number-like run of 13+ digits."""
    while True:
        value = f"{rng.getrandbits(digits * 4):0{digits}x}"
        if not _CARD_NUMBER_LIKE.search(value):
            return value


def random_uuid4(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def build_cards(seed: int, count: int) -> list[Card]:
    rng = random.Random(f"cards:{seed}")
    countries = list(COUNTRIES)
    weights = [COUNTRIES[c][0] for c in countries]
    cards, seen = [], set()
    while len(cards) < count:
        token = "tok_" + random_hex(rng, 16)
        if token in seen:
            continue
        seen.add(token)
        country = rng.choices(countries, weights)[0]
        _, currency, median = COUNTRIES[country]
        new_card = rng.random() < NEW_CARD_SHARE
        cards.append(
            Card(
                token=token,
                home_country=country,
                currency=currency,
                median_minor=max(100, int(median * rng.lognormvariate(0, 0.5))),
                sigma=round(rng.uniform(0.4, 0.9), 3),
                history_count=rng.randint(0, 2) if new_card else rng.randint(10, 90),
            )
        )
    return cards


def build_merchants(seed: int, count: int) -> list[Merchant]:
    rng = random.Random(f"merchants:{seed}")
    merchants, seen = [], set()
    while len(merchants) < count:
        merchant_id = "mer_" + random_hex(rng, 8)
        if merchant_id in seen or merchant_id.startswith("mer_bad"):
            continue  # mer_bad... is the blacklist seed's range
        seen.add(merchant_id)
        merchants.append(Merchant(merchant_id, rng.choice(MCCS)))
    return merchants


def draw_amount(card: Card, rng: random.Random, ceiling: Decimal) -> Decimal:
    """One amount from the card's log-normal ticket distribution, 2 dp, within (0, ceiling].

    Sampling is in integer minor units; money is only ever handled as Decimal.
    """
    ceiling_minor = int(ceiling * 100)
    while True:
        minor = int(card.median_minor * rng.lognormvariate(0, card.sigma))
        if 100 <= minor <= ceiling_minor:  # at least 1.00
            return Decimal(minor).scaleb(-2)
