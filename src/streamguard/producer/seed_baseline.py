"""Write 30-day spending baselines for every synthetic card into sg.card_baseline (D-03).

The stream cannot carry 30-day-old events past the 2-minute watermark, so the history is
simulated here from the same catalog the producer uses (same --seed and --cards):

    python -m streamguard.producer.seed_baseline --seed 42 --cards 5000

Each card gets its catalog history_count (0-2 for ~2% "new" cards, else 10-90) amounts drawn
from its own distribution; the average is their exact Decimal mean, rounded to 2 dp.
Idempotent: absolute values, upserted on card_token.
"""

from __future__ import annotations

import argparse
import random
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path

from streamguard.common.settings import load_env, pg_connect
from streamguard.producer import db
from streamguard.producer.catalog import Card, build_cards, draw_amount


def baseline_for(card: Card, seed: int, ceiling: Decimal) -> tuple[str, Decimal, int]:
    rng = random.Random(f"history:{seed}:{card.token}")
    amounts = [draw_amount(card, rng, ceiling) for _ in range(card.history_count)]
    if not amounts:
        return card.token, Decimal("0.00"), 0
    avg = (sum(amounts, Decimal(0)) / len(amounts)).quantize(Decimal("0.01"), ROUND_HALF_EVEN)
    return card.token, avg, len(amounts)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m streamguard.producer.seed_baseline", description=__doc__.splitlines()[0]
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--cards", type=int, default=5000)
    ap.add_argument("--db", help="database (default POSTGRES_DB)")
    ap.add_argument("--env-file", type=Path)
    args = ap.parse_args(argv)

    env = load_env(args.env_file)
    conn = pg_connect(env, args.db, app="streamguard-seed-baseline")
    try:
        ceiling = Decimal(str(db.read_dq_config(conn)["amount_ceiling"]))
        rows = [baseline_for(c, args.seed, ceiling) for c in build_cards(args.seed, args.cards)]
        written = db.upsert_baselines(conn, rows)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM sg.card_baseline")
            total = cur.fetchone()[0]
    finally:
        conn.close()
    no_history = sum(1 for _, _, n in rows if n == 0)
    print(
        f"Upserted {written} card baselines (seed {args.seed}); {no_history} cards have no "
        f"history (amount-spike rule skipped for them). sg.card_baseline now has {total} rows."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
