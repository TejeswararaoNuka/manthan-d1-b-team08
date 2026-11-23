"""StreamGuard payment-event producer.

Random mode (default): realistic traffic to payments.raw, keyed by card_token.
    python -m streamguard.producer --rate 200 --burst-rate 1000 --burst-every 60 \
        --burst-seconds 5 --duration 300 --seed 42

Scenario mode: a fixed sequence plus data/expected/<scenario>.json with the exact alerts and
dead letters it must produce (see scenarios.py for the preconditions).
    python -m streamguard.producer --scenario all

Connects to KAFKA_BOOTSTRAP and POSTGRES_HOST from .env (PC 1), or the container environment.
Run seed_baseline with the same --seed/--cards first, so amount-spike baselines match.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from confluent_kafka import KafkaException, Producer

from streamguard.common.settings import REPO_ROOT, kafka_producer_conf, load_env, pg_connect
from streamguard.common.validation import parse_event_time
from streamguard.producer import db
from streamguard.producer.catalog import Merchant, build_cards, build_merchants
from streamguard.producer.scenarios import (
    SCENARIOS,
    build_scenario,
    default_anchor,
    expected_document,
)
from streamguard.producer.stream import EventStream, StreamConfig, target_rate

DEFAULT_TOPIC = "payments.raw"
REPORT_EVERY = 10.0


class Counters:
    def __init__(self) -> None:
        self.sent = self.delivered = self.failed = 0
        self.last_error = ""

    def on_delivery(self, err, _msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            self.last_error = str(err)


def produce(producer: Producer, topic: str, key: str, value: bytes, callback) -> None:
    while True:
        try:
            producer.produce(topic, key=key.encode(), value=value, on_delivery=callback)
            return
        except BufferError:  # local queue full: let deliveries drain
            producer.poll(0.2)


# -- random mode -------------------------------------------------------------------------------


def run_random(args, env) -> int:
    conn = pg_connect(env, args.db, app="streamguard-producer")
    try:
        rules = db.read_rule_config(conn)
        dq = db.read_dq_config(conn)
        blacklist = db.read_blacklist(conn)
    finally:
        conn.close()
    velocity = rules["VELOCITY"]["params"]
    cfg = StreamConfig(
        amount_ceiling=Decimal(str(dq["amount_ceiling"])),
        velocity_max_tx=int(velocity["max_tx"]),
        velocity_window_seconds=int(velocity["window_seconds"]),
        blacklist_rate=args.blacklist_rate,
        travel_rate=args.travel_rate,
    )
    stream = EventStream(
        build_cards(args.seed, args.cards),
        build_merchants(args.seed, args.merchants),
        blacklist,
        cfg,
        args.seed,
    )
    cap = args.cards * cfg.velocity_max_tx / cfg.velocity_window_seconds
    print(
        f"Producing to {args.topic} on {env['KAFKA_BOOTSTRAP']}: {args.cards} cards, "
        f"{args.merchants} merchants, {len(blacklist)} blacklisted, seed {args.seed}"
    )
    bursts = args.burst_every > 0 and args.burst_seconds > 0
    print(
        f"Target {args.rate:g}/s"
        + (
            f", bursts of {args.burst_rate:g}/s for {args.burst_seconds:g}s every "
            f"{args.burst_every:g}s"
            if bursts
            else ", no bursts"
        )
        + f"; per-card velocity cap allows at most ~{cap:.0f}/s"
    )
    if args.rate > cap or (bursts and args.burst_rate > cap):
        print(f"  note: rates above ~{cap:.0f}/s are capped; use more --cards to reach them")

    producer = Producer(kafka_producer_conf(env, "streamguard-producer"))
    counters = Counters()
    start = prev = last_report = time.perf_counter()
    last_sent, allowance, burst = 0, 0.0, False
    try:
        while True:
            t = time.perf_counter()
            elapsed = t - start
            if args.duration and elapsed >= args.duration:
                break
            rate, burst = target_rate(
                elapsed, args.rate, args.burst_rate, args.burst_every, args.burst_seconds
            )
            allowance = min(allowance + rate * (t - prev), max(rate * 0.5, 1.0))
            prev = t
            now = datetime.now(UTC)
            while allowance >= 1:
                allowance -= 1
                event = stream.next_event(now)
                if event is None:
                    continue
                produce(
                    producer,
                    args.topic,
                    event["card_token"],
                    json.dumps(event).encode(),
                    counters.on_delivery,
                )
                counters.sent += 1
            producer.poll(0)
            if t - last_report >= REPORT_EVERY:
                actual = (counters.sent - last_sent) / (t - last_report)
                _report(elapsed, counters, actual, rate, burst, stream.capped)
                last_sent, last_report = counters.sent, t
            time.sleep(0.005)
    except KeyboardInterrupt:
        print("Interrupted, flushing...")
    remaining = producer.flush(30)
    elapsed = time.perf_counter() - start
    _report(
        elapsed,
        counters,
        counters.sent / max(elapsed, 1e-9),
        args.rate,
        False,
        stream.capped,
        final=True,
    )
    if remaining or counters.failed:
        print(
            f"WARNING: {remaining} undelivered, {counters.failed} failed. "
            f"Last error: {counters.last_error}",
            file=sys.stderr,
        )
        return 1
    return 0


def _report(elapsed, c: Counters, actual, target, burst, capped, final=False) -> None:
    label = "done " if final else ""
    print(
        f"[{label}{elapsed:6.0f}s] sent={c.sent} delivered={c.delivered} failed={c.failed} "
        f"rate={actual:.1f}/s target={target:.0f}/s{' BURST' if burst else ''} "
        f"capped={capped}",
        flush=True,
    )


# -- scenario mode -----------------------------------------------------------------------------


def run_scenario(args, env) -> int:
    now = datetime.now(UTC)
    anchor = parse_event_time(args.anchor) if args.anchor else default_anchor(now)
    conn = pg_connect(env, args.db, app="streamguard-scenario")
    try:
        rules = db.read_rule_config(conn)
        dq = db.read_dq_config(conn)
        blacklist = db.read_blacklist(conn)
        diffs = db.config_differences(rules, dq, REPO_ROOT / "config")
        if diffs and not args.allow_config_drift:
            print(
                "ERROR: database config differs from config/*.yaml, so the expected results "
                "would be wrong:",
                file=sys.stderr,
            )
            for d in diffs:
                print(f"  {d}", file=sys.stderr)
            print("Fix: python scripts/apply_sql.py --reset-config", file=sys.stderr)
            return 1
        if not blacklist:
            print(
                "ERROR: sg.merchant_blacklist is empty; run scripts/apply_sql.py", file=sys.stderr
            )
            return 1
        sc = build_scenario(args.scenario, anchor, now, Merchant(blacklist[0], "5999"))
        if sc.baselines:
            db.upsert_baselines(conn, ((c, a, n) for c, (a, n) in sc.baselines.items()))
    finally:
        conn.close()

    print(
        f"Scenario '{sc.name}' anchored at {anchor.isoformat()} -> {args.topic} "
        f"({len(sc.messages)} messages)"
    )
    producer = Producer(kafka_producer_conf(env, "streamguard-scenario"))
    deliveries: dict[int, tuple[int, int]] = {}
    errors: list[str] = []

    def callback_for(i: int):
        def cb(err, msg):
            if err is None:
                deliveries[i] = (msg.partition(), msg.offset())
            else:
                errors.append(f"message {i}: {err}")

        return cb

    for phase in (1, 2):
        batch = [(i, m) for i, m in enumerate(sc.messages) if m.phase == phase]
        if phase == 2 and batch and args.phase_gap > 0:
            print(f"  waiting {args.phase_gap}s so the processor's watermark moves on...")
            time.sleep(args.phase_gap)
        for i, m in batch:
            produce(producer, args.topic, m.key, m.value, callback_for(i))
        if producer.flush(30):
            errors.append(f"phase {phase}: messages not delivered within 30s")
        print(f"  phase {phase}: {len(batch)} messages sent")
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1

    doc = expected_document(
        sc,
        topic=args.topic,
        rules={r: {"version": x["version"], "severity": x["severity"]} for r, x in rules.items()},
        deliveries=deliveries,
        phase_gap=args.phase_gap,
        generated_at=datetime.now(UTC),
    )
    out = Path(args.expected_dir) / f"{sc.name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    c = doc["counts"]
    print(
        f"Wrote {out}: {c['alerts']} alerts, {c['dead_letters']} dead letters, "
        f"{c['enriched_rows']} enriched rows ({c['late_rows']} late), "
        f"{c['duplicates_dropped']} duplicates, {len(doc['must_not_alert'])} must-not-alert"
    )
    return 0


# -- CLI ---------------------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m streamguard.producer", description="StreamGuard payment-event producer"
    )
    p.add_argument("--rate", type=float, default=200, help="events/s outside bursts")
    p.add_argument("--burst-rate", type=float, default=1000, help="events/s during a burst")
    p.add_argument("--burst-every", type=float, default=60, help="seconds between bursts (0=off)")
    p.add_argument("--burst-seconds", type=float, default=5, help="burst length in seconds")
    p.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until Ctrl+C)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--cards", type=int, default=5000)
    p.add_argument("--merchants", type=int, default=2000)
    p.add_argument(
        "--blacklist-rate",
        type=float,
        default=0.001,
        help="share of events at a blacklisted merchant",
    )
    p.add_argument(
        "--travel-rate",
        type=float,
        default=0.002,
        help="share of events outside the card's home country",
    )
    p.add_argument("--topic", default=DEFAULT_TOPIC)
    p.add_argument("--db", help="database to read config from (default POSTGRES_DB)")
    p.add_argument("--env-file", type=Path)
    s = p.add_argument_group("scenario mode")
    s.add_argument("--scenario", choices=SCENARIOS, help="emit a fixed sequence instead")
    s.add_argument(
        "--anchor",
        help="scenario start, ISO UTC e.g. 2026-11-20T10:00:00.000Z "
        "(default: 40 min ago, floored to the minute)",
    )
    s.add_argument(
        "--phase-gap", type=float, default=15, help="seconds between phase 1 and phase 2"
    )
    s.add_argument("--expected-dir", default=str(REPO_ROOT / "data" / "expected"))
    s.add_argument(
        "--allow-config-drift",
        action="store_true",
        help="run even if database thresholds differ from config/*.yaml",
    )
    args = p.parse_args(argv)
    if args.cards < 1 or args.merchants < 1 or args.rate <= 0:
        p.error("--cards, --merchants and --rate must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env = load_env(args.env_file)
    try:
        return run_scenario(args, env) if args.scenario else run_random(args, env)
    except KafkaException as exc:
        print(f"ERROR: Kafka: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
