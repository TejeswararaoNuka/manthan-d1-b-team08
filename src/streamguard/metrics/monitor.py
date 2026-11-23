"""Pipeline monitor: consumer lag and latency alerts, independent of where Spark runs (ADR-02).

    python -m streamguard.metrics             # every 15 s, until stopped
    python -m streamguard.metrics --once      # one sample (tests, manual checks)

Spark keeps its Kafka offsets in the checkpoint and never commits them to a consumer group,
so lag = (latest offsets from Kafka admin) - (ops.stream_offsets.last_processed_offset + 1),
per partition. Needs only Kafka and PostgreSQL, so it can run on any machine.

Each cycle writes ops.lag_samples and checks thresholds from ops.health_config (seeded from
LAG_ALERT_THRESHOLD / LATENCY_ALERT_SECONDS). An alert goes to ops.alerts and the log when a
check starts failing, again every alert_repeat_minutes while it keeps failing, and a RESOLVED
row when it recovers.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from psycopg2.extras import Json

from streamguard.common.settings import load_env, pg_connect, require

TOPIC = "payments.raw"
SOURCE = "monitor"


def compute_lag(
    end_offsets: dict[int, int], last_processed: dict[int, int]
) -> tuple[int | None, dict[str, int | None]]:
    """Total lag over partitions with a processed offset (None if there is none yet), and lag
    per partition (None = the processor has not reported this partition yet)."""
    per: dict[str, int | None] = {}
    for partition, end in sorted(end_offsets.items()):
        last = last_processed.get(partition)
        per[str(partition)] = None if last is None else max(end - (last + 1), 0)
    known = [v for v in per.values() if v is not None]
    return (sum(known) if known else None), per


@dataclass
class _Check:
    firing: bool = False
    last_raised: datetime | None = None


@dataclass
class AlertTracker:
    """Raise on change to failing, remind every `repeat`, resolve on recovery."""

    repeat: timedelta
    checks: dict[str, _Check] = field(default_factory=dict)

    def evaluate(
        self, name: str, value: float | None, threshold: float | None, now: datetime
    ) -> str | None:
        """Returns 'RAISE', 'REMIND', 'RESOLVE' or None."""
        if value is None or threshold is None:
            return None
        check = self.checks.setdefault(name, _Check())
        if value > threshold:
            if not check.firing:
                check.firing, check.last_raised = True, now
                return "RAISE"
            if now - check.last_raised >= self.repeat:
                check.last_raised = now
                return "REMIND"
            return None
        if check.firing:
            check.firing = False
            return "RESOLVE"
        return None


def kafka_end_offsets(admin: AdminClient, topic: str) -> dict[int, int]:
    meta = admin.list_topics(topic, timeout=10).topics.get(topic)
    if meta is None or meta.error is not None:
        raise KafkaException(f"topic {topic} not available: {meta.error if meta else 'missing'}")
    request = {TopicPartition(topic, p): OffsetSpec.latest() for p in meta.partitions}
    futures = admin.list_offsets(request, request_timeout=10)
    return {tp.partition: f.result().offset for tp, f in futures.items()}


def _health_config(cur) -> dict[str, float]:
    cur.execute("SELECT key, value FROM ops.health_config")
    return {k: float(v) for k, v in cur.fetchall() if isinstance(v, (int, float, str))}


def run_cycle(conn, admin: AdminClient, tracker: AlertTracker, topic: str = TOPIC) -> dict:
    """One sample: write ops.lag_samples, evaluate thresholds, write/log ops.alerts."""
    now = datetime.now(UTC)
    end = kafka_end_offsets(admin, topic)
    with conn, conn.cursor() as cur:
        cfg = _health_config(cur)
        tracker.repeat = timedelta(minutes=cfg.get("alert_repeat_minutes", 10))
        cur.execute(
            "SELECT partition, last_processed_offset FROM ops.stream_offsets WHERE topic = %s",
            (topic,),
        )
        lag, per = compute_lag(end, dict(cur.fetchall()))
        unknown = sum(1 for v in per.values() if v is None)
        cur.execute(
            """INSERT INTO ops.lag_samples
                   (topic, sampled_at, consumer_lag, lag_by_partition, unknown_partitions)
               VALUES (%s, %s, %s, %s, %s)""",
            (topic, now, lag, Json(per), unknown),
        )
        cur.execute(
            "SELECT batch_id, p95_latency_seconds FROM ops.stream_metrics "
            "ORDER BY recorded_at DESC, batch_id DESC LIMIT 1"
        )
        latest = cur.fetchone()
        p95 = float(latest[1]) if latest and latest[1] is not None else None

        lag_limit = cfg.get("lag_alert_threshold")
        lat_limit = cfg.get("latency_alert_seconds")
        busiest = sorted(((p, v) for p, v in per.items() if v), key=lambda pv: pv[1], reverse=True)[
            :3
        ]
        messages = {
            "lag": (
                lag,
                lag_limit,
                (
                    f"consumer lag {lag:,} > {lag_limit:,.0f} on {topic} "
                    f"(top partitions: {', '.join(f'p{p}={v:,}' for p, v in busiest) or 'n/a'})"
                    if lag is not None and lag_limit is not None
                    else ""
                ),
                (
                    f"consumer lag back to {lag:,} (<= {lag_limit:,.0f}) on {topic}"
                    if lag is not None and lag_limit is not None
                    else ""
                ),
            ),
            "latency": (
                p95,
                lat_limit,
                (
                    f"p95 processing latency {p95:.1f}s > {lat_limit:.0f}s "
                    f"(batch {latest[0] if latest else '?'})"
                    if p95 is not None and lat_limit is not None
                    else ""
                ),
                (
                    f"p95 processing latency back to {p95:.1f}s (<= {lat_limit:.0f}s)"
                    if p95 is not None and lat_limit is not None
                    else ""
                ),
            ),
        }
        raised = []
        for name, (value, limit, bad, good) in messages.items():
            action = tracker.evaluate(name, value, limit, now)
            if action is None:
                continue
            severity, text = (
                ("LOW", f"RESOLVED: {good}")
                if action == "RESOLVE"
                else (
                    "HIGH",
                    f"{'STILL FIRING' if action == 'REMIND' else 'ALERT'}: {bad}",
                )
            )
            cur.execute(
                "INSERT INTO ops.alerts (raised_at, severity, source, message) "
                "VALUES (%s, %s, %s, %s)",
                (now, severity, f"{SOURCE}:{name}", text),
            )
            raised.append((severity, text))
    for severity, text in raised:
        print(f"{now:%Y-%m-%dT%H:%M:%SZ} *** OPS {severity} *** {text}", flush=True)
    return {"lag": lag, "per_partition": per, "p95": p95, "alerts": raised}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m streamguard.metrics")
    ap.add_argument("--interval", type=float, default=15)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--db", help="database (default POSTGRES_DB)")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args(argv)

    env = load_env()
    require(env, "KAFKA_BOOTSTRAP")
    admin = AdminClient({"bootstrap.servers": env["KAFKA_BOOTSTRAP"]})
    conn = pg_connect(env, args.db, app="streamguard-monitor")
    tracker = AlertTracker(repeat=timedelta(minutes=10))
    print(f"Monitor: {args.topic} lag every {args.interval:g}s", flush=True)
    try:
        while True:
            try:
                s = run_cycle(conn, admin, tracker, args.topic)
                lag = "unknown" if s["lag"] is None else f"{s['lag']:,}"
                p95 = "n/a" if s["p95"] is None else f"{s['p95']:.1f}s"
                print(f"{datetime.now(UTC):%H:%M:%S} lag={lag} p95={p95}", flush=True)
            except KafkaException as exc:
                print(f"WARNING: Kafka unavailable: {exc}", file=sys.stderr, flush=True)
            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0
    finally:
        conn.close()
