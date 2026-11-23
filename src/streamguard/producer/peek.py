"""Print the last N messages of a Kafka topic, without the Kafka CLI.

    python -m streamguard.producer.peek payments.dlq -n 20
    python -m streamguard.producer.peek payments.raw -n 5 --partition 3

Reads with manual partition assignment and never commits offsets, so it does not disturb
any consumer group. Timestamps are printed as ISO UTC (not epoch milliseconds).
"""

from __future__ import annotations

import argparse
import base64
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition

from streamguard.common.settings import load_env, require


def start_offsets(watermarks: dict[int, tuple[int, int]], n: int) -> dict[int, int]:
    """Per partition, where to start so that its last n messages (or all it has) are read."""
    return {p: max(low, high - n) for p, (low, high) in watermarks.items() if high > low}


def last_n(records: list[tuple[int, int, int, bytes | None, bytes | None]], n: int):
    """records: (timestamp_ms, partition, offset, key, value). Newest n, oldest first."""
    return sorted(records, key=lambda r: (r[0], r[1], r[2]))[-n:] if n > 0 else []


def render(data: bytes | None) -> str:
    if data is None:
        return "null"
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return "base64:" + base64.b64encode(data).decode("ascii")


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def peek(env, topic: str, n: int, partition: int | None, timeout: float) -> list[tuple]:
    require(env, "KAFKA_BOOTSTRAP")
    consumer = Consumer(
        {
            "bootstrap.servers": env["KAFKA_BOOTSTRAP"],
            "group.id": f"streamguard-peek-{uuid.uuid4()}",  # never committed
            "enable.auto.commit": False,
            "enable.partition.eof": False,
        }
    )
    try:
        meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
        if meta is None or meta.error is not None:
            raise SystemExit(f"Topic {topic!r} not found ({meta.error if meta else 'missing'})")
        parts = sorted(meta.partitions) if partition is None else [partition]
        if partition is not None and partition not in meta.partitions:
            raise SystemExit(f"Topic {topic!r} has no partition {partition}")
        marks = {
            p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in parts
        }
        starts = start_offsets(marks, n)
        if not starts:
            return []
        consumer.assign([TopicPartition(topic, p, off) for p, off in starts.items()])
        ends = {p: marks[p][1] for p in starts}
        done: set[int] = set()
        records = []
        deadline = time.monotonic() + timeout
        while len(done) < len(starts) and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            p = msg.partition()
            records.append((msg.timestamp()[1], p, msg.offset(), msg.key(), msg.value()))
            if msg.offset() >= ends[p] - 1:
                done.add(p)
                consumer.pause([TopicPartition(topic, p)])
        return last_n(records, n)
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m streamguard.producer.peek",
        description="Print the last N messages of a Kafka topic",
    )
    ap.add_argument("topic", help="e.g. payments.raw, payments.alerts, payments.dlq")
    ap.add_argument("-n", type=int, default=10, help="number of messages (default 10)")
    ap.add_argument("--partition", type=int)
    ap.add_argument("--timeout", type=float, default=20, help="seconds to wait for data")
    ap.add_argument("--env-file", type=Path)
    args = ap.parse_args(argv)
    try:
        rows = peek(load_env(args.env_file), args.topic, args.n, args.partition, args.timeout)
    except KafkaException as exc:
        print(f"ERROR: Kafka: {exc}", file=sys.stderr)
        return 1
    for ts, p, off, key, value in rows:
        print(f"{fmt_ts(ts)} p{p} o{off} key={render(key)} {render(value)}")
    print(f"-- {len(rows)} message(s) from {args.topic}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
