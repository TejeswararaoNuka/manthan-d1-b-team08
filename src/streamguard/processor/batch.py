"""One micro-batch, end to end, on plain Python records (no Spark needed).

Order (CLAUDE.md, ADR-01): validate -> DLQ rows -> dedup -> lateness -> insert
sg.enriched_transactions -> rule SQL -> insert sg.alerts -> recompute sg.agg_1min -> metrics
and processor state, all in ONE PostgreSQL transaction; then, after commit, publish the
batch's dead letters to payments.dlq and its alerts to payments.alerts.

A batch that runs again with the same batch_id (crash after commit) writes nothing and only
re-publishes, so Kafka is at-least-once and PostgreSQL stays idempotent.
"""

from __future__ import annotations

import base64
import json
import statistics
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from psycopg2.extras import execute_values

from streamguard.common.validation import DQConfig, InvalidEvent, format_event_time, validate_event
from streamguard.rules.engine import insert_alerts, run_rules

ALERTS_TOPIC = "payments.alerts"
DLQ_TOPIC = "payments.dlq"


@dataclass(frozen=True)
class Record:
    """One Kafka message as the processor sees it."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class Publisher(Protocol):
    def publish(self, topic: str, key: str | None, value: bytes) -> None: ...

    def flush(self) -> None: ...


@dataclass
class BatchContext:
    conn: Any  # psycopg2 connection (autocommit off)
    publisher: Publisher
    query_name: str = "processor"
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))


@dataclass
class BatchResult:
    batch_id: int
    rerun: bool = False
    input_rows: int = 0
    dead_letters: int = 0
    duplicates: int = 0
    inserted: int = 0
    late: int = 0
    alerts: list[str] = field(default_factory=list)  # alert_ids published
    new_alerts: int = 0
    watermark: datetime | None = None


def render_raw(value: bytes | None) -> str:
    if value is None:
        return ""
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return "base64:" + base64.b64encode(value).decode("ascii")


def _key(record: Record) -> str | None:
    return record.key.decode("utf-8", errors="replace") if record.key is not None else None


# -- reading config / state -----------------------------------------------------------------


def _load_config(cur) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    cur.execute("SELECT rule_id, enabled, severity, params, version FROM sg.rule_config")
    rules = {
        r[0]: {"enabled": r[1], "severity": r[2], "params": r[3], "version": r[4]}
        for r in cur.fetchall()
    }
    cur.execute("SELECT key, value FROM sg.dq_config")
    return rules, dict(cur.fetchall())


def _lock_state(cur, query_name: str) -> tuple[int, datetime | None] | None:
    cur.execute(
        "SELECT last_batch_id, watermark FROM ops.processor_state "
        "WHERE query_name = %s FOR UPDATE",
        (query_name,),
    )
    return cur.fetchone()


# -- publishing --------------------------------------------------------------------------------


def _alert_rows(cur, alert_ids: list[str]) -> list[dict[str, Any]]:
    if not alert_ids:
        return []
    cur.execute(
        """SELECT alert_id, rule_id, rule_version, card_token, event_ids, severity, details,
                  txn_status, event_time, alert_time
             FROM sg.alerts WHERE alert_id = ANY (%s) ORDER BY event_time, alert_id""",
        (alert_ids,),
    )
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _alert_message(row: dict[str, Any]) -> bytes:
    return json.dumps(
        {
            "alert_id": row["alert_id"],
            "rule_id": row["rule_id"],
            "rule_version": row["rule_version"],
            "card_token": row["card_token"],
            "event_ids": list(row["event_ids"]),
            "severity": row["severity"],
            "details": row["details"],
            "txn_status": row["txn_status"],
            "event_time": format_event_time(row["event_time"]),
            "alert_time": format_event_time(row["alert_time"]),
        }
    ).encode()


def _dlq_message(row: dict[str, Any]) -> bytes:
    return json.dumps(
        {
            "raw_value": row["raw_value"],
            "reason_code": row["reason_code"],
            "error_detail": row["error_detail"],
            "source_topic": row["source_topic"],
            "partition": row["kafka_partition"],
            "offset": row["kafka_offset"],
            "failed_at": format_event_time(row["failed_at"]),
        }
    ).encode()


def _publish(ctx: BatchContext, dlq: list[tuple[str | None, dict]], alerts: list[dict]) -> None:
    for key, row in dlq:
        ctx.publisher.publish(DLQ_TOPIC, key, _dlq_message(row))
    for row in alerts:
        ctx.publisher.publish(ALERTS_TOPIC, row["alert_id"], _alert_message(row))
    ctx.publisher.flush()


# -- the batch -------------------------------------------------------------------------------


def process_records(records: Sequence[Record], batch_id: int, ctx: BatchContext) -> BatchResult:
    started = time.perf_counter()
    result = BatchResult(batch_id=batch_id, input_rows=len(records))
    if not records:
        return result
    now = ctx.now()
    conn = ctx.conn
    with conn, conn.cursor() as cur:
        state = _lock_state(cur, ctx.query_name)
        rules, dq_values = _load_config(cur)
        dq = DQConfig.from_values(dq_values)

        # 1. validate
        valid: list[tuple[Record, dict[str, Any]]] = []
        dlq_rows: list[tuple[str | None, dict[str, Any]]] = []
        for rec in records:
            try:
                valid.append((rec, validate_event(rec.value, dq, now)))
            except InvalidEvent as err:
                dlq_rows.append(
                    (
                        _key(rec),
                        {
                            "reason_code": err.reason_code,
                            "error_detail": err.detail[:1000],
                            "raw_value": render_raw(rec.value),
                            "source_topic": rec.topic,
                            "kafka_partition": rec.partition,
                            "kafka_offset": rec.offset,
                            "failed_at": now,
                        },
                    )
                )
        result.dead_letters = len(dlq_rows)

        if state is not None and batch_id <= state[0]:
            return _rerun(cur, ctx, result, valid, dlq_rows)

        # 2. dead letters (natural key = source position)
        if dlq_rows:
            execute_values(
                cur,
                """INSERT INTO sg.dlq_events (reason_code, error_detail, raw_value, source_topic,
                                              kafka_partition, kafka_offset, failed_at)
                   VALUES %s
                   ON CONFLICT (source_topic, kafka_partition, kafka_offset) DO NOTHING""",
                [
                    (
                        r["reason_code"],
                        r["error_detail"],
                        r["raw_value"],
                        r["source_topic"],
                        r["kafka_partition"],
                        r["kafka_offset"],
                        r["failed_at"],
                    )
                    for _, r in dlq_rows
                ],
            )

        # 3. dedup within the batch: the first Kafka position wins (D-08)
        valid.sort(key=lambda rv: (rv[0].topic, rv[0].partition, rv[0].offset))
        unique: dict[str, tuple[Record, dict[str, Any]]] = {}
        for rec, ev in valid:
            unique.setdefault(ev["event_id"], (rec, ev))
        result.duplicates = len(valid) - len(unique)

        # 4. lateness against the watermark from earlier batches (D-04 guard)
        watermark_before = state[1] if state else None
        rows = []
        for rec, ev in unique.values():
            is_late = watermark_before is not None and ev["event_time"] < watermark_before
            rows.append((rec, ev, is_late))

        # 5. insert enriched rows; rules run only on rows actually inserted (D-08)
        inserted: set[str] = set()
        if rows:
            returned = execute_values(
                cur,
                """INSERT INTO sg.enriched_transactions
                       (event_id, card_token, merchant_id, merchant_category, amount, currency,
                        country, channel, event_time, status, schema_version, kafka_partition,
                        kafka_offset, ingested_at, is_late)
                   VALUES %s
                   ON CONFLICT (event_id) DO NOTHING
                   RETURNING event_id::text""",
                [
                    (
                        ev["event_id"],
                        ev["card_token"],
                        ev["merchant_id"],
                        ev["merchant_category"],
                        ev["amount"],
                        ev["currency"],
                        ev["country"],
                        ev["channel"],
                        ev["event_time"],
                        ev["status"],
                        ev["schema_version"],
                        rec.partition,
                        rec.offset,
                        now,
                        late,
                    )
                    for rec, ev, late in rows
                ],
                fetch=True,
                page_size=1000,
            )
            inserted = {r[0] for r in returned}
        new_rows = [(rec, ev, late) for rec, ev, late in rows if ev["event_id"] in inserted]
        result.inserted = len(new_rows)
        result.late = sum(1 for _, _, late in new_rows if late)

        # 6. rules -> alerts
        candidates = run_rules(cur, rules, sorted(inserted))
        new_alert_ids = insert_alerts(cur, candidates, rules)
        result.new_alerts = len(new_alert_ids)
        result.alerts = [c.alert_id for c in candidates]

        # 7. 1-minute aggregates, absolute values for every window this batch touched
        windows = {
            (ev["event_time"].replace(second=0, microsecond=0), ev["channel"], ev["country"])
            for _, ev, _ in new_rows
        }
        if candidates:
            cur.execute(
                """SELECT date_trunc('minute', event_time), channel, country
                     FROM sg.enriched_transactions WHERE event_id = ANY (%s::uuid[])""",
                ([c.trigger_event_id for c in candidates],),
            )
            windows |= {tuple(r) for r in cur.fetchall()}
        _upsert_agg(cur, windows)

        # 8. metrics and state
        new_watermark = watermark_before
        valid_times = [ev["event_time"] for _, ev in valid]
        if valid_times:
            seconds = int(dq_values["watermark_seconds"])
            proposed = min(max(valid_times), now) - timedelta(seconds=seconds)
            if new_watermark is None or proposed > new_watermark:
                new_watermark = proposed
        result.watermark = new_watermark
        # Processing latency = processed_at - event_time over every inserted row (D-11), late
        # ones included; never negative (a future-dated event is "on time").
        latencies = [max((now - ev["event_time"]).total_seconds(), 0.0) for _, ev, _ in new_rows]
        p95 = (
            statistics.quantiles(latencies, n=20, method="inclusive")[18]
            if len(latencies) > 1
            else (latencies[0] if latencies else None)
        )
        elapsed = max(time.perf_counter() - started, 1e-6)
        cur.execute(
            """INSERT INTO ops.stream_metrics
                   (batch_id, recorded_at, input_rows, processed_rows_per_sec,
                    p95_latency_seconds, dlq_count, alert_count, late_count)
               VALUES (%s, now(), %s, %s, %s, %s, %s, %s)
               ON CONFLICT (batch_id) DO UPDATE SET
                   recorded_at = EXCLUDED.recorded_at, input_rows = EXCLUDED.input_rows,
                   processed_rows_per_sec = EXCLUDED.processed_rows_per_sec,
                   p95_latency_seconds = EXCLUDED.p95_latency_seconds,
                   dlq_count = EXCLUDED.dlq_count, alert_count = EXCLUDED.alert_count,
                   late_count = EXCLUDED.late_count""",
            (
                batch_id,
                len(records),
                round(len(records) / elapsed, 2),
                round(p95, 3) if p95 is not None else None,
                len(dlq_rows),
                len(new_alert_ids),
                result.late,
            ),
        )
        cur.execute(
            """INSERT INTO ops.processor_state
                   (query_name, last_batch_id, watermark, watermark_before, updated_at)
               VALUES (%s, %s, %s, %s, now())
               ON CONFLICT (query_name) DO UPDATE SET
                   last_batch_id = EXCLUDED.last_batch_id, watermark = EXCLUDED.watermark,
                   watermark_before = EXCLUDED.watermark_before, updated_at = now()""",
            (ctx.query_name, batch_id, new_watermark, watermark_before),
        )
        # Progress for the lag monitor (ADR-02): max offset per partition in this batch.
        last_offsets: dict[tuple[str, int], int] = {}
        for rec in records:
            key = (rec.topic, rec.partition)
            last_offsets[key] = max(last_offsets.get(key, -1), rec.offset)
        execute_values(
            cur,
            """INSERT INTO ops.stream_offsets (topic, partition, last_processed_offset, updated_at)
               VALUES %s
               ON CONFLICT (topic, partition) DO UPDATE SET
                   last_processed_offset = EXCLUDED.last_processed_offset,
                   updated_at = EXCLUDED.updated_at""",
            [(t, p, off, now) for (t, p), off in sorted(last_offsets.items())],
        )
        alert_rows = _alert_rows(cur, result.alerts)
    # committed: now Kafka (at-least-once, ADR-01)
    _publish(ctx, dlq_rows, alert_rows)
    return result


def _rerun(cur, ctx, result, valid, dlq_rows) -> BatchResult:
    """Same batch_id again: everything is already committed. Re-publish what this batch
    produced (its dead letters, and alerts that reference its events)."""
    result.rerun = True
    event_ids = sorted({ev["event_id"] for _, ev in valid})
    alert_ids: list[str] = []
    if event_ids:
        cur.execute("SELECT alert_id FROM sg.alerts WHERE event_ids && %s::text[]", (event_ids,))
        alert_ids = [r[0] for r in cur.fetchall()]
    result.alerts = alert_ids
    alert_rows = _alert_rows(cur, alert_ids)
    cur.connection.commit()
    _publish(ctx, dlq_rows, alert_rows)
    return result


def _upsert_agg(cur, windows: set[tuple]) -> None:
    if not windows:
        return
    starts, channels, countries = zip(*sorted(windows), strict=True)
    cur.execute(
        """INSERT INTO sg.agg_1min
               (window_start, channel, country, tx_count, amount_sum, alert_count, updated_at)
           SELECT k.window_start, k.channel, k.country, t.tx_count, t.amount_sum,
                  coalesce(a.alert_count, 0), now()
             FROM unnest(%s::timestamptz[], %s::text[], %s::text[])
                  AS k(window_start, channel, country)
            CROSS JOIN LATERAL (
                  SELECT count(*) AS tx_count, coalesce(sum(e.amount), 0) AS amount_sum
                    FROM sg.enriched_transactions e
                   WHERE e.event_time >= k.window_start
                     AND e.event_time < k.window_start + interval '1 minute'
                     AND e.channel = k.channel AND e.country = k.country) t
             LEFT JOIN LATERAL (
                  SELECT count(*) AS alert_count
                    FROM sg.alerts al
                    JOIN sg.enriched_transactions te
                      ON te.event_id = (al.details ->> 'trigger_event_id')::uuid
                   WHERE al.event_time >= k.window_start
                     AND al.event_time < k.window_start + interval '1 minute'
                     AND te.channel = k.channel AND te.country = k.country) a ON true
           ON CONFLICT (window_start, channel, country) DO UPDATE SET
               tx_count = EXCLUDED.tx_count, amount_sum = EXCLUDED.amount_sum,
               alert_count = EXCLUDED.alert_count, updated_at = now()""",
        (list(starts), list(channels), list(countries)),
    )
