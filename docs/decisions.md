# StreamGuard — Decisions Log

The full design write-up, including the topic design and architecture diagram, is in [architecture.md](architecture.md).

## Problem
Fraud on card payments is caught hours late from a next-morning batch report. We need a Kafka/Spark pipeline that validates events, applies configurable fraud rules within seconds, stores alerts idempotently, and records analyst feedback.

---

## D-01 … D-12: Defaults for gaps in the use-case card

**Status:** Proposed (2026-11-20). The team can change any of these; record the change here.

| ID | Gap | Decision |
|----|-----|----------|
| D-01 | **Allowed currencies** | `USD, EUR, GBP, INR, JPY, AUD, CAD, SGD`, stored in config. Any other currency goes to the DLQ with reason `CURRENCY_NOT_ALLOWED`. |
| D-02 | **Amount ceiling**, and whether amounts in different currencies can be compared | A static FX table in config converts each amount to `amount_usd`. Ceiling is 50,000 USD-equivalent. An amount ≤ 0 or above the ceiling goes to the DLQ (`AMOUNT_OUT_OF_RANGE`). |
| D-03 | **Seeding the 30-day average** for synthetic cards | A seed script writes `card_profile(card_token, avg_amount_usd_30d, txn_count_30d, as_of)` from 30 days of generated history. The producer draws amounts from each card's profile. A nightly batch job refreshes the profile; the stream never updates it. The average uses whatever history the card has in the last 30 days, even 1 or 2 transactions: with 1 prior transaction the average is that amount, with 2 it is their mean. The rule is skipped only when `txn_count_30d = 0`, because there is nothing to compare against. Keeping the profile fixed during a run makes replays deterministic. |
| D-04 | **Events later than the 2-minute watermark** | Spark drops them from stateful operators without any error, so lateness is checked explicitly against the watermark. Late events get reason code `LATE_EVENT` and `is_late=true` and are still written to `enriched_txn`. They are excluded from the windowed rules (velocity, travel); per-event rules (blacklist, amount spike) still run. The `late_events_total` metric counts them. They are not sent to the DLQ, because they are valid data. |
| D-05 | **Feedback entry when Superset is read-only** | A small Streamlit app lists open alerts and writes `alert_feedback(alert_id PK, label TP/FP, analyst, noted_at, comment)` with an upsert. Superset only reads this table. SQL Lab DML stays disabled. |
| D-06 | **Velocity window type** | A sliding window per card, evaluated on every event: count of events in `(t − 60s, t]` > 5. Implemented as SQL in `sql/rules/` that runs in PostgreSQL against `enriched_txn` for the cards in each micro-batch, so it is testable without Spark. The travel rule uses the same approach with a 30-minute lookback. Tumbling windows would miss bursts that straddle a minute boundary. |
| D-07 | **Alert dedup key per rule** | `alert_id = sha256(rule_id ‖ key)`. **Velocity:** `card_token ‖ minute_floor(trigger_event_time)` (at most 1 alert per card per minute). **Impossible travel:** `card_token ‖ trigger event_id`. **Amount spike:** `event_id`. **Blacklist:** `event_id`. |
| D-08 | **Dedup state lifetime** | Within a micro-batch, Spark drops duplicate `event_id`s (`dropDuplicatesWithinWatermark`, about 2 minutes of state). Across batches and replays, `enriched_txn.event_id` is the primary key with `ON CONFLICT DO NOTHING`, and rules run only on rows actually inserted. When duplicates disagree, the first one wins. |
| D-09 | **Rule config source and reload** | A `fraud_rule(rule_id, enabled, params JSONB, version)` table and a `merchant_blacklist` table. The processor re-reads both every micro-batch. Each alert stores `rule_version`. |
| D-10 | **Which `status` values the rules evaluate** | All statuses, because declines are a fraud signal. `status` is copied onto the alert. |
| D-11 | **Time semantics** | `event_time` is UTC ISO-8601 with milliseconds. An event more than 24h in the future goes to the DLQ (`FUTURE_EVENT_TIME`). The processor adds `ingest_time`, and latency = `processed_at − event_time`. |
| D-12 | **Definitions of "backlog" and "precision"** | Backlog = alerts with no feedback, also shown split into those older than 15 minutes. Daily precision per rule = TP / (TP + FP), grouped by the day the alert was created. Unreviewed alerts are shown separately and not counted as FP. |

---

## ADR-01: Delivery guarantees — at-least-once with idempotent writes

**Status:** Accepted (2026-11-20)

### Context
Spark Structured Streaming reads `payments.raw`. In each micro-batch, `foreachBatch` writes to PostgreSQL and also to the Kafka topic `payments.alerts`.

**What Spark actually guarantees**
- **Kafka source:** offsets are stored in the checkpoint. After a crash, Spark re-reads exactly the same offset range for the failed micro-batch.
- **`foreachBatch`:** at-least-once. A batch can run again with the same `batchId`. A Postgres-only sink can become effectively-once after a crash by committing the data and `batchId` in one transaction. That does **not** cover operator replays: a new checkpoint or offset reset restarts batch IDs and changes batch boundaries.
- **Kafka sink:** **at-least-once only**. Spark's Kafka writer does not use Kafka transactions. Producer retries or a batch that runs again will write **duplicate records**.
- **Two sinks in one `foreachBatch` cannot be atomic.** We write to Postgres first (one transaction), then to Kafka. If the job crashes between the two, the batch runs again: the Postgres write does nothing, and Kafka gets duplicates.

### Options considered

| Option | Cost |
|--------|------|
| A custom transactional Kafka producer inside `foreachBatch`, plus `read_committed` consumers | Still not atomic with Postgres, because there is no two-phase commit across the two. |
| Outbox: Spark writes only to Postgres; Debezium or a relay publishes alerts to Kafka | Adds a component to build and operate. |
| Switch engine to Flink, which has two-phase-commit sinks | Out of scope. |
| **At-least-once + idempotent writes** | Duplicates on `payments.alerts` must be tolerated downstream. |

### Decision
At-least-once with idempotent writes:
- The `alerts` table has `alert_id` as its primary key and uses `ON CONFLICT DO NOTHING`. This keeps the first `created_at` and any analyst state.
- `enriched_txn` uses `event_id` as its primary key.
- `agg_1min` uses `(window_start, merchant_category, country)` as its key and is upserted with **absolute values from state**. It never uses `count = count + excluded.count`.
- `payments.alerts` is keyed by `alert_id` and log-compacted, so duplicates collapse. Consumers deduplicate by key.

### Consequences: what a replay does
Applies to an offset reset, or a new checkpoint with `startingOffsets`:
- Spark re-reads `payments.raw` and rebuilds its dedup state. Rule SQL reads `enriched_txn`, where replayed events already exist.
- With the same rule version and profile snapshot, it produces the same `alert_id`s. Postgres ends up with no new rows, and Kafka gets duplicates that compaction removes.
- Caveats:
  - Different micro-batch boundaries can change which events count as late. The replay test therefore uses `Trigger.AvailableNow` on a bounded input.
  - If thresholds changed since the original run, the replay can create new alerts. Their `rule_version` makes them traceable.
