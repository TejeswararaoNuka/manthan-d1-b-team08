# StreamGuard — Decisions Log

The full design write-up, including the topic design and architecture diagram, is in [architecture.md](architecture.md).

## Problem
Fraud on card payments is caught hours late from a next-morning batch report. We need a Kafka/Spark pipeline that validates events, applies configurable fraud rules within seconds, stores alerts idempotently, and records analyst feedback.

---

## D-01 … D-15: Defaults for gaps in the use-case card

**Status:** Proposed (2026-11-20). The team can change any of these; record the change here.

| ID | Gap | Decision |
|----|-----|----------|
| D-01 | **Allowed currencies** | `INR, USD, EUR, GBP, SGD, AED`, stored in `sg.dq_config.allowed_currencies` (defaults in `config/dq.yaml`). Any other currency goes to the DLQ with reason `CURRENCY_NOT_ALLOWED`. *(Changed 2026-11-20, see D-13.)* |
| D-02 | **Amount ceiling**, and whether amounts in different currencies can be compared | No FX conversion. The ceiling is a flat 500,000.00 (`sg.dq_config.amount_ceiling`, sized for INR, the main currency), compared with `amount` as sent, in the event's own currency. An amount ≤ 0 or above the ceiling goes to the DLQ (`AMOUNT_OUT_OF_RANGE`). For other currencies this is only a loose sanity bound, and `sg.agg_1min.amount_sum` adds amounts across currencies, so it is a volume indicator, not money. *(Changed 2026-11-20, see D-13.)* |
| D-03 | **Seeding the 30-day average** for synthetic cards | A seed script writes `sg.card_baseline(card_token, avg_amount_30d, tx_count_30d, updated_at)` from 30 days of generated history. The producer draws amounts from each card's profile. A nightly batch job refreshes the profile; the stream never updates it. The average uses whatever history the card has in the last 30 days, even 1 or 2 transactions: with 1 prior transaction the average is that amount, with 2 it is their mean. The rule is skipped only when `tx_count_30d = 0`, because there is nothing to compare against. Keeping the profile fixed during a run makes replays deterministic. |
| D-04 | **Events later than the 2-minute watermark** | Spark drops them from stateful operators without any error, so lateness is checked explicitly against the watermark. Late events get reason code `LATE_EVENT` and `is_late=true` and are still written to `sg.enriched_transactions`. They are excluded from the windowed rules (velocity, travel); per-event rules (blacklist, amount spike) still run. The `late_events_total` metric counts them. They are not sent to the DLQ, because they are valid data. **Watermark guard (decided 2026-11-20):** a *valid* event up to 24h in the future would otherwise advance the watermark by up to 24h and make all live traffic late. The processor therefore judges lateness against `min(newest event_time seen, wall-clock now) − 120s`; future-dated events are still stored, but cannot move the watermark past real time. **Implementation (2026-11-20):** lateness is explicit in `process_records`, not Spark's `withWatermark`: Spark's operators run before `foreachBatch` and silently discard rows behind the watermark, so late events could never be kept or counted. The watermark is stored in `ops.processor_state` and updated in the same transaction as each batch; an event is late when `event_time` < the watermark left by *earlier* batches. |
| D-05 | **Feedback entry** | In Superset, no separate app. Analysts use a second Superset database connection that logs in as `feedback_writer`, with *Allow DML* enabled on that connection only, and upsert into `sg.alert_feedback(alert_id PK, label TRUE_POSITIVE/FALSE_POSITIVE, analyst, noted_at, comment)` from SQL Lab. `feedback_writer` can use only `sg.alert_feedback` (SELECT, INSERT, UPDATE; PostgreSQL needs SELECT for `ON CONFLICT ... DO UPDATE`). A trigger sets `sg.alerts.status = 'CLOSED'`. The dashboard connection (`superset_ro`) stays read-only. *(Changed 2026-11-20, see D-13.)* |
| D-06 | **Velocity window type** | A sliding window per card, evaluated on every event: count of events in `(t − 60s, t]` > `max_tx` (5). The trigger is the event where the count first exceeds it, and `event_ids` holds every event in that window. Implemented as SQL in `sql/rules/` that runs in PostgreSQL against `sg.enriched_transactions` for the cards in each micro-batch, so it is testable without Spark. The travel rule uses the same approach: an earlier event in another country within `(t − 30min, t)` triggers, so exactly 30 minutes apart does **not** alert; `event_ids` = [earlier, trigger]. Amount spike alerts when amount > 5 × average (exactly 5.00× does not). Tumbling windows would miss bursts that straddle a minute boundary. |
| D-07 | **Alert dedup key per rule** | `alert_id = sha256("<rule_id>|<key part>|...")`, UTF-8, lowercase hex (`streamguard.common.alert_id`); timestamps in keys are the UTC minute floor written `YYYY-MM-DDTHH:MM:00Z`. **Velocity:** `card_token ‖ minute_floor(trigger_event_time)` (at most 1 alert per card per minute). **Impossible travel:** `card_token ‖ trigger event_id`. **Amount spike:** `event_id`. **Blacklist:** `event_id`. |
| D-08 | **Dedup state lifetime** | No Spark `dropDuplicatesWithinWatermark` (it drops late rows silently and cannot be tested with a static DataFrame). Within a micro-batch, `process_records` keeps the first Kafka position per `event_id`; across batches and replays, `sg.enriched_transactions.event_id` is the primary key with `ON CONFLICT DO NOTHING`, and rules run only on rows actually inserted. When duplicates disagree, the first one wins. *(Changed 2026-11-20.)* |
| D-09 | **Rule config source and reload** | A `sg.rule_config(rule_id, name, enabled, severity, params JSONB, version, updated_at)` table and a `sg.merchant_blacklist` table. A trigger bumps `version` on every real change. The processor re-reads both every micro-batch. Each alert stores `rule_version`. Defaults are in `config/rules.yaml`; `scripts/apply_sql.py` inserts them, keeps runtime edits on re-run, and restores them with `--reset-config`. *(Changed 2026-11-20, see D-13.)* |
| D-10 | **Which `status` values the rules evaluate** | All statuses, because declines are a fraud signal. `status` is copied onto the alert as `txn_status`. |
| D-11 | **Time semantics** | `event_time` is UTC ISO-8601 with milliseconds. An event more than 24h in the future goes to the DLQ (`FUTURE_EVENT_TIME`). The processor adds `ingest_time`, and latency = `processed_at − event_time`. |
| D-12 | **Definitions of "backlog" and "precision"** | Backlog = alerts with no feedback, also shown split into those older than 15 minutes. Daily precision per rule = TP / (TP + FP), grouped by the day the alert was created. Unreviewed alerts are shown separately and not counted as FP. |
| D-13 | **Database layout** (added 2026-11-20) | Schema `sg` holds business tables (`sql/ddl/01_sg.sql`) and schema `ops` holds observability (`sql/ddl/02_ops.sql`); views are in `sql/views/`, seed data in `sql/seed/`; `scripts/apply_sql.py --db <name>` applies all of it in one transaction and is safe to re-run. Renames from the first draft: `enriched_txn` → `sg.enriched_transactions`, `fraud_rule` → `sg.rule_config`, `card_profile` → `sg.card_baseline`, and `sg.agg_1min` is keyed by `(window_start, channel, country)` instead of merchant category. `sg.alerts` has `status` `OPEN/CLOSED` (analyst state) and `txn_status` (payment status, D-10). `sg.dlq_events` is unique on `(source_topic, kafka_partition, kafka_offset)`. Lag, latency and backlog thresholds are in `ops.health_config` (defaults in `config/dq.yaml`, `LAG_ALERT_THRESHOLD` / `LATENCY_ALERT_SECONDS` from `.env` override them). `ops.v_pipeline_health` reads them from there. |
| D-14 | **Producer and scenario contract** (added 2026-11-20) | `python -m streamguard.producer` sends realistic traffic (5,000 cards, 2,000 merchants plus the 20 blacklisted ids, seed 42) and keeps every card at or under the velocity threshold, so velocity alerts come only from deliberate fraud; at 5,000 cards that caps throughput near 417 events/s (use `--cards` to go higher). `seed_baseline` writes `sg.card_baseline` from the same seed. `--scenario` sends a fixed sequence and writes `data/expected/<scenario>.json` with the exact alerts, dead letters (with Kafka partition/offset), enriched and late rows. It assumes a **fresh processor checkpoint** (or the bounded replay test), because its event times start 40 minutes in the past and a watermark from earlier traffic would make them late; it refuses to run if thresholds differ from `config/*.yaml`. Card tokens and event IDs derive from the anchor, so runs never collide and the same `--anchor` reproduces the same bytes. Phase 2 (after `--phase-gap`) carries the late event, a cross-batch duplicate and the DQ failures, so lateness is judged against a watermark from an earlier batch. |
| D-15 | **Single-PC setup** (added 2026-11-20; replaces the PC 1 + team-laptops setup) | Docker and all Python tooling run on one Windows PC; `.env` uses `localhost`; compose publishes Kafka, PostgreSQL and Superset on `127.0.0.1` only. Nothing is pushed to a remote. Hex hashes (e.g. `alert_id`) are exempt from the no-13-digit-run rule: sha256 hex contains such runs in about 1 in 9 IDs by chance, and they are not card data. |

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
- The `sg.alerts` table has `alert_id` as its primary key and uses `ON CONFLICT DO NOTHING`. This keeps the first `alert_time` and any analyst state.
- `sg.enriched_transactions` uses `event_id` as its primary key.
- `sg.agg_1min` uses `(window_start, channel, country)` as its key and is upserted with **absolute values from state**. It never uses `count = count + excluded.count`.
- `payments.alerts` is keyed by `alert_id` and log-compacted, so duplicates collapse. Consumers deduplicate by key.

### Consequences: what a replay does
Applies to an offset reset, or a new checkpoint with `startingOffsets`:
- Spark re-reads `payments.raw` and rebuilds its dedup state. Rule SQL reads `sg.enriched_transactions`, where replayed events already exist.
- With the same rule version and profile snapshot, it produces the same `alert_id`s. Postgres ends up with no new rows, and Kafka gets duplicates that compaction removes.
- Caveats:
  - Different micro-batch boundaries can change which events count as late. The replay test therefore uses `Trigger.AvailableNow` on a bounded input.
  - If thresholds changed since the original run, the replay can create new alerts. Their `rule_version` makes them traceable.
