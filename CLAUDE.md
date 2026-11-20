# CLAUDE.md — StreamGuard

## Goal
Payment fraud is caught hours late from a next-morning batch report. StreamGuard ingests payment
events from Kafka, validates and deduplicates them, applies fraud rules in near real time, writes
enriched data, 1-minute aggregates and alerts to PostgreSQL, and gives analysts a live Superset view
and a feedback loop. Consumers: fraud analyst, risk manager, platform on-call engineer.
Design references: `docs/use_case_card.md`, `docs/architecture.md`, `docs/decisions.md`.

## Team setup
- Only **PC 1** runs Docker (Kafka, PostgreSQL, processor, Superset).
- Other laptops connect to PC 1 using `POSTGRES_HOST` and `KAFKA_BOOTSTRAP` from `.env`
  (copy `.env.example`). They install `requirements.txt` only: no Docker, no Java, no pyspark.
- Tests use `POSTGRES_TEST_DB`, **never** `POSTGRES_DB`.
- Plain `pytest` runs unit tests only. Marked tests: `spark` (Java 17, PC 1 or CI), `kafka`,
  `integration`.
- Helper scripts in `scripts/` are Python, never bash, so they run on Windows.

## Stack
Python 3.13 · Kafka `apache/kafka:3.7.1` (KRaft) · Spark Structured Streaming 4.0.1 (Java 17) ·
PostgreSQL 16 · Superset 6.0.0 · Docker Compose. **No Airflow.**

## Message schema rules
**Payment event** (topic `payments.raw`, Kafka key = `card_token`), JSON:
| Field | Rule |
|-------|------|
| `event_id` | UUID4 string |
| `card_token` | `tok_` + 16 lowercase hex; never digits-only |
| `merchant_id` | `mer_` + 8 lowercase hex |
| `merchant_category` | 4-digit string (MCC), e.g. `"5411"` |
| `amount` | decimal, 2 dp, sent as JSON number or string; never float maths in Python (use `Decimal`) |
| `currency` | ISO 4217 code, must be in the allowed list (config) |
| `country` | ISO 3166-1 alpha-2 |
| `channel` | `POS` \| `ECOM` \| `ATM` |
| `event_time` | ISO 8601 UTC with `Z`, e.g. `2026-11-20T10:15:30.123Z` |
| `status` | `APPROVED` \| `DECLINED` \| `PENDING` |
| `schema_version` | int, currently `1` |

**Alert** (topic `payments.alerts`, Kafka key = `alert_id`, log-compacted):
`alert_id` (sha256 hex of `rule_id` + rule dedup key), `rule_id`, `rule_version`, `card_token`,
`event_ids` (array), `severity` (`LOW|MEDIUM|HIGH`), `details` (JSON object), `event_time`,
`alert_time`. Dedup keys per rule are in `docs/decisions.md` (D-07).

**Dead letter** (topic `payments.dlq`): `raw_value` (original payload as string/base64),
`reason_code`, `error_detail`, `source_topic`, `partition`, `offset`, `failed_at`.

## Data-quality reason codes
| Code | Meaning | Route |
|------|---------|-------|
| `MALFORMED_JSON` | payload is not valid JSON | DLQ |
| `MISSING_FIELD` | required field absent/null or wrong format | DLQ |
| `AMOUNT_OUT_OF_RANGE` | amount <= 0 or above ceiling | DLQ |
| `CURRENCY_NOT_ALLOWED` | currency not in allowed list | DLQ |
| `FUTURE_EVENT_TIME` | event_time > now + 24h | DLQ |
| `LATE_EVENT` | older than the 2-minute watermark | **Not DLQ**: kept in `sg.enriched_transactions` with `is_late=true`, excluded from windowed rules, counted in metrics (D-04) |

## Idempotency
- Every PostgreSQL write is an upsert on a natural key (`INSERT ... ON CONFLICT`).
- `sg.enriched_transactions` unique on `event_id`; `alerts` unique on `alert_id` (`DO NOTHING`, keeps analyst
  state); `sg.agg_1min` on its window key, written with absolute values, never `count = count + ...`.
- Delivery is at-least-once (ADR-01). A replay of the same input must not create duplicate rows.
  Kafka alert duplicates are acceptable; consumers dedup by `alert_id`.

## Rules and thresholds
- Rule and DQ thresholds live in PostgreSQL config tables (`sg.rule_config`,
  `sg.merchant_blacklist`, `sg.dq_config`) and are re-read **every micro-batch**. Never hard-code a threshold in Python or SQL.
- `config/*.yaml` holds defaults that `scripts/apply_sql.py` loads into those tables.
- Rule logic lives in `sql/rules/*.sql`, one file per rule, so it is testable against PostgreSQL
  without Spark. Rules run in PostgreSQL against `sg.enriched_transactions` for the cards in the batch.

## Processor design
- Entry point `process_batch(df, batch_id)` used by `foreachBatch`.
- It must be testable with a static DataFrame: unit/spark tests build a DataFrame in memory and
  call it directly. No Kafka in unit tests.
- Order inside a batch: validate -> DLQ invalid -> dedup -> upsert `sg.enriched_transactions` -> run rule SQL
  -> upsert `alerts` -> publish alerts to Kafka -> upsert `agg_1min`.
- Keep pure logic (validation, hashing, parsing) in plain Python under `streamguard/common` so it
  is unit-testable without Spark.

## Security
- Synthetic tokens only. No card-number-like values (13-19 consecutive digits) anywhere: code,
  tests, fixtures, seed data, logs.
- Secrets come only from environment variables / `.env`. Never commit `.env` or real credentials.

## Conventions
- Python: black + ruff, line length 100, target py313. snake_case for modules, functions, tables
  and columns; Kafka topics `payments.<name>`; SQL files `NN_<name>.sql` where order matters.
- Commit prefixes: `feat:`, `fix:`, `test:`, `docs:`, `ci:`, `chore:`. Small, verified commits.
- One test per acceptance criterion; name tests after the behaviour they prove.
- Plan before code: propose a plan, wait for approval, then implement.
- When a decision changes, update `docs/decisions.md` (and `docs/architecture.md`) in the same change.
