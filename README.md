# StreamGuard – Real-Time Payment Fraud Signals

Streaming pipeline that ingests synthetic payment events from Kafka, validates and deduplicates them,
applies configurable fraud rules in near real time, writes alerts and enriched data to PostgreSQL,
and gives analysts a live Superset view plus a feedback loop.

> Status: skeleton only. Setup and run instructions are coming.

## Layout

| Path | Purpose |
|------|---------|
| `deploy/` | Docker Compose and per-service images (Kafka, PostgreSQL, processor, producer, Superset) |
| `src/streamguard/` | Python package: producer, processor, rules, metrics, feedback, common |
| `sql/` | DDL, views, seed data and rule SQL |
| `config/` | YAML defaults for rules and DQ thresholds (loaded into PostgreSQL) |
| `tests/` | `unit/`, `integration/`, `spark/` |
| `scripts/` | Python helper scripts (cross-platform) |
| `docs/` | Use case, architecture, decisions, prompts, runbook |

## Docs

- [Use case card](docs/use_case_card.md)
- [Architecture](docs/architecture.md)
- [Decisions](docs/decisions.md)
- [CLAUDE.md](CLAUDE.md): rules for working on this repo
