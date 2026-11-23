# StreamGuard – Real-Time Payment Fraud Signals

Streaming pipeline that ingests synthetic payment events from Kafka, validates and deduplicates them,
applies configurable fraud rules in near real time, writes alerts and enriched data to PostgreSQL,
and gives analysts a live Superset view plus a feedback loop.

Kafka (KRaft) → Spark Structured Streaming processor → PostgreSQL → Superset, with a lag/latency
monitor and an analyst feedback CLI. Everything runs on **one Windows PC** with Docker (D-15).

## One-command start

Needs Docker Desktop and Python 3.13 (standard library is enough for this step).

```bash
python scripts/start.py
```

It copies `.env.example` to `.env` if missing (change every `change-me` before real use), runs
`docker compose up -d --build`, waits until PostgreSQL, Kafka and Superset are healthy, applies
the SQL, seeds the 30-day card baselines, builds the dashboard and prints the URLs. Safe to rerun.

| What | URL |
|---|---|
| Superset dashboard | http://localhost:8088/superset/dashboard/streamguard/ (admin from `.env`) |
| Spark UI | http://localhost:4040 |
| PostgreSQL | `localhost:5432` |
| Kafka (clients outside Docker) | `localhost:29092` |

### Other laptops

Not used: by decision D-15 the team works on this one PC, and compose publishes every port on
`127.0.0.1` only, so nothing is reachable from the network. To allow LAN access again, change the
`127.0.0.1:` port bindings in `deploy/docker-compose.yml` to `0.0.0.0:`, set `KAFKA_HOST_IP` in
`.env` to this PC's LAN IP, restart the stack, and on the other laptop set `POSTGRES_HOST` and
`KAFKA_BOOTSTRAP` (`<LAN IP>:29092`) in its own `.env`. Record that as a new decision first.

## Python setup (for the CLIs and tests)

```bash
python -m venv .venv
source .venv/Scripts/activate        # Git Bash; PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -e .
```

`confluent-kafka` is pinned to 2.10.0: newer wheels are blocked by Windows Smart App Control.

## Producer and scenarios

```bash
DC="docker compose --env-file .env -f deploy/docker-compose.yml"

# Continuous traffic: 200 events/s, 1,000/s bursts every 60 s
$DC --profile producer up -d producer
$DC --profile producer stop producer

# Or from the venv, for a fixed time
python -m streamguard.producer --rate 200 --duration 120

# Peek at any topic without the Kafka CLI
python -m streamguard.producer.peek payments.alerts -n 5
```

A **scenario** sends a fixed sequence (every rule, its boundary cases and every data-quality reason
code) and writes the exact expected result to `data/expected/<scenario>.json` (D-14). It needs a
fresh processor checkpoint:

```bash
$DC --profile producer stop producer
$DC stop processor
$DC run --rm --no-deps processor --reset --available-now
$DC up -d processor
python -m streamguard.producer --scenario all
python scripts/verify_scenario.py data/expected/all.json --wait 90   # PASS / FAIL with differences
```

`python scripts/reset_demo.py` empties the demo completely: it stops the processor, recreates
the three topics, truncates the data tables (config is kept), deletes the checkpoint and restarts
the processor. It asks for confirmation (`--yes` skips it).

## Analysts

```bash
python -m streamguard.feedback open --limit 20
python -m streamguard.feedback label 5ff66c04 TP --analyst asha
python -m streamguard.feedback report --days 7
```

See [docs/feedback_loop.md](docs/feedback_loop.md) and [docs/dashboard_design.md](docs/dashboard_design.md).

## Tests

Tests use `POSTGRES_TEST_DB`, never `POSTGRES_DB`.

```bash
python scripts/check.py      # black + ruff + pytest for everything except spark (needs the stack)
pytest                       # unit tests only (no Docker needed)
pytest -m integration        # PostgreSQL test database
pytest -m kafka              # throwaway Kafka topics
```

Spark tests need Java 17 and pyspark, so they run in the processor container:

```bash
$DC run --rm --no-deps --entrypoint python processor -m pytest -m spark
```

CI (`.github/workflows/ci.yml`) runs black, ruff and every group, Spark included, against
PostgreSQL 16 and Kafka 3.7.1 service containers.

## Layout

| Path | Purpose |
|------|---------|
| `deploy/` | Docker Compose and images (Kafka, PostgreSQL, processor, producer/monitor, Superset) |
| `src/streamguard/` | Python package: producer, processor, rules, metrics, feedback, common |
| `sql/` | DDL, views, seed data, rule SQL, dashboard layer |
| `config/` | YAML defaults for rules and DQ/health thresholds (loaded into PostgreSQL) |
| `tests/` | `unit/`, `integration/`, `spark/` |
| `scripts/` | Python helper scripts (start, check, reset, SQL, dashboard, verification) |
| `docs/` | Use case, architecture, decisions, runbook, dashboard and feedback guides |

## Docs

- [Use case card](docs/use_case_card.md)
- [Architecture](docs/architecture.md)
- [Decisions](docs/decisions.md) (D-01 … D-15, ADR-01, ADR-02)
- [Operations runbook](docs/runbook/runbook.md) (health, lag, replay, DLQ, broker down, thresholds, reset)
- [Replay background](docs/runbook/replay.md)
- [CLAUDE.md](CLAUDE.md): rules for working on this repo
