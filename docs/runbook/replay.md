# Runbook: offset reset and replay

Step-by-step version with backup and proof SQL: [runbook.md](runbook.md) §5.

Why this works the way it does: ADR-02 in `docs/decisions.md` (Spark keeps offsets in its
checkpoint, not in a Kafka consumer group). All commands run on this PC from the repo root,
with `.venv` active. `DC` = `docker compose --env-file .env -f deploy/docker-compose.yml`.

## When to replay
- Rule logic or thresholds were fixed and past events must be re-evaluated.
- The processor's checkpoint is lost or corrupt.
- A practice run (the use-case card asks for one offset reset + replay).

A replay is safe: every write is idempotent (ADR-01). Rows already in PostgreSQL are not
duplicated; Kafka may receive duplicate alerts, which consumers drop by `alert_id`.

## Steps
1. Check health first: `SELECT * FROM ops.v_pipeline_health;` and note `sg.alerts` counts.
2. Stop the processor: `$DC stop processor`
3. Reset the checkpoint, stored watermark and processed offsets, then replay the retained
   topic (7 days) in one bounded run:
   `$DC run --rm --no-deps processor --reset --available-now --starting-offsets earliest`
   - To start from specific offsets instead, pass Spark's JSON form with all 6 partitions,
     e.g. `--starting-offsets '{"payments.raw":{"0":1200,"1":-2,"2":-2,"3":-2,"4":-2,"5":-2}}'`
     (-2 = earliest, -1 = latest).
   - Always use `--reset`: deleting the checkpoint folder alone keeps the stored watermark and
     `last_batch_id`, so the new batch IDs 0..N would be skipped as re-runs.
   - `kafka-consumer-groups --reset-offsets` does **not** work: Spark has no committed group.
4. While it runs, the monitor shows the backlog as lag and raises an ops alert above the
   threshold; this is expected. Watch `$DC logs -f monitor`.
5. Resume live processing: `$DC up -d processor` (continues from the replay's checkpoint).
6. Verify: lag back under the threshold (a `RESOLVED` line in the monitor log), `sg.alerts`
   count unchanged unless thresholds changed (new alerts then carry the new `rule_version`).
   For a scenario: `python scripts/verify_scenario.py data/expected/<scenario>.json`.

## Caveats
- Batch boundaries differ on a replay, so lateness can differ for events near the watermark;
  the scenario replay therefore uses `--available-now` on bounded input (ADR-01).
- Replay creates new alerts only if thresholds changed since the original run.
