# StreamGuard operations runbook

For whoever runs the demo stack and the teammates who use it. Every command below is taken from
the repo (`deploy/docker-compose.yml`, `scripts/`, `src/streamguard/`) and was run on the Docker
laptop on 2026-11-20 unless marked otherwise.

## 0. Conventions

**Where a step runs**
- **[Docker laptop]**: the laptop that runs Docker. Every `$DC`, `psql`, checkpoint and
  `scripts/start.py` / `scripts/reset_demo.py` step runs here.
- **[any laptop]**: any laptop with the repo, an activated `.venv` and a `.env` that points at
  the Docker laptop, or just a browser. That includes the Docker laptop itself.

**LAN access is off today (D-15).** Compose publishes every port on `127.0.0.1`, so
"any laptop" currently means the Docker laptop only. To let teammates connect over the LAN,
follow [README → Other laptops](../../README.md#other-laptops) and record the change as a new
decision. Then use `<LAN-IP>` wherever this runbook says `localhost`. §11 covers an IP
change.

**Shell.** Git Bash from the repo root. Define this once per shell:

```bash
DC="docker compose --env-file .env -f deploy/docker-compose.yml"
```

**psql** [Docker laptop] opens a SQL prompt as the owner role; paste the SQL blocks into it:

```bash
$DC exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

If Git Bash says "the input device is not a TTY", put `winpty` in front of that command.
Read-only SQL can also run **[any laptop]** in Superset → SQL Lab, database
**StreamGuard (dash)**: role `superset_ro`, which can SELECT from `sg`, `ops` and `dash`.

| URL | Where | Login |
|---|---|---|
| http://localhost:8088/superset/dashboard/streamguard/ | any laptop (browser) | `SUPERSET_ADMIN_*` from `.env` |
| http://localhost:4040 (Spark UI) | any laptop (browser) | none |
| `localhost:5432` PostgreSQL, `localhost:29092` Kafka | any laptop (clients) | `.env` |

## 1. Start and stop

**Start everything** [Docker laptop]. This builds images, waits for health, applies SQL, seeds
baselines, builds the dashboard, starts the processor and monitor, and prints the URLs. It is
safe to rerun.

```bash
python scripts/start.py
```

**Start traffic** [Docker laptop]: 200 events/s, with 1,000/s bursts:

```bash
$DC --profile producer up -d producer
```

Or produce for a fixed time [any laptop, `.venv`]:

```bash
python -m streamguard.producer --rate 200 --duration 120
```

**Stop traffic only** [Docker laptop]:

```bash
$DC --profile producer stop producer
```

**Stop the whole stack, keeping all data** [Docker laptop]:

```bash
$DC --profile producer stop        # containers stay; `$DC up -d` brings them back
$DC --profile producer down        # removes containers too; volumes (data) are kept
```

Never add `-v` to `down`. It deletes the `streamguard_pgdata` and `streamguard_kafkadata`
volumes, which means every event, alert, analyst label and the Superset metadata.

**Start again after a stop** [Docker laptop]: `$DC up -d`, or `python scripts/start.py`.

## 2. Check health

**Container states** [Docker laptop]. kafka, postgres and superset must show `healthy`.
processor and monitor have no health check and should show `running`.

```bash
$DC ps
```

**Dashboard** [any laptop]: Superset dashboard → **Pipeline health** tab. It has five charts:
Pipeline status, Consumer lag (payments.raw), p95 processing latency (s), Dead letters by
reason, and Ops alerts. The **Overview** tab's "Data Source" row must say *Live*. *Demo* or
*Mixed* means some panels show seeded demo data (see `docs/dashboard_design.md`).

**ops tables** [Docker laptop psql, or any laptop SQL Lab]:

```sql
SELECT overall_status, lag_status, latency_status, consumer_lag, p95_latency_seconds,
       seconds_since_last_batch, batch_id, input_rows, dlq_count, late_count
  FROM ops.v_pipeline_health;

SELECT raised_at, severity, source, message FROM ops.alerts ORDER BY raised_at DESC LIMIT 10;

SELECT query_name, last_batch_id, watermark, updated_at FROM ops.processor_state;
```

How to read them:
- `overall_status` is `OK`, `ALERT` (lag or latency over the `ops.health_config` threshold)
  or `UNKNOWN` (no data yet).
- **Also check `seconds_since_last_batch`.** The trigger is 5 s, so anything above about
  30 s means the processor is not running batches. `overall_status` does **not** catch this:
  during the broker-down test it stayed `OK` at 60 s.
- `ops.alerts` gets a HIGH row when a threshold is crossed, a repeat every 10 min while it
  is still firing, and a LOW `RESOLVED:` row when it clears.

**Spark UI** [any laptop]: http://localhost:4040 → **Structured Streaming** tab → query
`processor`. Look at input rate against process rate, batch duration (should stay under 5 s
at the normal 200/s), and the latest batch ID. That ID should match `ops.processor_state`.
The UI exists only while the processor is running.

**Logs** [Docker laptop]:

```bash
$DC logs --tail 20 processor   # one line per batch: "batch N: X in, Y new (L late), D dup, Q DLQ, A alerts"
$DC logs --tail 20 monitor     # "HH:MM:SS lag=657 p95=6.4s", plus *** OPS *** lines
```

## 3. Read lag

Spark keeps its Kafka offsets in the checkpoint, not in a consumer group (ADR-02), so
`kafka-consumer-groups.sh` shows nothing for StreamGuard. Lag is computed by the `monitor`
service every 15 s: the Kafka end offsets minus `ops.stream_offsets` (the offsets the processor
committed to PostgreSQL).

- **Live** [Docker laptop]: `$DC logs -f monitor`. `lag=unknown` right after a reset is
  normal until the first batch commits.
- **History** [Docker laptop psql / any laptop SQL Lab]:

  ```sql
  SELECT sampled_at, consumer_lag, lag_by_partition, unknown_partitions
    FROM ops.lag_samples WHERE topic = 'payments.raw'
   ORDER BY sampled_at DESC LIMIT 10;
  ```

- **Raw numbers** [Docker laptop]: Kafka end offsets against what the processor has stored:

  ```bash
  MSYS_NO_PATHCONV=1 $DC exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic payments.raw
  ```

  ```sql
  SELECT partition, last_processed_offset, updated_at
    FROM ops.stream_offsets WHERE topic = 'payments.raw' ORDER BY partition;
  ```

The threshold is `ops.health_config.lag_alert_threshold`, 5,000 messages by default. Lag
above it during a replay, after a pause, or after a broker outage is expected and clears by
itself.

## 4. Pause and resume the processor

[Docker laptop]

```bash
$DC stop processor     # pause: the checkpoint and ops.processor_state stay as they are
$DC start processor    # resume from the checkpoint; nothing is lost or duplicated
```

- While paused, events wait in Kafka (7-day retention), and the monitor raises a lag alert
  once 5,000 messages are behind.
- Do not use `$DC pause`. It freezes the JVM in the middle of a batch.
- `restart: unless-stopped` keeps a stopped processor stopped, including across Docker
  restarts.

## 5. Replay (offset reset)

Background: [replay.md](replay.md) and ADR-01. A replay is safe:
- `sg.enriched_transactions`, `sg.alerts` and `sg.dlq_events` are written `ON CONFLICT DO
  NOTHING`, so events already stored are not inserted again and analyst state is kept.
- Rules run only for events that are new to PostgreSQL, so replaying stored events creates
  no alerts.
- Kafka may get duplicate alert messages; consumers dedup by `alert_id`.

1. **Record the before counts** [Docker laptop]. Run the queries in [Proof of replay](#proof-of-replay).
2. **Stop the processor** [Docker laptop]:

   ```bash
   $DC stop processor
   ```

3. **Back up the checkpoint** [Docker laptop]. The `data/checkpoints/*` folders are
   git-ignored.

   ```bash
   python -c "import shutil, time; shutil.copytree('data/checkpoints/processor', 'data/checkpoints/processor.bak-' + time.strftime('%Y%m%d-%H%M%S'))"
   ```

4. **Delete the checkpoint and replay** [Docker laptop]. `--reset` deletes the contents of
   `data/checkpoints/processor` **and** clears the stored watermark and offsets
   (`ops.processor_state`, `ops.stream_offsets`). `--available-now` processes the backlog in
   bounded batches and then exits. Pick one starting point:

   ```bash
   # a) everything Kafka still holds (7 days)
   $DC run --rm --no-deps processor --reset --available-now --starting-offsets earliest

   # b) a specific offset per partition (Spark needs all 6 partitions; -2 = earliest, -1 = latest)
   $DC run --rm --no-deps processor --reset --available-now \
     --starting-offsets '{"payments.raw":{"0":271866,"1":279691,"2":261097,"3":263734,"4":269092,"5":263790}}'
   ```

   - To pick offsets for (b), take `ops.stream_offsets` (§3) and subtract how far back you
     want to go.
   - The same values work as `-e STARTING_OFFSETS=...`, which compose passes to the
     processor.
   - **Do not delete the folder by hand without `--reset`.** A new checkpoint restarts batch
     IDs at 0. The processor would treat those IDs as already done (≤
     `ops.processor_state.last_batch_id`) and only re-publish them. The old watermark would
     also mark the replayed events as late.
   - `kafka-consumer-groups.sh --reset-offsets` does nothing here (ADR-02).

5. **Restart live processing** [Docker laptop]. It continues from the checkpoint the replay
   left behind:

   ```bash
   $DC up -d processor
   ```

6. **Verify** [Docker laptop]:
   - Run the [Proof of replay](#proof-of-replay) queries again; the counts must match.
   - The monitor log shows `RESOLVED: consumer lag back to ...` once the backlog is
     processed (`$DC logs -f monitor`).
   - For a scenario run: `python scripts/verify_scenario.py data/expected/all.json`.

**Roll back to the backup** [Docker laptop], only if the replay run failed part-way. Use the
backup folder name from step 3:

```bash
$DC stop processor
python -c "import shutil; shutil.rmtree('data/checkpoints/processor'); shutil.copytree('data/checkpoints/processor.bak-20261120-193203', 'data/checkpoints/processor')"
$DC up -d processor
```

## 6. Dead-letter spike

Signs:
- The **Dead letters by reason** chart jumps.
- `dlq_count` rises in `ops.v_pipeline_health`.
- `Q DLQ` in the processor log is above 0 on most batches.

The random producer sends almost no bad events (6 so far on 2026-11-20), so a steady stream is
abnormal.

1. **Which reason and since when** [Docker laptop psql / any laptop SQL Lab]:

   ```sql
   SELECT reason_code, count(*), min(failed_at), max(failed_at)
     FROM sg.dlq_events
    WHERE failed_at > now() - interval '15 minutes'
    GROUP BY reason_code ORDER BY count(*) DESC;

   SELECT failed_at, reason_code, error_detail, left(raw_value, 200) AS raw
     FROM sg.dlq_events ORDER BY id DESC LIMIT 10;
   ```

2. **Look at the messages themselves** [any laptop, `.venv`]:

   ```bash
   python -m streamguard.producer.peek payments.dlq -n 5
   ```

3. **Act on the reason**:

   | Reason | Likely cause | Action |
   |---|---|---|
   | `MALFORMED_JSON`, `MISSING_FIELD` | producer bug or a schema change | stop the producer (`$DC --profile producer stop producer`) and fix it; the processor keeps running |
   | `CURRENCY_NOT_ALLOWED` | a new, legitimate currency | if the business agrees, extend `allowed_currencies` (§8) |
   | `AMOUNT_OUT_OF_RANGE` | amounts over `amount_ceiling` (500000.00) or ≤ 0 | check the producer; raise the ceiling only by decision (C3/D-01) |
   | `FUTURE_EVENT_TIME` | producer clock more than 24 h ahead | fix the clock on the sending machine |

   `LATE_EVENT` is never a dead letter. It shows as `late_count` in `ops.stream_metrics`
   (D-04).

4. **Keep the processor running.** Dead letters are stored once (unique on topic,
   partition, offset), and a replay does not duplicate them. There is no redrive tool:
   events that were fixed must be produced again with the same `event_id`.

## 7. Broker down

**What happens.** This was tested on 2026-11-20 by stopping Kafka for 45 s.
- The producer keeps buffering (`delivered` stops growing, `failed=0`) and delivers
  everything once Kafka is back.
- The processor's Spark query fails and Docker restarts the container
  (`restart: unless-stopped`). It then resumes from the checkpoint with no loss.
- The monitor logs `WARNING: Kafka unavailable` but writes no ops alert. In
  `ops.v_pipeline_health` only `seconds_since_last_batch` grows.

1. **Confirm** [Docker laptop]:

   ```bash
   $DC ps kafka
   $DC logs --tail 50 kafka
   $DC logs --tail 5 monitor      # "WARNING: Kafka unavailable: ... Broker transport failure"
   ```

2. **Bring it back** [Docker laptop]. Data survives in the `streamguard_kafkadata` volume:

   ```bash
   $DC up -d kafka
   ```

   Then wait for `healthy` in `$DC ps` (about 20–60 s).

3. **Make sure the processor and monitor are running** [Docker laptop]:

   ```bash
   $DC up -d processor monitor
   $DC logs --tail 5 processor    # "batch N: ..." lines again
   ```

4. **Verify** [Docker laptop / any laptop]:
   - Lag spikes, then clears: `RESOLVED: consumer lag back to ...` in the monitor log.
   - `seconds_since_last_batch` is back under 10.

**If Kafka will not start.**
- Check disk space and Docker Desktop memory. Kafka has `mem_limit: 1g`.
- If the topics are missing, recreate them with `$DC run --rm kafka-init`. It is idempotent.

## 8. Rotate thresholds (rule_config)

- Thresholds live in `sg.rule_config.params` and `sg.dq_config`, and are re-read on every
  micro-batch. There is no restart.
- Every real change to a rule row bumps its `version` (trigger). New alerts carry the new
  `rule_version`; old alerts are not changed.
- Writes need the owner role, so this runs in **psql on the Docker laptop**.

```sql
-- see the current values
SELECT rule_id, enabled, severity, version, params, updated_at FROM sg.rule_config ORDER BY rule_id;

-- change one parameter (keys: VELOCITY max_tx/window_seconds, IMPOSSIBLE_TRAVEL window_minutes,
-- AMOUNT_SPIKE multiplier/lookback_days)
UPDATE sg.rule_config SET params = params || '{"max_tx": 6}' WHERE rule_id = 'VELOCITY';

-- switch a rule off / on, or change its severity
UPDATE sg.rule_config SET enabled = false WHERE rule_id = 'AMOUNT_SPIKE';
UPDATE sg.rule_config SET severity = 'MEDIUM' WHERE rule_id = 'IMPOSSIBLE_TRAVEL';

-- DQ settings (keys: amount_ceiling, allowed_currencies, max_future_hours, watermark_seconds)
UPDATE sg.dq_config SET value = '["INR","USD","EUR","GBP","SGD","AED","JPY"]', updated_at = now()
 WHERE key = 'allowed_currencies';

-- check that it took effect: new alerts appear under the new version within a few batches
SELECT rule_id, rule_version, count(*) FROM sg.alerts
 WHERE alert_time > now() - interval '10 minutes' GROUP BY 1, 2 ORDER BY 1, 2;
```

**Back to the defaults** in `config/rules.yaml` / `config/dq.yaml` [Docker laptop]. A plain
`apply_sql.py` keeps runtime edits; `--reset-config` overwrites them:

```bash
$DC run --rm --no-deps --entrypoint python processor scripts/apply_sql.py --reset-config
```

Changing a default permanently means editing the YAML in a commit and recording it in
`docs/decisions.md`.

## 9. Reset before the demo

[Docker laptop] This **deletes all pipeline data**. It does the following:
- Stops the processor and the producer.
- Deletes and recreates `payments.raw`, `payments.alerts` and `payments.dlq`.
- Truncates the `sg` data tables (including analyst labels) and the ops progress and metrics
  tables.
- Deletes the checkpoint and restarts the processor.

It keeps the config tables, the merchant blacklist and the card baselines.

```bash
python scripts/reset_demo.py              # asks you to type "reset"; --yes skips the question
$DC run --rm --no-deps --entrypoint python processor scripts/build_dash_layer.py
$DC --profile producer up -d producer     # start fresh traffic
```

Then check:
- The Overview "Data Source" chart. With empty tables some panels may show *Demo*
  fallbacks until live data arrives (`docs/dashboard_design.md`).
- `$DC logs --tail 5 processor` shows batches again.

## 10. Teammates on other laptops

Needs LAN access enabled first (§0).

- **Browser only** [any laptop]: `http://<LAN-IP>:8088/superset/dashboard/streamguard/` and
  `http://<LAN-IP>:4040`.
- **CLIs** [any laptop]: clone the repo, then:

  ```bash
  python -m venv .venv
  source .venv/Scripts/activate
  pip install -r requirements.txt
  pip install -e .
  ```

  In `.env`, set `POSTGRES_HOST=<LAN-IP>` and `KAFKA_BOOTSTRAP=<LAN-IP>:29092`. Then:

  ```bash
  python -m streamguard.producer.peek payments.alerts -n 5
  python -m streamguard.feedback open --limit 20
  python -m streamguard.feedback label <alert_id prefix> TP --analyst <name>
  ```

## 11. The Docker laptop's IP changed

Only matters with LAN access enabled. Signs on the other laptops:
- Superset will not load.
- Kafka clients connect, then time out: Kafka still advertises the old address.

1. **Find the new IP** [Docker laptop]:

   ```bash
   python -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect(('192.0.2.1', 9)); print(s.getsockname()[0])"
   ```

   No packet is sent. `ipconfig` in PowerShell shows the same address.

2. **Update `.env`** [Docker laptop]: set `KAFKA_HOST_IP=<new IP>`.
3. **Recreate Kafka** [Docker laptop]. Compose sees the changed advertised listener and
   recreates only Kafka. The processor and monitor use `kafka:9092` inside Docker and are not
   affected.

   ```bash
   $DC up -d kafka
   ```

4. **Teammates** [any laptop]: set `POSTGRES_HOST` and `KAFKA_BOOTSTRAP` in their `.env` to
   the new IP, and use the new IP in the browser URLs.
5. **Check** [any laptop]: `python -m streamguard.producer.peek payments.alerts -n 1` returns a
   message.

To avoid this, ask for a DHCP reservation for the Docker laptop.

## Proof of replay

Run this in psql [Docker laptop] **before step 2** of §5 (with the processor stopped, so the
counts are final) and again after step 6, then paste both results below.

The `VALUES` list is the replayed range per partition: `lo` = the `--starting-offsets` value,
`hi` = `last_processed_offset` from `ops.stream_offsets` at the moment the processor stopped.
Events after `hi` arrive during the replay and are new, so they are not counted.

```sql
WITH r(p, lo, hi) AS (VALUES (0, 271866, 273866), (1, 279691, 281691), (2, 261097, 263097),
                              (3, 263734, 265734), (4, 269092, 271092), (5, 263790, 265790)),
ev AS (SELECT e.event_id, e.processed_at FROM sg.enriched_transactions e
         JOIN r ON e.kafka_partition = r.p AND e.kafka_offset BETWEEN r.lo AND r.hi)
SELECT now()::timestamp(0)                                         AS checked_at,
       (SELECT count(*) FROM ev)                                   AS events_in_range,
       (SELECT max(processed_at)::timestamp(0) FROM ev)            AS last_processed_at,
       (SELECT count(*) FROM sg.dlq_events d JOIN r
           ON d.source_topic = 'payments.raw' AND d.kafka_partition = r.p
          AND d.kafka_offset BETWEEN r.lo AND r.hi)                AS dlq_in_range,
       (SELECT count(*) FROM sg.alerts a
           JOIN ev ON ev.event_id::text = a.details ->> 'trigger_event_id')
                                                                   AS alerts_triggered_in_range,
       (SELECT max(a.alert_time)::timestamp(0) FROM sg.alerts a
           JOIN ev ON ev.event_id::text = a.details ->> 'trigger_event_id')
                                                                   AS last_alert_time,
       (SELECT count(*) FROM (SELECT 1 FROM sg.alerts
           GROUP BY rule_id, card_token, event_ids
           HAVING count(*) > 1) AS d)                              AS duplicate_alert_groups;
```

**Pass if:**
- `events_in_range`, `dlq_in_range` and `alerts_triggered_in_range` are equal before and
  after.
- `last_processed_at` and `last_alert_time` did not move. Rows were kept, not rewritten.
- `duplicate_alert_groups` is 0. No two alerts share the same rule, card and events.

Alerts are counted by their trigger event (`details.trigger_event_id`), not by any event they
reference. A newer event's velocity or travel window may include replayed events, and that
is a correct new alert, not a duplicate.

**Recorded run, 2026-11-20 (UTC).** The range was the last 2,001 offsets of each of the 6
partitions. The processor log showed `batch 0: 9997 in, 0 new`: every replayed event was
already stored.

| | checked_at | events_in_range | last_processed_at | dlq_in_range | alerts_triggered_in_range | last_alert_time | duplicate_alert_groups |
|---|---|---|---|---|---|---|---|
| before | 2026-11-20 14:01:52 | 12006 | 2026-11-20 14:01:36 | 0 | 113 | — | 0 |
| after  | 2026-11-20 14:03:52 | 12006 | 2026-11-20 14:01:36 | 0 | 113 | 2026-11-20 14:01:36 | 0 |

The "before" alert count was taken with an earlier version of the query. It was confirmed
afterwards: 113 alerts triggered in the range have `alert_time` before 14:01:52, and 0
were created since.

Your run:

```text
before:

after:
```
