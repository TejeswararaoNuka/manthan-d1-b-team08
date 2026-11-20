# Feedback loop: labelling alerts and daily precision

Analysts mark each alert as a true or false positive; the dashboard's **Precision** tab and the
CLI report show daily precision per rule: `true positives / labelled` (NULL when nothing is
labelled; unreviewed alerts are shown separately and never counted as false positives, D-12).

## Labelling: the CLI (always available)

Run on this PC with `.venv` active (`source .venv/Scripts/activate`):

```bash
python -m streamguard.feedback open --limit 20                 # open alerts, oldest first
python -m streamguard.feedback open --rule VELOCITY
python -m streamguard.feedback label 5ff66c04 TP --analyst asha  # unique 8+ char prefix is enough
python -m streamguard.feedback label 5ff66c04 FP --analyst asha --comment "cardholder confirmed"
python -m streamguard.feedback report --days 7                 # daily precision per rule
```

Relabelling overwrites the earlier label (upsert on `alert_id`). Labelling closes the alert, so it
leaves the backlog. Reads go through the dash layer as `superset_ro`; labels are written as
`feedback_writer`, which can only write `sg.alert_feedback` (D-05).

## Labelling: Superset SQL Lab

`scripts/build_superset_dashboard.py` creates the connection **StreamGuard feedback (DML)**
(`feedback_writer`, *Allow DML* on that connection only). In SQL Lab, pick it and run:

```sql
INSERT INTO sg.alert_feedback (alert_id, label, analyst, comment)
VALUES ('<full alert_id>', 'TRUE_POSITIVE', 'asha', 'optional note')
ON CONFLICT (alert_id) DO UPDATE
SET label = EXCLUDED.label, analyst = EXCLUDED.analyst,
    noted_at = now(), comment = EXCLUDED.comment;
```

If a DML connection is not permitted in Superset, use the CLI only: everything above works
without it, and the dashboard connection (`superset_ro`) stays read-only either way.

## Where labels go (dash layer)

`scripts/build_dash_layer.py` records in `dash.source_status` which table is current, and the
CLI writes there:

| `alertFeedback` mode | When | Labels written to |
|---|---|---|
| Real | `sg.alerts` has rows and `sg.alert_feedback` exists | `sg.alert_feedback` |
| Fallback | `sg.alerts` has rows, `sg.alert_feedback` missing | `dash_feedback.alert_feedback` (same columns and key) |
| Demo | `sg.alerts` is empty | `demo.alert_feedback`; every CLI output says **(demo data)** |

Demo labels are wiped when the dash layer is rebuilt (demo data is re-seeded).

### For Aashi: creating `sg.alert_feedback`

It is part of `sql/ddl/01_sg.sql` (`python scripts/apply_sql.py`). If it must be created by
hand, this is the exact SQL (run as `POSTGRES_USER`):

```sql
CREATE TABLE IF NOT EXISTS sg.alert_feedback (
    alert_id  char(64)    PRIMARY KEY REFERENCES sg.alerts (alert_id),
    label     text        NOT NULL CHECK (label IN ('TRUE_POSITIVE', 'FALSE_POSITIVE')),
    analyst   text        NOT NULL,
    noted_at  timestamptz NOT NULL DEFAULT now(),
    comment   text
);
GRANT USAGE ON SCHEMA sg TO superset_ro, feedback_writer;
GRANT SELECT ON sg.alert_feedback TO superset_ro;
-- SELECT is required by PostgreSQL for INSERT ... ON CONFLICT DO UPDATE.
GRANT SELECT, INSERT, UPDATE ON sg.alert_feedback TO feedback_writer;
```

(`01_sg.sql` also adds the trigger that closes an alert when it is labelled.) Then move any
labels collected in the fallback table and switch the dashboard back:

```bash
python scripts/build_dash_layer.py --migrate-feedback
```

It upserts `dash_feedback.alert_feedback` into `sg.alert_feedback` (rows whose alert exists in
`sg.alerts`; the rest are reported as skipped), keeps the fallback table, and rebuilds `dash.*`
with `alertFeedback` = Real.

### For Nuka: `feedback_writer` missing

If the role cannot log in, the CLI prints a warning and writes as the `.env` user (which can write
everything), and no SQL Lab DML connection is created. Recreate the role
(`deploy/postgres/init/02_roles.sql`, password `FEEDBACK_WRITER_PASSWORD` in `.env`), then rerun
`python scripts/apply_sql.py`, `python scripts/build_dash_layer.py` and
`python scripts/build_superset_dashboard.py`.

## Without Python on a laptop

Everything runs on this PC (D-15). Inside the processor container (it mounts `src` and `tests`):

```bash
DC="docker compose --env-file .env -f deploy/docker-compose.yml"
$DC exec processor python -m streamguard.feedback open --limit 20
$DC exec processor python -m streamguard.feedback label 5ff66c04 TP --analyst asha
$DC exec processor python -m streamguard.feedback report --days 7
$DC run --rm --no-deps --entrypoint python processor -m pytest                 # unit tests
$DC run --rm --no-deps --entrypoint python processor -m pytest -m integration  # test DB
```

In Git Bash, prefix with `MSYS_NO_PATHCONV=1` if a path argument gets rewritten.

## Tests

`tests/integration/test_feedback.py` (POSTGRES_TEST_DB only; the session fixture applies the full
schema, so missing tables are created): labelling closes and relabelling updates; unknown,
ambiguous and too-short prefixes are rejected; precision = TP / labelled and NULL without labels;
demo mode labels demo alerts and prints "(demo data)"; a missing `sg.alert_feedback` uses the
fallback table and `--migrate-feedback` moves the rows back.
