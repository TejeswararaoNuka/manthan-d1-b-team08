# StreamGuard dashboard design

Superset dashboard **StreamGuard** (`http://127.0.0.1:8088/superset/dashboard/streamguard/`),
built by two scripts on PC 1:

```bash
source .venv/Scripts/activate
python scripts/build_dash_layer.py          # schema dash: real data, or demo data where missing
python scripts/build_superset_dashboard.py  # connection, datasets, charts, dashboard; verifies counts
```

Rerun both whenever real tables appear or change; dataset names and columns never change.

## Data layer (`dash.*`) and the Live / Demo rule

All Superset datasets point at `dash.*` only. `build_dash_layer.py` decides per source:

| Source (dash view) | Real source | When missing or empty |
|---|---|---|
| `enrichedTransactions` | `sg.enriched_transactions` | `demo.*` |
| `alerts` | `sg.alerts` | `demo.*` |
| `dlqEvents` | `sg.dlq_events` | `demo.*` |
| `streamMetrics` | `ops.stream_metrics` | **Derived** per minute from real enriched/DLQ rows (events/s from `ingested_at`, p95 from `processed_at − event_time`); `demo.*` if those are demo too |
| `lagSamples` | `ops.lag_samples` | **Not Available** (no rows) with real metrics, else `demo.*` |
| `ruleConfig`, `healthConfig` | `sg.rule_config`, `ops.health_config` | `demo.*` |
| `alertFeedback`, `opsAlerts` | `sg.alert_feedback`, `ops.alerts` | follow `alerts` / `streamMetrics`: empty is normal for live data (no reviews, no incidents), so they never switch to demo while their parent is real |

Demo data is synthetic (`tok_dec0de…`, `mer_dec0…`), covers the last 2 hours, all 4 rules and
all 5 DLQ reason codes, and is re-seeded on every run. The script never writes to `sg.*` or
`ops.*` and refuses `POSTGRES_TEST_DB`. It prints `Source | Real or Demo | Rows` and stores it
in `dash.source_status`; **`dash."dataSource"`** turns that into the Overview banner:
`Data Source: Live`, `Data Source: Demo (synthetic numbers, not real)` or
`Data Source: Mixed (demo: …)`. Never present numbers without that banner.

A view's source is fixed when the script runs: if a real table is emptied later, the view keeps
showing the (empty) real table until the script is rerun. It never switches to demo silently.

## Tabs and charts

All dataset views are in `sql/dash/02_datasets.sql`, built from the dash base views (not from
`sql/views`, so they work even if those are missing).

**Overview**: Data Source (banner) · Open alerts · Alerts (last hour) · Events per second (last
5 minutes of data) · Consumer lag (text, `Not Available` when unknown) · Alerts per minute by rule.

**Alerts**: Top triggered rules (bar) · Rules: totals and open · Alert backlog (open, oldest
first, overdue after `backlog_overdue_minutes`) · Recent alerts.

**Precision**: Precision by rule (all time) · Labelled vs unreviewed · Daily precision per rule ·
Precision trend by rule · Recent labels. Labelling: `docs/feedback_loop.md`.

**Pipeline health**: Pipeline status (lag and p95 latency against `ops.health_config`, DLQ rate
over the last 15 minutes, seconds since last batch) · Consumer lag over time · p95 processing
latency over time · Dead letters by reason · Ops alerts.

"Last hour / last 5 minutes" are measured back from the newest data point, so a paused pipeline
shows its last known state instead of zeros (and demo data stays readable).

## Labels and colours

Chart titles start with `SG · `. Colours come from the dashboard's `label_colors` (no theme in
`superset_config.py`): VELOCITY `#2563EB`, IMPOSSIBLE_TRAVEL `#9333EA`, AMOUNT_SPIKE `#EA580C`,
MERCHANT_BLACKLIST `#DC2626`; MALFORMED_JSON `#6B7280`, MISSING_FIELD `#0891B2`,
AMOUNT_OUT_OF_RANGE `#CA8A04`, CURRENCY_NOT_ALLOWED `#7C3AED`, FUTURE_EVENT_TIME `#DB2777`.
Dashboard CSS only rounds chart corners and enlarges the Data Source banner. Auto-refresh: 30 s.

## Fallbacks

| If… | Then |
|---|---|
| `sql/views` missing | Nothing to do: datasets are built from base tables. |
| `ops.stream_metrics` missing | `streamMetrics` is Derived; lag shows `Not Available` (or demo values in demo mode). |
| `superset_ro` missing | `build_superset_dashboard.py` connects with the `.env` user and prints so. **Note for Nuka:** recreate `superset_ro` (deploy/postgres/init/02_roles.sql) and rerun both scripts, because the `.env` user can write. |
| `data/expected/all.json` missing | Verification still runs: `build_superset_dashboard.py` compares every dataset's row count in Superset with SQL. |
| Handlebars chart unavailable | Not used: the headline is big-number charts plus a one-cell table. |
| Theme in `superset_config.py` not allowed | Not used: colours are `label_colors` + CSS only. |
| No Python on a laptop | Single-PC setup (D-15): run everything on this PC with `.venv` active. |
| Superset unreachable | Not built (no Streamlit fallback): the same numbers are in `dash.*` for SQL clients. |

## Verified (2026-11-20)

`build_superset_dashboard.py`: 14 datasets, 20 charts, 4 tabs; every dataset's row count in
Superset equals SQL; all 16 charts open in Explore. Data Source: Live.
