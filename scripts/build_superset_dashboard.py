"""Create (or re-create) the StreamGuard Superset dashboard on the dash.* layer.

    python scripts/build_dash_layer.py          # first: build/refresh dash.*
    python scripts/build_superset_dashboard.py  # then: connection, datasets, charts, dashboard

Uses the Superset REST API at SUPERSET_URL (default http://127.0.0.1:8088) as
SUPERSET_ADMIN_USER. The data connection logs in as superset_ro (read-only); if that role is
missing it falls back to the .env user and says so (docs/dashboard_design.md). Datasets point
only at dash.*. Re-running replaces the dashboard and its charts ("SG · ..."); datasets and
the connection are reused. Ends by comparing each dataset's row count through Superset with
a direct SQL count.
"""

from __future__ import annotations

import http.cookiejar
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from psycopg2 import sql  # noqa: E402

from streamguard.common.settings import load_env, pg_connect  # noqa: E402

DB_NAME = "StreamGuard (dash)"
SLUG = "streamguard"
PREFIX = "SG · "
DATASETS = [
    "dataSource",
    "headline",
    "alertsPerMinute",
    "topRules",
    "alertBacklog",
    "rulePrecisionDaily",
    "recentAlerts",
    "pipelineHealth",
    "lagOverTime",
    "latencyOverTime",
    "dlqByReason",
    "opsAlerts",
]
RULE_COLORS = {
    "VELOCITY": "#2563EB",
    "IMPOSSIBLE_TRAVEL": "#9333EA",
    "AMOUNT_SPIKE": "#EA580C",
    "MERCHANT_BLACKLIST": "#DC2626",
}
REASON_COLORS = {
    "MALFORMED_JSON": "#6B7280",
    "MISSING_FIELD": "#0891B2",
    "AMOUNT_OUT_OF_RANGE": "#CA8A04",
    "CURRENCY_NOT_ALLOWED": "#7C3AED",
    "FUTURE_EVENT_TIME": "#DB2777",
}
CSS = """
.dashboard-header .header-title span { color: #0F172A; }
.dashboard-component-chart-holder { border-radius: 8px; }
[data-test-chart-name="SG · Data Source"] td { font-size: 18px; font-weight: 600; }
"""


class Superset:
    def __init__(self, base: str, user: str, password: str):
        self.base = base.rstrip("/")
        jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        self.token = self.call(
            "POST",
            "/api/v1/security/login",
            {"username": user, "password": password, "provider": "db", "refresh": True},
        )["access_token"]
        self.csrf = self.call("GET", "/api/v1/security/csrf_token/")["result"]

    def call(self, method: str, path: str, body=None):
        req = urllib.request.Request(
            self.base + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
        )
        if body is not None:
            req.add_header("Content-Type", "application/json")
        req.add_header("Referer", self.base)
        if getattr(self, "token", None):
            req.add_header("Authorization", f"Bearer {self.token}")
        if getattr(self, "csrf", None):
            req.add_header("X-CSRFToken", self.csrf)
        try:
            raw = self.opener.open(req, timeout=60).read()
        except urllib.error.HTTPError as exc:
            detail = exc.read()[:500]
            raise SystemExit(f"Superset {method} {path} -> {exc.code}: {detail!r}") from None
        return json.loads(raw) if raw else {}

    def find(self, resource: str, col: str, opr: str, value: str) -> list[dict]:
        q = f"(filters:!((col:{col},opr:{opr},value:'{value}')),page_size:100)"
        return self.call("GET", f"/api/v1/{resource}/?q={urllib.parse.quote(q)}")["result"]


def metric(expression: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def big_number(expression: str, label: str, subheader: str) -> dict:
    return {
        "viz_type": "big_number_total",
        "metric": metric(expression, label),
        "subheader": subheader,
        "y_axis_format": "SMART_NUMBER",
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "time_range": "No filter",
        "adhoc_filters": [],
    }


def table(columns: list[str], order: str | None = None, desc: bool = True, rows: int = 500):
    return {
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": columns,
        "order_by_cols": [json.dumps([order, not desc])] if order else [],
        "row_limit": rows,
        "server_page_length": 10,
        "time_range": "No filter",
        "adhoc_filters": [],
        "include_search": False,
        "table_timestamp_format": "%Y-%m-%d %H:%M:%S",
    }


def line(x: str, expression: str, label: str, groupby: list[str] | None = None, grain="PT1M"):
    return {
        "viz_type": "echarts_timeseries_line",
        "x_axis": x,
        "time_grain_sqla": grain,
        "metrics": [metric(expression, label)],
        "groupby": groupby or [],
        "adhoc_filters": [],
        "row_limit": 10000,
        "time_range": "No filter",
        "show_legend": True,
        "rich_tooltip": True,
        "truncate_metric": True,
        "y_axis_format": "SMART_NUMBER",
        "markerEnabled": False,
    }


def bar(x: str, expression: str, label: str) -> dict:
    return {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": x,
        "metrics": [metric(expression, label)],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 100,
        "time_range": "No filter",
        "orientation": "vertical",
        "show_legend": False,
        "y_axis_format": "SMART_NUMBER",
    }


# (tab, row, width, height, title, dataset, params)
CHARTS = [
    ("Overview", 0, 12, 10, "Data Source", "dataSource", table(["data_source", "checked_at"])),
    (
        "Overview",
        1,
        3,
        30,
        "Open alerts",
        "headline",
        big_number("MAX(open_alerts)", "Open alerts", "alerts with no analyst feedback"),
    ),
    (
        "Overview",
        1,
        3,
        30,
        "Alerts (last hour)",
        "headline",
        big_number("MAX(alerts_last_hour)", "Alerts (last hour)", "newest hour of alerts"),
    ),
    (
        "Overview",
        1,
        3,
        30,
        "Events per second",
        "headline",
        big_number("MAX(events_per_sec)", "Events per second", "last 5 minutes of data"),
    ),
    ("Overview", 1, 3, 30, "Consumer lag", "headline", table(["consumer_lag_text"])),
    (
        "Overview",
        2,
        12,
        50,
        "Alerts per minute by rule",
        "alertsPerMinute",
        line("minute", "SUM(alerts)", "Alerts", ["rule_id"]),
    ),
    (
        "Alerts",
        0,
        6,
        50,
        "Top triggered rules",
        "topRules",
        bar("rule_id", "SUM(alerts_total)", "Alerts"),
    ),
    (
        "Alerts",
        0,
        6,
        50,
        "Rules: totals and open",
        "topRules",
        table(
            ["rule_id", "enabled", "severity", "alerts_total", "alerts_last_hour", "open_alerts"],
            "alerts_total",
        ),
    ),
    (
        "Alerts",
        1,
        12,
        50,
        "Alert backlog (open, oldest first)",
        "alertBacklog",
        table(
            [
                "alert_time",
                "age_minutes",
                "is_overdue",
                "rule_id",
                "severity",
                "card_token",
                "alert_id",
            ],
            "alert_time",
            desc=False,
        ),
    ),
    (
        "Alerts",
        2,
        6,
        50,
        "Daily precision per rule",
        "rulePrecisionDaily",
        table(
            [
                "alert_day",
                "rule_id",
                "alerts",
                "labelled",
                "true_positives",
                "false_positives",
                "unreviewed",
                "precision",
            ],
            "alert_day",
        ),
    ),
    (
        "Alerts",
        2,
        6,
        50,
        "Recent alerts",
        "recentAlerts",
        table(["alert_time", "rule_id", "severity", "label", "card_token"], "alert_time"),
    ),
    (
        "Pipeline health",
        0,
        12,
        18,
        "Pipeline status",
        "pipelineHealth",
        table(
            [
                "consumer_lag",
                "lag_status",
                "p95_latency_seconds",
                "latency_status",
                "dlq_rate_pct_15min",
                "last_batch_at",
                "seconds_since_batch",
            ]
        ),
    ),
    (
        "Pipeline health",
        1,
        6,
        50,
        "Consumer lag (payments.raw)",
        "lagOverTime",
        line("sampled_at", "MAX(consumer_lag)", "Consumer lag"),
    ),
    (
        "Pipeline health",
        1,
        6,
        50,
        "p95 processing latency (s)",
        "latencyOverTime",
        line("recorded_at", "MAX(p95_latency_seconds)", "p95 latency (s)"),
    ),
    (
        "Pipeline health",
        2,
        6,
        50,
        "Dead letters by reason",
        "dlqByReason",
        bar("reason_code", "SUM(dead_letters)", "Dead letters"),
    ),
    (
        "Pipeline health",
        2,
        6,
        50,
        "Ops alerts",
        "opsAlerts",
        table(["raised_at", "severity", "source", "message"], "raised_at"),
    ),
]


def connection_uri(env, conn) -> tuple[str, bool]:
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_ro')")
        has_ro = cur.fetchone()[0]
    conn.rollback()
    user, pw = (
        ("superset_ro", env.get("SUPERSET_RO_PASSWORD", ""))
        if has_ro
        else (env["POSTGRES_USER"], env["POSTGRES_PASSWORD"])
    )
    quoted = urllib.parse.quote(pw, safe="")
    # Superset runs in Docker: the database host is the compose service name.
    return f"postgresql+psycopg2://{user}:{quoted}@postgres:5432/{env['POSTGRES_DB']}", has_ro


def layout(chart_ids: list[int]) -> dict:
    pos: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["TABS-main"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": "StreamGuard"}},
        "TABS-main": {"type": "TABS", "id": "TABS-main", "children": [], "parents": ["ROOT_ID"]},
    }
    for (tab, row, width, height, title, _, _), chart_id in zip(CHARTS, chart_ids, strict=True):
        tab_id = "TAB-" + tab.lower().replace(" ", "-")
        if tab_id not in pos:
            pos[tab_id] = {
                "type": "TAB",
                "id": tab_id,
                "children": [],
                "parents": ["ROOT_ID", "TABS-main"],
                "meta": {"text": tab, "defaultText": "Tab title", "placeholder": "Tab title"},
            }
            pos["TABS-main"]["children"].append(tab_id)
        row_id = f"ROW-{tab_id}-{row}"
        if row_id not in pos:
            pos[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [],
                "parents": ["ROOT_ID", "TABS-main", tab_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            pos[tab_id]["children"].append(row_id)
        cid = f"CHART-{chart_id}"
        pos[cid] = {
            "type": "CHART",
            "id": cid,
            "children": [],
            "parents": ["ROOT_ID", "TABS-main", tab_id, row_id],
            "meta": {
                "chartId": chart_id,
                "width": width,
                "height": height,
                "sliceName": PREFIX + title,
            },
        }
        pos[row_id]["children"].append(cid)
    return pos


def main() -> int:
    env = load_env()
    base = env.get("SUPERSET_URL", "http://127.0.0.1:8088")
    ss = Superset(base, env["SUPERSET_ADMIN_USER"], env["SUPERSET_ADMIN_PASSWORD"])
    conn = pg_connect(env, app="streamguard-superset-build")

    # 1. connection (read-only role)
    uri, has_ro = connection_uri(env, conn)
    body = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": uri,
        "expose_in_sqllab": True,
        "allow_dml": False,
    }
    found = ss.find("database", "database_name", "eq", DB_NAME)
    db_id = (
        ss.call("PUT", f"/api/v1/database/{found[0]['id']}", body) and found[0]["id"]
        if found
        else ss.call("POST", "/api/v1/database/", body)["id"]
    )
    print(f"connection '{DB_NAME}' (id {db_id}) as {'superset_ro' if has_ro else 'the .env user'}")

    # 2. datasets on dash.* only
    ds_ids: dict[str, int] = {}
    for name in DATASETS:
        match = [
            d
            for d in ss.find("dataset", "table_name", "eq", name)
            if d.get("schema") == "dash" and d["database"]["id"] == db_id
        ]
        ds_id = (
            match[0]["id"]
            if match
            else ss.call(
                "POST",
                "/api/v1/dataset/",
                {"database": db_id, "schema": "dash", "table_name": name},
            )["id"]
        )
        ss.call("PUT", f"/api/v1/dataset/{ds_id}/refresh")
        ds_ids[name] = ds_id
    print(f"{len(ds_ids)} datasets on dash.*")

    # 3. replace dashboard and charts
    for d in ss.find("dashboard", "slug", "eq", SLUG):
        ss.call("DELETE", f"/api/v1/dashboard/{d['id']}")
    old = [c["id"] for c in ss.find("chart", "slice_name", "sw", PREFIX)]
    if old:
        ss.call(
            "DELETE", f"/api/v1/chart/?q={urllib.parse.quote('!(' + ','.join(map(str, old)) + ')')}"
        )
    dash_id = ss.call(
        "POST",
        "/api/v1/dashboard/",
        {"dashboard_title": "StreamGuard", "slug": SLUG, "published": True},
    )["id"]
    chart_ids = []
    for _, _, _, _, title, dataset, params in CHARTS:
        params = {**params, "datasource": f"{ds_ids[dataset]}__table"}
        chart_ids.append(
            ss.call(
                "POST",
                "/api/v1/chart/",
                {
                    "slice_name": PREFIX + title,
                    "viz_type": params["viz_type"],
                    "datasource_id": ds_ids[dataset],
                    "datasource_type": "table",
                    "params": json.dumps(params),
                    "dashboards": [dash_id],
                },
            )["id"]
        )
    metadata = {
        "color_scheme": "supersetColors",
        "label_colors": {**RULE_COLORS, **REASON_COLORS},
        "refresh_frequency": 30,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "cross_filters_enabled": False,
    }
    ss.call(
        "PUT",
        f"/api/v1/dashboard/{dash_id}",
        {
            "position_json": json.dumps(layout(chart_ids)),
            "json_metadata": json.dumps(metadata),
            "css": CSS,
        },
    )
    print(f"dashboard: {base}/superset/dashboard/{SLUG}/ ({len(chart_ids)} charts, 3 tabs)")

    # 4. verify: rows through Superset == rows by SQL
    print(f"\n{'Dataset':<20}| {'Superset':>9} | {'SQL':>9} | ok")
    bad = 0
    for name, ds_id in ds_ids.items():
        result = ss.call(
            "POST",
            "/api/v1/chart/data",
            {
                "datasource": {"id": ds_id, "type": "table"},
                "queries": [{"metrics": [metric("COUNT(*)", "n")], "columns": [], "row_limit": 1}],
                "result_format": "json",
                "result_type": "full",
            },
        )
        via_superset = result["result"][0]["data"][0]["n"]
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT count(*) FROM dash.{}").format(sql.Identifier(name)))
            via_sql = cur.fetchone()[0]
        conn.rollback()
        ok = via_superset == via_sql
        bad += not ok
        print(f"{name:<20}| {via_superset:>9,} | {via_sql:>9,} | {'yes' if ok else 'NO'}")
    conn.close()
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
