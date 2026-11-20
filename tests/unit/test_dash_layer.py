import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("dash", REPO_ROOT / "scripts/build_dash_layer.py")
dash = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dash)


@pytest.fixture
def tables(monkeypatch):
    """Pretend database: relation -> row count (absent = table missing)."""
    state: dict[str, int] = {}
    monkeypatch.setattr(dash, "_exists", lambda cur, rel: rel in state)
    monkeypatch.setattr(dash, "_has_rows", lambda cur, rel: state.get(rel, 0) > 0)
    return state


ALL_REAL = {real: 10 for real, _, _ in dash.SOURCES.values()}


def test_sources_with_rows_are_real_and_missing_or_empty_ones_are_demo(tables):
    tables.update(ALL_REAL)
    tables["sg.dlq_events"] = 0
    del tables["sg.alerts"]
    modes = dash.decide_modes(None)
    assert modes["enrichedTransactions"] == "Real"
    assert modes["dlqEvents"] == "Demo" and modes["alerts"] == "Demo"


def test_empty_feedback_and_ops_alerts_stay_real_when_their_parent_is_real(tables):
    tables.update(ALL_REAL)
    tables["sg.alert_feedback"] = 0
    tables["ops.alerts"] = 0
    modes = dash.decide_modes(None)
    assert modes["alertFeedback"] == "Real" and modes["opsAlerts"] == "Real"


def test_missing_stream_metrics_are_derived_from_real_enriched_rows(tables):
    tables.update(ALL_REAL)
    del tables["ops.stream_metrics"]
    del tables["ops.lag_samples"]
    modes = dash.decide_modes(None)
    assert modes["streamMetrics"] == "Derived"
    assert modes["lagSamples"] == "Not Available"


def test_empty_database_is_all_demo(tables):
    assert set(dash.decide_modes(None).values()) == {"Demo"}


def test_not_available_view_keeps_columns_but_returns_no_rows():
    parts = repr(dash.view_sql("lagSamples", "Not Available"))
    assert "'demo'" in parts and "WHERE false" in parts
    assert "consumer_lag" in parts


def test_dash_layer_refuses_the_test_database(monkeypatch, capsys):
    monkeypatch.setattr(
        dash, "load_env", lambda: {"POSTGRES_DB": "streamguard", "POSTGRES_TEST_DB": "sg_test"}
    )
    assert dash.main(["--db", "sg_test"]) == 2
    assert "refusing" in capsys.readouterr().err
