import importlib.util
import re
from pathlib import Path

import pytest

from streamguard.common.config_defaults import (
    ConfigError,
    load_dq_config,
    load_health_config,
    load_rules,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "config"


def _load_apply_sql():
    spec = importlib.util.spec_from_file_location("apply_sql", REPO_ROOT / "scripts/apply_sql.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_rules_yaml_defines_four_rules_with_agreed_thresholds():
    rules = {r.rule_id: r for r in load_rules(CONFIG / "rules.yaml")}

    assert set(rules) == {"VELOCITY", "IMPOSSIBLE_TRAVEL", "AMOUNT_SPIKE", "MERCHANT_BLACKLIST"}
    assert rules["VELOCITY"].params == {"max_tx": 5, "window_seconds": 60}
    assert rules["IMPOSSIBLE_TRAVEL"].params == {"window_minutes": 30}
    assert rules["AMOUNT_SPIKE"].params == {"multiplier": 5, "lookback_days": 30}
    assert rules["MERCHANT_BLACKLIST"].enabled is True
    assert all(r.enabled for r in rules.values())


def test_dq_yaml_has_flat_ceiling_as_exact_decimal_string_and_agreed_currencies():
    dq = load_dq_config(CONFIG / "dq.yaml")

    assert dq["amount_ceiling"] == "500000.00"
    assert dq["allowed_currencies"] == ["INR", "USD", "EUR", "GBP", "SGD", "AED"]
    assert dq["max_future_hours"] == 24
    assert dq["watermark_seconds"] == 120


def test_unquoted_float_amount_ceiling_is_rejected(tmp_path):
    bad = tmp_path / "dq.yaml"
    bad.write_text(
        "dq_config:\n  amount_ceiling: 500000.50\n  allowed_currencies: [INR]\n"
        "  max_future_hours: 24\n  watermark_seconds: 120\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="quoted"):
        load_dq_config(bad)


def test_rule_with_unknown_severity_is_rejected(tmp_path):
    bad = tmp_path / "rules.yaml"
    bad.write_text("rules:\n  VELOCITY:\n    name: v\n    severity: CRITICAL\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="severity"):
        load_rules(bad)


def test_health_thresholds_from_env_override_yaml_defaults():
    defaults = load_health_config(CONFIG / "dq.yaml", {})
    overridden = load_health_config(
        CONFIG / "dq.yaml", {"LAG_ALERT_THRESHOLD": "123", "LATENCY_ALERT_SECONDS": "7.5"}
    )

    assert defaults == {
        "lag_alert_threshold": 5000,
        "latency_alert_seconds": 30,
        "backlog_overdue_minutes": 15,
    }
    assert overridden["lag_alert_threshold"] == 123
    assert overridden["latency_alert_seconds"] == "7.5"
    assert overridden["backlog_overdue_minutes"] == 15


def test_sql_files_run_ddl_then_views_then_seed_in_name_order():
    files = _load_apply_sql().sql_files(REPO_ROOT / "sql")
    names = [f"{p.parent.name}/{p.name}" for p in files]

    phases = [n.split("/")[0] for n in names]
    assert phases == sorted(phases, key=["ddl", "views", "seed"].index)
    assert names[:2] == ["ddl/01_sg.sql", "ddl/02_ops.sql"]
    for phase in ("ddl", "views", "seed"):
        in_phase = [n for n in names if n.startswith(phase + "/")]
        assert in_phase == sorted(in_phase)
    assert "seed/01_merchant_blacklist.sql" in names


def test_sql_and_config_contain_no_card_number_like_values():
    card_like = re.compile(r"(?<!\d)\d{13,19}(?!\d)")
    paths = [*(REPO_ROOT / "sql").rglob("*.sql"), *CONFIG.glob("*.yaml")]

    offenders = [p.name for p in paths if card_like.search(p.read_text(encoding="utf-8"))]

    assert paths and offenders == []
