"""Load and validate the default configuration in config/*.yaml.

Pure Python (no database), so it is unit-testable. scripts/apply_sql.py writes the result
into sg.rule_config, sg.dq_config and ops.health_config.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

SEVERITIES = frozenset({"LOW", "MEDIUM", "HIGH"})
_RULE_ID = re.compile(r"^[A-Z][A-Z_]*$")
_CURRENCY = re.compile(r"^[A-Z]{3}$")

# Environment variables that override ops.health_config defaults (they also drive the monitor).
HEALTH_ENV_OVERRIDES = {
    "lag_alert_threshold": "LAG_ALERT_THRESHOLD",
    "latency_alert_seconds": "LATENCY_ALERT_SECONDS",
}


class ConfigError(ValueError):
    """A config/*.yaml file is missing a value or has one in the wrong format."""


@dataclass(frozen=True)
class RuleDefault:
    rule_id: str
    name: str
    enabled: bool
    severity: str
    params: dict[str, Any]


def _read_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a mapping")
    return data


def _section(data: dict[str, Any], key: str, path: Path) -> dict[str, Any]:
    section = data.get(key)
    if not isinstance(section, dict) or not section:
        raise ConfigError(f"{path}: '{key}' must be a non-empty mapping")
    return section


def _json_number(value: Any, where: str) -> int | str:
    """Return an int as-is, any other number as an exact decimal string (never a float)."""
    if isinstance(value, bool):
        raise ConfigError(f"{where}: expected a number, got {value!r}")
    if isinstance(value, int):
        return value
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise ConfigError(f"{where}: expected a number, got {value!r}") from None
    if not number.is_finite():
        raise ConfigError(f"{where}: expected a finite number, got {value!r}")
    return int(number) if number == number.to_integral_value() else str(number)


def load_rules(path: Path) -> list[RuleDefault]:
    rules = _section(_read_yaml(path), "rules", path)
    result = []
    for rule_id, spec in rules.items():
        where = f"{path}: rule {rule_id}"
        if not isinstance(rule_id, str) or not _RULE_ID.match(rule_id):
            raise ConfigError(f"{where}: rule_id must be UPPER_SNAKE_CASE")
        if not isinstance(spec, dict):
            raise ConfigError(f"{where}: must be a mapping")
        name = spec.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(f"{where}: 'name' is required")
        enabled = spec.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ConfigError(f"{where}: 'enabled' must be true or false")
        severity = spec.get("severity")
        if severity not in SEVERITIES:
            raise ConfigError(f"{where}: 'severity' must be one of {sorted(SEVERITIES)}")
        params = spec.get("params") or {}
        if not isinstance(params, dict):
            raise ConfigError(f"{where}: 'params' must be a mapping")
        params = {k: _json_number(v, f"{where}: params.{k}") for k, v in params.items()}
        result.append(RuleDefault(rule_id, name.strip(), enabled, severity, params))
    return result


def load_dq_config(path: Path) -> dict[str, Any]:
    dq = _section(_read_yaml(path), "dq_config", path)
    required = {"amount_ceiling", "allowed_currencies", "max_future_hours", "watermark_seconds"}
    missing = required - dq.keys()
    if missing:
        raise ConfigError(f"{path}: dq_config is missing {sorted(missing)}")

    ceiling = dq["amount_ceiling"]
    if isinstance(ceiling, float):
        raise ConfigError(f'{path}: amount_ceiling must be quoted, e.g. "500000.00"')
    try:
        ceiling_dec = Decimal(str(ceiling))
    except InvalidOperation:
        raise ConfigError(f"{path}: amount_ceiling is not a number: {ceiling!r}") from None
    if not ceiling_dec.is_finite() or ceiling_dec <= 0:
        raise ConfigError(f"{path}: amount_ceiling must be > 0")

    currencies = dq["allowed_currencies"]
    if (
        not isinstance(currencies, list)
        or not currencies
        or not all(isinstance(c, str) and _CURRENCY.match(c) for c in currencies)
    ):
        raise ConfigError(f"{path}: allowed_currencies must be a list of ISO 4217 codes")
    if len(set(currencies)) != len(currencies):
        raise ConfigError(f"{path}: allowed_currencies has duplicates")

    result: dict[str, Any] = {
        "amount_ceiling": str(ceiling_dec.quantize(Decimal("0.01"))),
        "allowed_currencies": list(currencies),
    }
    for key in ("max_future_hours", "watermark_seconds"):
        value = dq[key]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ConfigError(f"{path}: {key} must be a positive integer")
        result[key] = value
    return result


def load_health_config(path: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    health = _section(_read_yaml(path), "health_config", path)
    result = {k: _json_number(v, f"{path}: health_config.{k}") for k, v in health.items()}
    for key, var in HEALTH_ENV_OVERRIDES.items():
        raw = (env or {}).get(var, "").strip()
        if raw:
            result[key] = _json_number(raw, f"environment variable {var}")
    return result
