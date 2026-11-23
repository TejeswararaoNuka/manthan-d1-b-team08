"""Connection settings from .env (laptops) or the container environment (Docker on PC 1)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[3]


def load_env(env_file: Path | None = None) -> dict[str, str]:
    """Values from .env (given path, else ./.env, else repo-root .env), then os.environ on top."""
    candidates = [env_file] if env_file else [Path.cwd() / ".env", REPO_ROOT / ".env"]
    values: dict[str, str] = {}
    for path in candidates:
        if path and path.is_file():
            values = {k: v for k, v in dotenv_values(path).items() if v is not None}
            break
    values.update(os.environ)
    return values


def require(env: Mapping[str, str], *keys: str) -> None:
    missing = [k for k in keys if not env.get(k)]
    if missing:
        raise SystemExit(f"Missing in .env / environment: {', '.join(missing)}")


def pg_connect(env: Mapping[str, str], dbname: str | None = None, app: str = "streamguard"):
    import psycopg2

    require(env, "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD")
    return psycopg2.connect(
        host=env["POSTGRES_HOST"],
        port=int(env["POSTGRES_PORT"]),
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
        dbname=dbname or env.get("POSTGRES_DB"),
        connect_timeout=10,
        application_name=app,
    )


def kafka_producer_conf(env: Mapping[str, str], client_id: str) -> dict[str, object]:
    """Producer settings from architecture.md section 4: acks=all, idempotent, lz4."""
    require(env, "KAFKA_BOOTSTRAP")
    return {
        "bootstrap.servers": env["KAFKA_BOOTSTRAP"],
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
    }
