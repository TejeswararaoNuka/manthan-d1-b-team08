"""One-command start on the Docker PC: stack up, schema applied, baselines seeded, dashboard built.

    python scripts/start.py            # standard library only; needs Docker Desktop running

Steps: copy .env.example to .env if missing -> docker compose up -d --build -> wait until
PostgreSQL, Kafka and Superset are healthy -> apply SQL to POSTGRES_DB -> seed card baselines ->
build the dash layer and the Superset dashboard -> print the URLs.
SQL, seeding and dashboard steps run inside the project's containers, so no Python packages are
needed on the host. Safe to rerun. The random producer is not started (see README).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "--env-file", ".env", "-f", "deploy/docker-compose.yml"]
HEALTHY = {"postgres", "kafka", "superset"}
WAIT_SECONDS = 420


def run(args: list[str], title: str, **kw) -> None:
    print(f"\n--> {title}", flush=True)
    code = subprocess.run(args, cwd=REPO_ROOT, **kw).returncode
    if code:
        raise SystemExit(f"FAILED: {title} (exit {code})")


def in_container(service: str, script_args: list[str], title: str, extra_env=()) -> None:
    """Run a repo script inside a project container (src, sql, config, scripts are mounted)."""
    env_flags = [f for pair in extra_env for f in ("-e", pair)]
    run(
        [
            *COMPOSE,
            "run",
            "--rm",
            "--no-deps",
            *env_flags,
            "--entrypoint",
            "python",
            service,
            *script_args,
        ],
        title,
    )


def service_states() -> dict[str, tuple[str, str]]:
    out = subprocess.run(
        [*COMPOSE, "ps", "-a", "--format", "json"], cwd=REPO_ROOT, capture_output=True, text=True
    ).stdout.strip()
    rows = [json.loads(line) for line in out.splitlines() if line.strip()] if out else []
    if len(rows) == 1 and isinstance(rows[0], list):  # older compose prints one JSON array
        rows = rows[0]
    return {r["Service"]: (r.get("State", ""), r.get("Health", "")) for r in rows}


def wait_healthy() -> None:
    print(f"\n--> waiting for {', '.join(sorted(HEALTHY))} to be healthy", flush=True)
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        states = service_states()
        pending = sorted(s for s in HEALTHY if states.get(s, ("", ""))[1] != "healthy")
        init = states.get("kafka-init", ("", ""))[0]
        if not pending and init in ("exited", ""):
            print("    all healthy", flush=True)
            return
        print(f"    waiting: {', '.join(pending) or 'kafka-init'}", flush=True)
        time.sleep(10)
    raise SystemExit(f"FAILED: services not healthy after {WAIT_SECONDS}s; see docker compose ps")


def main() -> int:
    if shutil.which("docker") is None:
        raise SystemExit("Docker is not installed or not on PATH")
    env_file = REPO_ROOT / ".env"
    if not env_file.exists():
        shutil.copyfile(REPO_ROOT / ".env.example", env_file)
        print("Created .env from .env.example: change every 'change-me' value before real use.")
        print("(The first start sets database passwords from .env; later edits need `down -v`.)")

    run([*COMPOSE, "up", "-d", "--build"], "starting the stack (first run builds images)")
    wait_healthy()
    in_container("processor", ["scripts/apply_sql.py"], "applying SQL to POSTGRES_DB")
    in_container("processor", ["-m", "streamguard.producer.seed_baseline"], "seeding baselines")
    in_container("processor", ["scripts/build_dash_layer.py"], "building the dash layer")
    in_container(
        "processor",
        ["scripts/build_superset_dashboard.py"],
        "building the Superset dashboard",
        extra_env=["SUPERSET_URL=http://superset:8088"],
    )
    run([*COMPOSE, "up", "-d", "processor", "monitor"], "making sure processor and monitor run")

    # Single-PC setup (D-15): ports are published on 127.0.0.1 only, so URLs are localhost.
    print(
        "\nStreamGuard is up (this PC only, D-15):\n"
        "  Superset dashboard  http://localhost:8088/superset/dashboard/streamguard/\n"
        "  Spark UI            http://localhost:4040\n"
        "  PostgreSQL          localhost:5432    Kafka  localhost:29092\n"
        "Next: start traffic with\n"
        "  docker compose --env-file .env -f deploy/docker-compose.yml --profile producer "
        "up -d producer"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
