"""Reset the demo to an empty pipeline: topics, data tables and processor checkpoint.

    python scripts/reset_demo.py          # asks for confirmation
    python scripts/reset_demo.py --yes

Stops the processor (and the random producer, if running), deletes and recreates payments.raw,
payments.alerts and payments.dlq with their agreed settings (kafka-init), truncates the sg data
tables and the ops progress/metrics tables (config tables, the merchant blacklist and card
baselines are kept), deletes the processor checkpoint folder, then restarts the processor.
Deletes data irreversibly. Standard library only; needs the Docker stack running.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "--env-file", ".env", "-f", "deploy/docker-compose.yml"]
TOPICS = ["payments.raw", "payments.alerts", "payments.dlq"]
KAFKA_TOPICS = "/opt/kafka/bin/kafka-topics.sh"
# Data only. Kept: sg.rule_config, sg.dq_config, sg.merchant_blacklist, sg.card_baseline,
# ops.health_config. ops.processor_state/stream_offsets must go with the checkpoint (D-04, ADR-02).
TRUNCATE = (
    "TRUNCATE sg.alert_feedback, sg.alerts, sg.enriched_transactions, sg.agg_1min, "
    "sg.dlq_events, ops.stream_metrics, ops.stream_offsets, ops.processor_state, "
    "ops.lag_samples, ops.alerts"
)
CHECKPOINT = REPO_ROOT / "data" / "checkpoints" / "processor"


def compose(*args: str, title: str, capture: bool = False) -> str:
    print(f"--> {title}", flush=True)
    proc = subprocess.run([*COMPOSE, *args], cwd=REPO_ROOT, capture_output=capture, text=True)
    if proc.returncode:
        raise SystemExit(f"FAILED: {title}\n{proc.stderr if capture else ''}")
    return proc.stdout if capture else ""


def kafka_topics(*args: str) -> str:
    return compose(
        "exec",
        "-T",
        "kafka",
        KAFKA_TOPICS,
        "--bootstrap-server",
        "localhost:9092",
        *args,
        title=f"kafka-topics {' '.join(args[:2])}",
        capture=True,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    args = ap.parse_args(argv)
    if not args.yes:
        answer = input("This deletes all pipeline data, topics and the checkpoint. Type 'reset': ")
        if answer.strip() != "reset":
            print("Cancelled.")
            return 1

    compose("--profile", "producer", "stop", "processor", "producer", title="stopping processor")

    kafka_topics("--delete", "--if-exists", "--topic", "payments\\..*")
    for _ in range(30):  # deletion is asynchronous
        remaining = [t for t in kafka_topics("--list").split() if t in TOPICS]
        if not remaining:
            break
        time.sleep(2)
    else:
        raise SystemExit(f"FAILED: topics still present: {remaining}")
    compose("run", "--rm", "kafka-init", title="recreating topics with their agreed settings")

    compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        f'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "{TRUNCATE}"',
        title="truncating sg data tables and ops progress tables",
    )

    if CHECKPOINT.exists():
        print(f"--> deleting checkpoint {CHECKPOINT.relative_to(REPO_ROOT)}", flush=True)
        shutil.rmtree(CHECKPOINT)

    compose("up", "-d", "processor", title="restarting processor (new checkpoint, latest offsets)")
    print(
        "\nDemo reset. Rebuild the dashboard layer so it reflects the empty tables:\n"
        "  docker compose --env-file .env -f deploy/docker-compose.yml run --rm --no-deps "
        "--entrypoint python processor scripts/build_dash_layer.py"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
