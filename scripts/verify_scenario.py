"""Check that PostgreSQL holds exactly what a producer scenario says it must.

    python scripts/verify_scenario.py data/expected/all.json [--db NAME] [--wait 120]

End-to-end check (D-14): stop the random producer, reset the processor
(python -m streamguard.processor --reset, or the compose service command), run
`python -m streamguard.producer --scenario all`, then run this script.
Exit code 0 = match, 1 = differences (listed), 2 = usage/connection error.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from streamguard.common.settings import load_env, pg_connect  # noqa: E402
from streamguard.processor.verify import compare  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("expected", type=Path, help="data/expected/<scenario>.json")
    ap.add_argument("--db", help="database (default POSTGRES_DB)")
    ap.add_argument(
        "--wait",
        type=float,
        default=0,
        help="keep re-checking for up to this many seconds (processor catching up)",
    )
    args = ap.parse_args(argv)

    try:
        doc = json.loads(args.expected.read_text(encoding="utf-8"))
        conn = pg_connect(load_env(), args.db, app="streamguard-verify")
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    deadline = time.monotonic() + args.wait
    try:
        while True:
            problems = compare(conn, doc)
            conn.rollback()
            if not problems or time.monotonic() >= deadline:
                break
            time.sleep(5)
    finally:
        conn.close()

    c = doc["counts"]
    if problems:
        print(f"FAIL: scenario '{doc['scenario']}' ({len(problems)} differences)")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(
        f"PASS: scenario '{doc['scenario']}': {c['alerts']} alerts, {c['dead_letters']} dead "
        f"letters, {c['enriched_rows']} enriched rows ({c['late_rows']} late) all match; "
        f"{len(doc['must_not_alert'])} must-not-alert cases produced no alert"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
