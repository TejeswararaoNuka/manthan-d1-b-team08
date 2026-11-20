"""Local pre-commit check: black, ruff and pytest (all groups except spark).

    python scripts/check.py

Runs from the activated .venv on Windows. Integration and kafka tests need the Docker stack and
POSTGRES_TEST_DB; spark tests run in the processor container (see README). Exit code 0 only if
every step passes.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PATHS = ["src", "tests", "scripts"]
STEPS = [
    ("black", [sys.executable, "-m", "black", "--check", *PATHS]),
    ("ruff", [sys.executable, "-m", "ruff", "check", *PATHS]),
    ("pytest", [sys.executable, "-m", "pytest", "-q", "-m", "not spark"]),
]


def main() -> int:
    results = []
    for name, cmd in STEPS:
        print(f"\n=== {name}: {' '.join(cmd[2:])}", flush=True)
        results.append((name, subprocess.run(cmd, cwd=REPO_ROOT).returncode))
    print("\n=== summary")
    for name, code in results:
        print(f"  {name:<7} {'ok' if code == 0 else f'FAILED (exit {code})'}")
    return 0 if all(code == 0 for _, code in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
