import importlib.util
from pathlib import Path

import pytest

from streamguard.common.settings import load_env, pg_connect

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_TABLES = (
    "sg.alert_feedback, sg.alerts, sg.enriched_transactions, sg.agg_1min, sg.dlq_events, "
    "sg.card_baseline, ops.stream_metrics, ops.processor_state, ops.alerts"
)


class FakePublisher:
    def __init__(self):
        self.messages = []  # (topic, key, value bytes)
        self.flushes = 0

    def publish(self, topic, key, value):
        self.messages.append((topic, key, value))

    def flush(self):
        self.flushes += 1

    def on(self, topic):
        return [(k, v) for t, k, v in self.messages if t == topic]


@pytest.fixture(scope="session")
def test_env():
    env = load_env()
    if not env.get("POSTGRES_TEST_DB"):
        pytest.skip("POSTGRES_TEST_DB is not set")
    assert env["POSTGRES_TEST_DB"] != env.get("POSTGRES_DB"), "tests must never use POSTGRES_DB"
    return env


@pytest.fixture(scope="session")
def test_schema(test_env):
    spec = importlib.util.spec_from_file_location("apply_sql", REPO_ROOT / "scripts/apply_sql.py")
    apply_sql = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(apply_sql)
    assert apply_sql.main(["--db", test_env["POSTGRES_TEST_DB"], "--reset-config"]) == 0
    return test_env


@pytest.fixture
def db(test_schema):
    """Connection to POSTGRES_TEST_DB with empty data tables and default config."""
    conn = pg_connect(test_schema, test_schema["POSTGRES_TEST_DB"])
    with conn, conn.cursor() as cur:
        cur.execute("SET client_min_messages = warning")
        cur.execute(f"TRUNCATE {DATA_TABLES}")
        cur.execute("SELECT to_regclass('ops.processor_state') IS NOT NULL")
        assert cur.fetchone()[0]
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def publisher():
    return FakePublisher()
