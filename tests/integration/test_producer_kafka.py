"""Producer, scenario and peek against a real Kafka broker and PostgreSQL (POSTGRES_TEST_DB).

Uses a throwaway topic, never payments.raw. Run: pytest -m "kafka and integration"
"""

import json
import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from streamguard.common.settings import load_env, pg_connect
from streamguard.producer import peek, producer

pytestmark = [pytest.mark.kafka, pytest.mark.integration]


@pytest.fixture(scope="module")
def env():
    values = load_env()
    if not values.get("KAFKA_BOOTSTRAP") or not values.get("POSTGRES_TEST_DB"):
        pytest.skip("KAFKA_BOOTSTRAP / POSTGRES_TEST_DB not set")
    assert values["POSTGRES_TEST_DB"] != values.get("POSTGRES_DB")
    conn = pg_connect(values, values["POSTGRES_TEST_DB"])
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('sg.rule_config') IS NOT NULL")
        ready = cur.fetchone()[0]
    conn.close()
    if not ready:
        pytest.skip("run scripts/apply_sql.py --db <POSTGRES_TEST_DB> first")
    return values


@pytest.fixture
def topic(env):
    admin = AdminClient({"bootstrap.servers": env["KAFKA_BOOTSTRAP"]})
    name = f"payments.test-{uuid.uuid4().hex[:12]}"
    admin.create_topics([NewTopic(name, num_partitions=6, replication_factor=1)])[name].result(30)
    yield name
    admin.delete_topics([name])[name].result(30)


def test_scenario_messages_land_on_kafka_at_the_positions_in_the_expected_file(
    env, topic, tmp_path
):
    code = producer.main(
        [
            "--scenario",
            "all",
            "--topic",
            topic,
            "--phase-gap",
            "0",
            "--db",
            env["POSTGRES_TEST_DB"],
            "--expected-dir",
            str(tmp_path),
        ]
    )
    assert code == 0
    doc = json.loads((tmp_path / "all.json").read_text(encoding="utf-8"))

    rows = peek.peek(env, topic, 100, None, timeout=30)
    assert len(rows) == doc["counts"]["messages_sent"] == 43
    by_position = {(p, off): (key, value) for _, p, off, key, value in rows}
    for dead in doc["dead_letters"]:
        key, _ = by_position[(dead["partition"], dead["offset"])]
        assert key.decode() == dead["key"]
    # Kafka key = card_token, so each card's events share one partition.
    partitions = {}
    for _, p, _, key, _ in rows:
        partitions.setdefault(key, set()).add(p)
    assert all(len(ps) == 1 for ps in partitions.values())


def test_scenario_writes_the_spike_baseline_to_postgres(env, topic, tmp_path):
    assert (
        producer.main(
            [
                "--scenario",
                "spike",
                "--topic",
                topic,
                "--phase-gap",
                "0",
                "--db",
                env["POSTGRES_TEST_DB"],
                "--expected-dir",
                str(tmp_path),
            ]
        )
        == 0
    )
    doc = json.loads((tmp_path / "spike.json").read_text(encoding="utf-8"))
    ((card, base),) = doc["baselines"].items()
    conn = pg_connect(env, env["POSTGRES_TEST_DB"])
    with conn.cursor() as cur:
        cur.execute(
            "SELECT avg_amount_30d::text, tx_count_30d FROM sg.card_baseline "
            "WHERE card_token = %s",
            (card,),
        )
        assert cur.fetchone() == (base["avg_amount_30d"], base["tx_count_30d"])
    conn.close()


def test_random_mode_delivers_keyed_valid_events_at_the_target_rate(env, topic):
    code = producer.main(
        [
            "--rate",
            "100",
            "--burst-every",
            "0",
            "--duration",
            "3",
            "--topic",
            topic,
            "--db",
            env["POSTGRES_TEST_DB"],
        ]
    )
    assert code == 0
    rows = peek.peek(env, topic, 10_000, None, timeout=30)
    assert 200 <= len(rows) <= 400
    for _, _, _, key, value in rows:
        assert json.loads(value)["card_token"] == key.decode()
