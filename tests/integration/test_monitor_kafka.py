"""Monitor against a real Kafka broker (throwaway topic) and POSTGRES_TEST_DB."""

import uuid
from datetime import timedelta

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from streamguard.metrics.monitor import AlertTracker, run_cycle

pytestmark = [pytest.mark.kafka, pytest.mark.integration]


@pytest.fixture
def topic(test_env):
    admin = AdminClient({"bootstrap.servers": test_env["KAFKA_BOOTSTRAP"]})
    name = f"payments.test-{uuid.uuid4().hex[:12]}"
    admin.create_topics([NewTopic(name, num_partitions=2, replication_factor=1)])[name].result(30)
    producer = Producer({"bootstrap.servers": test_env["KAFKA_BOOTSTRAP"]})
    for i in range(30):
        producer.produce(name, value=b"x", partition=i % 2)  # 15 per partition
    producer.flush(30)
    yield admin, name
    admin.delete_topics([name])[name].result(30)


def test_monitor_lag_equals_unprocessed_messages_and_raises_then_resolves(db, topic):
    admin, name = topic
    with db, db.cursor() as cur:
        cur.execute("UPDATE ops.health_config SET value = '10' WHERE key = 'lag_alert_threshold'")
        cur.execute(
            "INSERT INTO ops.stream_offsets (topic, partition, last_processed_offset) "
            "VALUES (%s, 0, 4), (%s, 1, 14)",
            (name, name),
        )
    tracker = AlertTracker(repeat=timedelta(minutes=10))

    first = run_cycle(db, admin, tracker, name)  # p0: 15 - 5 = 10, p1: 0
    assert first["per_partition"] == {"0": 10, "1": 0} and first["lag"] == 10
    assert first["alerts"] == []  # 10 is not > 10

    with db, db.cursor() as cur:
        cur.execute(
            "UPDATE ops.stream_offsets SET last_processed_offset = 0 WHERE topic = %s", (name,)
        )
    second = run_cycle(db, admin, tracker, name)  # 14 + 14 = 28
    assert second["lag"] == 28
    assert [s for s, _ in second["alerts"]] == ["HIGH"]
    assert "consumer lag 28 > 10" in second["alerts"][0][1]

    with db, db.cursor() as cur:
        cur.execute(
            "UPDATE ops.stream_offsets SET last_processed_offset = 14 WHERE topic = %s", (name,)
        )
    third = run_cycle(db, admin, tracker, name)
    assert third["lag"] == 0 and third["alerts"][0][1].startswith("RESOLVED")

    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM ops.lag_samples WHERE topic = %s", (name,))
        assert cur.fetchone()[0] == 3
        cur.execute("SELECT severity FROM ops.alerts WHERE source = 'monitor:lag' ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == ["HIGH", "LOW"]
    db.rollback()
