"""process_batch with a static DataFrame (no Kafka): the foreachBatch entry point.

Needs pyspark + Java 17; runs in the processor container:
    docker compose ... run --rm --entrypoint python processor -m pytest -m spark
"""

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from streamguard.common.validation import format_event_time

pytestmark = [pytest.mark.spark, pytest.mark.integration]

pyspark = pytest.importorskip("pyspark")

NOW = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
CARD = "tok_00112233aabbccdd"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamguard-spark-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def event(n, *, merchant="mer_0a1b2c3d", seconds=0):
    return {
        "event_id": str(uuid.UUID(int=n, version=4)),
        "card_token": CARD,
        "merchant_id": merchant,
        "merchant_category": "5411",
        "amount": "250.00",
        "currency": "INR",
        "country": "IN",
        "channel": "POS",
        "event_time": format_event_time(NOW - timedelta(minutes=1) + timedelta(seconds=seconds)),
        "status": "APPROVED",
        "schema_version": 1,
    }


def kafka_rows(values):
    return [
        (CARD.encode(), v, "payments.raw", 0, offset, NOW, 0) for offset, v in enumerate(values)
    ]


def test_process_batch_handles_a_static_kafka_dataframe(spark, db, publisher):
    from streamguard.processor import job
    from streamguard.processor.batch import ALERTS_TOPIC, DLQ_TOPIC, BatchContext
    from streamguard.processor.schema import KAFKA_SOURCE_SCHEMA

    values = [
        json.dumps(event(1)).encode(),
        json.dumps(event(2, merchant="mer_bad00001", seconds=5)).encode(),
        json.dumps(event(1)).encode(),  # duplicate event_id
        b"{broken",
    ]
    df = spark.createDataFrame(kafka_rows(values), KAFKA_SOURCE_SCHEMA)
    job.configure(BatchContext(conn=db, publisher=publisher, now=lambda: NOW))

    job.process_batch(df, 0)

    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM sg.enriched_transactions")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT rule_id FROM sg.alerts")
        assert cur.fetchall() == [("MERCHANT_BLACKLIST",)]
        cur.execute("SELECT reason_code, kafka_offset FROM sg.dlq_events")
        assert cur.fetchall() == [("MALFORMED_JSON", 3)]
    db.rollback()
    assert len(publisher.on(ALERTS_TOPIC)) == 1
    assert len(publisher.on(DLQ_TOPIC)) == 1


def test_payment_schema_parses_a_valid_event(spark):
    from pyspark.sql.functions import col, from_json

    from streamguard.processor.schema import PAYMENT_EVENT_SCHEMA

    df = spark.createDataFrame([(json.dumps(event(7)),)], ["value"])
    row = df.select(from_json(col("value"), PAYMENT_EVENT_SCHEMA).alias("e")).first()["e"]
    assert row["card_token"] == CARD and row["amount"] == "250.00"
