"""process_batch with a static DataFrame (no Kafka): the foreachBatch entry point.

Needs pyspark + Java 17, so it runs in the processor container on this PC:
    docker compose --env-file .env -f deploy/docker-compose.yml run --rm --no-deps \
        --entrypoint python processor -m pytest -m spark
"""

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from streamguard.common.validation import format_event_time

pytestmark = [pytest.mark.spark, pytest.mark.integration]

NOW = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
CARD = "tok_00112233aabbccdd"


def event(n, *, seconds=0, drop=None, **overrides):
    e = {
        "event_id": str(uuid.UUID(int=n, version=4)),
        "card_token": CARD,
        "merchant_id": "mer_0a1b2c3d",
        "merchant_category": "5411",
        "amount": "250.00",
        "currency": "INR",
        "country": "IN",
        "channel": "POS",
        "event_time": format_event_time(NOW - timedelta(minutes=1) + timedelta(seconds=seconds)),
        "status": "APPROVED",
        "schema_version": 1,
        **overrides,
    }
    if drop:
        del e[drop]
    return json.dumps(e).encode()


def kafka_df(spark, values):
    from streamguard.processor.schema import KAFKA_SOURCE_SCHEMA

    rows = [(CARD.encode(), v, "payments.raw", 0, off, NOW, 0) for off, v in enumerate(values)]
    return spark.createDataFrame(rows, KAFKA_SOURCE_SCHEMA)


@pytest.fixture
def process(db, publisher):
    from streamguard.processor import job
    from streamguard.processor.batch import BatchContext

    job.configure(BatchContext(conn=db, publisher=publisher, now=lambda: NOW))
    return job.process_batch


def counts(db):
    with db.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM sg.enriched_transactions), "
            "(SELECT count(*) FROM sg.alerts), (SELECT count(*) FROM sg.dlq_events)"
        )
        result = cur.fetchone()
    db.rollback()
    return result


def test_invalid_events_go_to_dlq_with_the_right_reason_code(spark, db, process):
    values = [
        b"{not json",
        event(1, amount="-10.00"),
        event(2, currency="XXX"),
        event(3, drop="merchant_id"),
        event(4),  # valid
    ]
    process(kafka_df(spark, values), 0)

    with db.cursor() as cur:
        cur.execute("SELECT kafka_offset, reason_code FROM sg.dlq_events ORDER BY kafka_offset")
        assert cur.fetchall() == [
            (0, "MALFORMED_JSON"),
            (1, "AMOUNT_OUT_OF_RANGE"),
            (2, "CURRENCY_NOT_ALLOWED"),
            (3, "MISSING_FIELD"),
        ]
        cur.execute("SELECT count(*) FROM sg.enriched_transactions")
        assert cur.fetchone()[0] == 1
    db.rollback()


@pytest.mark.parametrize(
    "second_batch_id", [0, 1], ids=["same-batch-id-rerun", "new-batch-id-replay"]
)
def test_process_batch_twice_on_same_dataframe_keeps_row_and_alert_counts(
    spark, db, process, second_batch_id
):
    values = [event(n, seconds=5 * n) for n in range(1, 7)]  # velocity: 6 in 25 s
    values.append(event(7, seconds=30, merchant_id="mer_bad00001"))  # blacklist
    values.append(event(1, seconds=5))  # duplicate event_id
    values.append(b"{broken")
    df = kafka_df(spark, values)

    process(df, 0)
    first = counts(db)
    process(df, second_batch_id)

    assert first == (7, 2, 1)
    assert counts(db) == first


def test_payment_schema_parses_a_valid_event(spark):
    from pyspark.sql.functions import col, from_json

    from streamguard.processor.schema import PAYMENT_EVENT_SCHEMA

    df = spark.createDataFrame([(event(7).decode(),)], ["value"])
    row = df.select(from_json(col("value"), PAYMENT_EVENT_SCHEMA).alias("e")).first()["e"]
    assert row["card_token"] == CARD and row["amount"] == "250.00"
