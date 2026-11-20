"""Explicit Spark schemas: the Kafka source rows and the payment event (CLAUDE.md).

Validation itself runs on the raw bytes in streamguard.common.validation, because Spark's
from_json turns malformed JSON and missing/badly-typed fields into the same nulls, which
would merge MALFORMED_JSON and MISSING_FIELD. These schemas document the contract and
build static DataFrames for spark tests.
"""

from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Rows produced by Spark's Kafka source (the input of process_batch).
KAFKA_SOURCE_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("timestampType", IntegerType(), True),
    ]
)

# Payment event JSON. amount is a string so a JSON number keeps its exact digits (never a
# float); event_time is parsed separately (ISO 8601 UTC with Z).
PAYMENT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("card_token", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("amount", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("country", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("status", StringType(), False),
        StructField("schema_version", IntegerType(), False),
    ]
)
