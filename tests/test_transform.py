"""
Tests for Spark transform_raw.py
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test-etl")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def raw_data(spark):
    return spark.createDataFrame([
        ("u001", "page_view",  "2024-01-15T10:00:00", "/home",  None,  None),
        ("u002", "Purchase",   "2024-01-15T11:00:00", None,     150.0, None),
        (None,   "page_view",  "2024-01-15T12:00:00", "/about", None,  None),  # null user — should be dropped
        ("u001", "page_view",  "2024-01-15T10:00:00", "/home",  None,  None),  # duplicate — should be dropped
    ], ["user_id", "event", "timestamp", "page", "amount", "source"])


def test_null_users_removed(spark, raw_data):
    df = raw_data.filter(F.col("user_id").isNotNull())
    assert df.count() == 3


def test_event_lowercased(spark, raw_data):
    df = raw_data.withColumn("event", F.lower(F.trim(F.col("event"))))
    events = [r.event for r in df.collect()]
    assert all(e == e.lower() for e in events)


def test_amount_defaults_to_zero(spark, raw_data):
    df = raw_data.withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
    nulls = df.filter(F.col("amount").isNull()).count()
    assert nulls == 0


def test_duplicates_removed(spark, raw_data):
    df = (
        raw_data
        .filter(F.col("user_id").isNotNull())
        .dropDuplicates(["user_id", "event", "timestamp"])
    )
    assert df.count() == 2
