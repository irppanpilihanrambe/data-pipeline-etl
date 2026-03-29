"""
Spark Transform Job — Cleans and transforms raw data from staging.

Usage:
    spark-submit spark_jobs/transform_raw.py --date 2024-01-15
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

RAW_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("event", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("page", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("source", StringType(), True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ETL-Transform-Raw")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def transform(spark: SparkSession, date: str):
    # Read raw data
    df = spark.read.schema(RAW_SCHEMA).json(f"/data/raw/{date}/*.json")

    # Clean & enrich
    df_clean = (
        df
        .filter(F.col("user_id").isNotNull())
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .withColumn("event_hour", F.hour(F.to_timestamp(F.col("timestamp"))))
        .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn("event", F.lower(F.trim(F.col("event"))))
        .dropDuplicates(["user_id", "event", "timestamp"])
    )

    row_count = df_clean.count()
    print(f"[Transform] Processed {row_count} rows for {date}")

    # Write to processed layer
    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"/data/processed/{date}")
    )
    print(f"[Transform] Written to /data/processed/{date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark()
    transform(spark, args.date)
    spark.stop()
