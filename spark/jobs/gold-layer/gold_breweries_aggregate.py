"""
Build an analytical Gold layer with counts of breweries by type and location
based on the cleaned Silver layer.

This script:
1) Reads Silver Parquet for a given `ingestion_date`.
2) Aggregates counts grouped by `country`, `state`, and `brewery_type`.
3) Writes Parquet to the Gold bucket under the same `ingestion_date`.

Environment variables:
- DATASET_NAME   (default: "breweries")
- SILVER_BUCKET  (default: "silver")
- GOLD_BUCKET    (default: "gold")

Notes:
- Output is small and optimized for BI/reporting (roll-up ready).
"""

import os
import sys
from pyspark.sql import SparkSession, functions as F


def get_spark(app_name: str = "gold_breweries"):
    """
    Create a Spark session.

    Args:
        app_name: Spark app name.

    Returns:
        SparkSession with deltalake confs
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_silver(spark: SparkSession, bucket: str, dataset: str, ingestion_date: str):
    """
    Read Silver Parquet for the date.

    Args:
        spark: Spark session.
        bucket: Silver bucket.
        dataset: Dataset prefix.
        ingestion_date: Partition date.

    Returns:
        DataFrame with cleaned rows.
    """
    path = f"s3a://{bucket}/{dataset}/ingestion_date={ingestion_date}"
    return spark.read.parquet(path)


def aggregate(df):
    """
    Group and count by location and type.

    Args:
        df: Input DataFrame.

    Returns:
        Aggregated DataFrame.
    """
    return (
        df.groupBy("country", "state", "brewery_type")
        .agg(F.count("*").alias("breweries_count"))
    )


def write_gold(df, bucket: str, dataset: str, ingestion_date: str):
    """
    Write aggregated Parquet.

    Args:
        df: DataFrame to write.
        bucket: Gold bucket.
        dataset: Dataset prefix.
        ingestion_date: Partition date.
    """
    out = f"s3a://{bucket}/{dataset}/ingestion_date={ingestion_date}"
    (df
     .write
     .format("delta")
     .mode("overwrite")
     .partitionBy("country", "state")
     .save(out))


def main(ingestion_date: str):
    """
    Execute Gold aggregation.

    Args:
        ingestion_date: Partition date (YYYY-MM-DD).
    """
    dataset = os.getenv("DATASET_NAME", "breweries")
    dataset_gold = os.getenv("GOLD_DATASET_NAME", "breweries_agg")
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "gold")

    spark = get_spark()
    try:
        silver_df = read_silver(spark, silver_bucket, dataset, ingestion_date)
        agg_df = aggregate(silver_df)
        write_gold(agg_df, gold_bucket, dataset_gold, ingestion_date)
        print(f"OK gold {ingestion_date}")
    finally:
        spark.stop()


if __name__ == "__main__":
    # Accepts: --ingestion_date YYYY-MM-DD
    if len(sys.argv) >= 3 and sys.argv[1] == "--ingestion_date":
        main(sys.argv[2])
    else:
        raise SystemExit("Usage: gold_breweries_aggregate.py --ingestion_date YYYY-MM-DD")
