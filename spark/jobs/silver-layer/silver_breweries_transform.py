"""
Transform breweries JSON landed in the Bronze layer into a cleaned,
columnar Silver layer (Parquet) partitioned by country and state.

This script:
1) Reads the Bronze JSON for a given `ingestion_date`.
2) Normalizes types and trims strings; handles missing state/country.
3) Deduplicates by the brewery `id` keeping the latest record.
4) Writes Parquet to the Silver bucket partitioned by `country` and `state`:
   s3a://<SILVER_BUCKET>/<DATASET_NAME>/ingestion_date=YYYY-MM-DD/...

Environment variables:
- DATASET_NAME       (default: "breweries")
- BRONZE_BUCKET      (default: "bronze")
- SILVER_BUCKET      (default: "silver")

Notes:
- Input JSON files are arrays; we use `multiLine=true` to parse them.
- Partitioning by location accelerates typical geography-based queries.
"""

import os
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def get_spark(app_name: str = "silver_breweries"):
    """
    Create a Spark session.

    Args:
        app_name: Spark app name.

    Returns:
        SparkSession.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, bucket: str, dataset: str, ingestion_date: str):
    """
    Read Bronze JSON for the date.

    Args:
        spark: Spark session.
        bucket: Bronze bucket.
        dataset: Dataset prefix.
        ingestion_date: Partition date (YYYY-MM-DD).

    Returns:
        DataFrame with raw rows.
    """
    path = f"s3a://{bucket}/{dataset}/ingestion_date={ingestion_date}/page=*.json"
    return (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(path)
    )

def transform(df):
    """
    Clean, cast, and deduplicate.

    Args:
        df: Input DataFrame.

    Returns:
        Transformed DataFrame.
    """
    # Trim strings
    str_cols = [
        "id", "name", "brewery_type", "street", "address_1", "address_2", "address_3",
        "city", "state", "state_province", "postal_code", "country",
        "phone", "website_url",
    ]
    for c in str_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c).cast(T.StringType())))

    # Cast coordinates
    df = (
        df.withColumn("latitude", F.col("latitude").cast(T.DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(T.DoubleType()))
    )

    # Normalize location
    df = (
        df.withColumn("country", F.coalesce(F.col("country"), F.lit("Unknown")))
        .withColumn("state", F.coalesce(F.col("state"), F.lit("Unknown")))
    )

    # Deduplicate by id, prefer non-null updated_at then newest
    # If API lacks updated_at, this still provides stable de-dup.
    if "updated_at" in df.columns:
        order = Window.partitionBy("id").orderBy(
            F.col("updated_at").desc_nulls_last(),
            F.col("name").desc_nulls_last(),
        )
    else:
        order = Window.partitionBy("id").orderBy(F.col("name").desc_nulls_last())

    df = df.withColumn("rn", F.row_number().over(order)).where(F.col("rn") == 1).drop("rn")

    # Select a tidy schema (keep common fields; keep id as key)
    wanted = [
        "id", "name", "brewery_type", "street", "city", "state",
        "postal_code", "country", "longitude", "latitude",
        "phone", "website_url"
    ]
    keep = [c for c in wanted if c in df.columns]
    return df.select(*keep)


def write_silver(df, bucket: str, dataset: str, ingestion_date: str):
    """
    Write Parquet partitioned by country/state.

    Args:
        df: DataFrame to write.
        bucket: Silver bucket.
        dataset: Dataset prefix.
        ingestion_date: Partition date.
    """
    out = f"s3a://{bucket}/{dataset}/ingestion_date={ingestion_date}"
    (
        df.write.mode("overwrite")
        .partitionBy("country", "state")
        .parquet(out)
    )


def main(ingestion_date: str):
    """
    Execute Silver transform.

    Args:
        ingestion_date: Partition date (YYYY-MM-DD).
    """
    dataset = os.getenv("DATASET_NAME", "breweries")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")

    spark = get_spark()
    try:
        raw_df = read_bronze(spark, bronze_bucket, dataset, ingestion_date)
        tr_df = transform(raw_df)
        write_silver(tr_df, silver_bucket, dataset, ingestion_date)
        print(f"OK silver {ingestion_date}")
    finally:
        spark.stop()


if __name__ == "__main__":
    # Accepts: --ingestion_date YYYY-MM-DD
    if len(sys.argv) >= 3 and sys.argv[1] == "--ingestion_date":
        main(sys.argv[2])
    else:
        raise SystemExit("Usage: silver_breweries_transform.py --ingestion_date YYYY-MM-DD")
