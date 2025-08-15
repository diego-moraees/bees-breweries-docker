# spark/notebooks/gold-layer/gold_breweries.py
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

"""
Reads Silver parquet and writes aggregated counts by type and location.
"""

def main(ingestion_date: str):
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "gold")

    silver_base = f"s3a://{silver_bucket}/breweries"
    gold_base = f"s3a://{gold_bucket}/breweries_agg"

    spark = (
        SparkSession.builder
        .appName("gold_breweries")
        .getOrCreate()
    )

    df = spark.read.parquet(silver_base).where(col("ingestion_date") == ingestion_date)

    agg = (
        df.groupBy("ingestion_date", "country", "state", "brewery_type")
        .agg(count("id").alias("brewery_count"))
    )

    (
        agg.coalesce(1)  # dataset pequeno; 1 arquivo por praticidade
        .write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(gold_base)
    )

    print("Gold write completed:", gold_base)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ingestion_date", required=True)
    args = p.parse_args()
    main(args.ingestion_date)
