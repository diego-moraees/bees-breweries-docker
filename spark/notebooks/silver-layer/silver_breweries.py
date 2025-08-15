# spark/notebooks/silver-layer/silver_breweries.py
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, coalesce, lit, upper, lower
from pyspark.sql.types import DoubleType

"""
Reads Bronze JSON pages (arrays) and writes curated Parquet partitioned by ingestion_date/country/state.
"""

def main(ingestion_date: str):
    bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")

    bronze_base = f"s3a://{bronze_bucket}/openbrewerydb/ingestion_date={ingestion_date}"
    silver_base = f"s3a://{silver_bucket}/breweries"

    spark = (
        SparkSession.builder
        .appName("silver_breweries")
        .getOrCreate()
    )

    # Read only the paged files (each file is a JSON array)
    df = (
        spark.read
        .option("multiLine", "true")
        .json(f"{bronze_base}/page=*.json")
    )

    # Some payloads have "state_province" instead of "state"
    state_expr = coalesce(col("state"), col("state_province"))

    wanted = [
        "id", "name", "brewery_type", "address_1", "city",
        "state", "state_province", "postal_code", "country",
        "longitude", "latitude", "phone", "website_url"
    ]
    existing = [c for c in wanted if c in df.columns]
    df = df.select(*existing)

    curated = (
        df
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("state", upper(trim(state_expr)))
        .withColumn("city", trim(col("city")))
        .withColumn("postal_code", trim(col("postal_code")))
        .withColumn("brewery_type", lower(trim(col("brewery_type"))))
        .withColumn("longitude", col("longitude").cast(DoubleType()))
        .withColumn("latitude", col("latitude").cast(DoubleType()))
        .withColumn("ingestion_date", lit(ingestion_date))
        .filter(col("id").isNotNull())
        .filter(col("name").isNotNull())
    )

    (
        curated
        .repartition("country", "state")
        .write
        .mode("overwrite")
        .partitionBy("ingestion_date", "country", "state")
        .parquet(silver_base)
    )

    print("Silver write completed:", silver_base)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True)
    args = parser.parse_args()
    main(args.ingestion_date)
