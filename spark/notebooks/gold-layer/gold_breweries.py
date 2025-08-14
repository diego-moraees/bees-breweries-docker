# %% [markdown]
# Gold Notebook – Aggregation by type and location

# %%
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

parser = argparse.ArgumentParser()
parser.add_argument("--ingestion_date", required=True)
args = parser.parse_args()

SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "gold")

spark = SparkSession.builder.appName("gold_breweries").getOrCreate()

silver_base = f"s3a://{SILVER_BUCKET}/breweries"
silver_df = spark.read.parquet(silver_base).where(col("ingestion_date") == args.ingestion_date)

agg_df = (
    silver_df
    .groupBy("ingestion_date","country","state","brewery_type")
    .agg(count("id").alias("brewery_count"))
)

(
    agg_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .partitionBy("ingestion_date")
    .parquet(f"s3a://{GOLD_BUCKET}/breweries_agg")
)

print("Gold aggregation completed.")
