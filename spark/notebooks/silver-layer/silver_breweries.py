# %% [markdown]
# Silver Notebook – Curate breweries to Parquet partitioned by location

# %%
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim

parser = argparse.ArgumentParser()
parser.add_argument("--ingestion_date", required=True)
args = parser.parse_args()

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

spark = SparkSession.builder.appName("silver_breweries").getOrCreate()

base_path = f"s3a://{BRONZE_BUCKET}/openbrewerydb/ingestion_date={args.ingestion_date}"
bronze_df = spark.read.json(f"{base_path}/*.json")

# Select and basic cleaning
cols = ["id","name","brewery_type","address_1","city","state","postal_code","country","longitude","latitude","phone","website_url"]
bronze_df = bronze_df.select([c for c in cols if c in bronze_df.columns])

silver_df = (
    bronze_df
    .withColumn("country", trim(col("country")))
    .withColumn("state", trim(col("state")))
    .withColumn("ingestion_date", lit(args.ingestion_date))
)

(
    silver_df
    .repartition("country","state")
    .write
    .mode("overwrite")
    .partitionBy("ingestion_date","country","state")
    .parquet(f"s3a://{SILVER_BUCKET}/breweries")
)

print("Silver write completed.")
