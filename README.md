# BEES Data Engineering – Breweries Case (Docker + Airflow + Spark + MinIO)

**Goal:** Ingest breweries from Open Brewery DB, land raw data to a data lake (bronze), transform to partitioned columnar data (silver), and build an aggregated analytical layer (gold). The project runs locally with Docker: **Airflow** orchestrates; **Spark** transforms; **MinIO** emulates S3.

## Architecture

- **Bronze (Raw):** JSON snapshots fetched from Open Brewery DB API (no schema changes).
- **Silver (Curated):** Parquet data partitioned by `country` and `state` (location). Basic cleaning and typing.
- **Gold (Analytics):** Aggregation by `brewery_type` and location (country/state) with counts.

## Services

- **MinIO**: Local S3-compatible object store with `bronze`, `silver`, `gold` buckets.
- **Airflow**: Orchestrates the pipeline with retries, SLA, and task-level logging.
- **Spark + Jupyter**: PySpark (with Hadoop S3A) and Jupytext to keep notebooks as versionable `.py` files.

## Why Jupytext notebooks-as-code

We use `jupytext` to store notebooks as `.py` (percent format). They are easy to review in Git and can be executed as notebooks using **papermill** in Airflow.

## Quickstart

1. **Clone & configure env**

```bash
git clone https://github.com/<your-username>/bees-breweries-case.git
cd bees-breweries-case
cp .env.example .env
# Edit .env to adjust secrets and endpoints