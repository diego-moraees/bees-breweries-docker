import os
import json
import time
import datetime as dt
import logging
from io import BytesIO
import requests
from minio import Minio

log = logging.getLogger(__name__)

OPEN_BREWERY_BASE_URL = os.getenv("OPEN_BREWERY_BASE_URL", "https://api.openbrewerydb.org/v1")
OPEN_BREWERY_PAGE_SIZE = int(os.getenv("OPEN_BREWERY_PAGE_SIZE", "200"))
OPEN_BREWERY_MAX_PAGES = os.getenv("OPEN_BREWERY_MAX_PAGES")  # opcional
MAX_PAGES = int(OPEN_BREWERY_MAX_PAGES) if OPEN_BREWERY_MAX_PAGES else None

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
DATASET_NAME = os.getenv("DATASET_NAME", "breweries")


def _minio_client() -> Minio:
    if not (MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        raise RuntimeError("MINIO credentials missing: set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD")
    endpoint = MINIO_ENDPOINT_URL.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_ENDPOINT_URL.startswith("https://"),
    )


def _ensure_bucket(client: Minio, name: str):
    if not client.bucket_exists(name):
        client.make_bucket(name)
        log.info("Created bucket %s", name)


def fetch_and_land_raw(**context):
    """Fetch breweries by pages and land raw JSON pages into bronze bucket partitioned by ingestion_date."""
    client = _minio_client()
    _ensure_bucket(client, BRONZE_BUCKET)

    ingestion_date = dt.datetime.now(dt.timezone.utc).date().isoformat()  # <— timezone-aware
    base_path = f"{DATASET_NAME}/ingestion_date={ingestion_date}"         # <— usa DATASET_NAME

    session = requests.Session()
    page = 1
    total_objects = 0

    while True:
        if MAX_PAGES is not None and page > MAX_PAGES:
            log.info("Reached MAX_PAGES=%s. Stopping.", MAX_PAGES)
            break

        params = {"per_page": OPEN_BREWERY_PAGE_SIZE, "page": page}
        url = f"{OPEN_BREWERY_BASE_URL}/breweries"
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break

        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        object_name = f"{base_path}/page={page}.json"
        # use BytesIO (file-like)
        client.put_object(
            BRONZE_BUCKET,
            object_name,
            data=BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        total_objects += 1
        log.info("Wrote s3://%s/%s", BRONZE_BUCKET, object_name)

        page += 1
        time.sleep(0.2)

    log.info("Finished landing raw JSON pages: %d objects", total_objects)
    context["ti"].xcom_push(key="ingestion_date", value=ingestion_date)
