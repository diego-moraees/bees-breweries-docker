import os
import json
import time
import datetime as dt
import logging
import requests
from minio import Minio

log = logging.getLogger(__name__)

OPEN_BREWERY_BASE_URL = os.getenv("OPEN_BREWERY_BASE_URL", "https://api.openbrewerydb.org/v1")
OPEN_BREWERY_PAGE_SIZE = int(os.getenv("OPEN_BREWERY_PAGE_SIZE", "200"))
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

def _minio_client() -> Minio:
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

    ingestion_date = dt.datetime.utcnow().date().isoformat()
    base_path = f"openbrewerydb/ingestion_date={ingestion_date}"

    session = requests.Session()
    page = 1
    total_objects = 0

    while True:
        params = {"per_page": OPEN_BREWERY_PAGE_SIZE, "page": page}
        url = f"{OPEN_BREWERY_BASE_URL}/breweries"
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break

        payload = json.dumps(data).encode("utf-8")
        object_name = f"{base_path}/page={page}.json"
        client.put_object(BRONZE_BUCKET, object_name, data=payload, length=len(payload), content_type="application/json")
        total_objects += 1
        log.info("Wrote %s/%s", BRONZE_BUCKET, object_name)

        page += 1
        time.sleep(0.2)

    log.info("Finished landing raw JSON pages: %d objects", total_objects)
    context["ti"].xcom_push(key="ingestion_date", value=ingestion_date)
