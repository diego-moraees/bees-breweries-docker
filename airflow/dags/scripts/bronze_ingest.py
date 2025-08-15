# airflow/dags/scripts/bronze_ingest.py
import os
import json
import time
import logging
import datetime as dt
from io import BytesIO
from typing import Optional, List, Dict

import requests
from minio import Minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class BreweriesClient:
    def __init__(
            self,
            base_url: str = "https://api.openbrewerydb.org/v1",
            per_page: int = 200,
            timeout: int = 10,
            max_retries: int = 3,
            backoff_sec: int = 20,
    ):
        self.base_url = base_url.rstrip("/")
        self.per_page = per_page
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self.session = requests.Session()

    def _get(self, url: str):
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.session.get(url, timeout=self.timeout)
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                logging.warning("Attempt %s/%s failed: %s", attempt, self.max_retries, e)
                if attempt == self.max_retries:
                    raise
                time.sleep(self.backoff_sec)

    def iter_pages(self, start_page: int = 1, max_pages: Optional[int] = None):
        page = start_page
        while True:
            if max_pages is not None and (page - start_page + 1) > max_pages:
                logging.info("Reached max_pages=%s. Stopping.", max_pages)
                break
            url = f"{self.base_url}/breweries?per_page={self.per_page}&page={page}"
            logging.info("Requesting page %s ...", page)
            data = self._get(url)
            if not data:
                logging.info("No more data after page %s. Stopping.", page)
                break
            yield page, data
            page += 1
            time.sleep(0.2)  # be kind to public API

    def get_metadata(self) -> Dict:
        url = f"{self.base_url}/breweries/meta"
        try:
            return self._get(url)
        except Exception as e:
            logging.warning("Could not fetch metadata: %s", e)
            return {}


class BronzeWriter:
    def __init__(self):
        self.endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket = os.getenv("BRONZE_BUCKET", "bronze")

        if not (self.access_key and self.secret_key):
            raise RuntimeError("MINIO credentials are not set (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).")

        endpoint = self.endpoint_url.replace("http://", "").replace("https://", "")
        self.client = Minio(
            endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.endpoint_url.startswith("https://"),
        )
        self._ensure_bucket(self.bucket)

    def _ensure_bucket(self, name: str):
        if not self.client.bucket_exists(name):
            self.client.make_bucket(name)
            logging.info("Created bucket %s", name)

    def put_json(self, object_name: str, obj: Dict | List):
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        # IMPORTANT: pass a readable stream (BytesIO), not raw bytes
        self.client.put_object(
            self.bucket,
            object_name,
            data=BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        logging.info("Wrote s3://%s/%s", self.bucket, object_name)


def main():
    ingestion_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    base_path = f"openbrewerydb/ingestion_date={ingestion_date}"

    per_page = int(os.getenv("OPEN_BREWERY_PAGE_SIZE", "200"))
    base_url = os.getenv("OPEN_BREWERY_BASE_URL", "https://api.openbrewerydb.org/v1")
    max_pages_env = os.getenv("OPEN_BREWERY_MAX_PAGES")  # optional throttle for tests
    max_pages = int(max_pages_env) if max_pages_env else None

    client = BreweriesClient(base_url=base_url, per_page=per_page)
    writer = BronzeWriter()

    total_pages = 0
    total_records = 0

    # Try metadata first (optional)
    metadata = client.get_metadata()
    if metadata:
        writer.put_json(f"{base_path}/meta.json", metadata)

    for page, data in client.iter_pages(max_pages=max_pages):
        writer.put_json(f"{base_path}/page={page}.json", data)
        total_pages += 1
        total_records += len(data)

    logging.info("Done. Pages: %s | Rows: %s | Date: %s", total_pages, total_records, ingestion_date)
    print(ingestion_date)


if __name__ == "__main__":
    main()
