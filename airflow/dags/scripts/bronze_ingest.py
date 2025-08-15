"""
Ingest breweries data from the Open Brewery DB public API and land it in the
Bronze layer (raw zone) of a data lake hosted on MinIO.

This script:
1) Pulls the API collection (paged) and optional metadata.
2) Writes each page as a JSON object under a date-partitioned prefix:
   s3://<BRONZE_BUCKET>/<DATASET_NAME>/ingestion_date=YYYY-MM-DD/page=N.json
   and (optionally) meta.json alongside it.
3) Prints the `ingestion_date` to stdout (useful for downstream tasks).

Environment variables:
- MINIO_ENDPOINT_URL   (default: "http://minio:9000")
- MINIO_ROOT_USER      (required)
- MINIO_ROOT_PASSWORD  (required)
- BRONZE_BUCKET        (default: "bronze")
- DATASET_NAME         (default: "breweries")

- OPEN_BREWERY_BASE_URL  (default: "https://api.openbrewerydb.org/v1")
- OPEN_BREWERY_PAGE_SIZE (default: "200")
- OPEN_BREWERY_MAX_PAGES (optional integer; limits page count for tests)

Notes:
- Uses ingestion-time partitioning (ingestion_date). This preserves daily snapshots
  and enables incremental downstream processing and auditing.
"""

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
    """
    Simple client for the Open Brewery DB API.
    """

    def __init__(
            self,
            base_url: str = "https://api.openbrewerydb.org/v1",
            per_page: int = 200,
            timeout: int = 10,
            max_retries: int = 3,
            backoff_sec: int = 20,
    ):
        """
        Initialize the client.

        Args:
            base_url: API base URL.
            per_page: Page size for listings.
            timeout: Request timeout in seconds.
            max_retries: Max attempts per request.
            backoff_sec: Sleep seconds between retries.
        """
        self.base_url = base_url.rstrip("/")
        self.per_page = per_page
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self.session = requests.Session()

    def _get(self, url: str):
        """
        Perform GET with retries.

        Args:
            url: Absolute URL to request.

        Returns:
            Parsed JSON response.

        Raises:
            requests.RequestException: If all retries fail.
        """
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
        """
        Yield paginated brewery data.

        Args:
            start_page: First page number.
            max_pages: Optional max pages to fetch.

        Yields:
            Tuple of (page_number, list_of_rows).
        """
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
            time.sleep(0.2)

    def get_metadata(self) -> Dict:
        """
        Fetch collection metadata.

        Returns:
            Metadata dict or empty dict if unavailable.
        """
        url = f"{self.base_url}/breweries/meta"
        try:
            return self._get(url)
        except Exception as e:
            logging.warning("Could not fetch metadata: %s", e)
            return {}


class BronzeWriter:
    """
    Write JSON objects to a MinIO bucket (creates it if missing).
    """

    def __init__(self):
        """
        Initialize MinIO client from environment.

        Env:
            MINIO_ENDPOINT_URL, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, BRONZE_BUCKET
        """
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
        """
        Create the bucket if it does not exist.

        Args:
            name: Bucket name.
        """
        if not self.client.bucket_exists(name):
            self.client.make_bucket(name)
            logging.info("Created bucket %s", name)

    def put_json(self, object_name: str, obj: Dict | List):
        """
        Upload a dict/list as JSON to MinIO.

        Args:
            object_name: Object key (path) within the bucket.
            obj: Data to serialize as JSON.
        """
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.client.put_object(
            self.bucket,
            object_name,
            data=BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        logging.info("Wrote s3://%s/%s", self.bucket, object_name)


def main():
    """
    Run the ingestion and print the ingestion_date.

    Side effects:
        Writes JSON files to s3://<bucket>/<dataset>/ingestion_date=YYYY-MM-DD/.
    """
    ingestion_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    dataset = os.getenv("DATASET_NAME", "breweries")
    base_path = f"{dataset}/ingestion_date={ingestion_date}"

    per_page = int(os.getenv("OPEN_BREWERY_PAGE_SIZE", "200"))
    base_url = os.getenv("OPEN_BREWERY_BASE_URL", "https://api.openbrewerydb.org/v1")
    max_pages_env = os.getenv("OPEN_BREWERY_MAX_PAGES")
    max_pages = int(max_pages_env) if max_pages_env else None

    client = BreweriesClient(base_url=base_url, per_page=per_page)
    writer = BronzeWriter()

    total_pages = 0
    total_records = 0

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
