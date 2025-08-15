from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# Default behavior for all tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [os.getenv("AIRFLOW_ON_FAILED_EMAIL")],
    "email_on_failure": True,
    "email_on_success": False,
    "retries": 0,
}

# Common environment variables passed to tasks
COMMON_ENV = {
    # MinIO / buckets
    "MINIO_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
    "BRONZE_BUCKET": os.getenv("BRONZE_BUCKET", "bronze"),
    "SILVER_BUCKET": os.getenv("SILVER_BUCKET", "silver"),
    "GOLD_BUCKET": os.getenv("GOLD_BUCKET", "gold"),

    # Dataset
    "DATASET_NAME": os.getenv("DATASET_NAME", "breweries"),

    # Source API
    "OPEN_BREWERY_BASE_URL": os.getenv("OPEN_BREWERY_BASE_URL", "https://api.openbrewerydb.org/v1"),
    "OPEN_BREWERY_PAGE_SIZE": os.getenv("OPEN_BREWERY_PAGE_SIZE", "200"),
    # "OPEN_BREWERY_MAX_PAGES": "2",  # optional throttle for tests
}

ALERT_TO = os.getenv("ALERT_EMAIL_TO", os.getenv("AIRFLOW_ON_FAILED_EMAIL", os.getenv("AIRFLOW__SMTP__SMTP_USER", "diego.moraes.ext@gmail.com")))


# Spark cluster endpoint
SPARK_MASTER = "spark://spark-master:7077"

# Reusable JAR sets
AWS_JARS = [
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
]
DELTA_JARS = [
    "/opt/spark/jars/delta-core_2.12-2.4.0.jar",
    "/opt/spark/jars/delta-storage-2.4.0.jar",
]

# Reusable Spark conf (S3A + timezone); credentials go via ENV, not --conf
S3A_CONF = [
    "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
    "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
    "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
    "--conf", "spark.sql.session.timeZone=UTC",
]

# Delta Lake specific Spark conf (only for gold)
DELTA_CONF = [
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
]

def build_spark_cmd(app_path: str, extra_confs=None, extra_jars=None, app_args: str = "") -> str:
    """
    Build a safe spark-submit command string:
    - client deploy mode (driver runs in worker container)
    - S3A conf (no secrets on cmdline)
    - JAR set composed from AWS + optional extras (e.g., Delta)
    """
    jars = AWS_JARS + (extra_jars or [])
    confs = S3A_CONF + (extra_confs or [])
    return (
            "set -euo pipefail; "
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            + " ".join(confs) + " "
                                f"--jars {','.join(jars)} "
                                f"{app_path} {app_args}"
    )

with DAG(
        dag_id="breweries_elt_pipeline",
        description="Ingest Open Brewery DB and build bronze/silver/gold medallion layers",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2025, 1, 1),
        schedule=None,          # Airflow 2.10+ style (no schedule)
        catchup=False,
        tags=["breweries", "spark", "minio", "etl"],
) as dag:

    # Start marker
    start = EmptyOperator(task_id="start")


    # BRONZE: Python script prints ingestion_date on the last line; capture it via XCom
    bronze_land_raw = BashOperator(
        task_id="bronze_land_raw",
        bash_command="set -euo pipefail; python /opt/airflow/dags/scripts/bronze_ingest.py",
        env=COMMON_ENV,
        do_xcom_push=True,
    )

    # SILVER: uses S3A via ENV
    silver_cmd = build_spark_cmd(
        app_path="/opt/spark-apps/silver-layer/silver_breweries_transform.py",
        app_args="--ingestion_date {{ ti.xcom_pull(task_ids='bronze_land_raw') }}",
    )
    silver_build = BashOperator(
        task_id="silver_build",
        bash_command=silver_cmd,
        env={
            **COMMON_ENV,
            # Hadoop AWS SDK pick credentials from environment
            "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "admin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "admin123456"),
        },
    )

    # GOLD: enable Delta Lake and write aggregated output
    gold_cmd = build_spark_cmd(
        app_path="/opt/spark-apps/gold-layer/gold_breweries_aggregate.py",
        extra_confs=DELTA_CONF,
        extra_jars=DELTA_JARS,
        app_args="--ingestion_date {{ ti.xcom_pull(task_ids='bronze_land_raw') }}",
    )
    gold_build = BashOperator(
        task_id="gold_build",
        bash_command=gold_cmd,
        env={
            **COMMON_ENV,
            "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "admin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "admin123456"),
        },
        trigger_rule="none_failed_min_one_success",
    )

    notify_failure = EmailOperator(
        task_id="notify_failure",
        to=[ALERT_TO],
        subject="DAG breweries_elt_pipeline FAILED",
        html_content=(
            "DAG <b>breweries_elt_pipeline</b> failed at {{ ts }}.<br>"
            "Failed tasks: <b>{{ dag_run.get_task_instances(state='failed') | map(attribute='task_id') | list }}</b><br>"
            "Run ID: <code>{{ dag_run.run_id }}</code>"
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # End marker
    end = EmptyOperator(task_id="end")

    start >> bronze_land_raw >> silver_build >> gold_build >> end
    [bronze_land_raw, silver_build, gold_build] >> notify_failure