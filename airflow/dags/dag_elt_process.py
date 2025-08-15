from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="breweries_elt_pipeline",
        description="Ingest breweries from Open Brewery DB and build bronze/silver/gold medallion layers",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # set '@daily' if you want scheduling
        catchup=False,
        tags=["breweries", "medallion", "spark", "minio"],
) as dag:

    # --- BRONZE: run our Python script; it prints ingestion_date to stdout ---
    bronze_env = {
        "MINIO_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "admin"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "admin123456"),
        "BRONZE_BUCKET": os.getenv("BRONZE_BUCKET", "bronze"),
        "OPEN_BREWERY_BASE_URL": os.getenv("OPEN_BREWERY_BASE_URL", "https://api.openbrewerydb.org/v1"),
        "OPEN_BREWERY_PAGE_SIZE": os.getenv("OPEN_BREWERY_PAGE_SIZE", "200"),
        # Optional throttle for tests (comment to fetch all pages)
        # "OPEN_BREWERY_MAX_PAGES": "2",
    }

    bronze_land_raw = BashOperator(
        task_id="bronze_land_raw",
        bash_command="python /opt/airflow/dags/scripts/bronze_ingest.py",
        env=bronze_env,
        do_xcom_push=True,  # capture printed ingestion_date
    )

    # Common Spark conf for MinIO/S3A
    spark_conf = {
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER", "admin"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD", "admin123456"),
        "spark.sql.session.timeZone": "UTC",
    }

    spark_cmd_common = (
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
        "--conf spark.hadoop.fs.s3a.access.key=admin "
        "--conf spark.hadoop.fs.s3a.secret.key=admin123456 "
        "--conf spark.sql.session.timeZone=UTC "
        "--jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar "
    )

    # silver_build = SparkSubmitOperator(
    #     task_id="silver_build",
    #     conn_id="spark_default",
    #     application="/opt/spark-apps/silver-layer/silver_breweries.py",
    #     name="silver_breweries",
    #     conf=spark_conf,
    #     application_args=["--ingestion_date", "{{ ti.xcom_pull(task_ids='bronze_land_raw') }}"],
    #     deploy_mode="client",
    #     jars="/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
    #     verbose=True,
    # )
    #
    # gold_build = SparkSubmitOperator(
    #     task_id="gold_build",
    #     conn_id="spark_default",
    #     application="/opt/spark-apps/gold-layer/gold_breweries.py",
    #     name="gold_breweries",
    #     conf=spark_conf,
    #     application_args=["--ingestion_date", "{{ ti.xcom_pull(task_ids='bronze_land_raw') }}"],
    #     deploy_mode="client",
    #     jars="/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
    #     verbose=True,
    # )
    silver_build = BashOperator(
        task_id="silver_build",
        bash_command=(
                spark_cmd_common +
                "/opt/spark-apps/silver-layer/silver_breweries.py "
                "--ingestion_date {{ ti.xcom_pull(task_ids='bronze_land_raw') }}"
        ),
    )

    gold_build = BashOperator(
        task_id="gold_build",
        bash_command=(
                spark_cmd_common +
                "/opt/spark-apps/gold-layer/gold_breweries.py "
                "--ingestion_date {{ ti.xcom_pull(task_ids='bronze_land_raw') }}"
        ),
    )

bronze_land_raw >> silver_build >> gold_build
