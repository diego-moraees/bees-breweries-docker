from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# importa a task de bronze (agora dentro de dags/tasks)
from tasks.bronze_etl_task import fetch_and_land_raw

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="breweries_elt_pipeline",
        description="Ingest breweries from Open Brewery DB and build bronze/silver/gold layers",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # use '@daily' se quiser agendar
        catchup=False,
        tags=["breweries", "medallion", "spark", "minio"],
) as dag:

    bronze_land_raw = PythonOperator(
        task_id="bronze_land_raw",
        python_callable=fetch_and_land_raw,
        provide_context=True,
        retries=3,
    )

    silver_build = SparkSubmitOperator(
        task_id="silver_build",
        application="/opt/spark-apps/silver-layer/silver_breweries.py",
        name="silver_breweries",
        conn_id="spark_default",  # AIRFLOW_CONN_SPARK_DEFAULT já está no config.env
        application_args=[
            "--ingestion_date",
            "{{ ti.xcom_pull(key='ingestion_date', task_ids='bronze_land_raw') }}",
        ],
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "{{ env.MINIO_ROOT_USER or 'admin' }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ env.MINIO_ROOT_PASSWORD or 'admin123456' }}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        verbose=True,
    )

    gold_build = SparkSubmitOperator(
        task_id="gold_build",
        application="/opt/spark-apps/gold-layer/gold_breweries.py",
        name="gold_breweries",
        conn_id="spark_default",
        application_args=[
            "--ingestion_date",
            "{{ ti.xcom_pull(key='ingestion_date', task_ids='bronze_land_raw') }}",
        ],
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "{{ env.MINIO_ROOT_USER or 'admin' }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ env.MINIO_ROOT_PASSWORD or 'admin123456' }}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        verbose=True,
    )

    bronze_land_raw >> silver_build >> gold_build
