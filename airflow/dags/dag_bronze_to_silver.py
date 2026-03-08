"""
dag_bronze_to_silver.py
────────────────────────
DAG de transformação Bronze → Silver.

Pode ser disparada:
  - Automaticamente via TriggerDagRunOperator (pela DAG de ingestão)
  - Manualmente pelo Airflow UI

Ao ser disparada, recebe `source_date` via conf para processar
apenas a partição do dia — evitando reprocessar todo o Bronze.

Fluxo:
  check_bronze_files → spark_bronze_to_silver
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# =========================
# Argumentos padrão
# =========================
default_args = {
    "owner":           "moviepulse",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
}


# =========================
# Task: valida se há arquivos no Bronze para o dia
# =========================
def check_bronze_files(**context) -> bool:
    """
    Verifica se há arquivos na partição dt=source_date do Bronze.
    - source_date vem do conf quando disparado por trigger
    - fallback para a data de execução do Airflow (ds)
    """
    import boto3, os

    # Pega source_date do conf (enviado pelo trigger) ou usa ds do Airflow
    conf        = context["dag_run"].conf or {}
    source_date = conf.get("source_date", context["ds"])  # formato YYYY-MM-DD

    prefix = f"tmdb/trending_daily/dt={source_date}/"

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        region_name="us-east-1",
    )

    response = s3.list_objects_v2(Bucket="bronze", Prefix=prefix)
    arquivos  = response.get("Contents", [])

    if not arquivos:
        print(f"[check_bronze]   Nenhum arquivo em s3://bronze/{prefix}. Pulando Spark.")
        return False

    print(f"[check_bronze]  {len(arquivos)} arquivo(s) em s3://bronze/{prefix}. Prosseguindo.")

    # Salva source_date no XCom para a task Spark usar
    context["ti"].xcom_push(key="source_date", value=source_date)
    return True


# =========================
# DAG
# =========================
with DAG(
    dag_id="bronze_to_silver",
    description="Transforma Bronze (MinIO) → Silver (Postgres) via Spark — somente partição do dia",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,      # sem schedule próprio: disparada pelo trigger da ingestão
    catchup=False,
    tags=["spark", "silver", "moviepulse"],
) as dag:

    # ── Task 1: valida partição do dia no Bronze ──────────────────
    check_bronze = ShortCircuitOperator(
        task_id="check_bronze_files",
        python_callable=check_bronze_files,
        provide_context=True,
    )

    # ── Task 2: job Spark lê só a partição do dia ─────────────────
    spark_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/airflow/spark/jobs/silver_job.py",
        conn_id="spark_local",

        # JARs: S3A (MinIO) + JDBC (Postgres)
        jars=(
            "/opt/airflow/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/airflow/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/airflow/spark/jars/postgresql-42.7.3.jar"
        ),

        # source_date vem do XCom da task anterior
        application_args=[
            "--source-date",
            "{{ task_instance.xcom_pull(task_ids='check_bronze_files', key='source_date') }}"
        ],

        env_vars={
            "TMDB_API_KEY":     "{{ var.value.TMDB_API_KEY }}",
            "MINIO_ENDPOINT":   "http://minio:9000",
            "MINIO_ACCESS_KEY": "{{ var.value.MINIO_ACCESS_KEY }}",
            "MINIO_SECRET_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
            "PG_HOST":          "postgres",
            "PG_PORT":          "5432",
            "PG_DB":            "moviepulse",
            "PG_USER":          "moviepulse",
            "PG_PASSWORD":      "moviepulse",
        },

        conf={
            "spark.master":                                 "local[*]",
            "spark.hadoop.fs.s3a.impl":                     "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access":        "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled":   "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        },

        verbose=True,
    )

    # ── Fluxo ─────────────────────────────────────────────────────
    check_bronze >> spark_silver
