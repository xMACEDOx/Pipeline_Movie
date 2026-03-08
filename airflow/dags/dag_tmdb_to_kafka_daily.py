"""
dag_tmdb_to_kafka_daily.py
Ingestão diária TMDb → Kafka → dispara Silver via TriggerDagRunOperator
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "moviepulse",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tmdb_trending_daily_to_kafka",
    description="Busca trending daily no TMDb, publica no Kafka e dispara pipeline Silver",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule="@daily",
    catchup=False,
    tags=["tmdb", "kafka", "ingestão", "moviepulse"],
) as dag:

    run_producer = BashOperator(
        task_id="run_tmdb_producer",
        bash_command="python /opt/airflow/dags/tmdb_to_kafka.py",
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver",
        wait_for_completion=False,
        conf={"source_date": "{{ ds }}"},
        reset_dag_run=True,
    )

    run_producer >> trigger_silver
