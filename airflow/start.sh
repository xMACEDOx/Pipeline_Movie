#!/bin/bash
set -e

pip install --no-cache-dir requests kafka-python-ng boto3 psycopg2-binary apache-airflow-providers-apache-spark==4.10.0 --no-deps

echo "Aguardando banco de dados..."
sleep 20

/home/airflow/.local/bin/airflow db init
/home/airflow/.local/bin/airflow db migrate

/home/airflow/.local/bin/airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname Local \
  --role Admin \
  --email admin@moviepulse.com || true

/home/airflow/.local/bin/airflow connections add spark_local \
  --conn-type spark \
  --conn-host local \
  --conn-extra '{"deploy-mode": "client"}' || true

/home/airflow/.local/bin/airflow webserver &
/home/airflow/.local/bin/airflow scheduler