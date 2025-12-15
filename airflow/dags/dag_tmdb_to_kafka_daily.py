# =========================
# Imports
# =========================

# Manipulação de datas e intervalos de tempo
from datetime import datetime, timedelta

# Classe principal para definição de DAGs no Airflow
from airflow import DAG

# Operador que executa comandos no shell (bash)
from airflow.operators.bash import BashOperator


# =========================
# Argumentos padrão da DAG
# =========================

default_args = {
    # Responsável pela DAG (apenas informativo)
    "owner": "moviepulse",

    # Não depende do sucesso da execução anterior
    "depends_on_past": False,

    # Número de tentativas em caso de falha
    "retries": 3,

    # Intervalo entre as tentativas
    "retry_delay": timedelta(minutes=5),
}


# =========================
# Definição da DAG
# =========================

with DAG(
    # Identificador único da DAG no Airflow
    dag_id="tmdb_trending_daily_to_kafka",

    # Descrição exibida na interface do Airflow
    description="Busca trending daily no TMDb e publica 1 evento por filme no Kafka",

    # Argumentos padrão definidos acima
    default_args=default_args,

    # Data inicial a partir da qual o agendamento é válido
    start_date=datetime(2025, 12, 1),

    # Agendamento: executa uma vez por dia
    schedule="@daily",

    # Não executa DAGs retroativos (backfill)
    catchup=False,

    # Tags para organização na UI do Airflow
    tags=["tmdb", "kafka", "projetomovie"],
) as dag:

    # =========================
    # Task: Executar Producer Kafka
    # =========================

    run_producer = BashOperator(
        # Nome da task (aparece no grafo do Airflow)
        task_id="run_tmdb_producer",

        # Comando executado dentro do container Airflow
        # Chama o script que busca dados do TMDb
        # e publica eventos no Kafka
        bash_command="python /opt/airflow/dags/tmdb_to_kafka.py",
    )

    # =========================
    # Definição do fluxo
    # =========================

    # Como existe apenas uma task, ela é executada diretamente
    run_producer
