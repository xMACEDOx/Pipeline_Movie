# Pipeline_Movie

# Visão Geral

O MoviePulse Analytics é uma plataforma de dados desenvolvida para monitorar, consolidar e analisar, de forma contínua, o desempenho e as tendências do mercado cinematográfico a partir de dados públicos da API do The Movie Database (TMDb).

O projeto foi concebido com foco em engenharia de dados, analytics e entrega de valor ao negócio, simulando um cenário real de tomada de decisão orientada por dados no setor de entretenimento.


## Problema de Negócio

Empresas do setor de mídia, streaming e marketing enfrentam desafios para:

Identificar rapidamente tendências emergentes de consumo de conteúdo;

Entender se a popularidade de um filme está relacionada à qualidade percebida ou apenas ao hype momentâneo;

Acompanhar a evolução diária de indicadores como popularidade, avaliações e engajamento;

Disponibilizar informações confiáveis e atualizadas para áreas como produto, marketing e planejamento estratégico.

Essas análises exigem dados atualizados, estruturados, históricos e de fácil acesso, o que raramente é entregue de forma integrada por fontes externas.


## Solução Proposta

O MoviePulse Analytics resolve esse problema por meio de um pipeline de dados automatizado, capaz de:

Coletar diariamente dados de filmes em alta;

Enriquecer informações com detalhes técnicos, gêneros, produtoras e métricas de engajamento;

Armazenar dados históricos de forma estruturada;

Processar e consolidar KPIs analíticos;

Disponibilizar resultados via API e consultas analíticas de alta performance.

A solução foi projetada seguindo boas práticas de arquitetura de dados moderna, separando ingestão, processamento, armazenamento e entrega.


## Indicadores Estratégicos (KPIs)

Top filmes por popularidade (diário e semanal);

Variação de popularidade ao longo do tempo (momentum);

Participação de gêneros no ranking de filmes populares;

Relação entre avaliação média, número de votos e popularidade;

Distribuição de produtoras entre os filmes de maior destaque.


# Arquiteura do Projeto;

<img width="1023" height="707" alt="image" src="https://github.com/user-attachments/assets/3ce31a22-e4a2-4c57-995e-0cbec3094fb2" />

Essa arquitetura implementa um pipeline orientado a eventos, onde dados de mercado cinematográfico são ingeridos via Kafka, persistidos em uma camada histórica, processados com Spark e disponibilizados em camadas analíticas que atendem diferentes áreas do negócio, como Marketing e BI, com observabilidade e governança.



## Hands On

Estrutura do projeto:

```
|
|
|__Airflow
|        |__dags
|_spark
|     |__jobs
|     
|_.env
|
|_Docker-compose.yaml

```


No powershell:

```
Docker compose up -d

```

Para criar o tópico "movies.trending.daily" utilizando o kafka;

```
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic movies.trending.daily --partitions 3 --replication-factor 1

```
Vamos listar os topicos para garantir que deu certo, no powershell:

```
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list


```
Dentro da pasta "dags" criar o arquivo "tmdb_to_kafka.py"

Dentro do arquivo "tmdb_to_kafka.py" preencher com:
```
# =========================
# Imports
# =========================

# Biblioteca padrão para acessar variáveis de ambiente (ex: TMDB_API_KEY)
import os

# Usada para serializar os dados (dict -> JSON)
import json

# Usada para aplicar pequenos delays (throttle)
import time

# Trabalhar com datas e timestamps
from datetime import datetime, date

# Biblioteca para fazer requisições HTTP (chamada à API do TMDb)
import requests

# Cliente Kafka para produzir mensagens
from kafka import KafkaProducer


# =========================
# Configurações do ambiente
# =========================

# Chave da API do TMDb (vem do .env / docker-compose)
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# Endereço do Kafka dentro da rede Docker
# 'kafka:29092' funciona porque os containers estão na mesma network
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

# Nome do tópico Kafka onde os eventos serão publicados
TOPIC = "movies.trending.daily"


# =========================
# Função: buscar filmes em alta no TMDb
# =========================
def fetch_trending_movies():
    """
    Chama o endpoint 'trending/movie/day' do TMDb
    e retorna a lista de filmes em alta do dia.
    """

    # Endpoint do TMDb para filmes em tendência diária
    url = "https://api.themoviedb.org/3/trending/movie/day"

    # Parâmetros da requisição
    params = {
        "api_key": TMDB_API_KEY,   # autenticação
        "language": "pt-BR"        # idioma dos dados
    }

    # Executa a chamada HTTP
    response = requests.get(url, params=params, timeout=30)

    # Se a API retornar erro (4xx ou 5xx), lança exceção
    response.raise_for_status()

    # Retorna apenas a lista de filmes (campo 'results')
    return response.json()["results"]


# =========================
# Função principal (Producer Kafka)
# =========================
def main():
    """
    Função principal do producer:
    - Busca dados no TMDb
    - Constrói eventos
    - Publica no Kafka (1 evento por filme)
    """

    # Validação básica: sem API Key o pipeline não pode rodar
    if not TMDB_API_KEY:
        raise RuntimeError(" TMDB_API_KEY não encontrada nas variáveis de ambiente.")

    # Criação do Producer Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,

        # Serializa o valor (mensagem) como JSON em bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),

        # Serializa a key como string (movie_id)
        key_serializer=lambda k: str(k).encode("utf-8"),

        # Número de tentativas em caso de falha
        retries=5,

        # Garante que o broker confirme o recebimento da mensagem
        acks="all"
    )

    # Data de referência do evento (usada para partições e histórico)
    today = str(date.today())

    # Timestamp exato da extração (auditoria)
    extraction_ts = datetime.now().astimezone().isoformat()

    # Busca os filmes em tendência no TMDb
    movies = fetch_trending_movies()

    # Loop: 1 evento Kafka por filme
    for movie in movies:

        # Estrutura do evento publicado no Kafka
        event = {
            "event_type": "tmdb_trending_daily",   # tipo do evento
            "source_date": today,                  # data de referência
            "extraction_ts": extraction_ts,        # quando o dado foi coletado

            # Identificadores e métricas principais
            "movie_id": movie.get("id"),
            "title": movie.get("title"),
            "popularity": movie.get("popularity"),
            "vote_average": movie.get("vote_average"),
            "vote_count": movie.get("vote_count"),
            "adult": movie.get("adult"),
            "release_date": movie.get("release_date"),
            "original_language": movie.get("original_language"),

            # Payload bruto do TMDb (mantido para auditoria e reprocessamento)
            "raw": movie
        }

        # Envia o evento para o Kafka
        # movie_id é usado como key para garantir ordenação por filme
        producer.send(
            TOPIC,
            key=event["movie_id"],
            value=event
        )

        # Pequeno delay para evitar excesso de chamadas/produção
        time.sleep(0.05)

    # Garante que todas as mensagens foram enviadas
    producer.flush()

    print(f" Publicados {len(movies)} eventos no tópico {TOPIC}")


# =========================
# Ponto de entrada do script
# =========================
if __name__ == "__main__":
    main()
```

Agora vamos ver se o airflow está reconhecendo o script.

Dentro do powershell coloque o seguinte comando:

```
docker exec -it airflow ls -la /opt/airflow/dags
```
Deve aparecer : 

tmdb_to_kafka.py

#### Deu certo?

Agora vamos instalar as dependencias do airflow:

Dentro do powershell coloque o seguinte comando:

```
docker exec -it airflow bash -lc "python -m pip install --no-cache-dir kafka-python requests

```

Agora vamos confirmar se deu certo?? Vamos validar a conexão do airflow com o kafka.

Para isso, vamos abrir outro terminal para utilizar o consumer do Kafka, nele vamos colocar o seguinte comando :

```
docker exec -it kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 `
  --topic movies.trending.daily `
  --from-beginning

```

Voltando para o terminal principal, vamos utilizar o producer utilizadno esse comando:

```
docker exec -it airflow bash -lc "python -c \"from kafka import KafkaProducer; p=KafkaProducer(bootstrap_servers=['kafka:29092']); p.send('movies.trending.daily', b'{\\\"source\\\":\\\"powershell_test\\\",\\\"status\\\":\\\"ok\\\"}'); p.flush(); print('sent')\""
```

O resultado esperado no consumer é :

```
{"source":"powershell_test","status":"ok"}

```

### Se deu certo, vamos para o proximo passo.


Vamos criar nossa primeira dag com Airflow.


Dentro da pasta Airflow/dags vamos criar um arquivo chamado "dag_tmdb_to_kafka_daily.py" com o seguinte conteudo:

```
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

```

Agora vamos reiniciar o airflow e ver se a dag vai aparecer:

```
docker compose restart airflow
```

Vamos testar manual sem esperar o schedule:

```
docker exec -it airflow airflow dags test tmdb_trending_daily_to_kafka 2025-12-15
```

Vamos testar se está funcionando? 

Abra o terminal do consumer, se não estiver aberto rode novamente:

```
docker exec -it kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 `
  --topic movies.trending.daily `
  --from-beginning
```
Você deve ver os eventos chegando


Mas afinal, o que essa DAG faz? 

Esse DAG executa diariamente um producer Kafka que consome dados da API do TMDb e publica eventos de filmes em tendência. Ele possui retry automático, não depende de execuções anteriores e é desacoplado da lógica de negócio, que fica em um script Python externo.


### Agora vamos conectar nosso minio com o kafka.

Crie um bucket chamado "Bronze" no minio.

Abra o vs code e crie as pastas abaixo com a seguinte estrutura:

```
Pipeline_Movie/
  consumers/
    bronze/
      Dockerfile
      requirements.txt
      kafka_to_minio_bronze.py
```
      
Dentro do Requiriments.txt

```
kafka-python==2.0.2
boto3
python-dateutil
```

Dentro do DOCKERFILE;

```
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_to_minio_bronze.py .

CMD ["python", "kafka_to_minio_bronze.py"]
```

No nosso docker compose vamos adicionar esse serviço:

```
bronze-consumer:
  build: ./consumers/bronze
  container_name: bronze-consumer
  depends_on: [kafka, minio]
  networks: [moviepulse-net]
  environment:
    KAFKA_BOOTSTRAP: "kafka:29092"
    KAFKA_TOPIC: "movies.trending.daily"
    KAFKA_GROUP_ID: "bronze-writer-tmdb"

    MINIO_ENDPOINT: "http://minio:9000"
    MINIO_ACCESS_KEY: ${MINIO_ROOT_USER}
    MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD}
    MINIO_BUCKET: "bronze"

    BRONZE_PREFIX: "tmdb/trending_daily"
    BATCH_SIZE: "200"
    FLUSH_SECONDS: "10"
  restart: unless-stopped
```


Dentro do arquivo "kafka_to_minio_bronze.py":

```
import os
import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
import boto3

# =========================
# Config (env)
# =========================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "movies.trending.daily")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-writer-tmdb")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

# Quantos eventos juntar antes de gravar um arquivo
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
# Quanto tempo máximo esperar para gravar (mesmo sem bater BATCH_SIZE)
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "10"))

# Prefixo da camada Bronze no bucket
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "tmdb/trending_daily")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(cli, bucket: str):
    # cria bucket se não existir
    try:
        cli.head_bucket(Bucket=bucket)
    except Exception:
        cli.create_bucket(Bucket=bucket)


def build_object_key(event: dict) -> str:
    """
    Gera caminho particionado por dt=YYYY-MM-DD.
    Usa source_date do evento quando existir; senão, data UTC atual.
    """
    dt = event.get("source_date")
    if not dt:
        dt = datetime.now(timezone.utc).date().isoformat()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{BRONZE_PREFIX}/dt={dt}/events-{ts}.jsonl"


def main():
    cli = s3_client()
    ensure_bucket(cli, MINIO_BUCKET)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=GROUP_ID,
        enable_auto_commit=False,          # commit manual (seguro)
        auto_offset_reset="earliest",      # se o grupo for novo, começa do início
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=1000,          # para permitir flush por tempo
    )

    buffer = []
    last_flush = time.time()

    print(f"[bronze-consumer] Connected. topic={TOPIC} group={GROUP_ID} kafka={KAFKA_BOOTSTRAP}")
    print(f"[bronze-consumer] Writing to s3://{MINIO_BUCKET}/{BRONZE_PREFIX}/dt=...")

    while True:
        # lê mensagens (poll via iterator)
        wrote = False
        for msg in consumer:
            event = msg.value

            # adiciona metadados Kafka para auditoria/reprocesso
            event["_kafka"] = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key,
                "ts_ms": msg.timestamp,
            }

            buffer.append(event)

            # flush por tamanho
            if len(buffer) >= BATCH_SIZE:
                wrote = flush_buffer(cli, consumer, buffer)
                last_flush = time.time()

        # flush por tempo (mesmo sem mensagem chegando)
        if buffer and (time.time() - last_flush) >= FLUSH_SECONDS:
            wrote = flush_buffer(cli, consumer, buffer)
            last_flush = time.time()

        # descanso leve para não girar CPU
        time.sleep(0.2)


def flush_buffer(cli, consumer, buffer: list) -> bool:
    # monta um arquivo JSONL (1 evento por linha)
    obj_key = build_object_key(buffer[-1])  # usa source_date do último
    body = "\n".join(json.dumps(e, ensure_ascii=False) for e in buffer) + "\n"

    try:
        cli.put_object(
            Bucket=MINIO_BUCKET,
            Key=obj_key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

        # commit offsets somente após gravar no MinIO
        consumer.commit()

        print(f"[bronze-consumer] Wrote {len(buffer)} events -> s3://{MINIO_BUCKET}/{obj_key}")
        buffer.clear()
        return True

    except Exception as e:
        # NÃO commit: se falhar, reprocessa depois
        print(f"[bronze-consumer][ERROR] Failed to write batch. Reason: {e}")
        return False


if __name__ == "__main__":
    main()
```

### Pronto, agora temos a nossa ingestão para a camada bronze 
com a seguinte estrutura:

```
s3://bronze/
  tmdb/trending_daily/
    dt=YYYY-MM-DD/
      events-<timestamp>.jsonl
```

Agora vamos fazer o build da imagem e executar o nosso serviço.

No powershell:

```
docker compose build --no-cache bronze-consumer
docker compose up -d bronze-consumer
```


Verifique o seu bucket bronze no minio.

## Proximo passo, fazer o tratamento dos dados com o Apache Spark













