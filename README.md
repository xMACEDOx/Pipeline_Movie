#  MoviePulse Analytics

> Pipeline de dados end-to-end para monitorar tendências do mercado cinematográfico usando dados públicos da API do TMDb.

---

##  Visão Geral

O MoviePulse Analytics é uma plataforma de dados desenvolvida para monitorar, consolidar e analisar, de forma contínua, o desempenho e as tendências do mercado cinematográfico a partir de dados públicos da API do The Movie Database (TMDb).

O projeto foi concebido com foco em engenharia de dados, analytics e entrega de valor ao negócio, simulando um cenário real de tomada de decisão orientada por dados no setor de entretenimento.

---

##  Problema de Negócio

Empresas do setor de mídia, streaming e marketing enfrentam desafios para:

- Identificar rapidamente tendências emergentes de consumo de conteúdo;
- Entender se a popularidade de um filme está relacionada à qualidade percebida ou apenas ao hype momentâneo;
- Acompanhar a evolução diária de indicadores como popularidade, avaliações e engajamento;
- Disponibilizar informações confiáveis e atualizadas para áreas como produto, marketing e planejamento estratégico.

---

##  Solução Proposta

Pipeline de dados automatizado capaz de:

- Coletar diariamente dados de filmes em alta;
- Enriquecer informações com detalhes técnicos, gêneros e métricas de engajamento;
- Armazenar dados históricos de forma estruturada;
- Processar e consolidar KPIs analíticos;
- Disponibilizar resultados via consultas analíticas de alta performance.

---

##  Indicadores Estratégicos (KPIs)

- Top filmes por popularidade (diário e semanal);
- Variação de popularidade ao longo do tempo (momentum);
- Participação de gêneros no ranking de filmes populares;
- Relação entre avaliação média, número de votos e popularidade;
- Distribuição de produtoras entre os filmes de maior destaque.

---

##  Arquitetura do Projeto

<img width="1023" height="707" alt="image" src="https://github.com/user-attachments/assets/3ce31a22-e4a2-4c57-995e-0cbec3094fb2" />

Essa arquitetura implementa um pipeline orientado a eventos, onde dados de mercado cinematográfico são ingeridos via Kafka, persistidos em uma camada histórica, processados com Spark e disponibilizados em camadas analíticas.

### Stack de Tecnologias

| Camada | Ferramenta | Função |
|---|---|---|
| Ingestão | TMDb API + Airflow | Coleta diária de filmes em tendência |
| Streaming | Apache Kafka | Transporte de eventos em tempo real |
| Bronze | MinIO (S3) | Armazenamento raw particionado por data |
| Processamento | Apache Spark | Transformação e enriquecimento dos dados |
| Silver | PostgreSQL | Dados limpos e modelados |
| Orquestração | Apache Airflow | Agendamento e controle do pipeline |
| Visualização | Metabase | Dashboards para o negócio |
| Observabilidade | Grafana | Monitoramento da infraestrutura |

---

##  Estrutura do Projeto

```
Pipeline_Movie/
  .env                                ← variáveis de ambiente (NÃO commitar)
  .env.example                        ← template das variáveis
  docker-compose.yaml                 ← toda a infraestrutura
  airflow/
    start.sh                          ← script de inicialização do Airflow
    dags/
      tmdb_to_kafka.py                ← producer Kafka (lógica de ingestão)
      dag_tmdb_to_kafka_daily.py      ← DAG de ingestão + trigger Silver
      dag_bronze_to_silver.py         ← DAG de transformação Bronze → Silver
    requirements.txt                  ← dependências do Airflow
  spark/
    jobs/
      silver_job.py                   ← job PySpark Bronze → Silver
    jars/                             ← JARs S3A + JDBC (baixar manualmente)
  consumers/
    bronze/
      Dockerfile
      requirements.txt
      kafka_to_minio_bronze.py        ← consumer Kafka → MinIO
```

---

##  Fluxo Automatizado do Pipeline

O pipeline roda automaticamente todos os dias sem intervenção manual:

```
[00:00 UTC — @daily]
        │
        ▼
tmdb_trending_daily_to_kafka (DAG Airflow)
  ├── run_tmdb_producer         → busca filmes no TMDb e publica no Kafka
  └── trigger_bronze_to_silver  → dispara a DAG Silver passando source_date=hoje
                                          │
                                          ▼
                                bronze_to_silver (DAG Airflow)
                                  ├── check_bronze_files       → valida partição do dia no MinIO
                                  └── spark_bronze_to_silver   → lê dt=hoje e grava no Postgres
```

---

##  Modelo de Dados — Camada Silver

O Spark processa e entrega 4 tabelas no schema `silver` do PostgreSQL:

| Tabela | Descrição | Chave |
|---|---|---|
| `silver.dim_genres` | Catálogo de gêneros do TMDb | `genre_id` |
| `silver.dim_movies` | 1 linha por filme (deduplicado) | `movie_id` |
| `silver.bridge_movie_genres` | Relação N:N filme ↔ gênero | `(movie_id, genre_id)` |
| `silver.fact_trending` | Métricas diárias de popularidade | `(movie_id, source_date)` |

Estrutura no MinIO (camada Bronze):

```
s3://bronze/
  tmdb/trending_daily/
    dt=YYYY-MM-DD/
      events-<timestamp>.jsonl
```

---

##  Interfaces Disponíveis

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| Kafka UI | http://localhost:8085 | — |
| MinIO | http://localhost:9001 | minio / minio123 |
| pgAdmin | http://localhost:5050 | admin@moviepulse.com / moviepulse |
| Metabase | http://localhost:3000 | — |
| Grafana | http://localhost:3001 | — |
| Jupyter | http://localhost:8888 | — |

---

##  Hands On — Passo a Passo Completo

### Pré-requisitos

Antes de começar, certifique-se de ter instalado:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 24.x ou superior
- Docker Compose 2.x (já vem com o Docker Desktop)
- Git
- Mínimo de **8 GB de RAM** disponível para os containers
- Chave da API do TMDb → [themoviedb.org/settings/api](https://www.themoviedb.org/settings/api) *(gratuito)*

---

### 1. Clonar o repositório

```bash
git clone https://github.com/xmacedox/Pipeline_Movie.git
cd Pipeline_Movie
```

---

### 2. Configurar o arquivo .env

Copie o template e preencha com suas variáveis:

```bash
cp .env.example .env
```

Abra o `.env` e preencha:

```env
# TMDb — sua chave da API
TMDB_API_KEY=sua_chave_aqui

# Postgres
POSTGRES_USER=moviepulse
POSTGRES_PASSWORD=moviepulse
POSTGRES_DB=moviepulse

# Airflow
AIRFLOW_UID=50000
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@moviepulse.com

# MinIO
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
```

>  **Nunca commite o arquivo `.env` no Git.** Ele já está no `.gitignore`.

---

### 3. Criar o script de inicialização do Airflow

Crie o arquivo `airflow/start.sh` com o seguinte conteúdo:

```bash
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
```

>  **Importante:** Use `kafka-python-ng` e não `kafka-python==2.0.2`. O pacote original tem um bug com Python 3.12 que causa `ModuleNotFoundError: kafka.vendor.six.moves`.

---

### 4. Baixar os JARs do Spark

O Spark precisa de 3 JARs para se conectar ao MinIO e ao PostgreSQL. Execute dentro da pasta do projeto:

```bash
mkdir -p spark/jars
cd spark/jars

wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

cd ../..
```

>  Se não tiver `wget`, use `curl -O <url>` — o resultado é o mesmo.

---

### 5. Subir a infraestrutura

```bash
docker compose up -d
```

Aguarde **2 a 3 minutos** para todos os containers inicializarem. O Airflow demora um pouco mais pois instala as dependências no primeiro boot.

Verifique se todos estão rodando:

```bash
docker compose ps
```

Todos os containers devem aparecer com status `Up`.

---

### 6. Criar o tópico Kafka

Execute **apenas uma vez** após subir os containers:

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --topic movies.trending.daily \
  --partitions 3 \
  --replication-factor 1
```

Confirme que foi criado:

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Deve aparecer `movies.trending.daily` na lista.

>  Você também pode criar e monitorar o tópico pela interface do Kafka UI em http://localhost:8085

---

### 7. Configurar o Airflow

Acesse **http://localhost:8081** com as credenciais `admin / admin`.

#### 6.1 Criar Variables

Vá em **Admin → Variables → +** e crie as 3 variáveis abaixo:

| Key | Value |
|---|---|
| `TMDB_API_KEY` | sua chave do TMDb |
| `MINIO_ACCESS_KEY` | minio |
| `MINIO_SECRET_KEY` | minio123 |

#### 6.2 Criar Connection Spark

Vá em **Admin → Connections → +** e preencha:

| Campo | Valor |
|---|---|
| Connection Id | `spark_local` |
| Connection Type | `Spark` |
| Host | `local` |
| Extra | `{"deploy-mode": "client"}` |

Clique em **Save**.

#### 6.3 Verificar as DAGs

No menu principal confirme que as 2 DAGs aparecem:

- `tmdb_trending_daily_to_kafka` — ingestão diária (schedule: @daily)
- `bronze_to_silver` — transformação (schedule: None, disparada por trigger)

>  Se as DAGs não aparecerem, aguarde 30 segundos e recarregue a página.

---

### 8. Rodar o pipeline

#### Primeira execução manual

Dispare o pipeline pelo terminal:

```bash
docker exec -it airflow bash -c "/home/airflow/.local/bin/airflow dags trigger tmdb_trending_daily_to_kafka"
```

Acompanhe em tempo real no **Airflow UI** em http://localhost:8081.

A DAG de ingestão dispara a `bronze_to_silver` automaticamente ao terminar.

>  A partir daí, o pipeline roda sozinho **todo dia às 00:00 UTC** sem intervenção manual.

---

### 9. Validar os resultados

#### Bronze — MinIO (http://localhost:9001)

Acesse com `minio / minio123` e navegue em:

```
bronze → tmdb → trending_daily → dt=YYYY-MM-DD
```

Você deve ver arquivos `.jsonl` com os eventos do dia.

#### Silver — pgAdmin (http://localhost:5050)

Acesse com `admin@moviepulse.com / moviepulse`.

Conecte no servidor com:

| Campo | Valor |
|---|---|
| Host | `postgres` |
| Port | `5432` |
| Database | `moviepulse` |
| Username | `moviepulse` |
| Password | `moviepulse` |

>  Sempre use o prefixo `silver.` nas queries.

```sql
-- Contagem de registros em cada tabela
SELECT COUNT(*) FROM silver.dim_genres;
SELECT COUNT(*) FROM silver.dim_movies;
SELECT COUNT(*) FROM silver.bridge_movie_genres;
SELECT COUNT(*) FROM silver.fact_trending;

-- Top 10 filmes mais populares do último dia
SELECT
    m.title,
    t.popularity,
    t.vote_average,
    t.vote_count,
    STRING_AGG(g.genre_name, ', ') AS genres
FROM silver.fact_trending t
JOIN silver.dim_movies          m ON m.movie_id = t.movie_id
JOIN silver.bridge_movie_genres b ON b.movie_id = t.movie_id
JOIN silver.dim_genres          g ON g.genre_id = b.genre_id
WHERE t.source_date = (SELECT MAX(source_date) FROM silver.fact_trending)
GROUP BY m.title, t.popularity, t.vote_average, t.vote_count
ORDER BY t.popularity DESC
LIMIT 10;

-- Participação por gênero
SELECT g.genre_name, COUNT(*) AS total_filmes
FROM silver.bridge_movie_genres b
JOIN silver.dim_genres g ON g.genre_id = b.genre_id
GROUP BY g.genre_name
ORDER BY total_filmes DESC;
```

#### Conectar o pgAdmin ao Postgres

Na primeira vez que acessar o pgAdmin, você precisa registrar o servidor manualmente:

1. Clique com o botão direito em **Servers → Register → Server**
2. Na aba **General**: dê o nome `moviepulse`
3. Na aba **Connection** preencha:

| Campo | Valor |
|---|---|
| Host | `postgres` |
| Port | `5432` |
| Database | `moviepulse` |
| Username | `moviepulse` |
| Password | `moviepulse` |

4. Clique em **Save**

>  O host é `postgres` (nome do container na rede Docker) e **não** `localhost`.

#### Pelo terminal (psql)

```bash
docker exec -it postgres psql -U moviepulse -d moviepulse
```

---

## 🔧 Troubleshooting

| Problema | Solução |
|---|---|
| DAG não aparece no Airflow | Verifique se o arquivo está em `airflow/dags/` e aguarde 30s |
| `TMDB_API_KEY` vazia no Spark | Crie a Variable no Airflow UI (Admin → Variables) |
| Spark não conecta no MinIO | Verifique se os 3 JARs estão em `spark/jars/` |
| `relation does not exist` no pgAdmin | Adicione o prefixo `silver.` na query |
| pgAdmin rejeita o e-mail | Use um e-mail com domínio válido: `admin@moviepulse.com` |
| Bronze Consumer não grava | Verifique se o tópico `movies.trending.daily` foi criado no Kafka |
| Container não sobe | Verifique se as portas 8081, 9001, 5050 não estão em uso |
| `ModuleNotFoundError: kafka.vendor.six` | Use `kafka-python-ng` em vez de `kafka-python==2.0.2` no `start.sh` |
| Airflow não sobe após reinstalar provider | Delete o volume: `docker volume rm pipeline_movie_airflow_db` e suba novamente |

```bash
# Comandos úteis para debug
docker logs -f airflow           # logs do Airflow
docker logs -f bronze-consumer   # logs do consumer Kafka → MinIO
docker compose ps                # status de todos os containers
docker compose restart airflow   # reiniciar um serviço específico

# Recriar o Airflow do zero (quando necessário)
docker compose down
docker volume rm pipeline_movie_airflow_db
docker compose up -d
```

---

##  Próximos Passos

- [ ] Criar camada Gold com KPIs agregados (momentum, participação por gênero)
- [ ] Criar DAG `silver_to_gold` no Airflow (encadeada após a Silver)
- [ ] Conectar Metabase ao PostgreSQL para dashboards analíticos
- [ ] Configurar alertas no Grafana para monitorar falhas nas DAGs
