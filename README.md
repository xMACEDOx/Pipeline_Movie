# 🎬 MoviePulse Analytics

## Visão Geral

O MoviePulse Analytics é uma plataforma de dados desenvolvida para monitorar, consolidar e analisar, de forma contínua, o desempenho e as tendências do mercado cinematográfico a partir de dados públicos da API do The Movie Database (TMDb).

O projeto foi concebido com foco em engenharia de dados, analytics e entrega de valor ao negócio, simulando um cenário real de tomada de decisão orientada por dados no setor de entretenimento.

---

## Problema de Negócio

Empresas do setor de mídia, streaming e marketing enfrentam desafios para:

- Identificar rapidamente tendências emergentes de consumo de conteúdo;
- Entender se a popularidade de um filme está relacionada à qualidade percebida ou apenas ao hype momentâneo;
- Acompanhar a evolução diária de indicadores como popularidade, avaliações e engajamento;
- Disponibilizar informações confiáveis e atualizadas para áreas como produto, marketing e planejamento estratégico.

Essas análises exigem dados atualizados, estruturados, históricos e de fácil acesso, o que raramente é entregue de forma integrada por fontes externas.

---

## Solução Proposta

O MoviePulse Analytics resolve esse problema por meio de um pipeline de dados automatizado, capaz de:

- Coletar diariamente dados de filmes em alta;
- Enriquecer informações com detalhes técnicos, gêneros, produtoras e métricas de engajamento;
- Armazenar dados históricos de forma estruturada;
- Processar e consolidar KPIs analíticos;
- Disponibilizar resultados via consultas analíticas de alta performance.

A solução foi projetada seguindo boas práticas de arquitetura de dados moderna, separando ingestão, processamento, armazenamento e entrega.

---

## Indicadores Estratégicos (KPIs)

- Top filmes por popularidade (diário e semanal);
- Variação de popularidade ao longo do tempo (momentum);
- Participação de gêneros no ranking de filmes populares;
- Relação entre avaliação média, número de votos e popularidade;
- Distribuição de produtoras entre os filmes de maior destaque.

---

## Arquitetura do Projeto

<img width="1023" height="707" alt="image" src="https://github.com/user-attachments/assets/3ce31a22-e4a2-4c57-995e-0cbec3094fb2" />

Essa arquitetura implementa um pipeline orientado a eventos, onde dados de mercado cinematográfico são ingeridos via Kafka, persistidos em uma camada histórica, processados com Spark e disponibilizados em camadas analíticas que atendem diferentes áreas do negócio, como Marketing e BI, com observabilidade e governança.

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

## Estrutura do Projeto

```
Pipeline_Movie/
  .env                              ← variáveis de ambiente (NÃO commitar)
  .env.example                      ← template das variáveis
  docker-compose.yaml               ← toda a infraestrutura
  airflow/
    dags/
      tmdb_to_kafka.py              ← producer Kafka (lógica de ingestão)
      dag_tmdb_to_kafka_daily.py    ← DAG de ingestão + trigger Silver
      dag_bronze_to_silver.py       ← DAG de transformação Bronze → Silver
    requirements.txt                ← dependências do Airflow
  spark/
    jobs/
      silver_job.py                 ← job PySpark Bronze → Silver
    jars/                           ← JARs S3A + JDBC (baixar manualmente)
  consumers/
    bronze/
      Dockerfile
      requirements.txt
      kafka_to_minio_bronze.py      ← consumer Kafka → MinIO
```

---

## Fluxo Automatizado do Pipeline

O pipeline roda automaticamente todos os dias sem intervenção manual:

```
[00:00 UTC — @daily]
        │
        ▼
tmdb_trending_daily_to_kafka (DAG Airflow)
  ├── run_tmdb_producer       → busca filmes no TMDb e publica no Kafka
  └── trigger_bronze_to_silver → dispara a DAG Silver passando source_date=hoje
                                          │
                                          ▼
                                bronze_to_silver (DAG Airflow)
                                  ├── check_bronze_files   → valida partição do dia no MinIO
                                  └── spark_bronze_to_silver → lê dt=hoje e grava no Postgres
```

---

## Modelo de Dados — Camada Silver

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

## Interfaces Disponíveis

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

## Hands On

### Pré-requisitos

- Docker Desktop 24.x
- Docker Compose 2.x
- 8 GB de RAM disponível
- Chave da API do TMDb → [themoviedb.org/settings/api](https://www.themoviedb.org/settings/api) (gratuito)

---

### 1. Clonar e configurar o .env

```bash
git clone https://github.com/seu-usuario/Pipeline_Movie.git
cd Pipeline_Movie

cp .env.example .env
# Edite o .env e preencha suas variáveis, especialmente TMDB_API_KEY
```

---

### 2. Baixar os JARs do Spark

Necessários para o Spark se conectar ao MinIO e ao PostgreSQL:

```bash
mkdir -p spark/jars && cd spark/jars

wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

cd ../..
```

---

### 3. Subir a infraestrutura

```bash
docker compose up -d
```

Aguarde ~2 minutos e verifique se todos os containers estão rodando:

```bash
docker compose ps
```

---

### 4. Criar o tópico Kafka

Execute apenas uma vez após subir os containers:

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

---

### 5. Configurar o Airflow

Acesse **http://localhost:8081** (admin / admin).

**5.1 Criar Variables** → Admin → Variables → +:

| Key | Value |
|---|---|
| `TMDB_API_KEY` | sua chave do TMDb |
| `MINIO_ACCESS_KEY` | minio |
| `MINIO_SECRET_KEY` | minio123 |

**5.2 Criar Connection Spark** → Admin → Connections → +:

| Campo | Valor |
|---|---|
| Connection Id | `spark_local` |
| Connection Type | `Spark` |
| Host | `local` |
| Extra | `{"deploy-mode": "client"}` |

**5.3 Verificar as DAGs**

Confirme que as 2 DAGs aparecem na lista:
- `tmdb_trending_daily_to_kafka` — ingestão diária (schedule: @daily)
- `bronze_to_silver` — transformação (schedule: None, disparada por trigger)

---

### 6. Rodar o pipeline

**Primeira execução manual:**

```bash
docker exec -it airflow airflow dags trigger tmdb_trending_daily_to_kafka
```

Acompanhe em tempo real no Airflow UI. A DAG de ingestão dispara a `bronze_to_silver` automaticamente ao terminar.

**A partir daí, o pipeline roda sozinho todo dia às 00:00 UTC.**

---

### 7. Validar os resultados

**Bronze — MinIO (http://localhost:9001):**

Navegue em `bronze → tmdb → trending_daily → dt=YYYY-MM-DD` e confirme que há arquivos `.jsonl`.

**Silver — pgAdmin (http://localhost:5050):**

> ⚠️ Sempre use o prefixo `silver.` nas queries.

```sql
-- Contagem por tabela
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
```

---

## Troubleshooting

| Problema | Solução |
|---|---|
| DAG não aparece no Airflow | Verifique se o arquivo está em `airflow/dags/` e aguarde 30s |
| `TMDB_API_KEY` vazia no Spark | Crie a Variable no Airflow UI (Admin → Variables) |
| Spark não conecta no MinIO | Verifique se os 3 JARs estão em `spark/jars/` |
| `relation does not exist` no pgAdmin | Adicione o prefixo `silver.` na query |
| pgAdmin rejeita o e-mail | Use um e-mail com domínio válido: `admin@moviepulse.com` |
| Bronze Consumer não grava | Verifique se o tópico `movies.trending.daily` foi criado |
| Container não sobe | Verifique se as portas 8081, 9001, 5050 não estão em uso |

```bash
# Comandos úteis para debug
docker logs -f airflow          # logs do Airflow
docker logs -f bronze-consumer  # logs do consumer Kafka → MinIO
docker compose ps               # status de todos os containers
docker compose restart airflow  # reiniciar um serviço específico
```

---

## Próximos Passos

- [ ] Criar camada Gold com KPIs agregados (momentum, participação por gênero)
- [ ] Conectar Metabase ao PostgreSQL Silver para dashboards
- [ ] Configurar alertas no Grafana para falhas nas DAGs
- [ ] Adicionar DAG Gold ao pipeline automatizado
