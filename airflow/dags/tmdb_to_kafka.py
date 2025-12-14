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
        raise RuntimeError("❌ TMDB_API_KEY não encontrada nas variáveis de ambiente.")

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

    print(f"✅ Publicados {len(movies)} eventos no tópico {TOPIC}")


# =========================
# Ponto de entrada do script
# =========================
if __name__ == "__main__":
    main()
