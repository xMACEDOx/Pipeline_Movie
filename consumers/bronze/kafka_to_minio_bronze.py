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
