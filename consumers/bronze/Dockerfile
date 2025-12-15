FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip uninstall -y kafka || true \
 && pip install --no-cache-dir -r requirements.txt

COPY kafka_to_minio_bronze.py .
CMD ["python", "kafka_to_minio_bronze.py"]
