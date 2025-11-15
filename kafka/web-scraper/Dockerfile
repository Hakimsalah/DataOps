# Dockerfile pour kafka-producer
FROM python:3.10-slim

WORKDIR /app

# Installer les dépendances
RUN pip install --no-cache-dir kafka-python requests

# Copier le script producer
COPY producer_api.py .

CMD ["python", "producer_api.py"]
