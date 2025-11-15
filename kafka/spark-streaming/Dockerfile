# Dockerfile pour spark-consumer
FROM python:3.10-slim

# Installer Java + utilitaires requis pour Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME pour PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Créer le répertoire de l'application
WORKDIR /app

# Installer PySpark, Kafka Python et requests
RUN pip install --no-cache-dir pyspark==3.3.0 kafka-python requests

# Copier le script PySpark dans le conteneur
COPY stream_kafka_to_hdfs.py .

# Commande par défaut
CMD ["python", "stream_kafka_to_hdfs.py"]
