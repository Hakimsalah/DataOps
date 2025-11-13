# Real-Time E-commerce Price Analytics (DataOps)

## But
Collecter des données en parallèle (API DummyJSON, CSV Kaggle, MySQL metadata) → HDFS → Spark (fusion) → MySQL → Grafana.

## Pré-requis
- Docker & docker-compose

## Installation
1. Placer `ecommerce_kaggle_sample.csv` dans ./datasets/
2. Démarrer :
   ./setup.sh
   # ou manuellement :
   docker-compose up -d --build

## Tests rapides
- Vérifier Kafka topic :
  docker exec -it $(docker ps -qf "name=kafka") kafka-topics.sh --list --bootstrap-server kafka:9092
- Déposer CSV dans flume/spooldir/ pour ingestion Flume
- Lancer Spark streaming (voir README plus bas) :
  docker cp spark-jobs/stream_to_hdfs.py $(docker ps -qf "name=spark-master"):/tmp/stream_to_hdfs.py
  docker exec -it $(docker ps -qf "name=spark-master") /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/stream_to_hdfs.py

## Flux
- API DummyJSON → Kafka topic `realtime-products` (produced every 60s)
- Flume spooldir → HDFS `/data/flume/`
- Sqoop import MySQL metadata → HDFS `/data/sqoop/metadata`
- Spark streaming writes Kafka → Parquet HDFS `/data/kafka/realtime_parquet`
- Spark batch lit HDFS (Kafka+Flume+Sqoop), calcule avg price per category → MySQL `price_trends`
- Grafana lit MySQL pour dashboards

