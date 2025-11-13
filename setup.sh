#!/usr/bin/env bash
set -e

echo "1) Build & start Docker compose"
docker-compose up -d --build

echo "Attente des services (90s)..."
sleep 90

echo "2) Création topic Kafka (realtime-products)"
KAFKA_CONTAINER=$(docker ps -qf "ancestor=bitnami/kafka")
if [ -z "$KAFKA_CONTAINER" ]; then
  KAFKA_CONTAINER=$(docker ps -qf "name=kafka")
fi
docker exec -it $KAFKA_CONTAINER kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic realtime-products || true

echo "3) (Optionnel) Import metadata via Sqoop"
docker exec -it $(docker ps -qf "name=sqoop") bash /scripts/import_export.sh || true

echo
echo "Stack démarrée. Vérifier :"
echo " - Kafka: localhost:9092"
echo " - HDFS NN UI: http://localhost:9870"
echo " - Spark UI: http://localhost:8080"
echo " - Grafana: http://localhost:3000 (admin/admin)"
echo " - MySQL: localhost:3306 (user/password)"
