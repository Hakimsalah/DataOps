#!/bin/bash
set -e

echo "=== Sqoop import : products_metadata -> HDFS /data/sqoop/metadata ==="
sqoop import \
  --connect jdbc:mysql://mysql:3306/analytics \
  --username user \
  --password password \
  --table products_metadata \
  --target-dir /data/sqoop/metadata \
  -m 1 \
  --as-textfile

echo "=== Sqoop import terminé ==="

# Export (exemples - décommenter si nécessaire)
# echo "=== Sqoop export : /data/spark/results/csv -> analytics.price_trends ==="
# sqoop export \
#   --connect jdbc:mysql://mysql:3306/analytics \
#   --username user \
#   --password password \
#   --table price_trends \
#   --export-dir /data/spark/results/csv \
#   -m 1
# echo "=== Sqoop export terminé ==="
