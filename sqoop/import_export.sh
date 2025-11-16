#!/bin/bash

echo "Waiting for PostgreSQL to be ready..."
until timeout 1 bash -c 'cat < /dev/null > /dev/tcp/postgres/5432' 2>/dev/null
do
  echo "PostgreSQL is not ready yet... waiting..."
  sleep 5
done

echo "PostgreSQL is ready!"
sleep 10

echo "Waiting for Hadoop NameNode..."
until timeout 1 bash -c 'cat < /dev/null > /dev/tcp/namenode/9000' 2>/dev/null
do
  echo "NameNode not ready... waiting..."
  sleep 5
done

echo "HDFS is ready!"

echo "Downloading PostgreSQL JDBC driver..."
cd /tmp
curl -O https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Set classpath for Sqoop/Hadoop
export SQOOP_USER_CLASSPATH="/tmp/postgresql-42.6.0.jar"
export HADOOP_CLASSPATH="/tmp/postgresql-42.6.0.jar:$HADOOP_CLASSPATH"

echo "Starting Sqoop import..."
sqoop import \
  --connect jdbc:postgresql://postgres:5432/analytics \
  --username user \
  --password 123456 \
  --table ecommerce \
  --target-dir /user/hadoop/ecommerce \
  --delete-target-dir \
  --driver org.postgresql.Driver \
  --m 1

if [ $? -eq 0 ]; then
  echo "✅ Sqoop import completed successfully!"
  hdfs dfs -ls /user/hadoop/ecommerce
else
  echo "❌ Sqoop import failed!"
fi

# Keep container alive
tail -f /dev/null
