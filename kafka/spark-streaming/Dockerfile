FROM bde2020/spark-base:3.3.0-hadoop3.3

WORKDIR /app

RUN pip install --no-cache-dir kafka-python requests pyspark

COPY stream_kafka_to_hdfs.py .

CMD ["python", "stream_kafka_to_hdfs.py"]
