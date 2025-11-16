FROM bde2020/spark-worker:3.3.0-hadoop3.3

WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir kafka-python requests pyspark

# Create jars directory if it doesn't exist and copy Kafka connector
RUN mkdir -p /opt/spark/jars
COPY jars/* /opt/spark/jars/

# Copy your application code
COPY stream_kafka_to_hdfs.py .

# Use the correct command format
CMD ["spark-submit", "--master", "spark://spark-master:7077", "/app/stream_kafka_to_hdfs.py"]
