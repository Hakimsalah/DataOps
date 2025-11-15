from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()

schema = StructType() \
    .add("product_id", IntegerType()) \
    .add("product_name", StringType()) \
    .add("price", DoubleType()) \
    .add("category", StringType()) \
    .add("brand", StringType()) \
    .add("rating", DoubleType()) \
    .add("stock", IntegerType()) \
    .add("source", StringType()) \
    .add("ts", StringType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "realtime-products") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str")
parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
parsed = parsed.withColumn("ts_ts", to_timestamp(col("ts")))

out_path = "hdfs://namenode:9000/data/kafka/realtime_parquet"
checkpoint_path = "hdfs://namenode:9000/checkpoints/realtime"

query = parsed.writeStream.format("parquet") \
    .option("path", out_path) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

query.awaitTermination()
