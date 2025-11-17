from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, lit, current_date, when
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("fusion_kafka_flume_sqoop") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

# -----------------------------
# Schéma Kafka
# -----------------------------
kafka_schema = StructType() \
    .add("product_id", IntegerType()) \
    .add("product_name", StringType()) \
    .add("price", DoubleType()) \
    .add("category", StringType()) \
    .add("brand", StringType()) \
    .add("rating", DoubleType()) \
    .add("stock", IntegerType()) \
    .add("source", StringType()) \
    .add("ts", StringType())

# -----------------------------
# Lire Kafka en streaming
# -----------------------------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "realtime-products") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
kafka_parsed = json_df.select(from_json(col("json_str"), kafka_schema).alias("data")).select("data.*")
kafka_parsed = kafka_parsed.withColumn("ts_ts", col("ts").cast(TimestampType()))

# -----------------------------
# Lire Flume depuis HDFS (batch)
# -----------------------------
flume_path = "hdfs://namenode:9000/flume/events/FlumeData.*"
flume_df = spark.read.option("header","true").csv(flume_path)

if "InvoiceNo" in flume_df.columns:
    flume_df = flume_df.withColumnRenamed("InvoiceNo","product_id") \
                       .withColumnRenamed("status_code","price") \
                       .withColumn("price", col("price").cast(DoubleType())) \
                       .withColumn("category", lit("Flume")) \
                       .withColumn("product_name", lit(None).cast(StringType())) \
                       .withColumn("brand", lit(None).cast(StringType())) \
                       .withColumn("rating", lit(None).cast(DoubleType())) \
                       .withColumn("stock", lit(None).cast(IntegerType())) \
                       .withColumn("source", lit("flume")) \
                       .withColumn("ts_ts", lit(None).cast(TimestampType()))

# -----------------------------
# Lire Sqoop depuis HDFS (batch)
# -----------------------------
sqoop_path = "hdfs://namenode:9000/user/hadoop/ecommerce/*"
sqoop_df = spark.read.option("header","true").csv(sqoop_path)

if "Description" in sqoop_df.columns:
    sqoop_df = sqoop_df.withColumnRenamed("Description","product_name") \
                       .withColumnRenamed("UnitPrice","price") \
                       .withColumn("price", col("price").cast(DoubleType())) \
                       .withColumn("category", lit("Sqoop")) \
                       .withColumn("product_id", lit(None).cast(IntegerType())) \
                       .withColumn("brand", lit(None).cast(StringType())) \
                       .withColumn("rating", lit(None).cast(DoubleType())) \
                       .withColumn("stock", lit(None).cast(IntegerType())) \
                       .withColumn("source", lit("sqoop")) \
                       .withColumn("ts_ts", lit(None).cast(TimestampType()))

# -----------------------------
# Fonction pour écrire chaque DataFrame dans PostgreSQL
# -----------------------------
def write_to_postgres(df, table_name):
    jdbc_url = "jdbc:postgresql://postgres:5432/analytics"
    properties = {"user":"user","password":"123456","driver":"org.postgresql.Driver"}
    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)

# -----------------------------
# Fusionner Kafka (stream) + Flume (batch) + Sqoop (batch)
# -----------------------------
def process_batch(batch_df, batch_id):
    # Ajouter les catégories manquantes
    batch_df = batch_df.withColumn("category", when(col("category").isNull(), lit("Unknown")).otherwise(col("category")))

    # Fusionner avec Flume et Sqoop
    combined = batch_df.unionByName(flume_df, allowMissingColumns=True).unionByName(sqoop_df, allowMissingColumns=True)

    # Agrégation moyenne par catégorie
    agg = combined.groupBy("category").agg(avg("price").alias("avg_price"))
    agg = agg.withColumn("day", current_date())

    # Écrire les données agrégées dans la table principale
    write_to_postgres(agg, "price_trends")

    # Écrire chaque source séparément
    write_to_postgres(batch_df, "price_kafka")
    write_to_postgres(flume_df, "price_flume")
    write_to_postgres(sqoop_df, "price_sqoop")

# -----------------------------
# Lancer le streaming
# -----------------------------
query = kafka_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()
