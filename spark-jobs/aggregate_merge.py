from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit, when
from pyspark.sql.functions import to_date, current_date
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("aggregate-merge") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

kafka_path = "hdfs://namenode:9000/data/kafka/realtime_parquet"
flume_path = "hdfs://namenode:9000/data/flume/*"
sqoop_meta_path = "hdfs://namenode:9000/data/sqoop/metadata"

# Lire kafka parquet (si existe)
try:
    df_kafka = spark.read.parquet(kafka_path)
except Exception:
    df_kafka = spark.createDataFrame([], schema="product_name STRING,price DOUBLE,category STRING,brand STRING")

# Lire flume CSV (Kaggle)
df_flume = spark.read.option("header","true").csv(flume_path)
if "Description" in df_flume.columns:
    df_flume = df_flume.withColumnRenamed("Description","product_name").withColumnRenamed("UnitPrice","price")
    df_flume = df_flume.withColumn("price", col("price").cast("double"))
    df_flume = df_flume.withColumn("source", lit("kaggle"))
else:
    # Si structure différente, adapter ici
    pass

# Lire metadata sqoop
df_meta = spark.read.option("header","true").csv(sqoop_meta_path)

# Standardiser colonnes
df_k = df_kafka.select("product_name","price","category")
df_f = df_flume.select("product_name","price","InvoiceDate") \
       .withColumnRenamed("InvoiceDate","date") \
       .withColumn("category", lit(None).cast(StringType()))

# Union
combined = df_k.unionByName(df_f.select(df_k.columns), allowMissingColumns=True)

# Enrichir via metadata
if "product_name" in df_meta.columns:
    meta_sel = df_meta.select("product_name","brand","category").withColumnRenamed("category","meta_category")
    combined = combined.join(meta_sel, on="product_name", how="left")
    combined = combined.withColumn("final_category", when(col("category").isNotNull(), col("category")).otherwise(col("meta_category"))) \
                       .drop("category").drop("meta_category")
else:
    combined = combined.withColumn("final_category", lit(None).cast(StringType()))

# Agrégation moyenne par catégorie
agg = combined.groupBy("final_category").agg(avg("price").alias("avg_price")).withColumnRenamed("final_category","category")

# Ajouter date
agg = agg.withColumn("day", current_date())

# Écrire vers MySQL
jdbc_url = "jdbc:mysql://mysql:3306/analytics"
properties = {"user":"user","password":"password","driver":"com.mysql.cj.jdbc.Driver"}

agg_to_write = agg.select("category","avg_price","day")
agg_to_write.write.jdbc(url=jdbc_url, table="price_trends", mode="append", properties=properties)

print("Aggregation written to MySQL table analytics.price_trends")
