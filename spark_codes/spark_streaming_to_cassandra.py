from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# 1Create Spark session with Cassandra connector
spark = SparkSession.builder \
    .appName("ClickstreamKafkaToCassandra") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka config
kafka_bootstrap_servers = "localhost:9092"
topic = "product_views"  # Or "add_to_cart"

# Define schema for JSON payload
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("session_id", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("timestamp", StringType()) \
    .add("device_type", StringType()) \
    .add("geo_location", StringType())

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# Kafka returns value as binary → decode & parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write to Cassandra sink
query = df_parsed.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "clickstream_ks") \
    .option("table", "product_views") \
    .option("checkpointLocation", "/tmp/checkpoints/product_views") \
    .outputMode("append") \
    .start()

query.awaitTermination()
