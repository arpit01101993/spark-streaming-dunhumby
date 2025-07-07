from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# SparkSession
spark = SparkSession.builder \
    .appName("KafkaToClickHouse") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2-patch10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Kafka source (if you want streaming)
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "product_views") \
    .option("startingOffsets", "latest") \
    .load()

# Define schema for parsing JSON
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("session_id", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("timestamp", StringType()) \
    .add("device_type", StringType()) \
    .add("geo_location", StringType())

# Parse Kafka value JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# ClickHouse JDBC configs
clickhouse_url = "jdbc:clickhouse://localhost:8123/default"
clickhouse_table = "clickstream_events"
properties = {
    "user": "default",
    "password": "",   # add if needed
    "driver": "ru.yandex.clickhouse.ClickHouseDriver"
}

# Streaming sink: write micro-batches to ClickHouse via foreachBatch
def write_to_clickhouse(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", clickhouse_table) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .save()

query = df_parsed.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/clickhouse-checkpoint") \
    .start()

query.awaitTermination()
