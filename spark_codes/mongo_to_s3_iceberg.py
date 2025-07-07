from pyspark.sql import SparkSession

# Create Spark session with MongoDB + Iceberg configs
spark = SparkSession.builder \
    .appName("MongoToS3Iceberg") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://my-bucket/iceberg-warehouse/") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from MongoDB
mongo_df = spark.read.format("mongodb") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "ecommerce") \
    .option("collection", "orders") \
    .load()

mongo_df.show(5)
#Transform data (cleaning, schema enforcement)
# Example: convert nested docs, timestamps, etc. as needed
# mongo_df = mongo_df.withColumn("order_date", col("order_date").cast("timestamp"))

# Write to Iceberg table in S3
# This will create the table if it doesn't exist, or overwrite it
print("Writing to S3 Iceberg...")
mongo_df.writeTo("my_catalog.ecommerce.orders_iceberg") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print("Done writing to Iceberg!")

# Verify by reading the Iceberg table back
result = spark.sql("SELECT * FROM my_catalog.ecommerce.orders_iceberg LIMIT 5")
print("Sample data from S3 Iceberg table:")
result.show()

spark.stop()
