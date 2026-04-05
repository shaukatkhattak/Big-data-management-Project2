import os

from pyspark.sql import SparkSession

# Iceberg + catalog S3 FileIO: checkpoints must use s3a (not file:/), see README.
# Run: bash /notebooks/spark_submit.sh /notebooks/1_bronze.py
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "nyc_taxi_trips")
CHECKPOINT = "s3a://warehouse/streaming-checkpoints/bronze_taxi_trips"

# Spark Session Configuration for Iceberg and Kafka
spark = SparkSession.builder \
    .appName("BronzeLayerIngestion") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.rest.type", "rest") \
    .config("spark.sql.catalog.rest.uri", "http://rest:8181") \
    .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.rest.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.rest.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.rest.s3.access-key-id", "admin") \
    .config("spark.sql.catalog.rest.s3.secret-access-key", "password") \
    .config("spark.sql.catalog.rest.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Create table if not exists (Bronze Layer)
spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.default")
spark.sql("""
    CREATE TABLE IF NOT EXISTS rest.default.bronze_taxi_trips (
        value STRING,
        timestamp TIMESTAMP,
        topic STRING
    ) USING iceberg
""")

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Select relevant columns and cast
bronze_df = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp", "topic")

# Write to Iceberg
query = bronze_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("path", "rest.default.bronze_taxi_trips") \
    .option("checkpointLocation", CHECKPOINT) \
    .start()

print("Bronze layer ingestion started...")
query.awaitTermination()
