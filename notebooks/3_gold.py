from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Run: bash /notebooks/spark_submit.sh /notebooks/3_gold.py
CHECKPOINT = "s3a://warehouse/streaming-checkpoints/gold_invalid_trips"

spark = SparkSession.builder \
    .appName("GoldLayerAggregations") \
    .config("spark.sql.shuffle.partitions", "4") \
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
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3FileSystem") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.default")
spark.sql("""
    CREATE TABLE IF NOT EXISTS rest.default.gold_invalid_trips (
        is_invalid_trip BOOLEAN,
        trip_count LONG
    ) USING iceberg
    PARTITIONED BY (is_invalid_trip)
""")

silver_df = spark.readStream \
    .format("iceberg") \
    .option("path", "rest.default.silver_taxi_trips") \
    .load()

gold_df = silver_df.groupBy("is_invalid_trip").agg(count("*").alias("trip_count"))

query = gold_df.writeStream \
    .format("iceberg") \
    .outputMode("complete") \
    .trigger(processingTime="10 seconds") \
    .option("path", "rest.default.gold_invalid_trips") \
    .option("checkpointLocation", CHECKPOINT) \
    .start()

print("Gold layer started (invalid vs valid trip counts)...")
query.awaitTermination()
