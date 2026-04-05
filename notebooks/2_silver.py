from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Run: bash /notebooks/spark_submit.sh /notebooks/2_silver.py
CHECKPOINT = "s3a://warehouse/streaming-checkpoints/silver_taxi_trips"
ZONE_PATH = "/data/taxi_zone_lookup.parquet"

spark = SparkSession.builder \
    .appName("SilverLayerEnrichment") \
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

taxi_schema = StructType([
    StructField("VendorID", LongType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", LongType()),
    StructField("DOLocationID", LongType()),
    StructField("payment_type", LongType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType())
])

spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.default")
spark.sql("""
    CREATE TABLE IF NOT EXISTS rest.default.silver_taxi_trips (
        VendorID LONG,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        PULocationID LONG,
        DOLocationID LONG,
        fare_amount DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE,
        pickup_zone STRING,
        dropoff_zone STRING,
        is_invalid_trip BOOLEAN
    ) USING iceberg
""")

zones = spark.read.parquet(ZONE_PATH)
pu_zones = zones.select(
    col("LocationID").alias("pu_loc_id"),
    col("Zone").alias("pickup_zone")
)
do_zones = zones.select(
    col("LocationID").alias("do_loc_id"),
    col("Zone").alias("dropoff_zone")
)

bronze_df = spark.readStream \
    .format("iceberg") \
    .option("path", "rest.default.bronze_taxi_trips") \
    .load()

parsed_df = bronze_df.select(from_json(col("value"), taxi_schema).alias("data")) \
    .select("data.*") \
    .withColumn("pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
    .withColumn("dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
    .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")

# Cleaning (see README): unusable rows; TLC zone IDs 1–263; numerics for invalid flag
cleaned_df = parsed_df.filter(col("VendorID").isNotNull()) \
    .filter((col("PULocationID") >= 1) & (col("PULocationID") <= 263)) \
    .filter((col("DOLocationID") >= 1) & (col("DOLocationID") <= 263)) \
    .fillna(0, subset=["passenger_count", "fare_amount", "tip_amount"]) \
    .fillna(0, subset=["trip_distance", "total_amount"])

enriched_df = cleaned_df \
    .join(broadcast(pu_zones), col("PULocationID") == col("pu_loc_id"), "left") \
    .drop("pu_loc_id") \
    .join(broadcast(do_zones), col("DOLocationID") == col("do_loc_id"), "left") \
    .drop("do_loc_id")

silver_df = enriched_df.withColumn(
    "is_invalid_trip",
    (col("trip_distance") <= 0) | (col("total_amount") <= 0)
)

# Project to the Iceberg DDL only (extra JSON fields like RatecodeID otherwise break schema checks).
silver_out = silver_df.select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "pickup_zone",
    "dropoff_zone",
    "is_invalid_trip",
)

query = silver_out.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("path", "rest.default.silver_taxi_trips") \
    .option("checkpointLocation", CHECKPOINT) \
    .start()

print("Silver layer started (bronze -> parsed, zones, is_invalid_trip)...")
query.awaitTermination()
