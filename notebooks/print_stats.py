"""Optional: row counts / invalid % / snapshot peek. Run: bash /notebooks/spark_submit.sh /notebooks/print_stats.py"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PrintPipelineStats") \
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

def count_rows(name: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS c FROM {name}").collect()[0]["c"]


print("=== Row counts ===")
for table in (
    "rest.default.bronze_taxi_trips",
    "rest.default.silver_taxi_trips",
    "rest.default.gold_invalid_trips",
):
    try:
        n = count_rows(table)
        print(f"{table}: {n}")
    except Exception as e:
        print(f"{table}: (unavailable) {e}")

print("\n=== Invalid trip % (from gold table) ===")
try:
    spark.sql("""
        SELECT is_invalid_trip, trip_count
        FROM rest.default.gold_invalid_trips
        ORDER BY is_invalid_trip
    """).show(truncate=False)
    pct = spark.sql("""
        SELECT
            ROUND(
                SUM(CASE WHEN is_invalid_trip THEN trip_count ELSE 0 END) * 100.0
                / NULLIF(SUM(trip_count), 0),
                4
            ) AS invalid_trip_pct
        FROM rest.default.gold_invalid_trips
    """).collect()[0]["invalid_trip_pct"]
    print(f"Invalid trip % (copy into README / REPORT): {pct}")
except Exception as e:
    print(f"(gold query failed) {e}")

print("\n=== Recent bronze snapshots (for REPORT / screenshot) ===")
try:
    spark.sql("""
        SELECT committed_at, snapshot_id, operation
        FROM rest.default.bronze_taxi_trips.snapshots
        ORDER BY committed_at DESC
        LIMIT 8
    """).show(truncate=False)
except Exception as e:
    print(f"(snapshots query failed) {e}")

spark.stop()
