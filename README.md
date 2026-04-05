# Project 2: Streaming Lakehouse (NYC Taxi)

Kafka → Spark Structured Streaming → Iceberg (`rest.default.bronze_taxi_trips` → `silver_taxi_trips` → `gold_invalid_trips`).

Checkpoints: **`s3a://warehouse/streaming-checkpoints/...`** on MinIO (not `file:/checkpoints` — same S3 FileIO as the warehouse).

## Data

yellow trip Parquet files and `taxi_zone_lookup.parquet` (`LocationID`, `Zone`) under `data/`.

## Run

```bash
cd Project_2
docker compose up -d
python produce.py
```

Streaming jobs (use `spark_submit.sh` so the **Kafka** connector loads on Spark 3.5.x):

```bash
docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/1_bronze.py
docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/2_silver.py
docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/3_gold.py
```

Optional env on bronze: `KAFKA_TOPIC` (default `nyc_taxi_trips`), e.g.  
`docker exec -e KAFKA_TOPIC=nyc_taxi_trips spark-iceberg bash /notebooks/spark_submit.sh /notebooks/1_bronze.py`

Optional batch stats for the report:

```bash
docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/print_stats.py
```

## Design (medallion)

- **Bronze**: raw `value`, Kafka `timestamp`, `topic`.
- **Silver**: parse/cast, TLC zones 1–263, null handling, zone names, `is_invalid_trip` (`trip_distance <= 0` OR `total_amount <= 0`).
- **Gold**: `groupBy(is_invalid_trip).count` → `trip_count`; partitioned by `is_invalid_trip`.

**Invalid trip % (example from pipeline):** **27.27%** — confirm with `print_stats.py` or SQL on `rest.default.gold_invalid_trips` (see `REPORT.md`).

See **`REPORT.md`** for checkpoint triggers, restart check, snapshot query, and custom scenario.
