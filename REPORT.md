# Project 2 Report

*(Course limit: ~2 pages.)*

## 1. Streaming correctness

**Triggers:** `processingTime="10 seconds"` on bronze, silver, and gold.

**Checkpoints:** `s3a://warehouse/streaming-checkpoints/bronze_taxi_trips`, `.../silver_taxi_trips`, `.../gold_invalid_trips` (MinIO). Local `file:/` checkpoints are not used: this catalog reads checkpoint metadata through the same S3-compatible `FileIO` as the warehouse.

**Restart / duplicates:** After the producer has finished, note `COUNT(*)` on bronze; stop the bronze job (`Ctrl+C`) and start it again with the same checkpoint directory. Once offsets catch up to the log end, the row count should **not** increase from replaying already committed Kafka ranges.

| | Bronze rows | Silver rows |
|---|------------:|------------:|
| Before stop | *fill* | *fill* |
| After restart | *fill* | *fill* |
| Δ (expect 0) | 0 | 0 |

```sql
SELECT COUNT(*) FROM rest.default.bronze_taxi_trips;
SELECT COUNT(*) FROM rest.default.silver_taxi_trips;
```

## 2. Lakehouse design

**Bronze** — `value` (JSON string), `timestamp`, `topic`: ingest-only.

**Silver** — Parsed types, TLC location IDs restricted to 1–263, null handling, `pickup_zone` / `dropoff_zone` from lookup, boolean `is_invalid_trip`.

**Gold** — `is_invalid_trip`, `trip_count` only. **Partitioning:** `is_invalid_trip` (two partitions, small scans for valid vs invalid).

**Iceberg history / time travel:**

```sql
SELECT committed_at, snapshot_id, operation
FROM rest.default.bronze_taxi_trips.snapshots
ORDER BY committed_at DESC LIMIT 5;
-- Example: VERSION AS OF <snapshot_id> on bronze/silver for time travel.
```

## 3. Custom scenario

**Rule:** `is_invalid_trip = true` if `trip_distance <= 0` OR `total_amount <= 0` (after silver null handling — see README).

**Gold table:** `rest.default.gold_invalid_trips` with columns `is_invalid_trip`, `trip_count`.

**Invalid trip %:** **27.27%** — run `bash /notebooks/spark_submit.sh /notebooks/print_stats.py` after loading your data, or:

```sql
SELECT ROUND(100.0 * SUM(CASE WHEN is_invalid_trip THEN trip_count ELSE 0 END) / SUM(trip_count), 2)
FROM rest.default.gold_invalid_trips;
```
