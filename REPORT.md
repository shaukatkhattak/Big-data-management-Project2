# Project 2 Report

*(Course limit: ~2 pages.)*

## 1. Streaming correctness

**Triggers:** `processingTime="10 seconds"` on bronze, silver, and gold.

**Checkpoints:** `s3a://warehouse/streaming-checkpoints/bronze_taxi_trips`, `.../silver_taxi_trips`, `.../gold_invalid_trips` (MinIO). Local `file:/` checkpoints are not used: this catalog reads checkpoint metadata through the same S3-compatible `FileIO` as the warehouse.

**Restart / duplicates:** After the producer has finished, note `COUNT(*)` on bronze; stop the bronze job (`Ctrl+C`) and start it again with the same checkpoint directory. Once offsets catch up to the log end, the row count should **not** increase from replaying already committed Kafka ranges.


| | Bronze rows | Silver rows |
|---|------------:|------------:|
| Before stop | 2,075,155 | 2,056,680 |
| After restart | 2,075,155 | 2,056,680 |
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


**Gold table output:**

| is_invalid_trip | trip_count |
|-----------------|------------|
| false           | 1,986,187  |
| true            | 70,493     |

**Invalid trip %:** **3.43%** — run `bash /notebooks/spark_submit.sh /notebooks/print_stats.py` after loading your data, or:
## 4. Iceberg snapshot history (recent)

| committed_at           | snapshot_id        | operation |
|-----------------------|-------------------|-----------|
|2026-04-05 12:34:13.525|2986954144333945112| append    |
|2026-04-05 12:34:01.863|7781741461187833218| append    |
|2026-04-05 12:33:56.570|8887995332831200537| append    |
|2026-04-05 12:33:51.202|8868216062709925258| append    |
|2026-04-05 12:33:40.048|1650896116863470714| append    |
|2026-04-05 12:32:45.273|6144536002548663085| append    |
|2026-04-05 12:29:21.780|6169322497265971356| append    |
|2026-04-05 12:29:07.995|4677935109519200387| append    |

This confirms streaming, time travel, and correct checkpointing.

```sql
SELECT ROUND(100.0 * SUM(CASE WHEN is_invalid_trip THEN trip_count ELSE 0 END) / SUM(trip_count), 2)
FROM rest.default.gold_invalid_trips;
```
