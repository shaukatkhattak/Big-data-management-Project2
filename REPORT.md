# Project 2 Report

*(Course limit: ~2 pages.)*

## 1. Streaming correctness

**Triggers:** `processingTime="10 seconds"` on bronze, silver, and gold.

**Checkpoints (unchanged on restart):** all jobs use fixed MinIO paths so replays use the same state:

| Layer  | Checkpoint path |
|--------|-----------------|
| Bronze | `s3a://warehouse/streaming-checkpoints/bronze_taxi_trips` |
| Silver | `s3a://warehouse/streaming-checkpoints/silver_taxi_trips` |
| Gold   | `s3a://warehouse/streaming-checkpoints/gold_invalid_trips` |

Local `file:/` checkpoints are not used: the catalog reads checkpoint metadata through the same S3-compatible `FileIO` as the warehouse.

Deduplication on restart is enforced by Spark Structured Streaming commit logs plus Kafka/Iceberg sources: after the producer has finished and offsets/table data are fully processed, restarting **with the same checkpoint directories** must not append duplicate bronze/silver rows or change gold aggregates. Procedure and measured counts are in **§2**.

---

## 2. Pipeline validation (full stack restart)

**Goal:** prove no duplicate ingestion when bronze, silver, and gold are stopped and restarted with the same checkpoints.

**Steps**

1. Run the stack (`docker compose up -d`), load data, run `python produce.py`, then start all three streaming jobs (three terminals or background):

   ```bash
   docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/1_bronze.py
   docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/2_silver.py
   docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/3_gold.py
   ```

2. Wait until Kafka is drained and micro-batches settle (or producer has exited).

3. **Baseline —** capture stats:

   ```bash
   docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/print_stats.py
   ```

   Note bronze/silver `COUNT(*)` and the gold `trip_count` rows from the printed `DataFrame`.

4. **Stop all three jobs —** send **Ctrl+C** in each `docker exec` session running `spark_submit` (or stop the submitting processes equivalent to interrupting those streams). Do **not** delete MinIO buckets or checkpoint prefixes.

5. **Restart —** run the same three `docker exec ... spark_submit.sh` commands again. Checkpoint paths in the notebooks are fixed; do not override `checkpointLocation`.

6. **After catch-up —** run `print_stats.py` again.

**Pass criteria:** bronze and silver row counts are unchanged (Δ = 0). Gold is a **complete** aggregate over silver: the two `trip_count` values (valid vs invalid) must match the baseline; their sum should still equal silver row count.

**Example run (after pipeline + stats script):**

| Metric | Before stop | After restart | Δ (expect 0) |
|--------|------------:|--------------:|-------------:|
| `rest.default.bronze_taxi_trips` rows | 2,075,155 | 2,075,155 | 0 |
| `rest.default.silver_taxi_trips` rows | 2,056,680 | 2,056,680 | 0 |
| Gold `trip_count` where `is_invalid_trip = false` | 1,986,187 | 1,986,187 | 0 |
| Gold `trip_count` where `is_invalid_trip = true` | 70,493 | 70,493 | 0 |

Ad-hoc checks:

```sql
SELECT COUNT(*) FROM rest.default.bronze_taxi_trips;
SELECT COUNT(*) FROM rest.default.silver_taxi_trips;
SELECT is_invalid_trip, trip_count FROM rest.default.gold_invalid_trips ORDER BY is_invalid_trip;
```

---

## 3. Lakehouse design (layers, cleaning, enrichment, partitioning)

**Bronze** — Append raw Kafka payload: `value` (JSON string), `timestamp`, `topic`. No business rules.

**Silver** — Stream from bronze Iceberg table; parse JSON to typed columns.

- **Cleaning:** drop rows with null `VendorID`. Keep only TLC zone ids `PULocationID` and `DOLocationID` in **1–263**. Fill nulls with **0** for `passenger_count`, `fare_amount`, `tip_amount`, `trip_distance`, and `total_amount` before rules that depend on them.
- **Enrichment:** broadcast join to `taxi_zone_lookup.parquet` on `LocationID` to add **`pickup_zone`** and **`dropoff_zone`** names. Derive **`is_invalid_trip`** as `trip_distance <= 0 OR total_amount <= 0`.
- **Projection:** write only the Iceberg schema columns (parsed datetimes, zones, flag, etc.).

**Gold** — Stream from silver; `groupBy(is_invalid_trip).count` → **`trip_count`**. **`outputMode("complete")`** refreshes the aggregate table each trigger.

- **Partitioning:** table is **`PARTITIONED BY (is_invalid_trip)`** so valid vs invalid slices stay in separate partitions for small, targeted scans.

---

## 4. Gold table output (from `print_stats.py` / logs)

`rest.default.gold_invalid_trips`: columns `is_invalid_trip`, `trip_count`.

| is_invalid_trip | trip_count |
|-----------------|-----------:|
| false           | 1,986,187  |
| true            | 70,493     |

**Invalid trip % (from gold):** **3.43%** — recompute after your run with:

```bash
docker exec spark-iceberg bash /notebooks/spark_submit.sh /notebooks/print_stats.py
```

or:

```sql
SELECT ROUND(100.0 * SUM(CASE WHEN is_invalid_trip THEN trip_count ELSE 0 END) / SUM(trip_count), 2)
FROM rest.default.gold_invalid_trips;
```

---

## 5. Iceberg snapshot history (bronze metadata)

Recent rows from **`rest.default.bronze_taxi_trips.snapshots`** (same query as in logs / `print_stats.py`):

| committed_at            | snapshot_id         | operation |
|-------------------------|--------------------:|-----------|
| 2026-04-05 12:34:13.525 | 2986954144333945112 | append    |
| 2026-04-05 12:34:01.863 | 7781741461187833218 | append    |
| 2026-04-05 12:33:56.570 | 8887995332831200537 | append    |
| 2026-04-05 12:33:51.202 | 8868216062709925258 | append    |
| 2026-04-05 12:33:40.048 | 1650896116863470714 | append    |
| 2026-04-05 12:32:45.273 | 6144536002548663085 | append    |
| 2026-04-05 12:29:21.780 | 6169322497265971356 | append    |
| 2026-04-05 12:29:07.995 | 4677935109519200387 | append    |

Silver and gold snapshots support the same pattern, e.g. `rest.default.silver_taxi_trips.snapshots` / `rest.default.gold_invalid_trips.snapshots`.

```sql
SELECT committed_at, snapshot_id, operation
FROM rest.default.bronze_taxi_trips.snapshots
ORDER BY committed_at DESC
LIMIT 8;
-- Example: VERSION AS OF <snapshot_id> on bronze/silver for time travel.
```

This confirms streaming commits, time travel, and checkpoint-backed recovery.
