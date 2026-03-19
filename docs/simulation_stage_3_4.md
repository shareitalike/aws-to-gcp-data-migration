# STAGE 3 — Dataproc / Spark Processing

## 🔍 PROBE

**[MENTOR]** Your validated files are sitting in GCS. You need a Spark job on Dataproc that joins `orders` with `events`, enriches with user segments, and writes partitioned Parquet to a processed zone. Tell me about cluster sizing, partitioning strategy, and how you handle skew.

**[CANDIDATE]** I'd use an ephemeral Dataproc cluster — spin up per job, tear down after. Start with:

- 1 master (n1-standard-4), 4 workers (n1-standard-8, 200GB SSD each)
- Preemptible workers for 60% of capacity to save cost
- Auto-scaling: 4–16 workers based on YARN pending containers

For partitioning, I'd write output partitioned by `dt` and `source`. For the join, `orders` is large (~2 TB/day) and `events` is huge (~8 TB/day), so I'd repartition both on `user_id` before joining.

*Gap: No mention of AQE, broadcast join for small tables, or salting for data skew on hot user IDs.*

## 🔄 REFINE

**[MENTOR]** "Repartition both on `user_id`" — what if 5% of your users generate 40% of events? You've got a classic data skew problem. Your executor hosting the hot partition will OOM while the rest sit idle. How do you fix it?

**[CANDIDATE]** Two approaches:

1. **Enable AQE** (`spark.sql.adaptive.enabled=true`) — it detects skewed partitions at runtime and splits them.
2. **Salt key for worst offenders** — if we *know* hot user IDs (e.g., bot accounts), prefix the join key with a random salt (0–9), replicate the smaller table 10×, join on salted key, then drop the salt. This distributes hot keys across 10 partitions.
3. **Broadcast join** for small dimension tables (user_segments is ~50MB) — `broadcast(user_segments)` avoids shuffle entirely.

**[MENTOR]** Good. AQE should be your first line. Salting is a last resort because it adds complexity. Also — you said "200GB SSD each" but didn't size your shuffle partition count. Default is 200 — that's a disaster at 10TB. Set `spark.sql.shuffle.partitions` to at least 2000.

## 📝 ASSIGN — PySpark Job

**[MENTOR]** Write the PySpark job. Include the join, broadcast, partitioned write, and proper configs.

**[CANDIDATE]**

```python
# jobs/process_daily_orders.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, broadcast, date_format, coalesce, lit,
    sha2, concat_ws
)

def create_spark_session():
    return (SparkSession.builder
        .appName("daily_orders_processing")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2000")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.speculation", "true")
        .config("spark.speculation.multiplier", "1.5")
        .getOrCreate())

def main(run_date: str):
    spark = create_spark_session()

    # Read validated orders
    orders_path = f"gs://company-data-lake-validated/source=orders/dt={run_date}/"
    orders = (spark.read.parquet(orders_path)
        .filter(col("amount") > 0)
        .dropDuplicates(["order_id"])
    )

    # Read validated events
    events_path = f"gs://company-data-lake-validated/source=events/dt={run_date}/"
    events = (spark.read.parquet(events_path)
        .filter(col("event_type").isin(
            "purchase", "cart_add", "page_view"
        ))
        .dropDuplicates(["event_id"])
    )

    # Small dimension table — broadcast join
    segments_path = "gs://company-data-lake-ref/user_segments/latest/"
    user_segments = spark.read.parquet(segments_path)

    # Main enrichment join
    enriched = (orders
        .join(events, on="user_id", how="left")  # BUG: should be inner or
                                                   # needs null handling
        .join(broadcast(user_segments), on="user_id", how="left")
        .withColumn("process_date", lit(run_date))
        .withColumn("row_hash", sha2(
            concat_ws("||",
                col("order_id"),
                col("amount"),
                col("event_type")
            ), 256
        ))
    )

    # Partitioned write — overwrite by partition for idempotency
    output_path = "gs://company-data-lake-processed/enriched_orders/"
    (enriched.write
        .mode("overwrite")
        .partitionBy("process_date", "event_type")
        .parquet(output_path)
    )

    # Quality check
    input_count = orders.count()
    output_count = enriched.count()
    if output_count < input_count * 0.9:
        raise ValueError(
            f"Data quality check failed: "
            f"input={input_count}, output={output_count}"
        )

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1])
```

## 💣 FAILURE INJECTION — Spark OOM (Data Skew)

**[MENTOR]** Job ran for 45 minutes then failed. Here's the executor log:

```
2026-03-19T11:30:22Z WARN  TaskMemoryManager: Failed to allocate page
  (16777216 bytes), try again will trigger spilling
2026-03-19T11:30:25Z ERROR Executor: Exception in task 1247.0 in stage 4.0
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.unsafe.memory.HeapMemoryAllocator.allocate
  ...
Stage 4 shuffle read: Task 1247 read 12.4 GB while avg task read 45 MB
Partition key: user_id = "bot_crawler_001"
```

**[CANDIDATE]** Root cause: `user_id = "bot_crawler_001"` is a bot generating millions of events. One partition holds 12.4 GB while the average is 45 MB — classic skew.

**Immediate fix:** Filter out known bot IDs before the join:

```python
BOT_IDS = ["bot_crawler_001", "bot_crawler_002", "load_test_user"]

orders = orders.filter(~col("user_id").isin(BOT_IDS))
events = events.filter(~col("user_id").isin(BOT_IDS))
```

**Longer-term fix:** AQE's skew join should handle this, but the skew ratio (275:1) exceeds the default `skewedPartitionThresholdInBytes` (256MB). Tune it:

```python
.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128m")
.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

**Detection:** Monitor shuffle read distribution in Spark UI. Alert if max task shuffle read > 50× median via custom Spark listener pushing metrics to Cloud Monitoring.

**Prevention:** Upstream data quality rule — if any `user_id` appears in >1% of daily events, flag for review and auto-exclude in a config table.

**[MENTOR]** Good. I'll also note the bug in your code — the `orders.join(events, on="user_id", how="left")`. A left join on user_id will explode rows since one user has many events AND many orders. You probably want `orders.join(events, on=["user_id", "order_id"], how="inner")`, or aggregate events per user first before joining.

**[CANDIDATE]** You're right — I need to either:
1. Aggregate events: `events.groupBy("user_id").agg(count("event_id").alias("event_count"), collect_list("event_type").alias("event_types"))` and then join
2. Or join on a narrower key that creates a 1:1 relationship

I'll aggregate first — it prevents row explosion and gives us clean metrics per order.

---

## ── DEBRIEF ─────────────────────────────

**[MENTOR]**
- **Score: Passable**
- **Gaps:** Left join row explosion was a critical bug — would have silently multiplied data volume. Also, `mode("overwrite")` without `partitionBy` isolation can delete other dates' data. Should use `replaceWhere` or dynamic partition overwrite mode (`spark.sql.sources.partitionOverwriteMode=dynamic`).

**Tradeoff Questions:**

**1. Why ephemeral clusters over long-running?**

**[CANDIDATE]** Cost. A 16-node Dataproc cluster costs ~$15/hour. Our jobs run 2 hours/day. Ephemeral = $30/day. Long-running = $360/day. We save 92%. The 3-minute startup time is acceptable for batch.

**2. What breaks at 10× scale?**

**[CANDIDATE]** At 120 TB/day, single-job processing is too slow. I'd split by source system (parallel DAG tasks), use Dataproc Serverless for auto-scaling without cluster management, and move to a streaming micro-batch model if latency requirements tighten.

**3. How do you explain this to a PM?**

**[CANDIDATE]** "We're combining order data with user behavior events to create a unified view. Think of it like running a kitchen — raw ingredients come in, we clean them, combine them, and plate them. The whole process takes about 90 minutes and costs $30/day."

── STAGE 3 COMPLETE ─────────────────────

---

# STAGE 4 — BigQuery Loading & Modeling

## 🔍 PROBE

**[MENTOR]** Your processed Parquet is in GCS. You need to load it into BigQuery. Tell me about table design, partitioning, clustering, and how you guarantee idempotent loads — meaning if your pipeline retries, you don't get duplicates.

**[CANDIDATE]** BigQuery table design:

- **Partitioned** by `process_date` (day) — aligns with our daily batch, enables partition-level operations
- **Clustered** by `user_id, event_type` — most queries filter/group by these
- **Schema:** Define in Terraform with `google_bigquery_table` resource

For idempotent loads, I'd use `WRITE_TRUNCATE` disposition on the specific partition — each load replaces the full partition, so retries are safe.

*Gap: No mention of MERGE for SCD-2 patterns, no cost implications of WRITE_TRUNCATE vs. MERGE, no handling of late-arriving data.*

## 🔄 REFINE

**[MENTOR]** `WRITE_TRUNCATE` on a partition is the brute-force approach. What if you have late-arriving data — orders from yesterday landing today? You can't truncate yesterday's partition without losing today's previously-loaded yesterday data. You need a MERGE.

**[CANDIDATE]** Corrected approach:

1. **Stage table:** Load each batch into `enriched_orders_staging` with `WRITE_TRUNCATE`
2. **MERGE:** Merge staging into the main `enriched_orders` table using `order_id` + `event_id` as merge key
3. **Late data:** MERGE handles it — `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`
4. **Partition filter:** Always include `process_date` filter in MERGE to limit scan cost

## 📝 ASSIGN — BigQuery Load + MERGE SQL

**[MENTOR]** Write the BigQuery MERGE statement and the load job config.

**[CANDIDATE]**

```sql
-- bigquery/merge_enriched_orders.sql

-- Step 1: Load from GCS into staging (external table or load job)
LOAD DATA OVERWRITE `project.staging.enriched_orders_staging`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://company-data-lake-processed/enriched_orders/process_date=${RUN_DATE}/*']
);

-- Step 2: Data quality gate on staging
DECLARE staging_count INT64;
DECLARE staging_null_orders INT64;

SET staging_count = (SELECT COUNT(*) FROM `project.staging.enriched_orders_staging`);
SET staging_null_orders = (
  SELECT COUNT(*) FROM `project.staging.enriched_orders_staging`
  WHERE order_id IS NULL
);

-- Abort if staging is empty or has null PKs
IF staging_count = 0 THEN
  RAISE USING MESSAGE = 'Staging table is empty — aborting merge';
END IF;

IF staging_null_orders > 0 THEN
  RAISE USING MESSAGE = FORMAT('Found %d null order_ids in staging', staging_null_orders);
END IF;

-- Step 3: MERGE into production table
MERGE `project.analytics.enriched_orders` AS target
USING `project.staging.enriched_orders_staging` AS source
ON target.order_id = source.order_id
  AND target.process_date = source.process_date
  -- BUG: Missing partition filter on target table.
  -- Without it, BQ scans ALL partitions for the MERGE.
WHEN MATCHED THEN
  UPDATE SET
    target.amount = source.amount,
    target.currency = source.currency,
    target.event_count = source.event_count,
    target.event_types = source.event_types,
    target.user_segment = source.user_segment,
    target.row_hash = source.row_hash,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (order_id, user_id, amount, currency, event_count,
          event_types, user_segment, process_date, row_hash,
          created_at, updated_at)
  VALUES (source.order_id, source.user_id, source.amount,
          source.currency, source.event_count, source.event_types,
          source.user_segment, source.process_date, source.row_hash,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Step 4: Post-merge validation
DECLARE target_count INT64;
SET target_count = (
  SELECT COUNT(*)
  FROM `project.analytics.enriched_orders`
  WHERE process_date = '${RUN_DATE}'
);

IF target_count < staging_count THEN
  RAISE USING MESSAGE = FORMAT(
    'Post-merge count mismatch: staging=%d, target=%d',
    staging_count, target_count
  );
END IF;
```

## 💣 FAILURE INJECTION — BigQuery Schema Mismatch

**[MENTOR]** The load job just failed. Here's the error:

```
BigQuery error: load_job_abc123
Error: Provided Schema does not match Table
  project.staging.enriched_orders_staging.
Field 'amount' has changed type from FLOAT64 to STRING.
Source file: gs://company-data-lake-processed/enriched_orders/
  process_date=2026-03-19/event_type=purchase/part-00042.parquet
```

Upstream team pushed a code change that accidentally cast `amount` to string in their extraction.

**[CANDIDATE]**

**Immediate fix:**
1. Identify affected files: all Parquet files from the latest Spark run
2. Re-run the Spark job with an explicit cast: `.withColumn("amount", col("amount").cast("double"))`
3. Alternatively, fix at load time with a SQL view that casts

**Systemic fix:**
1. **Schema registry:** Store expected schemas in a central config (e.g., Firestore or a BQ metadata table). Spark job validates output schema against registry before writing.
2. **Schema enforcement in BQ load:** Use `schemaUpdateOptions = []` (no auto-update) so mismatches fail loudly.
3. **Contract testing:** CI pipeline that validates source system schemas haven't changed before deploying extraction code.

**Detection:** BQ load job failure alert → PagerDuty. Also, a daily schema drift check: compare Parquet file schemas against the registry.

**Prevention:** Schema evolution policy — any field type change requires a migration ticket and downstream impact assessment. Breaking changes go through a deprecation cycle.

**[MENTOR]** Correct. I'd add: never trust upstream. Your Spark job should defensively cast every column to the expected type:

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

EXPECTED_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    # ...
])

orders = spark.read.parquet(path)
for field in EXPECTED_SCHEMA:
    orders = orders.withColumn(field.name, col(field.name).cast(field.dataType))
```

---

## ── DEBRIEF ─────────────────────────────

**[MENTOR]**
- **Score: Strong**
- **Gaps:** MERGE missing partition filter is a cost bomb — at 365 days of data, you're scanning the full table every day. Fix: add `AND target.process_date = '${RUN_DATE}'` to the ON clause, or use `WHERE` predicate.

**Tradeoff Questions:**

**1. Why MERGE over WRITE_TRUNCATE?**

**[CANDIDATE]** MERGE handles late-arriving data without destroying previously-loaded records. WRITE_TRUNCATE is simpler but destructive — if yesterday's partition had 1M rows and today's late data has 5K rows, truncate destroys the 1M. MERGE adds the 5K. The cost is that MERGE requires a unique key and is slightly more expensive (DML vs. load job).

**2. What breaks at 10× scale?**

**[CANDIDATE]** MERGE on a 100+ TB table is expensive. At that scale, I'd switch to a staging→swap pattern: load into a temp table, validate, then `ALTER TABLE ... RENAME` to swap. Or use BigQuery's partition-level `COPY` operation. Also, slot contention — we'd need flat-rate reservations.

**3. How do you explain this to a PM?**

**[CANDIDATE]** "We load today's processed data into a staging area, run quality checks, then merge it into the production table. If the same data shows up twice — like from a retry — it updates instead of duplicating. Think of it like updating a spreadsheet: we match rows by order ID and update or insert as needed."

── STAGE 4 COMPLETE ─────────────────────

---

## 🎤 INTERVIEW CHECK (After Stages 3–4)

**[MENTOR]** *Explain the Spark processing and BigQuery loading pipeline end-to-end. No bullet points.*

**[CANDIDATE]** "After files are validated and sitting in GCS, a Dataproc Spark job picks them up. We use ephemeral clusters — they spin up, process, and terminate — which saves over 90% compared to always-on clusters. The job reads orders and events, deduplicates them by primary key, and joins them. Events get aggregated per user first to avoid row explosion from the many-to-many relationship. We broadcast-join a small user segments table to avoid unnecessary shuffles.

AQE is enabled to handle runtime skew — we've seen bot accounts create 275:1 partition imbalances. For known bad actors, we filter them upstream. The output is written as partitioned Parquet with dynamic partition overwrite, so retries are naturally idempotent — they just replace the day's partition.

From GCS, a BigQuery load job pulls the Parquet into a staging table. We run quality gates — no null primary keys, non-empty result set. Then a MERGE statement upserts into the production table, matching on order ID and process date. This handles both fresh loads and late-arriving corrections. We always include a partition filter in the MERGE to prevent full-table scans — that's a cost governance guardrail.

The key tradeoff is MERGE vs. truncate-and-replace. MERGE is more expensive per operation but handles late data gracefully. At our current scale of about 2 terabytes per day, MERGE cost is trivial compared to the engineering cost of handling late data with truncation."

**[MENTOR]** **Score: Strong.** You mentioned the bot skew, AQE, cost governance, and the MERGE tradeoff without being asked. Follow-up: *If BigQuery MERGE starts taking over an hour due to table growth, what's your plan?*

**[CANDIDATE]** "Three options in order of invasiveness: First, ensure we're using partition pruning — the MERGE should only touch today's partition. Second, move to flat-rate reservations instead of on-demand to guarantee slot availability. Third, if the table exceeds 50TB, switch to a partition-swap pattern — load into a shadow partition, validate, then swap using DML. This avoids the read-modify-write overhead of MERGE entirely."

── INTERVIEW CHECK COMPLETE ─────────────
