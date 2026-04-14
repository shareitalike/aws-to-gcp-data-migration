"""
Stage 4: PySpark processing — join orders + events, enrich with segments.

Runs LOCALLY with PySpark (no Dataproc needed).
Reads validated Parquet from GCS (or local fallback), enriches, and writes
partitioned output to GCS processed bucket (or local).

Key patterns demonstrated:
  - Broadcast join (small dimension table)
  - AQE (adaptive query execution)
  - Deduplication by primary key
  - Partitioned overwrite for idempotency
  - Data quality gate (row count check)

Usage:
  # From GCS (needs gcloud auth)
  python process_daily_orders.py --date 2026-03-19 --source gcs

  # From local files (no cloud needed)
  python process_daily_orders.py --date 2026-03-19 --source local
"""

import argparse
import sys
import yaml
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, broadcast, lit, sha2, concat_ws,
    count, collect_list, concat, array_join,
    current_timestamp,
)


def create_spark_session():
    """Create local Spark session with production-like configs."""
    return (SparkSession.builder
        .appName("daily_orders_processing")
        # Networking — critical for Windows stability
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.master", "local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128m")
        # Shuffle partitions — tuned for local (production would be 2000+)
        .config("spark.sql.shuffle.partitions", "20")
        # Parquet
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Dynamic partition overwrite for idempotency
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Speculation for stragglers
        .config("spark.speculation", "false")  # off for local
        # GCS connector (if reading from GCS)
        .config("spark.jars.packages",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.17")
        .getOrCreate())


def read_data(spark, source, run_date, config=None):
    """Read orders, events, and segments from either GCS or local."""
    import os
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    if source == "gcs":
        # Use YAML config if provided, otherwise fallback to .env
        if config:
            bucket_validated = config['gcs']['buckets']['validated']
        else:
            bucket_validated = os.getenv("GCS_BUCKET_VALIDATED")
            
        orders_path = f"gs://{bucket_validated}/source=orders/dt={run_date}/"
        events_path = f"gs://{bucket_validated}/source=events/dt={run_date}/"
        segments_path = f"gs://{bucket_validated}/source=user_segments/latest/"
    else:
        # Local fallback — read from generated data
        data_dir = Path(__file__).parent.parent / "01_sample_data" / "output_data"
        orders_path = str(data_dir / "orders.parquet")
        events_path = str(data_dir / "events.parquet")
        segments_path = str(data_dir / "user_segments.parquet")

    print(f"  Reading orders from: {orders_path}")
    orders = spark.read.parquet(orders_path)

    print(f"  Reading events from: {events_path}")
    events = spark.read.parquet(events_path)

    print(f"  Reading segments from: {segments_path}")
    segments = spark.read.parquet(segments_path)

    return orders, events, segments


def process(orders, events, segments, run_date):
    """Core processing: deduplicate, aggregate events, join, enrich."""

    # ── Step 1: Deduplicate orders by order_id ──
    print("  [1/5] Deduplicating orders...")
    orders_deduped = (orders
        .filter(col("amount") > 0)        # remove negative amounts
        .filter(col("user_id").isNotNull()) # remove null user_ids
        .dropDuplicates(["order_id"])
    )

    # ── Step 2: Deduplicate events and aggregate per user ──
    # IMPORTANT: Aggregate events per user BEFORE joining to avoid
    # row explosion (many orders × many events per user)
    print("  [2/5] Aggregating events per user...")
    events_deduped = events.dropDuplicates(["event_id"])
    events_agg = (events_deduped
        .groupBy("user_id")
        .agg(
            count("event_id").alias("event_count"),
            # Collect unique event types as comma-separated string
            concat_ws(",", collect_list("event_type")).alias("event_types"),
        )
    )

    # ── Step 3: Broadcast join with small user_segments table ──
    print("  [3/5] Broadcast joining user segments...")
    # user_segments is tiny (~1K rows) — broadcast avoids shuffle
    orders_with_segments = (orders_deduped
        .join(
            broadcast(segments.select("user_id", "segment", "lifetime_value", "country")),
            on="user_id",
            how="left",
        )
    )

    # ── Step 4: Join with aggregated events ──
    print("  [4/5] Joining orders with event aggregates...")
    enriched = (orders_with_segments
        .join(events_agg, on="user_id", how="left")
        .withColumn("process_date", lit(run_date))
        .withColumn("user_segment", col("segment"))
        .withColumn("row_hash", sha2(
            concat_ws("||",
                col("order_id"),
                col("amount").cast("string"),
                col("currency"),
            ), 256
        ))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .select(
            "order_id", "user_id", "amount", "currency", "status",
            "event_count", "event_types", "user_segment",
            "lifetime_value", "country", "process_date",
            "row_hash", "created_at", "updated_at",
        )
    )

    # ── Step 5: Data quality gate ──
    print("  [5/5] Running quality checks...")
    input_count = orders_deduped.count()
    output_count = enriched.count()
    print(f"     Input orders (after dedup):  {input_count:,}")
    print(f"     Output enriched rows:        {output_count:,}")

    if output_count == 0:
        raise ValueError("QUALITY CHECK FAILED: Output is empty!")
    if output_count > input_count * 1.1:
        raise ValueError(
            f"QUALITY CHECK FAILED: Row explosion detected! "
            f"Input={input_count}, Output={output_count}"
        )

    return enriched


def write_output(enriched, source, run_date, config=None):
    """Write partitioned Parquet to GCS or local."""
    import os
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    if source == "gcs":
        if config:
            bucket = config['gcs']['buckets']['processed']
        else:
            bucket = os.getenv("GCS_BUCKET_PROCESSED")
        output_path = f"gs://{bucket}/enriched_orders/"
    else:
        output_path = str(
            Path(__file__).parent / "output_data" / "enriched_orders"
        )

    print(f"  Writing output to: {output_path}")
    (enriched.write
        .mode("overwrite")
        .parquet(output_path)
    )
    print(f"  ✓ Output written successfully")


def main():
    parser = argparse.ArgumentParser(description="Process daily orders")
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD)")
    parser.add_argument(
        "--source", choices=["gcs", "local"], default="local",
        help="Data source: 'gcs' or 'local' (default: local)",
    )
    parser.add_argument(
        "--config", help="Path to YAML config file",
        default=str(Path(__file__).parent.parent / "config" / "pipeline_config.yaml")
    )
    args = parser.parse_args()

    # Load config if exists
    config = None
    if args.config and Path(args.config).exists():
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            print(f"  Loaded config from: {args.config}")

    print("=" * 60)
    print("  Stage 4: PySpark Processing")
    print(f"  Date:   {args.date}")
    print(f"  Source: {args.source}")
    print("=" * 60)
    print()

    spark = create_spark_session()

    try:
        orders, events, segments = read_data(spark, args.source, args.date, config)
        enriched = process(orders, events, segments, args.date)
        write_output(enriched, args.source, args.date, config)
    finally:
        spark.stop()

    print(f"\n  ✅ STAGE 4 COMPLETE — Processing done!\n")


if __name__ == "__main__":
    main()
