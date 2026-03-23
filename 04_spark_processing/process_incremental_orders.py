"""
🚀 process_incremental_orders.py — Support for High-Watermark ETL (Staff Level)

Instead of a full daily reload, this script demonstrates INCREMENTAL processing:
1.  Read the 'Last Successful Run' timestamp from a metadata store (or config).
2.  Filter the source table for 'updated_at > watermark'.
3.  Perform an idempotent MERGE into the production table.

This pattern drastically reduces compute costs by only processing changed records.
"""

import argparse
import sys
import yaml
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def create_spark_session():
    return (SparkSession.builder
        .appName("incremental_orders_processing")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate())

def get_watermark(config):
    """
    Simulate reading the high-watermark from a metadata database.
    In production, this would be a query to a metadata table.
    """
    # For demonstration, we'll use a hardcoded or config-provided watermark
    return config.get('watermark', '2026-03-21 00:00:00')

def process_incremental(spark, source_path, watermark_ts):
    """Filter records by the high-watermark timestamp."""
    print(f"  Filtering for records updated after: {watermark_ts}")
    
    # Read the full dataset (or specific partitions)
    raw_df = spark.read.parquet(source_path)
    
    # Apply the incremental filter
    incremental_df = raw_df.filter(col("updated_at") > lit(watermark_ts))
    
    print(f"  Found {incremental_df.count():,} new or updated records.")
    return incremental_df

def main():
    parser = argparse.ArgumentParser(description="Incremental Order Processor")
    parser.add_argument("--config", default="config/pipeline_config.yaml")
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    spark = create_spark_session()
    
    # Simulated incremental run
    watermark = get_watermark(config)
    
    # Use the 'processed' data as our source for this demo
    source_path = "04_spark_processing/output_data/enriched_orders/"
    
    if not Path(source_path).exists():
        print("❌ Error: Core processed data not found. Run Stage 4 first!")
        sys.exit(1)

    try:
        incremental_data = process_incremental(spark, source_path, watermark)
        
        if incremental_data.count() > 0:
            print("  ✅ Incremental processing successful!")
            # In a real pipeline, we would now MERGE these specific rows into BigQuery
        else:
            print("  ☕ No new data found since last run. Skipping load.")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
