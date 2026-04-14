"""
Stage 5b: Load processed Parquet from GCS into BigQuery staging table.

Uses the BigQuery Python client to create a load job from GCS.
Supports local Parquet → BQ upload for free-tier testing.

Prerequisites:
  1. Run 04_spark_processing/process_daily_orders.py first
  2. BQ tables must exist (Terraform or create_tables.sql)
  3. Authenticate: gcloud auth application-default login
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_STAGING = os.getenv("BQ_DATASET_STAGING", "staging")
BQ_TABLE_STAGING = os.getenv("BQ_TABLE_STAGING", "enriched_orders_staging")


def load_from_gcs(client, run_date):
    """Load Parquet from GCS processed bucket into BQ staging."""
    bucket = os.getenv("GCS_BUCKET_PROCESSED")
    uri = f"gs://{bucket}/enriched_orders/process_date={run_date}/*.parquet"
    table_ref = f"{PROJECT_ID}.{BQ_DATASET_STAGING}.{BQ_TABLE_STAGING}"

    print(f"  Loading from: {uri}")
    print(f"  Into:         {table_ref}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for completion

    return load_job


def load_from_local(client, run_date):
    """Load local Parquet files into BQ staging."""
    local_dir = (
        Path(__file__).parent.parent
        / "04_spark_processing" / "output_data" / "enriched_orders"
    )

    table_ref = f"{PROJECT_ID}.{BQ_DATASET_STAGING}.{BQ_TABLE_STAGING}"

    if not local_dir.exists():
        print(f"  ❌ Directory not found: {local_dir}")
        print(f"  Run process_daily_orders.py --date {run_date} first!")
        sys.exit(1)

    parquet_files = list(local_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"  ❌ No Parquet files in {local_dir}")
        sys.exit(1)

    print(f"  Found {len(parquet_files)} Parquet files in {local_dir}")
    print(f"  Loading into: {table_ref}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True, # Force BigQuery to read the schema from the files
    )

    # Load each file (concatenated into staging)
    for pf in parquet_files:
        print(f"    Uploading: {pf.name}")
        with open(pf, "rb") as f:
            load_job = client.load_table_from_file(
                f, table_ref, job_config=job_config
            )
            load_job.result()
            # After first file, switch to APPEND for subsequent files
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    return load_job


def verify_staging(client):
    """Check row count in staging table."""
    table_ref = f"{PROJECT_ID}.{BQ_DATASET_STAGING}.{BQ_TABLE_STAGING}"
    query = f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
    result = list(client.query(query).result())
    return result[0].cnt


def main():
    parser = argparse.ArgumentParser(description="Load to BQ staging")
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD)")
    parser.add_argument(
        "--source", choices=["gcs", "local"], default="local",
        help="Load from 'gcs' or 'local' Parquet files",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Stage 5b: Load into BigQuery Staging")
    print(f"  Date:   {args.date}")
    print(f"  Source: {args.source}")
    print("=" * 60)
    print()

    if not PROJECT_ID:
        print("❌ ERROR: Set GCP_PROJECT_ID in .env!")
        sys.exit(1)

    client = bigquery.Client(project=PROJECT_ID)

    print("[1/2] Loading data...")
    if args.source == "gcs":
        job = load_from_gcs(client, args.date)
    else:
        job = load_from_local(client, args.date)

    print(f"  ✓ Load job completed: {job.output_rows} rows loaded")

    print("\n[2/2] Verifying staging table...")
    row_count = verify_staging(client)
    print(f"  ✓ Staging table row count: {row_count:,}")

    if row_count == 0:
        print("  ❌ WARNING: Staging table is empty!")
    else:
        print(f"\n  ✅ STAGE 5b COMPLETE — {row_count:,} rows in BQ staging!\n")


if __name__ == "__main__":
    main()
