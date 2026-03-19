"""
Stage 3b: Validate landed files in GCS raw bucket.

For each file in the raw bucket:
  - Check file size > 0
  - For Parquet: validate schema against expected columns
  - For CSV: validate header row
  - Dedup check via local SQLite (simulates Firestore in production)
  - Valid → copy to validated bucket
  - Invalid → copy to quarantine bucket

Prerequisites:
  1. Run transfer_s3_to_gcs.py first
  2. GCS buckets must exist (Terraform or manual)
"""

import os
import sys
import io
import sqlite3
from pathlib import Path
from datetime import datetime

import pyarrow.parquet as pq
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

GCS_BUCKET_RAW = os.getenv("GCS_BUCKET_RAW")
GCS_BUCKET_VALIDATED = os.getenv("GCS_BUCKET_VALIDATED")
GCS_BUCKET_QUARANTINE = os.getenv("GCS_BUCKET_QUARANTINE")

# Schema registry — expected columns per source
EXPECTED_SCHEMAS = {
    "orders": {
        "required": ["order_id", "user_id", "amount", "currency", "process_date"],
        "optional": ["status", "created_at"],
    },
    "events": {
        "required": ["event_id", "user_id", "event_type", "timestamp", "process_date"],
        "optional": ["page_url", "session_id"],
    },
    "user_segments": {
        "required": ["user_id", "segment"],
        "optional": ["lifetime_value", "signup_date", "country"],
    },
}

# Local SQLite for dedup (simulates Firestore in production)
DB_PATH = Path(__file__).parent / "validation_dedup.db"


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_name TEXT PRIMARY KEY,
            processed_at TEXT,
            status TEXT
        )
    """)
    return conn


def is_already_processed(conn, file_name):
    row = conn.execute(
        "SELECT 1 FROM processed_files WHERE file_name = ?",
        (file_name,),
    ).fetchone()
    return row is not None


def mark_processed(conn, file_name, status):
    conn.execute(
        "INSERT OR REPLACE INTO processed_files VALUES (?, ?, ?)",
        (file_name, datetime.now().isoformat(), status),
    )
    conn.commit()


def detect_source(file_name):
    """Extract source name from partition-style path."""
    # e.g., source=orders/dt=2026-03-19/orders.parquet → orders
    for part in file_name.split("/"):
        if part.startswith("source="):
            return part.replace("source=", "")
    return None


def validate_parquet(gcs_client, bucket_name, blob_name, source):
    """Validate Parquet file schema against registry."""
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_bytes()

    try:
        table = pq.read_table(io.BytesIO(content))
    except Exception as e:
        return False, f"Corrupt Parquet: {e}"

    actual_cols = set(table.column_names)
    expected = EXPECTED_SCHEMAS.get(source)
    if not expected:
        return True, "No schema registered (passed by default)"

    missing = set(expected["required"]) - actual_cols
    if missing:
        return False, f"Missing required columns: {missing}"

    return True, f"Schema OK ({len(table)} rows, {len(actual_cols)} columns)"


def validate_csv(gcs_client, bucket_name, blob_name, source):
    """Validate CSV header against expected columns."""
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Read just the first line
    content = blob.download_as_text()
    first_line = content.split("\n")[0].strip()
    actual_cols = set(first_line.split(","))

    expected = EXPECTED_SCHEMAS.get(source)
    if not expected:
        return True, "No schema registered"

    missing = set(expected["required"]) - actual_cols
    if missing:
        return False, f"Missing required columns: {missing}"

    row_count = content.count("\n")
    return True, f"Header OK ({row_count} rows)"


def move_file(gcs_client, src_bucket, dst_bucket, blob_name):
    """Copy file to destination and delete from source."""
    src = gcs_client.bucket(src_bucket)
    dst = gcs_client.bucket(dst_bucket)
    src_blob = src.blob(blob_name)
    src.copy_blob(src_blob, dst, blob_name)
    src_blob.delete()


def main():
    print("=" * 60)
    print("  Stage 3b: Validate GCS Landing Files")
    print("=" * 60)
    print()

    if not all([GCS_BUCKET_RAW, GCS_BUCKET_VALIDATED, GCS_BUCKET_QUARANTINE]):
        print("❌ ERROR: Missing GCS bucket names in .env!")
        sys.exit(1)

    gcs = storage.Client()
    conn = get_db()

    bucket = gcs.bucket(GCS_BUCKET_RAW)
    blobs = list(bucket.list_blobs())

    if not blobs:
        print("  ⚠ No files found in raw bucket!")
        print("  Run transfer_s3_to_gcs.py first.")
        return

    print(f"  Found {len(blobs)} files in raw bucket\n")

    stats = {"validated": 0, "quarantined": 0, "skipped": 0}

    for blob in blobs:
        file_name = blob.name

        # Dedup check
        if is_already_processed(conn, file_name):
            print(f"  ⏭ SKIP: {file_name} (already processed)")
            stats["skipped"] += 1
            continue

        # Size check
        if blob.size == 0:
            print(f"  ❌ QUARANTINE: {file_name} (empty file)")
            move_file(gcs, GCS_BUCKET_RAW, GCS_BUCKET_QUARANTINE, file_name)
            mark_processed(conn, file_name, "quarantined:empty")
            stats["quarantined"] += 1
            continue

        # Detect source
        source = detect_source(file_name)
        if not source:
            print(f"  ⚠ WARN: {file_name} — unknown source, passing through")

        # Schema validation
        if file_name.endswith(".parquet"):
            is_valid, message = validate_parquet(
                gcs, GCS_BUCKET_RAW, file_name, source
            )
        elif file_name.endswith(".csv"):
            is_valid, message = validate_csv(
                gcs, GCS_BUCKET_RAW, file_name, source
            )
        else:
            is_valid, message = True, "Unknown format (passed)"

        if is_valid:
            print(f"  ✓ VALID:      {file_name} — {message}")
            move_file(gcs, GCS_BUCKET_RAW, GCS_BUCKET_VALIDATED, file_name)
            mark_processed(conn, file_name, "validated")
            stats["validated"] += 1
        else:
            print(f"  ❌ QUARANTINE: {file_name} — {message}")
            move_file(gcs, GCS_BUCKET_RAW, GCS_BUCKET_QUARANTINE, file_name)
            mark_processed(conn, file_name, f"quarantined:{message}")
            stats["quarantined"] += 1

    conn.close()

    # Summary
    print()
    print("─" * 60)
    print(f"  Validated:   {stats['validated']} files → {GCS_BUCKET_VALIDATED}")
    print(f"  Quarantined: {stats['quarantined']} files → {GCS_BUCKET_QUARANTINE}")
    print(f"  Skipped:     {stats['skipped']} files (already processed)")
    print(f"  Dedup DB:    {DB_PATH}")
    print("─" * 60)
    print(f"\n  ✅ STAGE 3b COMPLETE — Validation done!\n")


if __name__ == "__main__":
    main()
