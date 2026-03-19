"""
Stage 3a: Transfer data from S3 to GCS (raw landing zone).

Reads files from S3 and uploads them to GCS with the same
partition structure. Tracks transfers in a local manifest
to support idempotent re-runs.

Prerequisites:
  1. Run Terraform (or create GCS buckets manually)
  2. Run 02_aws_s3/upload_to_s3.py
  3. Fill in .env with GCP_PROJECT_ID, GCS_BUCKET_RAW
  4. Authenticate: gcloud auth application-default login
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

import boto3
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# AWS
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# GCP
GCS_BUCKET_RAW = os.getenv("GCS_BUCKET_RAW")

# Manifest for idempotent re-runs
MANIFEST_FILE = Path(__file__).parent / "transfer_manifest.json"


def load_manifest():
    if MANIFEST_FILE.exists():
        return json.loads(MANIFEST_FILE.read_text())
    return {"transferred": []}


def save_manifest(manifest):
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2))


def main():
    print("=" * 60)
    print("  Stage 3a: Transfer S3 → GCS")
    print("=" * 60)
    print()

    if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, GCS_BUCKET_RAW]):
        print("❌ ERROR: Missing credentials in .env file!")
        sys.exit(1)

    # Clients
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    gcs = storage.Client()
    gcs_bucket = gcs.bucket(GCS_BUCKET_RAW)

    manifest = load_manifest()
    already_transferred = set(manifest["transferred"])

    # List S3 objects
    print("[1/2] Listing S3 objects...")
    response = s3.list_objects_v2(Bucket=S3_BUCKET)
    if "Contents" not in response:
        print("  ❌ No objects found in S3 bucket!")
        sys.exit(1)

    s3_objects = response["Contents"]
    print(f"  Found {len(s3_objects)} objects in S3\n")

    # Transfer each file
    print("[2/2] Transferring files...")
    transferred = 0
    skipped = 0

    for obj in s3_objects:
        s3_key = obj["Key"]

        # Idempotency: skip already transferred
        if s3_key in already_transferred:
            print(f"  ⏭ SKIP (already transferred): {s3_key}")
            skipped += 1
            continue

        # Download from S3 to memory
        s3_response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = s3_response["Body"].read()
        size_mb = len(file_content) / (1024 * 1024)

        # Upload to GCS with same key structure
        gcs_blob = gcs_bucket.blob(s3_key)
        gcs_blob.upload_from_string(
            file_content,
            content_type="application/octet-stream",
        )

        print(f"  ✓ {s3_key} → gs://{GCS_BUCKET_RAW}/{s3_key} "
              f"({size_mb:.1f} MB)")

        # Update manifest
        manifest["transferred"].append(s3_key)
        transferred += 1

    save_manifest(manifest)

    # Summary
    print()
    print("─" * 60)
    print(f"  Transferred: {transferred} files")
    print(f"  Skipped:     {skipped} files (already done)")
    print(f"  Manifest:    {MANIFEST_FILE}")
    print("─" * 60)

    # Verify on GCS
    print("\n  GCS bucket contents:")
    blobs = list(gcs_bucket.list_blobs())
    for blob in blobs:
        print(f"  ✓ gs://{GCS_BUCKET_RAW}/{blob.name} "
              f"({blob.size / (1024*1024):.1f} MB)")

    print(f"\n  ✅ STAGE 3a COMPLETE — {transferred} files transferred to GCS!\n")


if __name__ == "__main__":
    main()
