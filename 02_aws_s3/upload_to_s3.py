"""
Stage 2: Upload generated sample data to AWS S3.

Uploads files to S3 with partition-style structure:
  s3://bucket/source=orders/dt=YYYY-MM-DD/orders.parquet
  s3://bucket/source=events/dt=YYYY-MM-DD/events.parquet
  s3://bucket/source=user_segments/latest/user_segments.parquet

Prerequisites:
  1. pip install -r requirements.txt
  2. Copy .env.example → .env and fill in AWS credentials
  3. Run 01_sample_data/generate_data.py first
"""

import os
import sys
from pathlib import Path
from datetime import datetime

import boto3
from dotenv import load_dotenv

# Load env
load_dotenv(Path(__file__).parent.parent / ".env")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

DATA_DIR = Path(__file__).parent.parent / "01_sample_data" / "output_data"
RUN_DATE = datetime.now().strftime("%Y-%m-%d")


def get_s3_client():
    """Create S3 client with credentials from .env."""
    if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET]):
        print("❌ ERROR: Missing AWS credentials in .env file!")
        print("   Copy .env.example → .env and fill in your values.")
        sys.exit(1)

    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )


def create_bucket_if_not_exists(s3):
    """Create S3 bucket if it doesn't exist."""
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"  ✓ Bucket '{S3_BUCKET}' exists")
    except s3.exceptions.ClientError:
        print(f"  Creating bucket '{S3_BUCKET}'...")
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=S3_BUCKET)
        else:
            s3.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        print(f"  ✓ Bucket '{S3_BUCKET}' created")


def upload_file(s3, local_path, s3_key):
    """Upload a single file to S3."""
    file_size = local_path.stat().st_size / (1024 * 1024)
    print(f"  Uploading {local_path.name} → s3://{S3_BUCKET}/{s3_key} "
          f"({file_size:.1f} MB)")
    s3.upload_file(str(local_path), S3_BUCKET, s3_key)


def main():
    print("=" * 60)
    print("  Stage 2: Upload Sample Data to AWS S3")
    print("=" * 60)
    print()

    if not DATA_DIR.exists():
        print("❌ ERROR: Run 01_sample_data/generate_data.py first!")
        sys.exit(1)

    s3 = get_s3_client()
    create_bucket_if_not_exists(s3)

    # Upload with partition-style keys (mimics production)
    uploads = {
        "orders.parquet": f"source=orders/dt={RUN_DATE}/orders.parquet",
        "events.parquet": f"source=events/dt={RUN_DATE}/events.parquet",
        "user_segments.parquet": "source=user_segments/latest/user_segments.parquet",
        "orders.csv": f"source=orders/dt={RUN_DATE}/orders.csv",
        "events.csv": f"source=events/dt={RUN_DATE}/events.csv",
    }

    print("[1/2] Uploading files...")
    for filename, s3_key in uploads.items():
        filepath = DATA_DIR / filename
        if filepath.exists():
            upload_file(s3, filepath, s3_key)
        else:
            print(f"  ⚠ Skipping {filename} (not found)")

    # Verify uploads
    print("\n[2/2] Verifying uploads...")
    response = s3.list_objects_v2(Bucket=S3_BUCKET)
    if "Contents" in response:
        total_size = 0
        for obj in response["Contents"]:
            size_mb = obj["Size"] / (1024 * 1024)
            total_size += size_mb
            print(f"  ✓ {obj['Key']} ({size_mb:.1f} MB)")
        print(f"\n  Total: {total_size:.1f} MB in {len(response['Contents'])} files")
    else:
        print("  ❌ No files found in bucket!")

    print("\n  ✅ STAGE 2 COMPLETE — Data uploaded to S3!\n")


if __name__ == "__main__":
    main()
