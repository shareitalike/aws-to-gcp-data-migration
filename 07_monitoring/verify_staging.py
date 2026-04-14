import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv('f:/pyspark_study/project_bigquery_auto/.env')

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_STAGING = os.getenv("BQ_DATASET_STAGING", "staging")
BQ_TABLE_STAGING = os.getenv("BQ_TABLE_STAGING", "enriched_orders_staging")

def verify_table():
    client = bigquery.Client(project=PROJECT_ID)
    
    # ─── Check Staging ───
    staging_ref = f"{PROJECT_ID}.{BQ_DATASET_STAGING}.{BQ_TABLE_STAGING}"
    print(f"🔍 Checking Staging Table: {staging_ref}...")
    
    try:
        query = f"SELECT count(*) FROM `{staging_ref}`"
        query_job = client.query(query)
        results = list(query_job.result())
        row_count = results[0][0]
        print(f"✅ SUCCESS: Staging table found with {row_count:,} rows.")
    except Exception as e:
        print(f"❌ ERROR: Could not find staging table. {e}")

if __name__ == "__main__":
    verify_table()
