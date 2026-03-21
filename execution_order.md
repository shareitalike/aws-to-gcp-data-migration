# Pipeline Execution Order

Here is the exact step-by-step list of all the files and scripts we have executed so far, in chronological order, along with what they did in the pipeline.

### 1. Source Data Generation
**1. `01_sample_data/generate_data.py`**
- **Action:** Created the fake, raw e-commerce data (`orders`, `events`, `user_segments`) locally on your machine.

### 2. Loading into AWS (Simulating the Old System)
**2. `02_aws_s3/upload_to_s3.py`**
- **Action:** Uploaded the locally generated data directly into an AWS S3 bucket. This represents the starting point of our migration project.

### 3. GCP Infrastructure Setup
**3. `03_gcs_ingestion/terraform/main.tf`** (via `terraform apply`)
- **Action:** Created four Google Cloud Storage (GCS) buckets (`raw`, `validated`, `quarantine`, `processed`) and two BigQuery databases (`staging` and `analytics`).

### 4. Transferring Data to GCP
**4. `03_gcs_ingestion/transfer_s3_to_gcs.py`**
- **Action:** Logged into AWS S3, downloaded all 5 data files, and uploaded them to the GCP `raw` bucket (`aws-gcp-migration-490909-data-lake-raw`).

### 5. Validating the Raw Data
**5. `03_gcs_ingestion/validate_landing.py`**
- **Action:** Scanned the files inside the `raw` bucket to ensure the headers and schemas were not corrupted. It then **moved** all those files to the `validated` bucket (`aws-gcp-migration-490909-data-lake-validated`).

### 6. Processing and Enriching Data (PySpark)
**6. `04_spark_processing/process_daily_orders.py`**
- **Action:** Used PySpark to join the `orders`, `events`, and `user_segments` tables together into one massive, clean table. It deduplicated columns, ran quality checks, and saved the final output to locally/GCS.

### 7. Loading to BigQuery Staging
**7. `05_bigquery_loading/load_staging.py`**
- **Action:** Uploaded the clean, processed data from the PySpark job straight into a BigQuery `staging` table (`staging.enriched_orders_staging`).

---

### ⏳ What is remaining to execute:

**8. `05_bigquery_loading/merge_production.sql`**
- **Next Step:** We will run this SQL file inside BigQuery to intelligently move the staging data into the final production analytics table (handling any deduplication or updates).

**9. `07_monitoring/data_quality_checks.py`**
- **Final Step:** Runs post-migration quality checks in BigQuery.
**10. `07_monitoring/cost_report.sql`**
- **Final Step:** Analyzes the cost of our BigQuery usage.
