# 🧑‍💻 Manual vs. Automated Implementation Flow

In this project, we automated almost everything using scripts (Python/Terraform/SQL) because that is how a Senior Data Engineer builds **production-grade pipelines**. However, to truly understand *how* the cloud works, it's great to know what these scripts were actually doing behind the scenes. 

Here is exactly what we automated and how you could have done it **manually (by clicking buttons in the UI)**:

---

### Step 1: Data Generation
- **Automated Way:** Ran `python generate_data.py`
- **Manual Way:** None. We needed fake source data, so a script had to generate thousands of mock e-commerce transactions and save them locally.

### Step 2: Upload Files to AWS S3
- **Automated Way:** `upload_to_s3.py` authenticated to AWS using `boto3` and uploaded the files.
- **Manual Way:** 
  1. Login to Amazon AWS Console.
  2. Search for "S3". 
  3. Click "Create Bucket" and enter a name.
  4. Drag and drop the local CSV/Parquet files from your `output_data` folder straight into the browser window.

### Step 3a: Create GCP Infrastructure (Buckets & Databases)
- **Automated Way:** `terraform apply` read our `main.tf` file and asked Google Cloud APIs to create everything instantly.
- **Manual Way:**
  1. Login to Google Cloud Console.
  2. Search for "Cloud Storage". Click "Create Bucket" 4 separate times and type the names manually.
  3. Search for "BigQuery". Click "Create Dataset" twice (`staging` and `analytics`).
  4. Inside BigQuery, click "Create Table" and manually type out every single column name (schema) to create your empty DB tables.

### Step 3b: Transfer Data from S3 → GCS
- **Automated Way:** `transfer_s3_to_gcs.py` used Python to download from AWS and upload to Google Cloud Storage.
- **Manual Way:** 
  - **Option 1 (Painful):** Download the files manually from AWS to your desktop, then drag and drop them into the GCP Storage bucket in your browser.
  - **Option 2 (Enterprise):** Use Google's **"Storage Transfer Service"** UI in the console. You click a few buttons, give GCP your AWS keys, and Google's backend servers securely pull the data over directly without touching your laptop.

### Step 3c & 4: Data Validation & PySpark Enrichment
- **Automated Way:** Custom Python/PySpark scripts checked file integrity, deleted bad files, joined tables, and filtered bad rows.
- **Manual Way:** There is no easy "UI click" for custom logic! You'd have to open the CSVs in Excel, write VLOOKUPs to join things, check for nulls, delete bad rows, and resave the file. PySpark does this on massive scale.

### Step 5a: Load Data into BigQuery (Staging)
- **Automated Way:** `load_staging.py` used the BigQuery SDK to stream the clean output files directly into the `staging` database.
- **Manual Way:**
  1. Open BigQuery SQL Workspace in your browser.
  2. Click on the `staging` dataset → **Create Table**.
  3. Select **Source: Google Cloud Storage**, paste the GCS URL, and select format: **Parquet**.
  4. Google will import the data from GCS into the database table for you.

---

### 👉 The Final Step (Step 5b): The MERGE (DO IT YOURSELF!)

Now that you understand what happened, **I want YOU to execute this final step manually!**

1. Go to your browser and open the [Google Cloud BigQuery Console](https://console.cloud.google.com/bigquery).
2. Make sure you are in your project: `aws-gcp-migration-490909`.
3. Go back to your editor and copy ALL the text inside **`05_bigquery_loading/merge_production.sql`**.
4. Paste it into the query editor box in the BigQuery console.
5. In the SQL text, find `${RUN_DATE}` right at the bottom (around line 82). Change it to exactly `'2026-03-19'`.
6. Hit the big blue **RUN** button!

This will physically move the data from `staging` into your final `analytics` production table. Let me know when you've hit RUN and what the output says!
