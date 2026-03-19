# 🚀 AWS → GCP Data Migration (Hands-On Project)

> A production-pattern migration: **S3 → GCS → Spark → BigQuery**
> Built for AWS/GCP **free tier** accounts.

---

## 📁 Project Structure

```
project_bigquery_auto/
├── docs/                          ← Simulation study materials
│   ├── simulation_stage_1_2.md    ← Architecture + Ingestion
│   ├── simulation_stage_3_4.md    ← Spark + BigQuery
│   ├── simulation_stage_5_6.md    ← Airflow + Monitoring
│   └── simulation_stage_7_final.md ← System Design Interview
│
├── 01_sample_data/                ← Generate test data
├── 02_aws_s3/                     ← Upload to S3
├── 03_gcs_ingestion/              ← Terraform + Transfer + Validate
├── 04_spark_processing/           ← PySpark enrichment
├── 05_bigquery_loading/           ← BQ staging + MERGE
├── 06_airflow_orchestration/      ← DAG (local Airflow)
├── 07_monitoring/                 ← Quality checks + cost
├── .env.example                   ← Credentials template
└── requirements.txt               ← Python dependencies
```

---

## ⚡ Prerequisites

| Tool | Install Command | Purpose |
|---|---|---|
| Python 3.10+ | Already installed | Everything |
| AWS CLI | `pip install awscli` | S3 access |
| GCloud CLI | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) | GCS/BQ access |
| Terraform | [terraform.io/downloads](https://www.terraform.io/downloads) | Infrastructure |
| Java 11+ | [adoptium.net](https://adoptium.net/) | PySpark needs it |

---

## 🔧 One-Time Setup

### Step 0.1 — Install Python dependencies

```powershell
cd F:\pyspark_study\project_bigquery_auto
pip install -r requirements.txt
```

### Step 0.2 — Configure credentials

```powershell
# Copy the template and fill in your values
copy .env.example .env
notepad .env
```

Fill in:
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — from AWS Console → IAM → Security Credentials
- `S3_BUCKET_NAME` — pick a globally unique name like `yourname-migration-demo`
- `GCP_PROJECT_ID` — from GCP Console
- `GCS_BUCKET_*` — use `{project-id}-data-lake-raw`, etc.

### Step 0.3 — Authenticate GCP

```powershell
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default login
```

---

## 🎯 Stage-by-Stage Execution

---

### Stage 1 — Generate Sample Data

```powershell
python 01_sample_data/generate_data.py
```

**Expected output:**
```
  ✓ orders:        100,000 rows, ~15 MB
  ✓ events:        500,000 rows, ~35 MB
  ✓ user_segments: 1,000 rows
  DATA QUALITY ISSUES (intentional):
  • Orders with null user_id:  ~500
  • Orders with negative amt:  2
  • Duplicate event_ids:       ~5000
```

**Verify:** Check `01_sample_data/output_data/` has `.csv` and `.parquet` files.

---

### Stage 2 — Upload to AWS S3

```powershell
python 02_aws_s3/upload_to_s3.py
```

**Verify:**
```powershell
aws s3 ls s3://YOUR-BUCKET-NAME/ --recursive
```

You should see partition-style paths:
```
source=orders/dt=2026-03-19/orders.parquet
source=events/dt=2026-03-19/events.parquet
source=user_segments/latest/user_segments.parquet
```

---

### Stage 3a — Create GCP Infrastructure (Terraform)

```powershell
cd 03_gcs_ingestion/terraform

# Initialize Terraform
terraform init

# Preview what will be created
terraform plan -var="project_id=YOUR_PROJECT_ID"

# Create it!
terraform apply -var="project_id=YOUR_PROJECT_ID" -auto-approve

cd ../..
```

**Creates:**
- 4 GCS buckets (raw, validated, quarantine, processed)
- 2 BigQuery datasets (staging, analytics)
- 2 BigQuery tables (staging, production with partitioning)

**Verify:**
```powershell
gsutil ls
# Should show gs://your-project-data-lake-raw/ etc.

bq ls staging
bq ls analytics
```

---

### Stage 3b — Transfer S3 → GCS

```powershell
python 03_gcs_ingestion/transfer_s3_to_gcs.py
```

**Verify:**
```powershell
gsutil ls -r gs://YOUR-PROJECT-data-lake-raw/
```

---

### Stage 3c — Validate Landing Files

```powershell
python 03_gcs_ingestion/validate_landing.py
```

**Expected:** Files move from `raw` → `validated` bucket. Bad files go to `quarantine`.

**Verify:**
```powershell
# Raw should be empty now
gsutil ls gs://YOUR-PROJECT-data-lake-raw/

# Validated should have the files
gsutil ls gs://YOUR-PROJECT-data-lake-validated/

# Quarantine should be empty (our test data is clean)
gsutil ls gs://YOUR-PROJECT-data-lake-quarantine/
```

---

### Stage 4 — PySpark Processing

**Option A: Local processing (no GCS needed)**
```powershell
python 04_spark_processing/process_daily_orders.py --date 2026-03-19 --source local
```

**Option B: From GCS (after Stage 3)**
```powershell
python 04_spark_processing/process_daily_orders.py --date 2026-03-19 --source gcs
```

**Expected output:**
```
  Input orders (after dedup):  ~99,000
  Output enriched rows:        ~99,000
  ✓ Output written successfully
```

**Verify:**
```powershell
# Local output
dir 04_spark_processing/output_data/enriched_orders/
```

---

### Stage 5a — Load into BigQuery Staging

```powershell
python 05_bigquery_loading/load_staging.py --date 2026-03-19 --source local
```

**Verify in BQ Console:**
```sql
SELECT COUNT(*) FROM `staging.enriched_orders_staging`;
-- Should show ~99,000 rows
```

---

### Stage 5b — MERGE into Production

Open [BigQuery Console](https://console.cloud.google.com/bigquery) and run:

1. Copy the contents of `05_bigquery_loading/merge_production.sql`
2. Replace `${RUN_DATE}` with `2026-03-19`
3. Run the query

**OR via CLI:**
```powershell
$sql = (Get-Content 05_bigquery_loading/merge_production.sql -Raw) -replace '\$\{RUN_DATE\}', '2026-03-19'
bq query --use_legacy_sql=false $sql
```

**Verify:**
```sql
SELECT COUNT(*), MIN(amount), MAX(amount), AVG(amount)
FROM `analytics.enriched_orders`
WHERE process_date = '2026-03-19';
```

---

### Stage 6 — Run Quality Checks

```powershell
python 07_monitoring/data_quality_checks.py --date 2026-03-19
```

**Expected:**
```
  ✅ PASS: Row Count > 0
  ✅ PASS: No Null Primary Keys
  ✅ PASS: No Negative Amounts
  ✅ PASS: Duplicate Rate < 1%
  ✅ PASS: User Segment Coverage > 50%
  ✅ PASS: Amount Range Sanity
  ✅ PASS: Freshness < 24 Hours
```

---

### Stage 7 — Cost Analysis (BQ Console)

Open [BigQuery Console](https://console.cloud.google.com/bigquery) and run the queries from `07_monitoring/cost_report.sql` to see:

- Query cost by table
- Slot usage over time
- Storage cost by dataset
- Full-table scan detection

---

### Bonus: Local Airflow (Optional)

```powershell
# Install Airflow locally
pip install apache-airflow==2.8.1

# Set Airflow home
$env:AIRFLOW_HOME = "F:\pyspark_study\project_bigquery_auto\06_airflow_orchestration"

# Initialize
airflow db init

# Test the DAG without scheduling
airflow dags test daily_pipeline 2026-03-19
```

---

## 🧹 Cleanup (Avoid Charges)

```powershell
# Delete GCP resources
cd 03_gcs_ingestion/terraform
terraform destroy -var="project_id=YOUR_PROJECT_ID" -auto-approve
cd ../..

# Delete S3 bucket
aws s3 rb s3://YOUR-BUCKET-NAME --force

# Delete local outputs
Remove-Item -Recurse 01_sample_data/output_data
Remove-Item -Recurse 04_spark_processing/output_data
```

---

## 📚 Study Materials

Read the simulation docs in `docs/` for detailed mentor-candidate discussions on every design decision:

| Doc | Topics |
|---|---|
| `simulation_stage_1_2.md` | Architecture choices, Terraform, IAM failures, dedup patterns |
| `simulation_stage_3_4.md` | Data skew, broadcast joins, MERGE vs truncate, schema drift |
| `simulation_stage_5_6.md` | Airflow retries, backfill bugs, cost governance, partition filters |
| `simulation_stage_7_final.md` | Full system design interview, scorecard, gap analysis |
