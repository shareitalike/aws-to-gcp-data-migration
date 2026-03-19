# Practical AWS → GCP Migration Project (Free Tier)

A hands-on project using real AWS/GCP services that you can execute step-by-step on free tier accounts.

## Free Tier Constraints & Workarounds

| Service | Free Tier Limit | Our Approach |
|---|---|---|
| **S3** | 5 GB, 2K PUT/month | Generate ~50 MB sample data |
| **GCS** | 5 GB, 5K ops/month | Same small dataset |
| **BigQuery** | 1 TB queries, 10 GB storage | Small tables, partition filters |
| **Dataproc** | ❌ Not free | **Local PySpark** instead |
| **Cloud Functions** | 2M invocations/month | Use `gcloud` CLI scripts |
| **Terraform** | Free (CLI) | Use for GCS + BQ only |
| **Airflow** | ❌ Composer not free | **Local Airflow** (pip install) |

> [!IMPORTANT]
> We use **local PySpark** and **local Airflow** instead of Dataproc/Composer to stay free. The code patterns are identical — only the execution environment changes.

## Proposed Project Structure

```
f:\pyspark_study\project_bigquery_auto\
├── README.md                      # [NEW] Master execution guide
├── requirements.txt               # [NEW] Python dependencies
├── setup_env.ps1                  # [NEW] Environment setup script
│
├── 01_sample_data\
│   └── generate_data.py           # [NEW] Create realistic CSV/Parquet
│
├── 02_aws_s3\
│   ├── upload_to_s3.py            # [NEW] Upload sample data to S3
│   └── s3_config.example.env      # [NEW] AWS credentials template
│
├── 03_gcs_ingestion\
│   ├── terraform\
│   │   ├── main.tf                # [NEW] GCS buckets + BQ dataset
│   │   ├── variables.tf           # [NEW] 
│   │   └── outputs.tf             # [NEW]
│   ├── transfer_s3_to_gcs.py      # [NEW] S3→GCS using boto3+gcs
│   └── validate_landing.py        # [NEW] Schema validation script
│
├── 04_spark_processing\
│   ├── process_daily_orders.py    # [NEW] PySpark enrichment job
│   └── run_local.ps1              # [NEW] Local Spark execution
│
├── 05_bigquery_loading\
│   ├── create_tables.sql          # [NEW] BQ table DDL
│   ├── load_staging.py            # [NEW] GCS→BQ load
│   └── merge_production.sql       # [NEW] Idempotent MERGE
│
├── 06_airflow_orchestration\
│   ├── dags\
│   │   └── daily_pipeline.py      # [NEW] Full Airflow DAG
│   └── setup_local_airflow.ps1    # [NEW] Local Airflow setup
│
└── 07_monitoring\
    ├── data_quality_checks.py     # [NEW] Post-load validation
    └── cost_report.sql            # [NEW] BQ cost analysis query
```

## Proposed Changes

### Component 1: Sample Data Generator
#### [NEW] [generate_data.py](file:///f:/pyspark_study/project_bigquery_auto/01_sample_data/generate_data.py)
- Generate 100K orders + 500K events + 1K user segments
- Realistic fields: order_id, user_id, amount, currency, timestamps
- Output as both CSV (for S3) and Parquet (for direct testing)
- Include intentional data quality issues (nulls, duplicates) for testing

---

### Component 2: AWS S3 Upload
#### [NEW] [upload_to_s3.py](file:///f:/pyspark_study/project_bigquery_auto/02_aws_s3/upload_to_s3.py)
- Upload generated data to S3 with partition structure
- Uses `boto3` with credentials from `.env` file

---

### Component 3: GCS Ingestion + Terraform
#### [NEW] [main.tf](file:///f:/pyspark_study/project_bigquery_auto/03_gcs_ingestion/terraform/main.tf)
- Create GCS buckets (raw, validated, quarantine, processed)
- Create BigQuery dataset + tables
- Lifecycle rules for cost optimization

#### [NEW] [transfer_s3_to_gcs.py](file:///f:/pyspark_study/project_bigquery_auto/03_gcs_ingestion/transfer_s3_to_gcs.py)
- Read from S3, write to GCS raw bucket
- Partition-aware transfer with manifest tracking

#### [NEW] [validate_landing.py](file:///f:/pyspark_study/project_bigquery_auto/03_gcs_ingestion/validate_landing.py)
- Schema validation, size check, dedup via local SQLite
- Move valid → validated prefix, bad → quarantine

---

### Component 4: PySpark Processing (Local)
#### [NEW] [process_daily_orders.py](file:///f:/pyspark_study/project_bigquery_auto/04_spark_processing/process_daily_orders.py)
- Join orders + events, broadcast user_segments
- Deduplicate, enrich, write partitioned Parquet
- AQE enabled, skew handling

---

### Component 5: BigQuery Loading
#### [NEW] [create_tables.sql](file:///f:/pyspark_study/project_bigquery_auto/05_bigquery_loading/create_tables.sql)
- DDL for staging + production tables with partitioning/clustering

#### [NEW] [load_staging.py](file:///f:/pyspark_study/project_bigquery_auto/05_bigquery_loading/load_staging.py)
- Load Parquet from GCS into BQ staging table

#### [NEW] [merge_production.sql](file:///f:/pyspark_study/project_bigquery_auto/05_bigquery_loading/merge_production.sql)
- MERGE statement with partition filter, quality gates

---

### Component 6: Airflow DAG (Local)
#### [NEW] [daily_pipeline.py](file:///f:/pyspark_study/project_bigquery_auto/06_airflow_orchestration/dags/daily_pipeline.py)
- Full DAG wiring all stages
- BashOperator for local Spark, BigQueryOperator for loads
- Retries, SLA, quality checks

---

### Component 7: Monitoring & Quality
#### [NEW] [data_quality_checks.py](file:///f:/pyspark_study/project_bigquery_auto/07_monitoring/data_quality_checks.py)
- Post-load validation (row counts, null PKs, freshness)

#### [NEW] [cost_report.sql](file:///f:/pyspark_study/project_bigquery_auto/07_monitoring/cost_report.sql)
- BQ INFORMATION_SCHEMA query for cost analysis

---

## Verification Plan

### Automated (Run These Commands)
Each stage has a verification step built into the guide:

1. **Data gen**: `python 01_sample_data/generate_data.py` → check output files exist
2. **S3 upload**: `python 02_aws_s3/upload_to_s3.py` → verify with `aws s3 ls`
3. **Terraform**: `terraform plan` → `terraform apply` → verify buckets in console
4. **Transfer**: `python 03_gcs_ingestion/transfer_s3_to_gcs.py` → `gsutil ls`
5. **Validation**: `python 03_gcs_ingestion/validate_landing.py` → check quarantine
6. **Spark**: `python 04_spark_processing/process_daily_orders.py` → check output Parquet
7. **BQ Load**: `python 05_bigquery_loading/load_staging.py` → query staging in BQ console
8. **BQ Merge**: Run merge SQL in BQ console → verify row counts
9. **Quality**: `python 07_monitoring/data_quality_checks.py` → all checks pass

### Manual Verification
- Open GCP Console → BigQuery → verify tables have data
- Open GCP Console → GCS → verify bucket structure
- Run `SELECT COUNT(*) FROM project.analytics.enriched_orders` in BQ
