# 📄 Senior DE Resume & Interview Guide: AWS to GCP Migration

This project is not just a "script," it's a **Production Data Architecture**. Use this guide to translate your work into "Strong Hire" signals for your resume and interviews.

---

## 🚀 1. Resume Bullet Points (Copy-Paste Ready)

*   **Designed and implemented an idempotent data migration pipeline** (AWS S3 → GCP GCS → BigQuery) using PySpark and SQL, ensuring zero data duplication during retries.
*   **Engineered a scalable "Landing-to-Validated" pattern** in GCS with automated Python-based data validation and a quarantine mechanism for corrupt records.
*   **Built distributed PySpark enrichment jobs** to join batch transactional data with high-cardinality user dimensions, optimized with proper partitioning on `process_date`.
*   **Optimized BigQuery query performance and cost** by implementing table partitioning and clustering via Terraform (IaC).
*   **Developed a comprehensive Data Quality Framework** running post-load checks (Null PKs, Range Sanity, Freshwater checks) to ensure 99.9% data reliability.
*   **Managed Infrastructure as Code (IaC)** using Terraform to automate the provisioning of GCS buckets, BigQuery datasets, and IAM policies.

---

## 🛠️ 2. Mapping Work to "Strong Hire" Signals

| Interviewer Signal | How WE implemented it in this project | File Reference |
| :--- | :--- | :--- |
| **Idempotency** | Used BigQuery `MERGE` statement instead of `INSERT` to handle updates/re-runs without duplicates. | `05_bigquery_loading/merge_production.sql` |
| **Partitioning/Cost** | Tables partitioned by `process_date` and clustered by `order_id` to minimize slot usage and cost. | `03_gcs_ingestion/terraform/main.tf` |
| **Failure Handling** | Built a "Quarantine" logic where non-compliant files are physically isolated from the valid stream. | `03_gcs_ingestion/validate_landing.py` |
| **Observability** | Automated Data Quality checks measuring Null PKs, Duplicate Rates, and Value Ranges. | `07_monitoring/data_quality_checks.py` |
| **Automation Flow** | Defined a DAG (Directed Acyclic Graph) to orchestrate tasks in a logical, dependency-aware order. | `06_airflow_orchestration/dags/daily_pipeline.py` |

---

## 💡 3. Interview "Killer" Questions & Answers

### Q1: "Why did you use BigQuery MERGE instead of just clearing and loading the table?"
**A:** "Truncate-and-load is risky if the job fails halfway, leaving the production table empty. Using `MERGE` makes the load **idempotent**. Even if the pipeline runs three times for the same date, BigQuery handles the deduplication at the DB level, ensuring the destination table is always in a consistent state without downtime."

### Q2: "How did you handle Data Skew in your Spark join?"
**A:** "For this specific volume, I kept it simple, but we partitioned the output by `process_date`. For larger volumes, I would implement **Broadcast Joins** for smaller dimension tables (like our `user_segments`) to avoid shuffles across the cluster."

### Q3: "What happens if a file with a bad schema arrives in your GCS raw bucket?"
**A:** "I implemented a `Validation Gate` before the Spark job ever touches the data. Using `validate_landing.py`, we check the header and basic schema. If it's bad, the file is moved to a `Quarantine` bucket and an alert is triggered, preventing the downstream Spark and BigQuery jobs from failing or corrupting data."

---

## 📈 4. Proposed Feature Additions (Next Level)
If you want to reach **Staff Engineer** level:
1.  **GCS Lifecycle Rules:** Use Terraform to automatically delete "temp" files in GCS after 7 days (Cost control!).
2.  **Great Expectations:** Integrate the Great Expectations library for even deeper data profiling.
3.  **Slack/Email Alerting:** Add a `FailureCallback` in the Airflow DAG to notify you immediately on Slack.
