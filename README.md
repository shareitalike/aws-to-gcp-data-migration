# ☁️ AWS-to-GCP Data Migration Pipeline

A professional-grade, end-to-end data engineering project migrating e-commerce workloads from AWS S3 to Google Cloud BigQuery using **PySpark**, **Terraform**, and **Airflow**.

---

## 🎯 Problem Statement
Organizations frequently move workloads between clouds to optimize for cost, performance, and managed service offerings (like BigQuery). This project demonstrates a **Production-Ready** migration strategy focusing on **Idempotency**, **Data Quality**, and **Infrastructure as Code (IaC)**.

## 🏛️ Architecture & Service Mapping
We utilize a **Medallion-inspired arrival pattern** with a strict schema validation gate.

| Component | AWS Service | GCP Service | Why? |
| :--- | :--- | :--- | :--- |
| **Object Storage** | Amazon S3 | Cloud Storage | Object-level immutability & scale. |
| **Data Processing** | EMR / Glue | Dataproc (Spark) | Distributed join & aggregation. |
| **Warehouse** | Redshift | BigQuery | Serverless scale & cost-efficiency. |
| **Orchestration** | MWAA (Airflow) | Cloud Composer | Complex dependency management. |

> [!TIP]
> **View Detailed Architecture Diagram & Service Mapping:** [architecture.md](docs/architecture.md)

---

## 🚀 Key Engineering Features

### 1. **Idempotent Design (MERGE Patterns)**
Instead of simple inserts, we use **BigQuery MERGE** to handle updates and retries. This ensures our production table remains consistent even if a job runs multiple times.
*   **See Implementation:** [merge_production.sql](05_bigquery_loading/merge_production.sql)

### 2. **Performance & Cost Optimization**
*   **Partitioning:** All tables are partitioned by `process_date`.
*   **Clustering:** Indexed by `order_id` to optimize filter & join performance.
*   **AQE (Adaptive Query Execution):** Enabled in Spark to handle data skew and coalescing.

### 3. **Production Data Reliability**
*   **Validation Gate:** A Python-based pre-load gate that quarantines corrupt data.
*   **DQ Monitoring:** Automated row count and null check framework.
*   **See Quality Checks:** [data_quality_checks.py](07_monitoring/data_quality_checks.py)

---

## 🛠️ Tech Stack
*   **Languages:** Python (PySpark), SQL (BigQuery Dialect).
*   **Infrastructure:** Terraform (GCP Provider).
*   **Cloud:** AWS (S3), GCP (GCS, Dataproc, BigQuery).
*   **Orchestration:** Apache Airflow.

---

## 📖 Deep Dives for Interviewers
*   [Technical Tradeoffs & Security](docs/tradeoffs_and_security.md): Why BigQuery over Snowflake?
*   [Interview Script & Follow-ups](docs/interview_script_migration.md): Word-for-word answers for recruiters.
*   [Code-to-Concept Mapping](docs/interview_code_mapping.md): How our code maps to "Senior DE" signals.
*   [Resume Gap Analysis](docs/resume_gap_analysis.md): How this project meets "Staff Engineer" criteria.

---

## 🏁 How to Run
1.  **Configure Environment:** Populate `.env` with cloud credentials.
2.  **Infrastructure:** Run `terraform apply` in `03_gcs_ingestion/terraform/`.
3.  **Local Simulation:**
    ```bash
    python 06_airflow_orchestration/mock_orchestrator.py
    ```
