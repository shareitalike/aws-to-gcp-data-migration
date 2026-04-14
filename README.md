# 🏗️ Cloud Data Engineering & ETL Migration Pipeline

A professional-grade, end-to-end **Data Engineering** project migrating 100K+ transactional records from AWS S3 to Google Cloud BigQuery using **PySpark**, **Terraform**, and **Airflow**.

---

## 📈 Business Problem
A growing e-commerce company is migrating its analytical platform from AWS to GCP to consolidate its data ecosystem and leverage **BigQuery's serverless scale**. The goal is to move legacy order data while ensuring zero data loss, high reliability, and cost-optimized processing.

## 🛠️ Key Challenges Solved
*   **Schema Drift:** Implemented a **Data Contract** and validation gate to prevent corrupt source data from breaking downstream analytics.
*   **Data Integrity:** Developed an **Idempotent MERGE pattern** in BigQuery to handle job retries and duplicate records gracefully.
*   **Performance at Scale:** Utilized **Spark AQE** and **Broadcast Joins** to optimize distributed processing of 100K+ records.
*   **Cost Efficiency:** Designed **Partitioned and Clustered** tables in BigQuery to reduce query scan costs by up to 90%.

---

---

## 🏛️ System Architecture
We utilize a **Medallion-inspired arrival pattern** (Bronze ➔ Silver ➔ Gold) with a strict schema validation gate.

![Architecture Diagram](docs/architecture.md)

| Component | AWS Service | GCP Service | Why? |
| :--- | :--- | :--- | :--- |
| **Object Storage** | Amazon S3 | Cloud Storage | Object-level immutability & scale. |
| **Data Processing** | EMR / Glue | Dataproc (Spark) | Distributed join & aggregation. |
| **Warehouse** | Redshift | BigQuery | Serverless scale & cost-efficiency. |
| **Orchestration** | MWAA (Airflow) | Cloud Composer | Complex dependency management. |

---

## 🚀 Technical Deep Dives (Senior Level)
*   **[Full Architecture Diagram](docs/architecture.md):** Detailed visualization of the Medallion data flow.
*   **[Pipeline Observability](docs/PIPELINE_OBSERVABILITY.md):** Real-time Data Quality Dashboards & reconciliation logic.
*   **[Design Decisions](docs/design_decisions.md):** The "Why" behind Spark, BigQuery, and Airflow.
*   **[dbt Incremental Logic](08_dbt_transformations/models/core/enriched_orders.sql):** High-watermark processing for warehouse cost-efficiency.
*   **[Data Contract & Schema](docs/data_contract.md):** How we enforce quality between AWS and GCP.
*   **[Automated Testing](tests/test_transform.py):** PyTest suite for verifying Spark transformations.

---

## 🛡️ Stability & Reliability
*   **Validation Gate:** A Python-based pre-load gate that quarantines corrupt data.
*   **DQ Monitoring:** Automated row count and null check framework.
*   **Testing:** 100% code coverage for core transformation logic.

---

## 🏁 How to Run
1.  **Configure Environment:** Populate `.env` with cloud credentials.
2.  **Infrastructure:** Run `terraform apply` in `03_gcs_ingestion/terraform/`.
3.  **Local Job Execution:**
    ```bash
    # Run full daily load
    python 04_spark_processing/process_daily_orders.py --date 2026-03-19 --source local
    
    # Run unit tests
    python -m pytest tests/test_transform.py
    ```
