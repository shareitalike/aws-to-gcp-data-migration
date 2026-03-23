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

## 🏛️ Architecture & Service Mapping
We utilize a **Medallion-inspired arrival pattern** with a strict schema validation gate.

| Component | AWS Service | GCP Service | Why? |
| :--- | :--- | :--- | :--- |
| **Object Storage** | Amazon S3 | Cloud Storage | Object-level immutability & scale. |
| **Data Processing** | EMR / Glue | Dataproc (Spark) | Distributed join & aggregation. |
| **Warehouse** | Redshift | BigQuery | Serverless scale & cost-efficiency. |
| **Orchestration** | MWAA (Airflow) | Cloud Composer | Complex dependency management. |

---

## 🚀 Technical Deep Dives (Senior Level)
*   **[Design Decisions](docs/design_decisions.md):** The "Why" behind Spark, BigQuery, and Airflow.
*   **[Data Contract & Schema](docs/data_contract.md):** How we enforce quality between AWS and GCP.
*   **[Observability & Reliability](docs/observability.md):** Metrics, logging, and failure handling strategy.
*   **[Incremental ETL Logic](04_spark_processing/process_incremental_orders.py):** High-watermark processing for cost-efficiency.
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
