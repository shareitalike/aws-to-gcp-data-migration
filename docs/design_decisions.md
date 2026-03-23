# ⚖️ Engineering Design Decisions

This document outlines the high-level design choices made for the AWS-to-GCP migration pipeline. These decisions reflect a **Senior-level** focus on scalability, reliability, and cost-efficiency.

---

## ☁️ 1. Why PySpark instead of Pandas?
While our current volume is 100K records, we designed for **Horizontal Scalability**.
*   **Pandas (In-Memory):** Great for small datasets, but fails when the data exceeds the RAM of a single machine.
*   **PySpark (Distributed):** Processes data across a cluster. By using Spark, our pipeline can handle **TB-level data** tomorrow without any code changes.
*   **AQE (Adaptive Query Execution):** We leveraged Spark 3.x's AQE to automatically handle data skew and optimize shuffle partitions at runtime.

---

## 🗄️ 2. Why BigQuery Partitioning & Clustering?
BigQuery's pricing model is based on "bytes scanned."
*   **Partitioning (by `process_date`):** Allows us to perform **Incremental Loads** and **Backfills** efficiently. A query for "Yesterday's Revenue" only scans yesterday's slice, reducing cost by 99% compared to a full table scan.
*   **Clustering (by `order_id`):** Physically organizes data. This significantly speeds up **MERGE operations** and point-lookups for specific orders, as BigQuery can "prune" the search space.

---

## 🕰️ 3. Why Airflow instead of Cron?
A production pipeline needs more than just a timer.
*   **Dependency Management:** Airflow's DAG ensures that the Spark job *only* starts if the GCS transfer and validation tasks succeed.
*   **Retry Logic:** If a network blip occurs during the S3-to-GCS transfer, Airflow will automatically retry the task up to 3 times before alerting.
*   **Observability:** The Airflow UI providing a visual Gantt chart of runtimes, making it easy to identify performance bottlenecks.

---

## 🛡️ 4. Why GCS "Landing-to-Validated" Pattern?
*   **Immutability:** We never modify the raw files from AWS. This allows us to "Reprocess" any day from scratch if a bug is found in our enrichment logic.
*   **Validation Gate:** By separating `raw/` from `validated/`, we ensure that the Spark compute costs are only incurred for high-quality, schema-correct data.
