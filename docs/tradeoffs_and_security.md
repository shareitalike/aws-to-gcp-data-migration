# ⚖️ Technical Tradeoffs & Security Design

Every architecture is the result of weighing options. This document explains the **"Why"** behind our technical choices.

## 1. Cloud Warehouse: BigQuery vs Snowflake

For this migration, we chose **BigQuery** for the following reasons:

| Feature | BigQuery (GCP) | Snowflake | Why it mattered for us? |
| :--- | :--- | :--- | :--- |
| **Operational Effort** | Serverless / No Infra | Virtual Warehouses | Reduced DevOps overhead; no cluster sizing needed. |
| **Pricing Model** | On-demand / Pay-per-scan | Per-minute compute | Lower cost for periodic batch workloads. |
| **Data Lake Integration** | Native GCS / External Tables | External Tables | Seamless integration with our GCS-based data lake. |
| **Scalability** | Global sharing of slots | Scaling groups | Instant scaling without manual configuration. |

---

## 2. Ingestion Pattern: Batch vs Streaming

We chose a **Batch (Daily)** approach for this migration.

*   **Why Batch?** E-commerce transaction data (like our `orders`) often requires complex joins and window functions across several thousand rows to calculate business metrics. Batch processing is more cost-efficient and easier to audit for daily reconciliation.
*   **Future Streaming:** To support near-real-time analytics (e.g., live stock tracking), we would extend the architecture to use **GCP Pub/Sub** and **Dataflow (Apache Beam)** for lower-latency ingestion.

---

## 3. Data Processing: Dataproc (Spark) vs Dataflow (Beam)

| Component | Choice | Rationale |
| :--- | :--- | :--- |
| **Spark on Dataproc** | **Selected** | Allows literal "lift-and-shift" of existing Spark workloads from AWS EMR with minimal code changes. Highly flexible for batch transformations. |
| **GCP Dataflow** | **Not Selected** | While powerful for streaming, Dataflow requires a different programming model (Beam). For a migration from Spark-heavy stacks, Dataproc is safer and faster. |

---

## 4. Security: Least Privilege Access (IAM)

We designed the service permissions following the "Principle of Least Privilege":

1.  **Ingestion Service Account:** Only has `roles/storage.objectCreator` permissions for the `raw/` bucket.
2.  **Processing Service Account:** Only has `roles/storage.objectAdmin` on the `validated/` and `processed/` buckets.
3.  **Loading Service Account:** Only has `roles/bigquery.dataEditor` on the `staging` dataset.
4.  **Monitoring Service Account:** Only has `roles/bigquery.jobUser` (read-only queries).

**Impact:** This design ensures that even if one service account is compromised, the entire database/data lake remains protected.
