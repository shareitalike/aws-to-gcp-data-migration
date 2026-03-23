# 🛠️ From Answer to Code: The Technical Mapping Guide

Interviewers don't just want to hear the "Answer"—they want to know you actually *wrote* the logic. This guide maps your verbal answers directly to the code we built.

---

## 🏗️ 1. Mapping Your Answers to the Code

| Verbal Concept | Specific File Reference | The "Golden Lines" to Mention |
| :--- | :--- | :--- |
| **Idempotent Loads** | `05_bigquery_loading/merge_production.sql` | Point to the `MERGE INTO ... USING ... ON order_id` section. Mention how it handles `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT`. |
| **Validation Gate** | `03_gcs_ingestion/validate_landing.py` | Mention the `check_schema()` function and how it moves files into `validated/` vs `quarantine/` folders using the `google.cloud.storage` library. |
| **Incremental Loads** | `04_spark_processing/process_daily_orders.py` | Mention the `--date` argument and the `.partitionBy("process_date")` command in the final `.write.parquet()` step. This saves space and cost. |
| **Distributed Join** | `04_spark_processing/process_daily_orders.py` | Explain how you joined `orders_df` with `segments_df` on `user_id` inside the Spark session to enrich the grain of the data. |
| **Infrastructure (IaC)** | `03_gcs_ingestion/terraform/main.tf` | Mention defining `google_bigquery_table` with `partitioning { type = "DAY" }` and `clustering = ["order_id"]`. |

---

## 🔍 5. To what extent will they ask about the code?

Interviewers for Senior DE roles usually **care about logic, not syntax.**

*   **They WILL ask:** *"How did you handle the join between orders and segments if segments is 100x larger than orders?"* (The "Scale" thought process).
*   **They WILL ask:** *"What happens if the BigQuery MERGE fails halfway?"* (Answer: It's an atomic transaction in BigQuery, so nothing happens; it rolls back automatically).
*   **They WON'T ask:** *"On line 42, did you use a single-quote or double-quote?"* 

**The Goal:** You should be able to sketch the **Flow Diagram** of any file on a whiteboard from memory.

---

## 🚑 6. The "Production Problem" Story: The Windows Spark Bug

This is your **best interview story**. It shows you can debug low-level infrastructure.

**Scenario:** *"Tell me about a time you hit a difficult technical blocker."*

**The Story:**
1.  **The Problem:** "While setting up the Spark processing environment on a Windows-based local machine, I hit a persistent `NullPointerException` during the Spark context initialization. The job simply wouldn't start."
2.  **The Investigation:** "I realized that the Spark BlockManager couldn't resolve the local hostname or bind to a network interface properly because of a mismatch between the Windows networking layer and the JVM's expectations."
3.  **The Resolution:** "I researched the internal Spark driver configurations and manually set the `spark.driver.host` and `spark.driver.bindAddress` properties to `localhost`. This forced Spark to ignore the system's external hostname resolution and fixed the deadlock. It taught me that even high-level Spark jobs can be blocked by low-level networking and JVM configurations."

**Why this is a "Strong Hire" signal:** It proves you don't just write "Clean Code," but you also understand the **Java/JVM infrastructure** that runs under Spark.
