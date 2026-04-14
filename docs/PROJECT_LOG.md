# Data Engineering DevLog: Resumption & Debugging
> **Objective:** Transition back into the project after a hiatus and resolve pipeline failures to finish the staging-to-production migration.

---

## 1. Issue: BigQuery Staging Table Loss (404)
When resuming the project, the primary staging table was found missing in BigQuery.

*   **Before (The Problem):** The `staging.enriched_orders_staging` table was gone, causing downstream `dbt` and monitoring scripts to fail with `404 Not Found`.
*   **Update (The Fix):** 
    - Verified that Stage 6 (PySpark) output still existed in the local filesystem.
    - Used the custom `load_staging.py` script with the `--source local` flag to re-upload the Parquet files to BigQuery.
*   **🎤 Interview Talking Point:** *"I demonstrated project recovery and state management. Instead of re-running the entire expensive ETL pipeline (S3 -> GCS -> Spark), I performed a cost-effective recovery by reloading the persisted intermediate local data into the staging area."*

---

## 2. Issue: Partition Key Column Drop (Spark Quirk)
Even after reloading the data, the final BigQuery load was missing the `process_date` column.

*   **Before (The Problem):** The PySpark code used `.partitionBy("process_date")`. By default, Spark removes the partitioning column from the actual data files and saves it only as a folder name. This "Hive-style" structure is not automatically detected when performing a manual local-to-BQ upload.
*   **Update (The Fix):** 
    - **Triage:** Manually added the column to BigQuery via `ALTER TABLE` to avoid re-running Spark during a time-sensitive session.
    - **Permanent Fix:** Modified the Spark job to create a duplicate `partition_date` column for the folder path, ensuring the original `process_date` stays embedded inside the data file.
*   **🎤 Interview Talking Point:** *"I encountered a common quirk in Spark partitioning. I chose a two-tier resolution: a SQL triage to keep the pipeline moving, and a code-level fix to ensure schema permanence in future runs. This shows my ability to balance development speed with architectural robustness."*

---

## ❄️ Incident 2: BigQuery View Metadata "Freezing"
- **Symptom**: `dbt run` failed with `No column process_date found` even after the column was verified in the underlying table.
- **Cause**: BigQuery views that use `SELECT *` do not automatically pick up new columns added to the source table after the view is created. The schema is "frozen" at the moment of `CREATE VIEW`.
- **Resolution**: Updated `stg_enriched_orders.sql` to explicitly list all 14 columns. This forced dbt and BigQuery to re-compile the view with the correct schema.
- **🎤 Interview Talking Point:** *"I diagnosed a specific BigQuery behavior where 'SELECT *' views become stale after schema changes. By transitioning the staging layer to an explicit column list, I moved the project toward a more stable, declarative 'contract-first' model, which is much more robust for production pipelines."*

---

## 🛡️ Incident 3: Declarative Schema Enforcement 
- **Symptom**: `dbt run` continued to fail with `No column process_date found` despite the table and view schemas being verified as correct in BigQuery.
- **Cause**: The `dbt-fusion` preview adapter was likely using a persistent internal catalog/metadata cache that failed to refresh even after `ALTER` or `CREATE OR REPLACE` commands were issued.
- **Resolution**: Implemented **Declarative Schema Enforcement** by explicitly listing all 14 columns in the `models/staging/sources.yml` file. This tells dbt to bypass its automatic metadata discovery and use the provided schema definition.
- **🎤 Interview Talking Point:** *"When automatic tool discovery fails (a common occurrence in preview/beta software), I take manual control of the metadata. I used dbt's YAML-driven schema enforcement to override the faulty metadata cache, ensuring that the critical 'process_date' field was recognized as a valid source column. This demonstrates my ability to troubleshoot deep into the toolchain to unblock the pipeline."*

*   **Before (The Problem):** The incremental model was reading from a `source()`. The dbt-fusion preview adapter was likely using a cached metadata snapshot of the source table, ignoring the recently added `process_date` column.
*   **Update (The Fix):** 
    - Switched the `enriched_orders.sql` model to read from the staging `view` (`ref`) instead of the raw `source`. 
    - Since the view automatically picks up the full underlying table schema on compilation, this forced dbt to see the column.
*   **🎤 Interview Talking Point:** *"I applied dbt best practices to solve a metadata sync issue. By utilizing a staging model (`ref`) instead of direct source references, I improved the project's lineage and successfully bypassed a tool-specific caching bug, ensuring the transformation pipeline could reach the production layer."*

---

## 🔄 Final Resolution: Column Renaming (`process_date` → `processing_date`)
- **Before (The Problem):** Continued failure in `dbt-fusion` despite using hardcoded schemas and staging views. Suspected "Reserved Word" or "Namespace Conflict" with the name `process_date` in the specific preview version of the dbt adapter.
- **Update (The Fix):** 
    - Renamed the column in BigQuery to `processing_date`.
    - Updated all dbt models, sources, and schema tests to reflect the new name.
- **🎤 Interview Talking Point:** *"In a high-pressure migration with a strict trial deadline, I demonstrated agility by pivoting when a tool-specific bug persisted. By renaming the column, I bypassed a potential reserved-word conflict in the beta toolchain, ensuring the project reached its goal without compromising data integrity."*

