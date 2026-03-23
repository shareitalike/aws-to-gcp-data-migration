# 📜 Data Contract: Analytical Orders v1.0

A **Data Contract** is an agreement between the Data Producer (AWS) and the Data Consumer (GCP Analytics). It ensures that any change in the source schema does not silently break the downstream dashboard.

---

## 🏗️ 1. Schema Definition (Enforced at Ingestion)

| Field Name | Data Type | Nullable? | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | STRING (UUID) | **NO** | Primary Key. Unique identifier for the transaction. |
| `user_id` | STRING | **NO** | Foreign Key to the User Profile table. |
| `amount` | FLOAT | **NO** | Transaction value. Must be > 0. |
| `currency` | STRING | YES | ISO 4217 Currency Code (e.g., USD). |
| `status` | STRING | **NO** | Order status (COMPLETED, PENDING, CANCELLED). |
| `process_date` | DATE | **NO** | System partition key (YYYY-MM-DD). |

---

## 🛡️ 2. Enforcement Logic

We enforce this contract at two distinct layers:

### Layer 1: Ingestion Gate (`validate_landing.py`)
*   **Header Check:** Ensures all mandatory columns exist.
*   **Quarantine:** Files missing `order_id` or `amount` are moved to `gs://...quarantine/` and are NOT processed by Spark.

### Layer 2: Transformation Gate (`process_daily_orders.py`)
*   **Primary Key Check:** Deduplicates `order_id` to ensure exactly-once semantics.
*   **Range Filter:** Automatically filters out any record with `amount <= 0`.
*   **Schema Evolution:** Spark's `.read.parquet()` is configured to gracefully handle additional columns arriving in the future WITHOUT breaking the existing MERGE logic.

---

## 🔄 3. Change Management
*   **Breaking Changes:** Any removal of columns or type changes (e.g., STRING to INT) requires a new version of the contract (v2.0) and a coordinated backfill.
*   **Non-Breaking:** Adding a new column is handled automatically by the "Schema Evolution" logic.
