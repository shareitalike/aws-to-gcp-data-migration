# 👁️ Observability & Reliability Engineering

A production pipeline is only as good as its monitoring. This document explains how we track health, performance, and failures in the AWS-to-GCP migration.

---

## 📊 1. Key Metrics Captured

We capture these metrics at every stage to ensure the pipeline is "Observable":

| Metric | Captured At | Why? |
| :--- | :--- | :--- |
| **Stage Duration** | Airflow Gantt Chart | Identifies bottlenecks (e.g., Spark shuffle taking too long). |
| **Row Count Mismatch** | `data_quality_checks.py` | Detects data loss between S3 landing and BigQuery production. |
| **Quarantine Rate** | `validate_landing.py` | Alerts if source data quality drops (e.g., >1% bad headers). |
| **Billed Bytes** | `cost_report.sql` | Ensures our partitioning/clustering is actually saving money. |

---

## 📝 2. Standard Logging Pattern (Structured)

Every script follows a structured logging format to make debugging easy in **Cloud Logging**:

```python
# Example Log Output
INFO [2026-03-23 15:42] starting ingestion for date=2026-03-19
INFO [2026-03-23 15:43] 100,000 rows read from S3
WARNING [2026-03-23 15:44] 15 rows quarantined (Schema Mismatch)
INFO [2026-03-23 15:45] Spark shuffle finalized across 4 partitions
INFO [2026-03-23 15:46] MERGE COMPLETE: 99,985 rows updated in production
```

---

## 🚑 3. Failure Handling Strategy

What happens when things go wrong?

| Failure Scenario | Automatic Resolution | Manual Action Needed? |
| :--- | :--- | :--- |
| **Spark Out of Memory (OOM)** | Airflow Retry (Max 2) | Yes, if retry fails (needs instance upgrade). |
| **GCS File Missing** | Task Fails / SLA Alert | Yes (Re-push from S3). |
| **Schema Mismatch** | Files moved to `quarantine/` | Yes (Audit bad files). |
| **BigQuery MERGE Conflict** | Atomic Rollback | No (BigQuery handles partial failure rollback). |
| **Late Data Arrival** | Idempotent MERGE | No (Next run will update existing records). |

---

## 🚨 4. Alerting Thresholds
We define "Severity 1" alerts if:
1.  The pipeline fails more than twice consecutively.
2.  The **Data Quality Score** drops below 98%.
3.  The **Process Duration** exceeds 4 hours for a single day.
