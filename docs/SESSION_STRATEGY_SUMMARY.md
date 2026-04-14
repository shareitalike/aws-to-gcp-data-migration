# 🧠 Technical Strategy & Troubleshooting Summary: April 14, 2026

This document captures the "Engineering Truths" of the session. Use this to prepare for "Behavioral" and "Technical Depth" interview questions.

---

## 1. The "Spark Networking" Breakthrough
**Problem**: Spark sessions were failing with `NullPointerException` or `Connection Refused` on local Windows execution.
**Technical Root Cause**: Spark was trying to bind to the Windows hostname, which resolved to an external IP or was blocked by the local firewall.
**Senior Fix**: Forced the Spark driver to bind to `127.0.0.1` (loopback). 
**Interview Answer**: *"I encountered networking conflicts on Windows where the Spark driver couldn't communicate with executors. I resolved this by forcing a loopback bind, ensuring absolute stability for the local development environment before moving to the cloud."*

---

## 2. The "dbt-fusion Metadata" Bug
**Problem**: Even after updating SQL models, `dbt run` would fail or use old column names (caching issue).
**Technical Root Cause**: `dbt-fusion` (the cloud-optimized dbt engine) caches metadata in the `target/` directory and doesn't always perform a partial parse correctly during schema renames.
**Senior Fix**: Implemented a "Clean Build" strategy (manual removal of `target/`) and switched to **Explicit Column Selection** instead of `SELECT *` to bypass metadata staleness.
**Interview Answer**: *"I found that relying on SELECT * in a metadata-cached environment like dbt-fusion leads to schema drift. I refactored the models to use explicit column mapping, which makes the pipeline more robust and self-documenting."*

---

## 3. The BigQuery "Partition Filter" Safety
**Problem**: BQ rejected dbt incremental runs with a 400 error.
**Technical Root Cause**: The production table has `require_partition_filter=true`. dbt's standard `SELECT max(process_date)` subquery didn't include a filter, so the BQ dry-run rejected it.
**Senior Fix**: Injected a "Wide" partition filter (`>= '2000-01-01'`) into the Jinja template.
**Interview Answer**: *"When working with BigQuery cost-guardrails, standard tools sometimes fail. I implemented a custom Jinja workaround to satisfy partition-filter requirements, ensuring that our automated dbt models could still run incrementally without compromising the warehouse's cost-safety settings."*

---

## 4. The "Observability" Evolution
**Problem**: Passing tests is not the same as having correct data.
**Strategy**: Added a **Reconciliation Layer**.
**Senior Addition**: A dbt model that performs a monetary reconciliation between Spark staging and BQ Gold.
**Interview Answer**: *"A green pipeline is not enough. I built a Data Quality Dashboard that reconciles Revenue Variance. This proves to stakeholders that the migration from AWS to GCP had zero data loss."*
