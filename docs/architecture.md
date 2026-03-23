# 🏗️ Project Architecture & Service Mapping

This document provides a high-level overview of the end-to-end data migration architecture and how AWS services map to their Google Cloud (GCP) equivalents.

## 1. End-to-End Data Flow (Mermaid Diagram)

```mermaid
graph LR
    subgraph "AWS Ecosystem"
        S3[Amazon S3 <br/> 'Raw CSVs']
    end

    subgraph "Ingestion & Validation"
        TRAN[Transfer Script <br/> 'S3 to GCS'] --> GCS_RAW[GCS Bucket <br/> 'raw/']
        VAL[Validation Script <br/> 'Python Gate'] --> GCS_VAL[GCS Bucket <br/> 'validated/']
        GCS_RAW --> VAL
        VAL --> GCS_QUA[GCS Bucket <br/> 'quarantine/']
    end

    subgraph "Processing Layer"
        SPARK[PySpark Job <br/> 'Dataproc / Local'] --> GCS_PRO[GCS Bucket <br/> 'enriched/']
        GCS_VAL --> SPARK
    end

    subgraph "Analytics Warehouse"
        BQ_STG[BigQuery Staging] --> BQ_MER[BigQuery MERGE]
        BQ_MER --> BQ_PROD[BigQuery Production <br/> 'analytics']
        GCS_PRO --> BQ_STG
    end

    subgraph "Orchestration & Quality"
        AIR[Apache Airflow] --> TRAN
        DQ[DQ Checks] --> BQ_PROD
    end
```

---

## 2. Cloud Service Mapping Table

| Component | AWS Service | GCP Service | Why the choice? |
| :--- | :--- | :--- | :--- |
| **Object Storage** | Amazon S3 | Cloud Storage (GCS) | Industry standard for data lakes; HDFS-compatible. |
| **Distributed Compute** | EMR / Glue | Dataproc | Managed Spark/Hadoop cluster; ideal for lift-and-shift migration. |
| **Data Warehouse** | Redshift | BigQuery | Serverless, highly scalable, and superior separation of storage/compute. |
| **Orchestration** | Managed Airflow (MWAA) | Cloud Composer | Native Airflow integration for complex DAG dependencies. |
| **IAM & Security** | AWS IAM | Cloud IAM | Built-in least-privilege access control across all resources. |
| **Monitoring** | CloudWatch | Cloud Logging / Monitoring | Centralized observability for job performance and failure metrics. |

---

## 3. Data Storage Layout (Best Practices)

We implemented a **Medallion-inspired** folder structure in GCS to ensure clear data lineage:

1.  `gs://...raw/`: Immutable landing zone for raw S3 files.
2.  `gs://...validated/`: Schema-verified files ready for processing.
3.  `gs://...quarantine/`: Corrupt or schema-mismatched files for manual audit.
4.  `gs://...processed/`: Enriched Parquet files (partitioned by `process_date`).
