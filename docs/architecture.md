# 🏗️ Cloud Data Architecture: AWS to GCP Medallion Pipeline

This diagram illustrates the end-to-end flow of the e-commerce migration project, following a **Medallion Architecture** (Bronze ➔ Silver ➔ Gold) with a dedicated **Orchestration Layer**.

## 📊 System Architecture Diagram

```mermaid
graph TD
    %% Source Layer
    subgraph "AWS Ecosystem (Source)"
        S3[("AWS S3 Raw CSVs")]
    end

    %% Ingestion Layer
    subgraph "Ingestion & Landing (Bronze)"
        GCS_LANDING[("GCS Landing Bucket")]
        TRANSFER{"Cloud Storage Transfer<br/>(SDK / Boto3)"}
    end

    %% Processing Layer
    subgraph "Processing (Silver)"
        subgraph "Custom Airflow Container"
            JAVA["JRE 17 / Spark Libs"]
            SPARK["PySpark Enrichment Logic"]
        end
        GCS_PROCESSED[("GCS Processed Bucket (Parquet)")]
    end

    %% Warehouse Layer
    subgraph "BigQuery Warehouse (Gold)"
        BQ_STAGING[("BQ Staging Table (Daily Transient)")]
        subgraph "dbt Managed Layer"
            DBT_MERGE{"dbt MERGE Logic (Incremental)"}
            BQ_PROD[("BQ Production Table (Medallion Gold)")]
            DBT_TEST["dbt Quality Tests (Circuit Breakers)"]
        end
    end

    %% Control Plane
    subgraph "Orchestration & Control"
        AIRFLOW{{"Airflow DAG<br/>(The Conductor)"}}
        ENV[".env / Secrets Management"]
    end

    %% Relationships
    S3 -->|Transfer Service| TRANSFER
    TRANSFER --> GCS_LANDING
    GCS_LANDING -->|Read| SPARK
    SPARK -->|Write| GCS_PROCESSED
    
    GCS_PROCESSED -->|Load Job| BQ_STAGING
    BQ_STAGING -->|ref| DBT_MERGE
    DBT_MERGE -->|Upsert| BQ_PROD
    BQ_PROD --> DBT_TEST
    
    %% Orchestration Paths
    AIRFLOW -.->|1. Trigger Transfer| TRANSFER
    AIRFLOW -.->|2. Trigger Spark| SPARK
    AIRFLOW -.->|3. Trigger BQ Load| BQ_STAGING
    AIRFLOW -.->|4. Trigger dbt| DBT_MERGE
    ENV -.->|Configure| AIRFLOW
```

---

## 🛠️ Component Breakdown (FOR INTERVIEW)

### 1. Ingestion (AWS ➔ GCP)
- **Tech**: GCS Transfer Service / Google Cloud SDK.
- **Narrative**: *"We treat AWS S3 as our immutable legacy source. We land data in GCS as-is to preserve raw history before any processing happens."*

### 2. Silver Layer (PySpark)
- **Tech**: Apache Spark 3.5.0.
- **Narrative**: *"Spark acts as our heavy-lifter. We perform complex deduplication and join orders with user segments here. We store the result in Parquet format to leverage columnar compression and schema preservation."*

### 3. Gold Layer (BigQuery & dbt)
- **Tech**: BigQuery + dbt-fusion.
- **Narrative**: *"We use the 'Staging-to-Production' design. dbt handles our incremental materialization, ensuring we only MERGE new data each night, significantly reducing BigQuery slot costs."*

### 4. Orchestration (Airflow & Docker)
- **Tech**: Apache Airflow 2.8.1 (LocalExecutor) + Custom Docker Image.
- **Narrative**: *"Airflow is the central nervous system. To ensure high availability and portability, we containerized the entire orchestration suite. Crucially, we extended the base Airflow image with OpenJDK 17 and Spark libraries, allowing high-performance PySpark jobs to run locally within the airflow-scheduler, ensuring that if any stage failed (like a 404 on a GCS bucket), the pipeline would halt and retry gracefully."*
