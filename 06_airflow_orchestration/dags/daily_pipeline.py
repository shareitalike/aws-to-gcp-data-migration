"""
Stage 6: Airflow DAG — orchestrate the full pipeline locally.
This version uses dbt for the core transformation (MERGE) and Quality Checks.

Install: pip install apache-airflow==2.8.1
Usage:
  1. export AIRFLOW_HOME=$(pwd)/06_airflow_orchestration
  2. airflow standalone
  3. Trigger DAG 'daily_pipeline' from the UI
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator

# Dynamic path resolution to ensure it works anywhere on your machine
PROJECT_DIR = str(Path(__file__).parent.parent.parent)
DBT_DIR = os.path.join(PROJECT_DIR, "08_dbt_transformations")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,  # Ensures sequential processing of days
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["aws-to-gcp", "dbt", "pyspark"],
    doc_md="""
    ## End-to-End AWS-GCP Medallion Pipeline
    1. **Spark**: Enriches raw data.
    2. **Load**: Moves Parquet to BQ Staging.
    3. **dbt**: Performs Atomic MERGE into Production and runs Quality tests.
    """,
) as dag:

    # Task 1: Spark Processing (Clean & Enrich)
    spark_process = BashOperator(
        task_id="spark_enrichment",
        bash_command=(
            f"python {PROJECT_DIR}/04_spark_processing/process_daily_orders.py "
            "--date {{ ds }} --source local"
        ),
    )

    # Task 2: Load to BigQuery Staging (Raw Load)
    bq_load_staging = BashOperator(
        task_id="bq_load_staging",
        bash_command=(
            f"python {PROJECT_DIR}/05_bigquery_loading/load_staging.py "
            "--date {{ ds }} --source local"
        ),
    )

    # Task 3: dbt Transformation (Incremental MERGE)
    # Note: We pass the project dir and profile dir to ensure dbt finds everything
    dbt_run = BashOperator(
        task_id="dbt_transformation_merge",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --profiles-dir . --target dev"
        ),
    )

    # Task 4: dbt Quality Tests (Circuit Breaker)
    dbt_test = BashOperator(
        task_id="dbt_quality_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --profiles-dir . --target dev"
        ),
    )

    # Flow: Spark -> Load -> Transform -> Test
    spark_process >> bq_load_staging >> dbt_run >> dbt_test
