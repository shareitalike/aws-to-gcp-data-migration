"""
Stage 6: Airflow DAG — orchestrate the full pipeline locally.

This DAG works with LOCAL Airflow (not Cloud Composer).
Install: pip install apache-airflow==2.8.1

Runs the pipeline:
  validate_gcs → spark_process → bq_load_staging → bq_merge → quality_check

Usage:
  1. export AIRFLOW_HOME=$(pwd)/06_airflow_orchestration
  2. airflow db init
  3. airflow dags test daily_pipeline 2026-03-19
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_DIR = str(Path(__file__).parent.parent.parent)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,  # ensure sequential date processing
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}


def check_processed_exists(**ctx):
    """Skip processing if already done for this date."""
    from pathlib import Path
    output_dir = (
        Path(PROJECT_DIR)
        / "04_spark_processing" / "output_data" / "enriched_orders"
        / f"process_date={ctx['ds']}"
    )
    if output_dir.exists() and list(output_dir.glob("*.parquet")):
        print(f"Already processed {ctx['ds']}, skipping")
        return False
    return True


def run_quality_checks(**ctx):
    """Post-load validation against BigQuery."""
    import os
    from google.cloud import bigquery
    from dotenv import load_dotenv

    load_dotenv(Path(PROJECT_DIR) / ".env")
    project_id = os.getenv("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)
    ds = ctx["ds"]

    # Check 1: Row count > 0
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `analytics.enriched_orders`
        WHERE process_date = '{ds}'
    """
    result = list(client.query(query).result())
    count = result[0].cnt
    if count == 0:
        raise ValueError(f"QUALITY FAIL: 0 rows for {ds}")
    print(f"  ✓ Row count: {count:,}")

    # Check 2: No null primary keys
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `analytics.enriched_orders`
        WHERE process_date = '{ds}' AND order_id IS NULL
    """
    result = list(client.query(query).result())
    nulls = result[0].cnt
    if nulls > 0:
        raise ValueError(f"QUALITY FAIL: {nulls} null order_ids")
    print(f"  ✓ No null PKs")

    # Check 3: No negative amounts
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `analytics.enriched_orders`
        WHERE process_date = '{ds}' AND amount < 0
    """
    result = list(client.query(query).result())
    negatives = result[0].cnt
    if negatives > 0:
        raise ValueError(f"QUALITY FAIL: {negatives} negative amounts")
    print(f"  ✓ No negative amounts")

    print(f"\n  ✅ All quality checks passed for {ds}")


with DAG(
    dag_id="daily_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["migration", "demo"],
    doc_md="""
    ## Daily Enriched Orders Pipeline
    Transfers, validates, processes, and loads e-commerce data.
    """,
) as dag:

    # Task 1: Check if already processed (idempotency)
    check_already_done = ShortCircuitOperator(
        task_id="check_already_processed",
        python_callable=check_processed_exists,
    )

    # Task 2: Validate landing files
    validate = BashOperator(
        task_id="validate_landing",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python 03_gcs_ingestion/validate_landing.py"
        ),
    )

    # Task 3: Run Spark processing (local)
    spark_process = BashOperator(
        task_id="spark_process",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python 04_spark_processing/process_daily_orders.py "
            "--date {{ ds }} --source local"
        ),
    )

    # Task 4: Load into BQ staging
    bq_load = BashOperator(
        task_id="bq_load_staging",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python 05_bigquery_loading/load_staging.py "
            "--date {{ ds }} --source local"
        ),
    )

    # Task 5: Run MERGE in BQ
    bq_merge = BashOperator(
        task_id="bq_merge_production",
        bash_command=(
            "bq query --use_legacy_sql=false "
            "\"$(cat " + f"{PROJECT_DIR}" + "/05_bigquery_loading/merge_production.sql "
            "| sed 's/${RUN_DATE}/{{ ds }}/g')\""
        ),
    )

    # Task 6: Quality checks
    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    # DAG flow
    (check_already_done
     >> validate
     >> spark_process
     >> bq_load
     >> bq_merge
     >> quality)
