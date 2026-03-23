import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Setup paths and environment
PROJECT_DIR = Path(__file__).parent.parent
load_dotenv(PROJECT_DIR / ".env")

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def run_command(command, task_id):
    log(f"Starting task: {task_id}", "INFO")
    log(f"Executing: {command}", "DEBUG")
    
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        cwd=PROJECT_DIR,
        env=env
    )
    
    for line in process.stdout:
        print(f"  | {line.strip()}")
        
    process.wait()
    
    if process.returncode == 0:
        log(f"Task {task_id} completed successfully!", "SUCCESS")
    else:
        log(f"Task {task_id} failed with exit code {process.returncode}", "ERROR")
        sys.exit(1)

def main():
    run_date = "2026-03-19" # Simulation date
    
    print("="*80)
    print(f"🚀 MOCK AIRFLOW ORCHESTRATOR — Pipeline Run for {run_date}")
    print("="*80)
    
    # 1. Validate Landing Files
    run_command(
        "python 03_gcs_ingestion/validate_landing.py",
        "validate_landing"
    )
    
    # 2. Spark processing
    run_command(
        f"python 04_spark_processing/process_daily_orders.py --date {run_date} --source local",
        "spark_process"
    )
    
    # 3. Load to BQ Staging
    run_command(
        f"python 05_bigquery_loading/load_staging.py --date {run_date} --source local",
        "bq_load_staging"
    )
    
    # 4. BigQuery Merge (Using the bq CLI)
    # We substitute the date in the SQL file on the fly
    sql_file = PROJECT_DIR / "05_bigquery_loading" / "merge_production.sql"
    with open(sql_file, "r") as f:
        sql = f.read().replace("${RUN_DATE}", run_date)
    
    # Simple check if current sql has hardcoded date from previous step, adapt if needed
    if "2026-03-19" in sql:
        final_sql = sql
    else:
        final_sql = sql
        
    log("Starting task: bq_merge_production", "INFO")
    # Using a temporary file for the SQL to avoid shell escaping issues
    tmp_sql = PROJECT_DIR / "05_bigquery_loading" / "tmp_merge.sql"
    with open(tmp_sql, "w") as f:
        f.write(final_sql)
        
    run_command(
        f"bq query --use_legacy_sql=false < \"{tmp_sql}\"",
        "bq_merge_production"
    )
    os.remove(tmp_sql)
    
    # 5. Data Quality Checks
    run_command(
        f"python 07_monitoring/data_quality_checks.py --date {run_date}",
        "quality_checks"
    )
    
    print("="*80)
    print("✅ PIPELINE ORCHESTRATION COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()
