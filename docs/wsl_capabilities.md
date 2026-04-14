# 🐧 WSL Training Guide tailored for your Data Migration Project

This guide expands on how to practically use your new Ubuntu WSL environment for your `project_bigquery_auto` data migration project. Think of this as your cheat sheet for acting like a Senior Data Engineer on a Linux server.

---

## 1. Bridging Windows and Linux
The most powerful feature of WSL is that it shares files with Windows. You can write your PySpark or Airflow code in Windows (using VS Code) and run it in Linux.

**Key Commands:**
*   `cd /mnt/f/pyspark_study/project_bigquery_auto` - This is how you navigate from Linux directly into your Windows `F:` drive project folder.
*   `explorer.exe .` - (Yes, with the dot!) Run this inside Ubuntu. It will instantly open a Windows File Explorer window precisely at your current Linux directory. Extremely useful!
*   `code .` - Opens VS Code in Windows connected to your current Linux folder.

## 2. Fast Data Inspection (No Excel Needed!)
When dealing with massive CSV or JSON files (like your simulated AWS landing data), opening them in Excel or Notepad will crash your computer. Linux tools handle gigabytes of data instantly.

*Navigate to your project directory first, then try these:*

**Check how many rows are in a file:**
*   `wc -l my_massive_data.csv` -> Returns the exact line count almost instantly.

**Peek at the first 5 or last 5 rows:**
*   `head -n 5 01_raw_data/sales.csv` -> See headers and the first 4 rows to check column structures.
*   `tail -n 5 01_raw_data/sales.csv` -> See the absolute latest data appended to the bottom.

**Find specific data (like searching for a specific Order ID):**
*   `grep "ORD_1005" 01_raw_data/sales.csv` -> Instantly searches millions of rows and prints only the row with that Order ID.

## 3. Running Your Migration Python Scripts
Since we installed Python 3 and the virtual environment in Ubuntu, you should run your PySpark and execution scripts here to perfectly mimic a deployed cloud environment.

**How to run your scripts:**
1.  Make sure your Airflow virtual environment is active (your terminal should say `(airflow_env)`).
2.  Navigate to your folder: `cd /mnt/f/pyspark_study/project_bigquery_auto`
3.  Execute your PySpark processing:
    *   `python3 04_spark_processing/process_daily_orders.py`

## 4. Process Management (Fixing "stuck" ports)
Earlier, your Airflow webserver couldn't start because port `8080` was in use. As a Data Engineer, you need to know how to kill stuck processes.

**Find out exactly what program is using port 8080:**
*   `sudo lsof -i :8080` (This lists the PID - Process ID - of the culprit).

**Forcefully kill that process:**
*   `kill -9 <PID>` (Replace `<PID>` with the number from the `lsof` command).

**Monitor your server's RAM and CPU usage (Airflow and Spark can be heavy!):**
*   `htop` (Press `q` to quit). This is the Linux equivalent of the Windows Task Manager, but much more detailed.

## 5. Creating Automation Scripts (Bash)
In production, you rarely run Python scripts one by one manually. You write a `.sh` (Bash) script to chain them together. 

**Example: Let's pretend you want a script to run your AWS-to-GCP pipeline locally.**
You would create a file called `run_pipeline.sh` inside Ubuntu with this text:

```bash
#!/bin/bash
echo "Starting Data Migration Pipeline..."

# Step 1: Generate Data
python3 01_generate_data.py
echo "Data generated."

# Step 2: Validate Data
python3 03_gcs_ingestion/validate_landing.py
echo "Validation complete."

# Step 3: PySpark Processing
python3 04_spark_processing/process_incremental_orders.py
echo "PySpark processing finished! Pipeline Success!"
```

**How to make it executable and run it:**
1.  `chmod +x run_pipeline.sh` (Grants Linux execution permissions to the file).
2.  `./run_pipeline.sh` (Runs the entire pipeline end-to-end).

---
*By mastering these specific Linux commands alongside your PySpark and Airflow skills, you instantly elevate your profile from someone who "knows code" to a Staff-level engineer who "knows systems."*
