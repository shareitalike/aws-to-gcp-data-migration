# 🐧 Stage 8: Setup Apache Airflow on WSL (Windows Subsystem for Linux)

Since native Windows has significant issues with Airflow's internal database (SQLite), the industry standard for Windows users is to use **WSL**.

Here is how you can set up a real, production-ready Airflow environment on your machine:

### 1. Install WSL (Ubuntu)
Open PowerShell as Administrator and run:
```powershell
wsl --install
```
*Restart your computer after this finishes.*

### 2. Set up the Linux Environment
Open the "Ubuntu" app from your Start menu and run:
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip python3-venv libpq-dev -y
```

### 3. Install Airflow in WSL
```bash
export AIRFLOW_HOME=~/airflow
pip install "apache-airflow[celery]==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
```

### 4. Initialize the Database (The part that fails on Windows!)
```bash
airflow db init
```
*This will work perfectly on Linux/WSL.*

### 5. Create a User and Start Airflow
```bash
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow standalone
```

### 6. Link your Project
In WSL, you can access your Windows files at `/mnt/f/pyspark_study/project_bigquery_auto/`.
You can create a symlink to your project DAGs:
```bash
ln -s /mnt/f/pyspark_study/project_bigquery_auto/06_airflow_orchestration/dags ~/airflow/dags
```

Now you can open `localhost:8080` in your Windows browser and see your pipeline running in a real Linux environment!
