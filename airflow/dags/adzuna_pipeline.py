from datetime import datetime, timedelta
import sys
import os
import glob
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pipeline import loader
from pipeline import api_data

def log_task_failure(context):
    """
    Extracts failure metadata out of the task context and prints clear diagnostics.
    """
    task_id = context.get('task_instance').task_id
    run_id = context.get('run_id')
    exception = context.get('exception')
    execution_date = context.get('execution_date')
    
    print("\n" + "="*60)
    print(f"🚨 PIPELINE TASK FAILURE DETECTED 🚨")
    print(f"Timestamp:      {datetime.utcnow().isoformat()} UTC")
    print(f"DAG Run:        {run_id}")
    print(f"Failed Task:    {task_id}")
    print(f"Execution Date: {execution_date}")
    print("-" * 60)
    print(f"Exception details:\n{exception}")
    print("="*60 + "\n")

# 2. Attach it to your DAG's default_args
default_args = {
    'owner': 'arfan_shaikh',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': log_task_failure, # Applied globally to all tasks in the DAG
}

with DAG(
    dag_id='adzuna_job_scraper_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
# Ensure the Airflow process can read your root repository volume mount
    sys.path.append(os.path.abspath("/app"))

    # Task 1 Callable: Extract raw data from the API
    def run_scraper():
        print("--- STEP 1: Starting Adzuna API Extraction ---")
        
        # Call your specific extraction function here
        api_data.get_data
        # api_data.scrape_raw() 
        print("--- Extraction Finished ---")

    # Task 2 Callable: Clean and insert data into Postgres
    def run_loader():
        print("--- STEP 2: Loading Ingested Data to PostgreSQL ---")
        raw_data_dir = "/app/data/raw"
        json_files = glob.glob(os.path.join(raw_data_dir, "*.json"))
        if not json_files:
            raise FileNotFoundError(f"No job data JSON files found in {raw_data_dir}!")
            
        # 3. Sort by modification time to automatically pick the absolute newest file
        latest_file = max(json_files, key=os.path.getmtime)
        print(f"Targeting newest raw data payload: {latest_file}")
        
        # 4. Pass the correct path to your loader function
        loader.load_json_to_db(latest_file)

        print("--- Loading Finished ---")


    default_args = {
        'owner': 'arfan_shaikh',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    with DAG(
        dag_id='adzuna_job_scraper_pipeline',
        default_args=default_args,
        description='End-to-end data pipeline: Scrape -> Load -> dbt Transform',
        schedule_interval=None,  # Triggered manually
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['ingestion', 'analytics'],
    ) as dag:

        # 1. SCRAPE TASK
        scrape_task = PythonOperator(
            task_id='scrape',
            python_callable=run_scraper,
        )

        # 2. LOAD TASK
        load_task = PythonOperator(
            task_id='load',
            python_callable=run_loader,
        )

        # 3. DBT BUILD TASK
        # Note: Using 'docker exec' ensures this runs inside your dedicated dbt container environment
        dbt_build_task = BashOperator(
            task_id='dbt_build',
            bash_command='docker exec dbt-container dbt build --profiles-dir /root/.dbt',
        )

        # Set up the sequential task dependencies (Lineage)
        scrape_task >> load_task >> dbt_build_task
    pass