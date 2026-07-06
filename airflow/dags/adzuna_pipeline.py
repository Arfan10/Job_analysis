import sys
import os
import glob
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException  # Required to breach thresholds

# Ensure the Airflow process can read your root repository volume mount
sys.path.append(os.path.abspath("/app"))

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

default_args = {
    'owner': 'arfan_shaikh',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': log_task_failure,
}

with DAG(
    dag_id='adzuna_job_scraper_pipeline',
    default_args=default_args,
    description='End-to-end data pipeline: Scrape -> Load -> Quality Check -> dbt Transform',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'analytics', 'quality-control'],
) as dag:

    def run_scraper():
        print("--- STEP 1: Starting Adzuna API Extraction ---")
        api_data.get_data() 
        print("--- Extraction Finished ---")

    def run_loader():
        print("--- STEP 2: Loading Ingested Data to PostgreSQL ---")
        raw_data_dir = "/app/data/raw"
        json_files = glob.glob(os.path.join(raw_data_dir, "*.json"))
        if not json_files:
            raise FileNotFoundError(f"No job data JSON files found in {raw_data_dir}!")
            
        latest_file = max(json_files, key=os.path.getmtime)
        print(f"Targeting newest raw data payload: {latest_file}")
        
        loader.load_json_to_db(latest_file)
        print("--- Loading Finished ---")

    # Task 3 Callable: Validate data volume and quality metrics
    def run_data_quality_check():
        print("--- STEP 3: Running Data Quality Threshold Checks ---")
        
        # Define thresholds
        MIN_ROW_COUNT = 10
        MAX_NULL_RATE = 0.15  # Fail if more than 15% of records are missing critical data
        
        # Connect via our hybrid fallback-safe loader connection configuration
        conn = loader.connect_db()
        cursor = conn.cursor()
        
        try:
            # Query row count and specific null ratios (e.g., checking 'title' and 'id' columns)
            # Adjust the table name ('raw_jobs') and column names to match your schema
            query = """
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN job_title IS NULL THEN 1 END)::float / NULLIF(COUNT(*), 0) as title_null_rate,
                    COUNT(CASE WHEN job_id IS NULL THEN 1 END)::float / NULLIF(COUNT(*), 0) as id_null_rate
                FROM raw_jobs;
            """
            cursor.execute(query)
            result = cursor.fetchone()
            
            if not result or result[0] == 0:
                raise AirflowException("Data Quality Failed: Target landing table is completely empty!")
                
            total_rows, title_null_rate, id_null_rate = result
            print(f"📋 QC Metrics | Total Rows: {total_rows} | Title Null Rate: {title_null_rate:.2%} | ID Null Rate: {id_null_rate:.2%}")
            
            # Threshold Validation Logic
            if total_rows < MIN_ROW_COUNT:
                raise AirflowException(f"Data Quality Breach: Total row count ({total_rows}) dropped below minimum safety limit of {MIN_ROW_COUNT}.")
                
            if title_null_rate > MAX_NULL_RATE or id_null_rate > MAX_NULL_RATE:
                raise AirflowException(
                    f"Data Quality Breach: Critical field null rate exceeds acceptable threshold ({MAX_NULL_RATE:.0%}). "
                    f"Found Title Nulls: {title_null_rate:.2%}, ID Nulls: {id_null_rate:.2%}"
                )
                
            print("✨ Data Quality Checks Passed Successfully! Proceeding to transformations.")
            
        finally:
            cursor.close()
            conn.close()

    # --- Task Definitions ---
    scrape_task = PythonOperator(
        task_id='scrape',
        python_callable=run_scraper,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=run_loader,
    )

    # Added Quality Control Step
    quality_check_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=run_data_quality_check,
    )

    dbt_build_task = BashOperator(
        task_id='dbt_build',
        bash_command='docker exec dbt-container dbt build --profiles-dir /root/.dbt',
    )

    # --- Updated Pipeline Lineage Dependencies ---
    scrape_task >> load_task >> quality_check_task >> dbt_build_task