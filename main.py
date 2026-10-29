# main.py
import sys
import subprocess
from pipeline.ingest import run_ingestion
from pipeline.loader import JobDataPipeline

def main():
    try:
        print("=== Step 1: Ingesting Data from Adzuna API ===")
        json_path = run_ingestion()

        print("=== Step 2: Staging Data & Uploading Raw Payload to S3/RDS ===")
        pipeline = JobDataPipeline()
        pipeline.process_and_load(json_path)

        print("=== Step 3: Executing dbt Models ===")
        subprocess.run(
            ["dbt", "run", "--profile", "analytics", "--profiles-dir", "profiles"],
            cwd="analytics",
            check=True  # Raises CalledProcessError if dbt returns non-zero exit code
        )

        print("=== Step 4: Executing dbt Data Quality Tests ===")
        subprocess.run(
            ["dbt", "test", "--profile", "analytics", "--profiles-dir", "profiles"],
            cwd="analytics",
            check=True
        )

        print("=== Pipeline Execution Complete ===")

    except Exception as err:
        print(f"CRITICAL: Pipeline execution halted due to unhandled error: {err}")
        sys.exit(1)  # Force non-zero exit code to mark ECS Task as FAILED

if __name__ == "__main__":
    main()