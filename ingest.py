import os
import sys
import json
import time
import logging
import datetime as dt
from typing import List, Dict, Any

from pipeline.api_data import get_data
from pipeline.loader import JobDataPipeline  # Updated to import the class

logger = logging.getLogger("IngestEngine")


def fetch_with_retry(max_retries: int = 3, backoff_factor: int = 2) -> List[Dict[str, Any]]:
    """
    Wrapper around get_data() that applies exponential backoff 
    to handle transient network failures or API rate limits.
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"📡 Requesting Adzuna API (Attempt {attempt}/{max_retries})...")
            data = get_data()
            if data:
                return data
            
            print(f"⚠️ Attempt {attempt}: Adzuna returned empty payload.")
        except Exception as err:
            print(f"⚠️ Attempt {attempt} failed with error: {err}")
            logger.warning(f"Ingestion attempt {attempt} failed: {err}")

        if attempt < max_retries:
            sleep_time = backoff_factor ** attempt
            print(f"⏳ Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

    return []


def run_ingestion() -> str:
    """
    Fetches raw job data, writes local JSON artifact, and invokes 
    the S3/RDS loader service. Returns absolute path to local artifact.
    """
    print("=== Starting Ingestion Stage ===")
    data = fetch_with_retry(max_retries=3, backoff_factor=2)

    if not data:
        print("💥 CRITICAL ERROR: Adzuna returned zero records after retries.")
        sys.exit(1)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    filename = f"job_data_{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d')}.json"
    file_path = os.path.join(raw_dir, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"📁 Saved {len(data)} raw records to: {file_path}")

    # Use the JobDataPipeline orchestrator class
    loader_service = JobDataPipeline()
    loader_service.process_and_load(file_path)
    
    print("✅ Ingestion & Staging Stage Complete")
    return file_path


if __name__ == "__main__":
    run_ingestion()