import os
import sys
import json
import datetime as dt

from pipeline.api_data import get_data
from pipeline.loader import load_json_to_db


def main():
    print("Fetching jobs from Adzuna...")
    data = get_data()

    if not data:
        print("ERROR: Adzuna returned no records")
        sys.exit(1)

    os.makedirs("data/raw", exist_ok=True)
    filename = f"job_data_{dt.datetime.now().strftime('%Y-%m-%d')}.json"
    path = os.path.join("data/raw", filename)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(data)} records to {path}")

    load_json_to_db(path)  # uploads to S3 raw/, then upserts into RDS
    print("Ingestion complete")


if __name__ == "__main__":
    main()