import os
import psycopg
import json
import datetime as dt
import logging
import re
import boto3
from botocore.exceptions import ClientError
from airflow.hooks.base import BaseHook
import sys

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    filename="loader_rejections.log",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# Skills extraction
# -----------------------------
SKILLS = [
    "python", "sql", "r", "scala", "java",
    "airflow", "dbt", "spark", "kafka", "hadoop",
    "aws", "azure", "gcp", "s3", "redshift", "bigquery",
    "snowflake", "postgres", "mysql", "mongodb",
    "docker", "kubernetes", "terraform",
    "tableau", "power bi", "looker",
    "pandas", "numpy", "pytorch", "tensorflow",
    "git", "ci/cd", "linux", "data bricks", "jupyter", "hdfs", "etl", "elt",
    "data visualization", "data modeling", "data warehousing"
]

def extract_skills(description: str) -> list:
    if not description:
        return []
    text = description.lower()
    found = []
    for skill in SKILLS:
        pattern = r"(?<![a-z0-9])" + re.escape(skill.lower()) + r"(?![a-z0-9])"
        if re.search(pattern, text):
            found.append(skill)
    return found

# -----------------------------
# S3 Ingestion Layer
# -----------------------------
def upload_to_s3(filepath: str) -> str:
    bucket_name = os.getenv("S3_BUCKET_NAME", "adzuna-job-pipeline-prod")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")
    
    # Generate dynamic partitioning key: raw/YYYY/MM/DD/jobs.json
    now = dt.datetime.now(dt.timezone.utc)
    s3_key = f"raw/{now.strftime('%Y/%m/%d')}/jobs.json"

    print(f"\n🔹 Uploading raw JSON artifact to S3: 's3://{bucket_name}/{s3_key}'...")

    # Strategy 1: Try Airflow S3Hook if running inside Airflow execution context
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_file(
            filename=filepath,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        print("✅ Raw JSON successfully uploaded to S3 via S3Hook.")
        return s3_key
    except Exception as airflow_err:
        print(f"Airflow S3Hook unavailable ({airflow_err}). Falling back to boto3...")

    # Strategy 2: Fallback to standalone boto3 client using environment variables
    try:
        s3_client = boto3.client("s3", region_name=aws_region)
        s3_client.upload_file(filepath, bucket_name, s3_key)
        print("✅ Raw JSON successfully uploaded to S3 via boto3 client.")
        return s3_key
    except ClientError as e:
        logger.error(f"S3 Upload failed: {e}")
        print(f"❌ Error uploading file to S3: {e}")
        raise e

# -----------------------------
# DB Connection
# -----------------------------
def connect_db():
    try:
        conn_profile = BaseHook.get_connection('postgres_default')
        return psycopg.connect(
            host=conn_profile.host,
            port=conn_profile.port or 5432,
            user=conn_profile.login,
            password=conn_profile.password,
            dbname=conn_profile.schema
        )
    except Exception as airflow_err:
        print(f"Airflow connection vault unavailable: {airflow_err}")
        print("Falling back to environment variables for DB connection...")
    try:
        return psycopg.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME")
        )
    except Exception as env_err:
        print(f"Database fallback connection failed: {env_err}")
        raise env_err

# -----------------------------
# Validation Layer
# -----------------------------
def validate_record(r):
    errors = []
    if not r.get("title"):
        errors.append("Missing title")
    if not r.get("Company"):
        errors.append("Missing company")

    smin = r.get("Salary_min")
    smax = r.get("Salary_max")

    if smin is not None and smax is not None:
        try:
            if float(smin) > float(smax):
                errors.append("salary_min > salary_max")
        except Exception:
            errors.append("Invalid salary values")

    return errors

# -----------------------------
# Main Loader
# -----------------------------
def load_json_to_db(filepath):
    # Step 1: Upload raw JSON file to S3 raw zone prior to DB processing
    upload_to_s3(filepath)

    # Step 2: Establish DB Connection & perform transformations/inserts
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_job_data (
        id SERIAL PRIMARY KEY,
        source_id VARCHAR(255) UNIQUE,
        job_title VARCHAR(255),
        company VARCHAR(255),
        location VARCHAR(255),
        salary_min NUMERIC(10, 2),
        salary_max NUMERIC(10, 2),
        job_descr TEXT,
        skills TEXT[],
        date_posted TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE
    );
    """)

    # Safe additive migrations
    cursor.execute("ALTER TABLE raw_job_data ADD COLUMN IF NOT EXISTS skills TEXT[];")
    cursor.execute("ALTER TABLE raw_job_data ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
    cursor.execute("ALTER TABLE raw_job_data ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;")

    with open(filepath) as f:
        records = json.load(f)

    valid_rows = []

    for r in records:
        errors = validate_record(r)

        if errors:
            logger.warning(f"Rejected row: {r} | {errors}")
            continue

        valid_rows.append({
            "source_id": r["id"],
            "job_title": r["title"],
            "company": r["Company"],
            "location": r["location"],
            "salary_min": r["Salary_min"],
            "salary_max": r["Salary_max"],
            "job_descr": r["job_descr"],
            "skills": extract_skills(r["job_descr"]),
            "date_posted": r["Date_posted"],
        })

    if valid_rows:
        fetched_ids = [r["source_id"] for r in valid_rows]

        cursor.execute("SELECT source_id FROM raw_job_data;")
        existing_ids = {row[0] for row in cursor.fetchall()}

        new_rows = [r for r in valid_rows if r["source_id"] not in existing_ids]
        still_present_rows = [r for r in valid_rows if r["source_id"] in existing_ids]

        # 1. Insert new jobs
        if new_rows:
            cursor.executemany("""
                INSERT INTO raw_job_data (
                    source_id, job_title, company, location,
                    salary_min, salary_max, job_descr, skills, date_posted,
                    last_seen_at, is_active
                )
                VALUES (
                    %(source_id)s, %(job_title)s, %(company)s, %(location)s,
                    %(salary_min)s, %(salary_max)s, %(job_descr)s, %(skills)s, %(date_posted)s,
                    CURRENT_TIMESTAMP, TRUE
                );
            """, new_rows)

        # 2. Update existing active jobs
        if still_present_rows:
            still_present_ids = [r["source_id"] for r in still_present_rows]
            cursor.execute("""
                UPDATE raw_job_data
                SET last_seen_at = CURRENT_TIMESTAMP, is_active = TRUE
                WHERE source_id = ANY(%s);
            """, (still_present_ids,))

        # 3. Deactivate unlisted jobs
        cursor.execute("""
            UPDATE raw_job_data
            SET is_active = FALSE
            WHERE source_id != ALL(%s);
        """, (fetched_ids,))

        conn.commit()

        print(f"{len(new_rows)} new rows inserted, "
              f"{len(still_present_rows)} rows confirmed active, "
              f"deactivation check run against {len(fetched_ids)} fetched ids")
    else:
        logger.warning(
            "No valid rows in this run — skipping deactivation pass entirely."
        )

    cursor.close()
    conn.close()

if __name__ == "__main__":
    json_path = sys.argv[1] if len(sys.argv) > 1 else "jobs.json"
    print(f"Executing loader for: {json_path}")
    load_json_to_db(json_path)