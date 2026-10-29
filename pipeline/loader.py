import os
import re
import sys
import json
import logging
import psycopg
import boto3
import datetime as dt
from typing import List, Dict, Any, Tuple, Optional, Set
from botocore.exceptions import ClientError
from airflow.hooks.base import BaseHook


# ------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------
logging.basicConfig(
    filename="loader_rejections.log",
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("JobLoader")


# ------------------------------------------------------------------
# Skill Extractor Service
# ------------------------------------------------------------------
class SkillExtractor:
    """Service to parse and extract relevant technical skills from job descriptions."""

    DEFAULT_SKILLS: List[str] = [
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

    def __init__(self, target_skills: Optional[List[str]] = None):
        skills_to_use = target_skills or self.DEFAULT_SKILLS
        # Pre-compile regular expressions for optimal batch parsing
        self._compiled_patterns = [
            (skill, re.compile(rf"(?<![a-z0-9]){re.escape(skill.lower())}(?![a-z0-9])"))
            for skill in skills_to_use
        ]

    def extract(self, description: Optional[str]) -> List[str]:
        if not description:
            return []
        
        text = description.lower()
        return [skill for skill, pattern in self._compiled_patterns if pattern.search(text)]


# ------------------------------------------------------------------
# S3 Ingestion Service
# ------------------------------------------------------------------
class S3IngestionService:
    """Handles archiving raw JSON payloads to Amazon S3 data lake."""

    def __init__(self):
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "adzuna-job-pipeline-prod")
        self.aws_region = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")
    @staticmethod
    def _generate_s3_key() -> str:
        now = dt.datetime.now(dt.timezone.utc)
        return f"raw/{now.strftime('%Y/%m/%d')}/jobs.json"

    def upload_file(self, filepath: str) -> str:
        s3_key = self._generate_s3_key()
        print(f"\n🔹 Uploading raw JSON artifact to S3: 's3://{self.bucket_name}/{s3_key}'...")

        # Strategy 1: Attempt Airflow S3Hook context
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            s3_hook = S3Hook(aws_conn_id="aws_default")
            s3_hook.load_file(
                filename=filepath,
                key=s3_key,
                bucket_name=self.bucket_name,
                replace=True
            )
            print("✅ Raw JSON successfully uploaded to S3 via Airflow S3Hook.")
            return s3_key
        except Exception as airflow_err:
            logger.info(f"Airflow S3Hook unavailable ({airflow_err}). Falling back to Boto3 client.")

        # Strategy 2: Standalone Boto3 Client fallback
        try:
            s3_client = boto3.client("s3", region_name=self.aws_region)
            s3_client.upload_file(filepath, self.bucket_name, s3_key)
            print("✅ Raw JSON successfully uploaded to S3 via Boto3 client.")
            return s3_key
        except ClientError as e:
            logger.error(f"S3 Upload failed: {e}")
            print(f"❌ Error uploading file to S3: {e}")
            raise e


# ------------------------------------------------------------------
# Database Repository Manager
# ------------------------------------------------------------------
class DatabaseRepository:
    """Handles connection management, table setup, and record upserts to RDS PostgreSQL."""

    def get_connection(self) -> psycopg.Connection:
        # Strategy 1: Airflow Connection Vault
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
            logger.info(f"Airflow connection vault unavailable ({airflow_err}). Falling back to environment variables.")

        # Strategy 2: Environment Variables
        db_host = os.getenv("DB_HOST")
        if not db_host:
            raise ValueError("CRITICAL: DB_HOST environment variable is missing or empty. Cannot establish TCP connection to RDS.")

        try:
            return psycopg.connect(
                host=db_host,
                port=int(os.getenv("DB_PORT", 5432)),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD"),
                dbname=os.getenv("DB_NAME", "postgres")
            )
        except Exception as env_err:
            logger.error(f"Database fallback connection failed: {env_err}")
            raise env_err

    def init_db(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cursor:
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

            cursor.execute("ALTER TABLE raw_job_data ADD COLUMN IF NOT EXISTS skills TEXT[];")
            cursor.execute("ALTER TABLE raw_job_data ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
            cursor.execute("ALTER TABLE raw_job_data ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;")
        conn.commit()

    def sync_records(self, conn: psycopg.Connection, valid_rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        if not valid_rows:
            logger.warning("No valid rows in this run — skipping database sync.")
            return 0, 0, 0

        fetched_ids = [r["source_id"] for r in valid_rows]

        with conn.cursor() as cursor:
            cursor.execute("SELECT source_id FROM raw_job_data;")
            existing_ids: Set[str] = {row[0] for row in cursor.fetchall()}

            new_rows = [r for r in valid_rows if r["source_id"] not in existing_ids]
            still_present_rows = [r for r in valid_rows if r["source_id"] in existing_ids]

            # 1. Bulk insert new job postings
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

            # 2. Update timestamps on existing active job postings
            if still_present_rows:
                still_present_ids = [r["source_id"] for r in still_present_rows]
                cursor.execute("""
                    UPDATE raw_job_data
                    SET last_seen_at = CURRENT_TIMESTAMP, is_active = TRUE
                    WHERE source_id = ANY(%s);
                """, (still_present_ids,))

            # 3. Soft-deactivate unlisted job postings
            cursor.execute("""
                UPDATE raw_job_data
                SET is_active = FALSE
                WHERE source_id != ALL(%s);
            """, (fetched_ids,))

        conn.commit()
        return len(new_rows), len(still_present_rows), len(fetched_ids)


# ------------------------------------------------------------------
# ETL Pipeline Orchestrator
# ------------------------------------------------------------------
class JobDataPipeline:
    """Main Orchestrator tying together Data Validation, S3 Storage, and Database Persistence."""

    def __init__(self):
        self.skill_extractor = SkillExtractor()
        self.s3_service = S3IngestionService()
        self.db_repo = DatabaseRepository()

    @staticmethod
    def validate_record(r: Dict[str, Any]) -> List[str]:
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
            except (ValueError, TypeError):
                errors.append("Invalid salary values")

        return errors

    def process_and_load(self, filepath: str) -> None:
        # Step 1: Push raw JSON payload to S3 Lake
        self.s3_service.upload_file(filepath)

        # Step 2: Extract & Validate JSON File
        with open(filepath, "r", encoding="utf-8") as f:
            records = json.load(f)

        valid_rows: List[Dict[str, Any]] = []
        for r in records:
            errors = self.validate_record(r)
            if errors:
                logger.warning(f"Rejected record ID {r.get('id', 'UNKNOWN')}: {errors}")
                continue

            valid_rows.append({
                "source_id": r["id"],
                "job_title": r["title"],
                "company": r["Company"],
                "location": r["location"],
                "salary_min": r["Salary_min"],
                "salary_max": r["Salary_max"],
                "job_descr": r["job_descr"],
                "skills": self.skill_extractor.extract(r["job_descr"]),
                "date_posted": r["Date_posted"],
            })

        # Step 3: Establish DB Connection and Execute Synchronisation
        with self.db_repo.get_connection() as conn:
            self.db_repo.init_db(conn)
            inserted, active, total_checked = self.db_repo.sync_records(conn, valid_rows)
            
            print(f"📊 Summary: {inserted} new rows inserted | "
                  f"{active} rows refreshed | "
                  f"deactivation pass completed across {total_checked} fetched IDs.")


# ------------------------------------------------------------------
# Application Entrypoint
# ------------------------------------------------------------------
if __name__ == "__main__":
    target_json = sys.argv[1] if len(sys.argv) > 1 else "jobs.json"
    print(f"=== Starting Data Ingestion Pipeline for: {target_json} ===")
    
    pipeline = JobDataPipeline()
    pipeline.process_and_load(target_json)