import os
import psycopg
import json
import datetime as dt
import logging
from airflow.hooks.base import BaseHook
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
# DB Connection
# -----------------------------
def connect_db():
    try:
        conn_profile = BaseHook.get_connection('postgres_default')

        conn = psycopg.connect(
                    host=conn_profile.host,
                    port=conn_profile.port or 5432,
                    user=conn_profile.login,
                    password=conn_profile.password,
                    dbname=conn_profile.schema
                    )
        return conn
    except Exception as e:
        print(f"Database connection initialization failed: {e}")
    raise


# -----------------------------
# Validation Layer
# -----------------------------
def validate_record(r):
    errors = []

    # Required fields
    if not r.get("title"):
        errors.append("Missing title")
    if not r.get("Company"):
        errors.append("Missing company")

    # Salary validation
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
        date_posted TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

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
            "date_posted": r["Date_posted"],
        })

    if valid_rows:
        cursor.executemany("""
            INSERT INTO raw_job_data (
                source_id, job_title, company, location,
                salary_min, salary_max, job_descr, date_posted
            )
            VALUES (
                %(source_id)s, %(job_title)s, %(company)s, %(location)s,
                %(salary_min)s, %(salary_max)s, %(job_descr)s, %(date_posted)s
            ) ON CONFLICT (source_id) DO NOTHING;
        """, valid_rows)
        

    conn.commit()
    cursor.close()
    conn.close()

    print(f"{len(valid_rows)} rows inserted")
if __name__ == "__main__":
    conn = connect_db()