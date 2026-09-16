import os
import psycopg
import re
from airflow.hooks.base import BaseHook

# -----------------------------
# Skills list (must match loader.py exactly)
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
# DB Connection (same fallback pattern as loader.py)
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
# Backfill
# -----------------------------
def backfill_skills():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT source_id, job_descr
        FROM raw_job_data
        WHERE skills IS NULL;
    """)
    rows = cursor.fetchall()
    print(f"{len(rows)} rows to backfill")

    updated = 0
    still_empty = 0

    for source_id, job_descr in rows:
        skills = extract_skills(job_descr)

        if not skills:
            # job_descr had no matches at all — track separately so you
            # can tell "no skills found" apart from "still not processed"
            still_empty += 1

        cursor.execute("""
            UPDATE raw_job_data
            SET skills = %s
            WHERE source_id = %s;
        """, (skills, source_id))
        updated += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"{updated} rows updated")
    print(f"{still_empty} rows had no skill matches in job_descr (set to empty array, not NULL)")

if __name__ == "__main__":
    backfill_skills()