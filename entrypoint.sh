#!/bin/sh
set -e

echo "=== Starting Adzuna Ingestion ==="
python ingest.py

echo "=== Starting dbt Transformations ==="
cd analytics
dbt run --profiles-dir ../profiles
dbt test --profiles-dir ../profiles

echo "=== Pipeline Completed Successfully ==="