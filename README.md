# UK Job Market Tightness Index

A production-grade data engineering pipeline that tracks supply and demand across the UK job market — built to demonstrate end-to-end DE skills for UK fintech roles.

**Live Dashboard:** *(coming Month 4)*  
**Architecture Diagram:** *(coming Month 1 completion)*  
**Status:** 🟡 In Progress — Month 1 (Local Pipeline)

---

## Business Problem

How tight is the UK job market for data roles right now? This pipeline answers that by ingesting daily job listings from the Adzuna API, transforming them into clean analytical models, and surfacing salary trends, role demand, and geographic concentration — updated every day automatically.

---

## Tech Stack

| Layer | Tool | Status |
|---|---|---|
| Ingestion | Python · Adzuna API | ✅ Built |
| Storage (Raw) | PostgreSQL 17 (Docker) | ✅ Built |
| Transformation | dbt Core | 🟡 In Progress |
| Orchestration | Apache Airflow | 📋 Planned |
| Cloud Storage | AWS S3 | 📋 Planned |
| Cloud DB | AWS RDS (PostgreSQL) | 📋 Planned |
| Compute | AWS EC2 | 📋 Planned |
| Batch Processing | PySpark · AWS Glue | 📋 Planned |
| Streaming | Apache Kafka | 📋 Planned |
| Data Quality | Great Expectations | 📋 Planned |
| Visualisation | Power BI | 📋 Planned |
| Containerisation | Docker · Docker Compose | ✅ Built |

---

## Architecture

```
Adzuna API
    │
    ▼
scraper.py ──► raw JSON (local /data/raw/YYYY-MM-DD/)
    │
    ▼
loader.py ──► PostgreSQL (bronze: raw_jobs)          ← ✅ Built
    │
    ▼
dbt Core ──► staging models → fact/dim models        ← 🟡 In Progress
    │
    ▼
Airflow DAG (daily orchestration)                    ← 📋 Planned
    │
    ├──► AWS S3 (bronze/silver/gold)                 ← 📋 Planned
    ├──► AWS RDS (PostgreSQL)                        ← 📋 Planned
    ├──► PySpark / AWS Glue (batch transforms)       ← 📋 Planned
    ├──► Kafka (streaming layer)                     ← 📋 Planned
    └──► Power BI Dashboard                          ← 📋 Planned
```

---

## Project Structure

```
uk-job-market-tightness-index/
├── data/
│   └── raw/                  # Raw JSON from Adzuna API
├── pipeline/
│   ├── scraper.py            # Adzuna API ingestion with retry logic
│   ├── loader.py             # JSON → PostgreSQL loader (idempotent)
│   └── db.py                 # PostgreSQL connection helper
├── dbt_project/
│   ├── models/
│   │   ├── staging/          # stg_adzuna__jobs.sql
│   │   ├── marts/            # fct_jobs.sql, dim_companies.sql, dim_locations.sql
│   └── dbt_project.yml
├── dags/                     # Airflow DAGs (planned)
├── docs/                     # Architecture diagrams, ADRs
├── docker-compose.yml
├── Dockerfile
├── .env.example
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.11+
- Adzuna API credentials ([register here](https://developer.adzuna.com/))

### Setup

```bash
git clone https://github.com/YOUR_USERNAME/uk-job-market-tightness-index.git
cd uk-job-market-tightness-index

cp .env.example .env
# Fill in your Adzuna API key and PostgreSQL credentials in .env

docker-compose up -d
```

### Environment Variables

| Variable | Description |
|---|---|
| `ADZUNA_APP_ID` | Adzuna API application ID |
| `ADZUNA_API_KEY` | Adzuna API key |
| `POSTGRES_HOST` | PostgreSQL host (default: `postgres`) |
| `POSTGRES_PORT` | PostgreSQL port (default: `5432`) |
| `POSTGRES_DB` | Database name |
| `POSTGRES_USER` | Database user |
| `POSTGRES_PASSWORD` | Database password |

### Run the Pipeline

```bash
# Ingest latest job listings from Adzuna
docker-compose exec app python pipeline/scraper.py

# Load raw JSON into PostgreSQL
docker-compose exec app python pipeline/loader.py

# Run dbt transformations
docker-compose exec app dbt build --project-dir dbt_project
```

---

## Data Models

### Bronze Layer — `raw_jobs`
Raw job listings as ingested from Adzuna. No transformation applied.

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL | Internal primary key |
| `source_id` | VARCHAR | Adzuna job ID (unique) |
| `title` | TEXT | Job title |
| `company` | TEXT | Company name |
| `location` | TEXT | Job location |
| `salary_min` | INTEGER | Minimum advertised salary |
| `salary_max` | INTEGER | Maximum advertised salary |
| `category` | TEXT | Job category (e.g. "IT Jobs") |
| `created_at` | TIMESTAMP | Listing publish date |

### Silver Layer — dbt Models *(🟡 In Progress)*

- **`stg_adzuna__jobs`** — Cleaned and typed staging model. Nulls filtered, columns renamed to standard convention.
- **`fct_jobs`** — Fact table with derived fields: `salary_band` (Junior/Mid/Senior), `is_remote` flag, `ingestion_date`.
- **`dim_companies`** — Dimension table of unique companies.
- **`dim_locations`** — Dimension table of unique locations.

### Gold Layer — Data Marts *(📋 Planned)*

- **`salary_trends`** — Average and median salary by role and city, month-over-month.
- **`role_demand`** — Weekly job posting volume by category, 4-week rolling average.
- **`company_activity`** — Top employers by posting volume, month-over-month growth.

---

## Roadmap

| Month | Focus | Status |
|---|---|---|
| Month 1 | Local pipeline — Docker · PostgreSQL · dbt · Airflow | 🟡 In Progress |
| Month 2 | Cloud deployment — AWS S3 · RDS · EC2 · Glue | 📋 Planned |
| Month 3 | Scale & quality — PySpark · Kafka · Great Expectations | 📋 Planned |
| Month 4 | Polish & launch — Power BI · documentation · applications | 📋 Planned |

---

## Key Design Decisions

**Why dbt for transformation?** dbt keeps transformation logic in SQL (the language every data team already uses), compiles to plain SQL that runs directly on PostgreSQL/RDS, and generates automatic lineage documentation. The alternative — writing transformation scripts in Python — would produce the same output but with no built-in testing, no lineage graph, and no standardised project structure. Full ADR in `docs/adr-001-dbt-vs-python-transforms.md` *(coming Week 4)*.

**Why idempotent loading?** The loader uses `ON CONFLICT DO NOTHING` on `source_id`. This means the pipeline can be re-run safely at any time without producing duplicate rows — critical for a daily orchestrated pipeline where failures and reruns are routine.

---

## About

Built as the primary portfolio project for a Data Engineering MSc specialisation, targeting Junior/Graduate Data Engineer roles in UK fintech and financial services.

**Stack rationale:** Every tool in this project appears in UK junior DE job descriptions — Python, SQL, dbt, Airflow, Docker, PostgreSQL, AWS, and Power BI. The pipeline is built on real public data (Adzuna job listings API) rather than synthetic datasets, producing genuinely useful output.

---

*Built by Ayan | [LinkedIn](https://linkedin.com/in/YOUR_PROFILE) | [GitHub](https://github.com/YOUR_USERNAME)*
