FROM python:3.12-slim

# Install system dependencies for psycopg2/dbt
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python requirements and dbt-postgres
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt dbt-postgres

# Copy full pipeline code and dbt project
COPY . .

# Make entrypoint executable
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/bin/sh", "entrypoint.sh"]