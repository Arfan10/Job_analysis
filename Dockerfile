# Use official Python base image
FROM python:3

# Prevent Python from writing .pyc files & enable stdout flushing
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required for psycopg
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

COPY requirements.txt .

# Install psycopg (binary version is easiest)
RUN pip install --no-cache-dir -r requirements.txt

# Copy test script into container
COPY . .

# Default command
CMD ["python", "main.py"]
