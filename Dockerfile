FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY requirements.txt .

RUN uv pip install --system -r requirements.txt
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-postgres
RUN apt-get update && apt-get install -y git

COPY . .

CMD ["python", "main.py"]