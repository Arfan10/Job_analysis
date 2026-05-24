FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY requirements.txt .

RUN uv pip install --system -r requirements.txt

COPY . .

CMD ["python", "main.py"]