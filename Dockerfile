# One image for the producer, consumer and transformer (dbt) services.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ingest/ ingest/
COPY costlab/ costlab/
COPY dbt/ dbt/
COPY grafana/dashboards/ grafana/dashboards/
