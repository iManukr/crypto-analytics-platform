# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Airflow image, extended with:
#   * the ingestion package's runtime dependencies, so the orchestrated batch
#     reconciliation runs the SAME code as the streaming service rather than a
#     reimplementation that can drift;
#   * a clickhouse-connect client for publishing run metadata;
#   * dbt in an isolated virtualenv, invoked as a subprocess.
# -----------------------------------------------------------------------------
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl git \
 && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/dbt-venv && chown -R airflow:root /opt/dbt-venv

USER airflow

# Airflow-environment packages, held to the official constraints for this
# release. Anything not named in the constraints file (clickhouse-connect) still
# resolves normally.
COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt, fully isolated. Nothing in here shares a site-packages with Airflow.
COPY dbt/requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt \
 && /opt/dbt-venv/bin/dbt --version

ENV PYTHONPATH=/opt/airflow:/opt/airflow/dags \
    DBT_PROFILES_DIR=/opt/airflow/dbt \
    DBT_PROJECT_DIR=/opt/airflow/dbt \
    PATH="/opt/dbt-venv/bin:${PATH}"
