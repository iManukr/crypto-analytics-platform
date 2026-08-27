# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Ingestion service image.
#
# Multi-stage so the wheels are built once and the runtime layer carries no
# compiler. Runs as a non-root user because nothing here needs root and a
# container that can write to its own filesystem is a container that can be
# persistently modified.
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# curl is used by the compose healthcheck against /metrics.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

WORKDIR /app
COPY ingestion/ /app/ingestion/
COPY scripts/ /app/scripts/

RUN useradd --create-home --uid 10001 ingest \
 && chown -R ingest:ingest /app
USER ingest

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS "http://localhost:${INGESTION_METRICS_PORT:-8000}/metrics" > /dev/null || exit 1

ENTRYPOINT ["python", "-m", "ingestion"]
