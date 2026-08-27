# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY exporter/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY exporter/main.py .

RUN useradd --create-home --uid 10002 exporter && chown -R exporter:exporter /app
USER exporter

EXPOSE 9101

HEALTHCHECK --interval=20s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS "http://localhost:${EXPORTER_PORT:-9101}/metrics" > /dev/null || exit 1

CMD ["python", "main.py"]
