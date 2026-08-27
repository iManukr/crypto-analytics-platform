"""Prometheus instrumentation for the ingestion service.

Metric design follows two rules:

* **Label cardinality stays bounded.** Labels are symbol, source, table and a
  small closed set of outcome/reason values. Nothing unbounded (no URLs, no
  error strings) ever becomes a label, because that is how a Prometheus server
  gets killed by its own data.
* **Every alert has a metric behind it.** Each gauge here is referenced by at
  least one rule in ``infra/prometheus/alerts.yml``; a metric nobody alerts or
  dashboards on is just cost.

The pair ``ingest_last_success_timestamp_seconds`` / ``ingest_source_lag_seconds``
is the important one: the first says "the service is alive and working", the
second says "the data is current". A pipeline can be healthy on the first and
badly broken on the second, and only the second is what a consumer cares about.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --------------------------------------------------------------------------- #
# Throughput and outcomes                                                      #
# --------------------------------------------------------------------------- #
API_REQUESTS = Counter(
    "ingest_api_requests_total",
    "Outbound requests to the external source API.",
    ["source", "outcome"],  # outcome: success | http_error | timeout | transport_error
)

API_LATENCY = Histogram(
    "ingest_api_latency_seconds",
    "Wall-clock latency of a single external API call.",
    ["source"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

ROWS_WRITTEN = Counter(
    "ingest_rows_written_total",
    "Rows successfully upserted into the OLTP database.",
    ["table", "symbol"],
)

ROWS_REJECTED = Counter(
    "ingest_rows_rejected_total",
    "Rows that failed validation and were quarantined instead of written.",
    ["table", "reason"],
)

CYCLES = Counter(
    "ingest_cycles_total",
    "Completed ingestion cycles.",
    ["outcome"],  # success | partial | failed
)

ERRORS = Counter(
    "ingest_errors_total",
    "Errors by broad category, for alert routing.",
    ["kind"],  # api | database | validation | unexpected
)

# --------------------------------------------------------------------------- #
# Freshness and health                                                         #
# --------------------------------------------------------------------------- #
LAST_SUCCESS = Gauge(
    "ingest_last_success_timestamp_seconds",
    "Unix time of the last cycle that wrote at least one row.",
)

SOURCE_LAG = Gauge(
    "ingest_source_lag_seconds",
    "Age of the newest candle held in the OLTP database, per symbol. "
    "This is the user-visible freshness number.",
    ["symbol"],
)

BACKFILL_GAP = Gauge(
    "ingest_backfill_gap_minutes",
    "Minutes of history the service detected as missing and attempted to heal.",
    ["symbol"],
)

ACTIVE_SOURCE = Gauge(
    "ingest_active_source",
    "1 for the source currently in use, 0 for the others. Reveals silent "
    "degradation from the live API to the offline replay generator.",
    ["source"],
)

CONSECUTIVE_FAILURES = Gauge(
    "ingest_consecutive_api_failures",
    "Consecutive failed attempts against the live API. Drives the fallback.",
)

UP = Gauge("ingest_up", "1 while the ingestion loop is running.")


def serve(port: int) -> None:
    """Expose /metrics on a daemon thread."""
    start_http_server(port)


def set_active_source(active: str, known: tuple[str, ...] = ("binance", "replay")) -> None:
    for name in known:
        ACTIVE_SOURCE.labels(source=name).set(1 if name == active else 0)
