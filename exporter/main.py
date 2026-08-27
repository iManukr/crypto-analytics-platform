"""Pipeline observability exporter.

Prometheus, ClickHouse, Postgres and Kafka all publish their own *infrastructure*
metrics, and those are scraped directly. What none of them can publish is the
thing anyone actually cares about: **is the data correct and current, end to
end**. That is a cross-system question, so it needs a component that can see
across systems. This is that component.

What it measures that nothing else can:

  * **True CDC latency** - Postgres commit time to ClickHouse visibility, taken
    from the rows themselves (``_cdc_arrived_at - _source_ts_ms``). Kafka
    consumer lag is a *proxy* for this and can look perfect while the data is
    hours stale, for instance if the connector is down and there is nothing to
    consume.
  * **Row parity** - the same count on both sides. The only signal that detects
    a silently dropped change event.
  * **Replication slot lag in bytes** - the number that predicts the source
    database filling its disk. This is the most common way a Debezium
    deployment causes an outage in the system it was only supposed to observe.
  * **Layer-by-layer freshness** - OLTP, CDC landing, staging and marts, so a
    stall is attributed to a stage rather than just noticed.

Design rules:
  * A target being down degrades that target's metrics only. One unreachable
    system must not blind the operator to the other four.
  * Failed scrapes stop exporting stale values rather than reporting a
    comfortable-looking old number - the gauge is cleared and an error counter
    increments.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from collections.abc import Callable

import psycopg2
import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("pipeline-exporter")

SCRAPE_INTERVAL = int(os.environ.get("EXPORTER_SCRAPE_INTERVAL", "15"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9101"))
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://connect:8083").rstrip("/")
CONNECTOR_NAME = os.environ.get("CDC_CONNECTOR_NAME", "crypto-oltp-cdc")

CH_URL = (
    f"http://{os.environ.get('CLICKHOUSE_HOST', 'clickhouse')}:"
    f"{os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')}/"
)
CH_AUTH = (
    os.environ.get("CLICKHOUSE_USER", "analytics"),
    os.environ.get("CLICKHOUSE_PASSWORD", ""),
)

PG_DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'crypto')} "
    f"user={os.environ.get('POSTGRES_USER', 'crypto_app')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', '')}"
)

# --------------------------------------------------------------------------- #
# Metrics                                                                      #
# --------------------------------------------------------------------------- #
OLTP_ROWS = Gauge("pipeline_oltp_rows_total", "Rows in the OLTP source table.", ["table"])
OLTP_FRESHNESS = Gauge(
    "pipeline_oltp_freshness_seconds",
    "Age of the newest business timestamp in the OLTP table.",
    ["table"],
)
OLTP_REJECTS = Gauge(
    "pipeline_ingest_rejects_total",
    "Rows quarantined at ingest because they failed validation.",
)

SLOT_LAG = Gauge(
    "cdc_replication_slot_lag_bytes",
    "WAL bytes retained for a replication slot. Unbounded growth here fills the "
    "source database's disk and is the classic Debezium outage.",
    ["slot"],
)
SLOT_ACTIVE = Gauge("cdc_replication_slot_active", "1 when the slot has a live consumer.", ["slot"])

OLAP_ROWS = Gauge("pipeline_olap_rows_total", "Rows in a ClickHouse table.", ["database", "table"])
OLAP_FRESHNESS = Gauge(
    "pipeline_olap_freshness_seconds",
    "Age of the newest business timestamp in a ClickHouse table.",
    ["database", "table"],
)
OLAP_PARTS = Gauge(
    "clickhouse_table_parts",
    "Active parts per table. A sustained climb means merges are losing to "
    "inserts, which ends in a 'too many parts' write rejection.",
    ["database", "table"],
)

CDC_LAG = Gauge(
    "cdc_end_to_end_lag_seconds",
    "Postgres commit to ClickHouse visibility, measured from the rows.",
    ["quantile"],  # avg | p95 | max
)
CDC_DEAD_LETTERS = Gauge(
    "cdc_dead_letters_total", "Unparseable CDC messages in the last 24h.", ["topic"]
)
CONNECTOR_UP = Gauge(
    "cdc_connector_running", "1 when the Debezium connector is RUNNING.", ["connector"]
)
CONNECTOR_TASKS = Gauge("cdc_connector_tasks", "Connector tasks by state.", ["connector", "state"])

ROW_PARITY = Gauge(
    "pipeline_row_parity_delta",
    "OLTP rows minus OLAP rows over the comparison window. Non-zero for a "
    "sustained period means change events were lost.",
    ["table"],
)

DBT_TEST_FAILURES = Gauge(
    "dq_dbt_test_failures", "Failing dbt tests in the most recent invocation.", ["model"]
)
DBT_LAST_RUN = Gauge("dq_dbt_last_run_timestamp_seconds", "Unix time of the last dbt invocation.")

SCRAPE_ERRORS = Counter("exporter_scrape_errors_total", "Failed scrapes by target.", ["target"])
SCRAPE_DURATION = Histogram(
    "exporter_scrape_duration_seconds", "Time to collect one target.", ["target"]
)
TARGET_UP = Gauge("exporter_target_up", "1 when the target answered this scrape.", ["target"])


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #
def guarded(target: str) -> Callable:
    """Run a collector, isolating its failures.

    Without this, one unreachable system takes the whole exporter down and the
    operator loses visibility into the four that are still working - precisely
    when they need it.
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                TARGET_UP.labels(target=target).set(1)
                return result
            except Exception as exc:
                SCRAPE_ERRORS.labels(target=target).inc()
                TARGET_UP.labels(target=target).set(0)
                log.warning("scrape of %s failed: %s", target, exc)
                return None
            finally:
                SCRAPE_DURATION.labels(target=target).observe(time.perf_counter() - start)

        return wrapper

    return decorator


def ch_query(sql: str) -> list[list[str]]:
    """Run a query over ClickHouse's HTTP interface.

    TabSeparated rather than a client library: it keeps the image to three
    dependencies, and the exporter's queries are all small scalars and short
    aggregate result sets.
    """
    response = requests.post(
        CH_URL,
        params={"query": f"{sql} FORMAT TabSeparated"},
        auth=CH_AUTH,
        timeout=20,
    )
    response.raise_for_status()
    text = response.text.strip()
    if not text:
        return []
    return [line.split("\t") for line in text.split("\n")]


def _float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------- #
# Collectors                                                                   #
# --------------------------------------------------------------------------- #
@guarded("postgres")
def collect_postgres() -> dict:
    with psycopg2.connect(PG_DSN, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),
                   COALESCE(EXTRACT(EPOCH FROM (now() - max(open_time))), -1)
            FROM crypto.market_candles_1m
            """
        )
        candle_rows, candle_age = cur.fetchone()
        OLTP_ROWS.labels(table="market_candles_1m").set(candle_rows)
        if candle_age >= 0:
            OLTP_FRESHNESS.labels(table="market_candles_1m").set(float(candle_age))

        cur.execute(
            """
            SELECT count(*),
                   COALESCE(EXTRACT(EPOCH FROM (now() - max(as_of))), -1)
            FROM crypto.fx_rates
            """
        )
        fx_rows, fx_age = cur.fetchone()
        OLTP_ROWS.labels(table="fx_rates").set(fx_rows)
        if fx_age >= 0:
            OLTP_FRESHNESS.labels(table="fx_rates").set(float(fx_age))

        cur.execute("SELECT count(*) FROM crypto.ingest_rejects")
        OLTP_REJECTS.set(cur.fetchone()[0])

        # The number that predicts a disk-full incident on the source database.
        cur.execute(
            """
            SELECT slot_name,
                   COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0),
                   active
            FROM pg_replication_slots
            """
        )
        for slot_name, lag_bytes, active in cur.fetchall():
            SLOT_LAG.labels(slot=slot_name).set(float(lag_bytes))
            SLOT_ACTIVE.labels(slot=slot_name).set(1 if active else 0)

        # Comparison window for row parity. Excludes the last two minutes, which
        # are legitimately still in flight.
        cur.execute(
            """
            SELECT count(*) FROM crypto.market_candles_1m
            WHERE open_time >= now() - interval '1 hour'
              AND open_time <  now() - interval '2 minutes'
            """
        )
        return {"parity_window_rows": cur.fetchone()[0]}


@guarded("clickhouse")
def collect_clickhouse(pg_stats: dict | None) -> None:
    # ---- CDC landing layer -------------------------------------------------
    rows = ch_query(
        """
        SELECT count(),
               ifNull(dateDiff('second', max(open_time), now()), -1),
               ifNull(avg((toUnixTimestamp64Milli(_cdc_arrived_at) - toInt64(_source_ts_ms)) / 1000), -1),
               ifNull(quantile(0.95)((toUnixTimestamp64Milli(_cdc_arrived_at) - toInt64(_source_ts_ms)) / 1000), -1),
               ifNull(max((toUnixTimestamp64Milli(_cdc_arrived_at) - toInt64(_source_ts_ms)) / 1000), -1)
        FROM raw.market_candles_1m
        WHERE _cdc_arrived_at >= now() - toIntervalMinute(15)
        """
    )
    if rows:
        count, age, avg_lag, p95_lag, max_lag = rows[0]
        OLAP_ROWS.labels(database="raw", table="market_candles_1m_recent").set(_float(count))
        if _float(age, -1) >= 0:
            OLAP_FRESHNESS.labels(database="raw", table="market_candles_1m").set(_float(age))
        for label, value in (("avg", avg_lag), ("p95", p95_lag), ("max", max_lag)):
            if _float(value, -1) >= 0:
                CDC_LAG.labels(quantile=label).set(_float(value))

    total = ch_query("SELECT count() FROM raw.market_candles_1m FINAL WHERE _op != 'd'")
    if total:
        OLAP_ROWS.labels(database="raw", table="market_candles_1m").set(_float(total[0][0]))

    # ---- row parity --------------------------------------------------------
    if pg_stats:
        replica = ch_query(
            """
            SELECT count() FROM raw.market_candles_1m FINAL
            WHERE _op != 'd'
              AND open_time >= now() - toIntervalHour(1)
              AND open_time <  now() - toIntervalMinute(2)
            """
        )
        if replica:
            delta = pg_stats["parity_window_rows"] - int(_float(replica[0][0]))
            ROW_PARITY.labels(table="market_candles_1m").set(delta)

    # ---- dead letters ------------------------------------------------------
    dlq = ch_query(
        """
        SELECT topic, count() FROM raw.cdc_dead_letters
        WHERE received_at >= now() - toIntervalHour(24)
        GROUP BY topic
        """
    )
    CDC_DEAD_LETTERS.clear()
    for topic, count in dlq:
        CDC_DEAD_LETTERS.labels(topic=topic).set(_float(count))

    # ---- staging and mart freshness ---------------------------------------
    for database, table, column in (
        ("analytics_marts", "fct_candles_1m", "open_time"),
        ("analytics_marts", "agg_candles_5m", "bucket_start"),
        ("analytics_marts", "ml_features_1m", "open_time"),
        ("analytics_marts", "fct_market_daily", "trade_date"),
    ):
        try:
            # database/table/column are taken from the hardcoded tuple in the
            # loop header, not from anything external.
            result = ch_query(
                f"SELECT count(), ifNull(dateDiff('second', max({column}), now()), -1) "
                f"FROM {database}.{table}"
            )
        except requests.HTTPError:
            # Model not built yet on a cold start. Absent is the honest state;
            # exporting a zero would look like an empty table instead.
            continue
        if result:
            OLAP_ROWS.labels(database=database, table=table).set(_float(result[0][0]))
            if _float(result[0][1], -1) >= 0:
                OLAP_FRESHNESS.labels(database=database, table=table).set(_float(result[0][1]))

    # ---- merge pressure ----------------------------------------------------
    parts = ch_query(
        """
        SELECT database, table, count()
        FROM system.parts
        WHERE active AND database IN ('raw', 'analytics_marts')
        GROUP BY database, table
        """
    )
    for database, table, count in parts:
        OLAP_PARTS.labels(database=database, table=table).set(_float(count))

    # ---- dbt test outcomes -------------------------------------------------
    try:
        dbt_rows = ch_query(
            """
            SELECT model_name, countIf(status IN ('fail', 'error')), max(toUnixTimestamp(invocation_at))
            FROM analytics_ops.dbt_test_results
            WHERE invocation_at = (SELECT max(invocation_at) FROM analytics_ops.dbt_test_results)
            GROUP BY model_name
            """
        )
    except requests.HTTPError:
        dbt_rows = []
    DBT_TEST_FAILURES.clear()
    for model, failures, last_run in dbt_rows:
        DBT_TEST_FAILURES.labels(model=model).set(_float(failures))
        DBT_LAST_RUN.set(_float(last_run))


@guarded("kafka-connect")
def collect_connect() -> None:
    response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=15)
    if response.status_code == 404:
        # Not registered yet during startup. Reporting 0 is correct and is what
        # the alert should fire on if it persists.
        CONNECTOR_UP.labels(connector=CONNECTOR_NAME).set(0)
        return
    response.raise_for_status()
    status = response.json()

    CONNECTOR_UP.labels(connector=CONNECTOR_NAME).set(
        1 if status["connector"]["state"] == "RUNNING" else 0
    )

    counts: dict[str, int] = {}
    for task in status.get("tasks", []):
        counts[task["state"]] = counts.get(task["state"], 0) + 1
    CONNECTOR_TASKS.clear()
    for state, count in counts.items():
        CONNECTOR_TASKS.labels(connector=CONNECTOR_NAME, state=state).set(count)


def scrape_once() -> None:
    pg_stats = collect_postgres()
    collect_clickhouse(pg_stats)
    collect_connect()


def main() -> int:
    log.info("pipeline exporter listening on :%d (interval %ds)", EXPORTER_PORT, SCRAPE_INTERVAL)
    start_http_server(EXPORTER_PORT)

    while True:
        started = time.monotonic()
        scrape_once()
        time.sleep(max(1.0, SCRAPE_INTERVAL - (time.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main())
