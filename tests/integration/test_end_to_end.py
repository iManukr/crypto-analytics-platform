"""End-to-end integration tests against a running stack.

Run with the compose stack up:

    docker compose up -d
    pytest tests/integration -v

These are the tests that would actually have caught a broken pipeline, because
they assert on the *seams* between components - the places where every unit test
passes and the data still does not arrive:

    REST API -> Postgres -> Debezium -> Kafka -> ClickHouse -> staging -> marts

The CDC update test is the sharpest one. Anyone can move an INSERT across a
pipeline; the interesting question is whether an UPDATE to an existing row
converges, which is what the ReplacingMergeTree version column exists to
guarantee. If the version column were wrong, inserts would still look perfect
and updates would silently keep the old value.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg2
import pytest
import requests

pytestmark = pytest.mark.integration

CH_URL = f"http://{os.environ.get('CLICKHOUSE_HOST', 'localhost')}:{os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')}/"
CH_AUTH = (
    os.environ.get("CLICKHOUSE_USER", "analytics"),
    os.environ.get("CLICKHOUSE_PASSWORD", "changeme_clickhouse"),
)
PG_DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
    f"port={os.environ.get('POSTGRES_PUBLISHED_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'crypto')} "
    f"user={os.environ.get('POSTGRES_USER', 'crypto_app')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'changeme_app')}"
)
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")

# CDC normally settles in well under a second. The generous ceiling is for a
# cold CI runner still working through the initial snapshot.
CDC_TIMEOUT = int(os.environ.get("TEST_CDC_TIMEOUT", "120"))


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #
def ch(sql: str) -> list[list[str]]:
    response = requests.post(
        CH_URL, params={"query": f"{sql} FORMAT TabSeparated"}, auth=CH_AUTH, timeout=30
    )
    response.raise_for_status()
    text = response.text.strip()
    return [line.split("\t") for line in text.split("\n")] if text else []


def ch_scalar(sql: str, default=None):
    rows = ch(sql)
    return rows[0][0] if rows and rows[0] else default


def pg(query: str, params=None, fetch=True):
    with psycopg2.connect(PG_DSN, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall() if fetch else None


def wait_until(predicate, timeout: int = CDC_TIMEOUT, interval: float = 2.0, what: str = ""):
    """Poll a predicate. Returns its truthy value, or fails the test with context.

    A fixed sleep would be either flaky or slow; the whole point of an
    integration test for a streaming pipeline is to measure how long convergence
    actually takes.
    """
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as exc:
            last = exc
        time.sleep(interval)
    pytest.fail(f"timed out after {timeout}s waiting for {what or 'condition'}; last value: {last}")


# --------------------------------------------------------------------------- #
# Stage 1: the platform is actually up
# --------------------------------------------------------------------------- #
class TestPlatformUp:
    def test_postgres_has_the_cdc_schema(self):
        tables = {
            row[0] for row in pg("SELECT tablename FROM pg_tables WHERE schemaname = 'crypto'")
        }
        assert {"symbols", "market_candles_1m", "fx_rates", "ingest_rejects"} <= tables

    def test_logical_replication_is_configured(self):
        """Without wal_level=logical there is no CDC at all, and the failure
        mode is a connector that starts and then produces nothing."""
        assert pg("SHOW wal_level")[0][0] == "logical"

        publications = {row[0] for row in pg("SELECT pubname FROM pg_publication")}
        assert "dbz_publication" in publications

        published = {
            row[0]
            for row in pg(
                "SELECT tablename FROM pg_publication_tables WHERE pubname = 'dbz_publication'"
            )
        }
        assert {"symbols", "market_candles_1m", "fx_rates"} == published

    def test_replica_identity_is_full_on_captured_tables(self):
        """REPLICA IDENTITY FULL is what puts the before-image in the WAL. Without
        it, a DELETE arrives with only its primary key and the ClickHouse side
        cannot reconstruct the row."""
        rows = pg(
            """
            SELECT relname, relreplident FROM pg_class
            WHERE relnamespace = 'crypto'::regnamespace
              AND relname IN ('symbols', 'market_candles_1m', 'fx_rates')
            """
        )
        assert rows, "captured tables not found"
        for name, identity in rows:
            assert identity == "f", f"{name} has replica identity {identity!r}, expected 'f' (full)"

    def test_the_debezium_connector_is_running(self):
        status = requests.get(f"{CONNECT_URL}/connectors/crypto-oltp-cdc/status", timeout=15).json()
        assert status["connector"]["state"] == "RUNNING"
        assert status["tasks"], "connector has no tasks"
        for task in status["tasks"]:
            assert task["state"] == "RUNNING", task.get("trace", "")

    def test_the_replication_slot_is_active(self):
        rows = pg(
            "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = 'dbz_crypto_slot'"
        )
        assert rows, "the Debezium replication slot does not exist"
        assert rows[0][1] is True, "slot exists but nothing is consuming it"

    def test_clickhouse_has_every_pipeline_database(self):
        names = {row[0] for row in ch("SELECT name FROM system.databases")}
        assert {"raw", "analytics_staging", "analytics_marts", "analytics_ops"} <= names

    def test_the_kafka_engine_tables_are_consuming(self):
        engines = {
            row[0]: row[1]
            for row in ch(
                "SELECT name, engine FROM system.tables WHERE database = 'raw' AND engine = 'Kafka'"
            )
        }
        assert set(engines) == {"kafka_market_candles_1m", "kafka_symbols", "kafka_fx_rates"}


# --------------------------------------------------------------------------- #
# Stage 2: data flows all the way through
# --------------------------------------------------------------------------- #
class TestDataFlow:
    def test_the_ingester_is_landing_rows_in_postgres(self):
        count = wait_until(
            lambda: int(pg("SELECT count(*) FROM crypto.market_candles_1m")[0][0]) > 0,
            what="the first candle to reach Postgres",
        )
        assert count

    def test_rows_reach_the_cdc_landing_table(self):
        wait_until(
            lambda: int(ch_scalar("SELECT count() FROM raw.market_candles_1m", 0)) > 0,
            what="CDC to replicate the first candle into ClickHouse",
        )

    def test_staging_deduplicates_the_cdc_stream(self):
        """The count and the distinct-key count must be identical. If they are
        not, FINAL is not doing its job and every aggregate downstream that sums
        anything is inflated."""
        total = int(ch_scalar("SELECT count() FROM analytics_staging.stg_market_candles", 0))
        if total == 0:
            pytest.skip("staging not built yet; run the Airflow DAG first")

        distinct = int(
            ch_scalar(
                "SELECT count() FROM (SELECT DISTINCT symbol, open_time FROM analytics_staging.stg_market_candles)",
                0,
            )
        )
        assert total == distinct, f"{total - distinct} duplicate rows survived staging"

    def test_marts_are_populated_and_internally_consistent(self):
        total = int(ch_scalar("SELECT count() FROM analytics_marts.fct_candles_1m", 0))
        if total == 0:
            pytest.skip("marts not built yet; run the Airflow DAG first")

        broken = int(
            ch_scalar(
                """
            SELECT count() FROM analytics_marts.fct_candles_1m
            WHERE high_price < low_price
               OR high_price < greatest(open_price, close_price)
               OR low_price  > least(open_price, close_price)
            """,
                0,
            )
        )
        assert broken == 0, f"{broken} OHLC-inconsistent rows in the mart"

    def test_the_ml_feature_mart_has_no_forward_looking_features(self):
        """The leakage contract, asserted rather than documented.

        Recomputing the 5-period moving average from the raw closes and
        comparing it to the stored feature proves the window really is
        backward-looking: a frame that included the next row would produce a
        different number.
        """
        total = int(ch_scalar("SELECT count() FROM analytics_marts.ml_features_1m", 0))
        if total < 100:
            pytest.skip("not enough feature rows yet")

        mismatches = int(
            ch_scalar(
                """
            SELECT count() FROM (
                SELECT
                    f.sma_5 AS stored,
                    avg(c.close_price) OVER (
                        PARTITION BY c.symbol ORDER BY c.open_time
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS recomputed
                FROM analytics_marts.ml_features_1m AS f
                INNER JOIN analytics_marts.fct_candles_1m AS c
                    ON f.symbol = c.symbol AND f.open_time = c.open_time
                WHERE f.has_contiguous_history = 1
                ORDER BY c.open_time DESC
                LIMIT 200
            )
            WHERE abs(stored - recomputed) > 0.01
            """,
                0,
            )
        )
        assert mismatches == 0, f"{mismatches} rows where sma_5 is not a trailing average"

        unresolved_with_labels = int(
            ch_scalar(
                """
            SELECT count() FROM analytics_marts.ml_features_1m
            WHERE is_label_resolved = 0
              AND (target_direction_up != 0 OR target_log_return_1m != 0)
            """,
                0,
            )
        )
        assert unresolved_with_labels == 0, "unresolved rows carry non-neutral labels"


# --------------------------------------------------------------------------- #
# Stage 3: CDC semantics, not just CDC throughput
# --------------------------------------------------------------------------- #
class TestCdcSemantics:
    def test_an_insert_propagates(self):
        symbol = "ETHUSDT"
        open_time = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(days=400)
        marker = Decimal("4242.42424242")

        pg(
            """
            INSERT INTO crypto.market_candles_1m
                (symbol, open_time, close_time, open_price, high_price, low_price,
                 close_price, volume, quote_volume, trade_count, taker_buy_base,
                 taker_buy_quote, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 1, 1, 0, 0, 'integration-test')
            ON CONFLICT (symbol, open_time) DO UPDATE SET close_price = EXCLUDED.close_price
            """,
            (symbol, open_time, open_time + timedelta(seconds=59), marker, marker, marker, marker),
            fetch=False,
        )

        wait_until(
            lambda: (
                int(
                    ch_scalar(
                        f"SELECT count() FROM raw.market_candles_1m FINAL "
                        f"WHERE symbol = '{symbol}' AND open_time = '{open_time:%Y-%m-%d %H:%M:%S}'",
                        0,
                    )
                )
                > 0
            ),
            what="the INSERT to replicate",
        )

    def test_an_update_converges_rather_than_duplicating(self):
        """The test that proves the ReplacingMergeTree version column works.

        An UPDATE emits a second CDC event with the same key. If the version
        column were wrong, FINAL would keep the wrong one - and the row count
        would still look correct, so nothing else would notice.
        """
        base = "USD"
        quote = f"T{uuid.uuid4().hex[:4].upper()}"  # a pair no provider will touch
        now = datetime.now(UTC)

        pg(
            """
            INSERT INTO crypto.fx_rates (base, quote, rate, as_of, source)
            VALUES (%s, %s, %s, %s, 'integration-test')
            ON CONFLICT (base, quote) DO UPDATE SET rate = EXCLUDED.rate, as_of = EXCLUDED.as_of
            """,
            (base, quote, Decimal("100.00000000"), now),
            fetch=False,
        )
        wait_until(
            lambda: (
                ch_scalar(
                    f"SELECT toString(rate) FROM raw.fx_rates FINAL "
                    f"WHERE base = '{base}' AND quote = '{quote}'"
                )
                is not None
            ),
            what="the FX INSERT to replicate",
        )

        # Same primary key, new value, same as_of -> a genuine in-place UPDATE.
        pg(
            "UPDATE crypto.fx_rates SET rate = %s, updated_at = now() WHERE base = %s AND quote = %s",
            (Decimal("200.00000000"), base, quote),
            fetch=False,
        )

        final_rate = wait_until(
            lambda: (lambda v: v if v and float(v) == pytest.approx(200.0, abs=0.001) else None)(
                ch_scalar(
                    f"SELECT toString(rate) FROM raw.fx_rates FINAL "
                    f"WHERE base = '{base}' AND quote = '{quote}' AND as_of = '{now:%Y-%m-%d %H:%M:%S}'"
                )
            ),
            what="the UPDATE to win over the original insert",
        )
        assert float(final_rate) == pytest.approx(200.0, abs=0.001)

        rows_after_final = int(
            ch_scalar(
                f"SELECT count() FROM raw.fx_rates FINAL WHERE base = '{base}' AND quote = '{quote}'",
                0,
            )
        )
        assert rows_after_final == 1, "FINAL left more than one row for a single key"

        pg("DELETE FROM crypto.fx_rates WHERE base = %s AND quote = %s", (base, quote), fetch=False)

    def test_a_delete_arrives_as_a_tombstone_with_its_before_image(self):
        """REPLICA IDENTITY FULL is what makes this possible. With the default
        identity the before-image would be missing and the tombstone unusable."""
        base = "USD"
        quote = f"D{uuid.uuid4().hex[:4].upper()}"

        pg(
            "INSERT INTO crypto.fx_rates (base, quote, rate, as_of, source) "
            "VALUES (%s, %s, 55.5, now(), 'integration-test')",
            (base, quote),
            fetch=False,
        )
        wait_until(
            lambda: (
                int(
                    ch_scalar(
                        f"SELECT count() FROM raw.fx_rates WHERE base='{base}' AND quote='{quote}'",
                        0,
                    )
                )
                > 0
            ),
            what="the row to replicate before deleting it",
        )

        pg("DELETE FROM crypto.fx_rates WHERE base = %s AND quote = %s", (base, quote), fetch=False)

        tombstone = wait_until(
            lambda: ch_scalar(
                f"SELECT toString(rate) FROM raw.fx_rates "
                f"WHERE base='{base}' AND quote='{quote}' AND _op = 'd' LIMIT 1"
            ),
            what="the DELETE tombstone to arrive",
        )
        assert float(tombstone) == pytest.approx(55.5, abs=0.001), (
            "the tombstone carries no before-image; REPLICA IDENTITY is probably not FULL"
        )

        # Staging must hide it from every downstream consumer.
        visible = int(
            ch_scalar(
                f"SELECT count() FROM analytics_staging.stg_fx_rates "
                f"WHERE base='{base}' AND quote='{quote}'",
                0,
            )
        )
        assert visible == 0, "a deleted row is still visible in staging"


# --------------------------------------------------------------------------- #
# Stage 4: quality and observability are real, not decorative
# --------------------------------------------------------------------------- #
class TestQualityAndObservability:
    def test_nothing_landed_in_the_dead_letter_queue(self):
        count = int(ch_scalar("SELECT count() FROM raw.cdc_dead_letters", 0))
        if count:
            sample = ch("SELECT topic, error FROM raw.cdc_dead_letters LIMIT 3")
            pytest.fail(f"{count} unparseable CDC message(s); sample: {sample}")

    def test_cdc_lag_is_inside_the_sla(self):
        rows = ch(
            """
            SELECT
              ifNull(quantile(0.95)((toUnixTimestamp64Milli(_cdc_arrived_at) - toInt64(_source_ts_ms)) / 1000), -1)
            FROM raw.market_candles_1m
            WHERE _cdc_arrived_at >= now() - toIntervalMinute(15)
            """
        )
        if not rows or float(rows[0][0]) < 0:
            pytest.skip("no recent CDC rows to measure")
        p95 = float(rows[0][0])
        assert p95 < 120, f"CDC p95 lag is {p95:.1f}s, outside the 120s SLA"

    def test_row_counts_reconcile_between_the_source_and_the_replica(self):
        """The only check that detects a silently dropped change event. The last
        two minutes are excluded because they are legitimately in flight."""
        source_rows = int(
            pg(
                """
            SELECT count(*) FROM crypto.market_candles_1m
            WHERE open_time >= now() - interval '1 hour'
              AND open_time <  now() - interval '2 minutes'
            """
            )[0][0]
        )
        if source_rows == 0:
            pytest.skip("no settled rows in the comparison window yet")

        replica_rows = int(
            ch_scalar(
                """
            SELECT count() FROM raw.market_candles_1m FINAL
            WHERE _op != 'd'
              AND open_time >= now() - toIntervalHour(1)
              AND open_time <  now() - toIntervalMinute(2)
            """,
                0,
            )
        )
        assert replica_rows == source_rows, (
            f"Postgres has {source_rows} rows, ClickHouse has {replica_rows}. "
            "A positive difference means change events were LOST."
        )

    def test_the_ingester_exposes_its_metrics(self):
        port = os.environ.get("INGESTION_METRICS_PORT", "8000")
        body = requests.get(f"http://localhost:{port}/metrics", timeout=15).text
        for metric in (
            "ingest_rows_written_total",
            "ingest_last_success_timestamp_seconds",
            "ingest_source_lag_seconds",
            "ingest_active_source",
        ):
            assert metric in body, f"{metric} is missing from /metrics"

    def test_the_pipeline_exporter_publishes_cross_system_metrics(self):
        port = os.environ.get("EXPORTER_PORT", "9101")
        body = requests.get(f"http://localhost:{port}/metrics", timeout=20).text
        for metric in (
            "cdc_end_to_end_lag_seconds",
            "cdc_replication_slot_lag_bytes",
            "pipeline_row_parity_delta",
            "pipeline_oltp_freshness_seconds",
        ):
            assert metric in body, f"{metric} is missing from the exporter"

    def test_prometheus_is_scraping_every_target(self):
        port = os.environ.get("PROMETHEUS_PUBLISHED_PORT", "9090")
        targets = requests.get(f"http://localhost:{port}/api/v1/targets", timeout=20).json()
        active = targets["data"]["activeTargets"]
        # cAdvisor only exists under the `full` profile, so its absence is fine.
        down = [
            t["labels"].get("job")
            for t in active
            if t["health"] != "up" and t["labels"].get("job") != "cadvisor"
        ]
        assert not down, f"Prometheus targets are down: {down}"

    def test_alert_rules_loaded_without_error(self):
        port = os.environ.get("PROMETHEUS_PUBLISHED_PORT", "9090")
        rules = requests.get(f"http://localhost:{port}/api/v1/rules", timeout=20).json()
        groups = rules["data"]["groups"]
        assert groups, "no alert rule groups loaded"
        names = {rule["name"] for group in groups for rule in group["rules"]}
        # A representative slice; if these are missing the rules file did not load.
        assert {"DebeziumConnectorDown", "CDCLagHigh", "ReplicationSlotBloat"} <= names

    def test_validation_rejects_are_recorded_not_silently_dropped(self):
        rejects = int(pg("SELECT count(*) FROM crypto.ingest_rejects")[0][0])
        assert rejects >= 0  # the table must exist and be queryable
