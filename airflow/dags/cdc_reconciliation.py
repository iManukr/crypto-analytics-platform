"""Hourly CDC reconciliation: does ClickHouse actually match Postgres?

Every other check in this platform is a *proxy* for correctness. Connector state
says the connector is running. Consumer lag says Kafka is being drained.
Freshness says something recent arrived. None of them can detect the failure
that matters most in a CDC pipeline: a change event that was silently lost, so
the replica is permanently short a row and nothing ever complains.

The only way to find that is to count both sides and compare. That is all this
DAG does, and it is the reason it exists separately from the main pipeline - it
must keep running and keep alerting even when the transformation DAG is failing.

Today is excluded from the strict comparison: rows are landing in Postgres
continuously and take a second or two to appear in ClickHouse, so a difference
there is propagation delay rather than loss. Completed days must match exactly.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

import pendulum
import psycopg2
from airflow.decorators import task
from airflow.models.dag import DAG
from pipeline_common import DEFAULT_ARGS, clickhouse_client

log = logging.getLogger(__name__)

# A completed day must match exactly. This tolerance exists only so that a
# genuinely borderline race at the midnight boundary does not page anyone.
ALLOWED_ROW_DIFFERENCE = int(os.environ.get("RECONCILIATION_TOLERANCE_ROWS", "0"))
LOOKBACK_DAYS = int(os.environ.get("RECONCILIATION_LOOKBACK_DAYS", "7"))


def _postgres_counts() -> dict[tuple[str, str], int]:
    dsn = (
        f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'crypto')} "
        f"user={os.environ.get('POSTGRES_USER', 'crypto_app')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', '')}"
    )
    query = """
        SELECT symbol, (open_time AT TIME ZONE 'UTC')::date AS d, count(*)
        FROM crypto.market_candles_1m
        WHERE open_time >= now() - make_interval(days => %s)
        GROUP BY 1, 2
    """
    with psycopg2.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(query, (LOOKBACK_DAYS,))
        return {(row[0], row[1].isoformat()): int(row[2]) for row in cur.fetchall()}


def _clickhouse_counts() -> dict[tuple[str, str], int]:
    """Counts from the deduplicated view of the CDC landing table.

    FINAL and the tombstone filter are both required: without FINAL a replayed
    event inflates the count and the reconciliation reports a false divergence;
    without the filter, deleted rows are counted that Postgres no longer has.
    """
    client = clickhouse_client()
    try:
        result = client.query(
            """
            SELECT symbol, toString(toDate(open_time)) AS d, count() AS n
            FROM raw.market_candles_1m FINAL
            WHERE open_time >= now() - toIntervalDay(%(days)s)
              AND _op != 'd'
            GROUP BY symbol, d
            """,
            parameters={"days": LOOKBACK_DAYS},
        )
        return {(row[0], row[1]): int(row[2]) for row in result.result_rows}
    finally:
        client.close()


with DAG(
    dag_id="cdc_reconciliation",
    description="Row-level parity check between the OLTP source and the OLAP replica",
    default_args={**DEFAULT_ARGS, "retries": 1},
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "data-quality", "monitoring"],
    doc_md=__doc__,
) as dag:

    @task(task_id="reconcile")
    def reconcile(**context) -> dict:
        source = _postgres_counts()
        replica = _clickhouse_counts()
        today = datetime.now(UTC).date().isoformat()

        keys = sorted(set(source) | set(replica))
        divergences = []
        rows_source = 0
        rows_replica = 0

        for key in keys:
            symbol, day = key
            in_source = source.get(key, 0)
            in_replica = replica.get(key, 0)
            rows_source += in_source
            rows_replica += in_replica

            if day == today:
                # In-flight; log the delta for visibility but never fail on it.
                if in_source != in_replica:
                    log.info(
                        "in-flight delta for %s %s: postgres=%d clickhouse=%d (lag, not loss)",
                        symbol,
                        day,
                        in_source,
                        in_replica,
                    )
                continue

            if abs(in_source - in_replica) > ALLOWED_ROW_DIFFERENCE:
                divergences.append(
                    {
                        "symbol": symbol,
                        "date": day,
                        "postgres": in_source,
                        "clickhouse": in_replica,
                        "difference": in_source - in_replica,
                    }
                )

        summary = {
            "days_compared": len(keys),
            "rows_postgres": rows_source,
            "rows_clickhouse": rows_replica,
            "divergences": len(divergences),
        }
        log.info("reconciliation summary: %s", summary)

        if divergences:
            detail = "\n".join(
                f"  {d['symbol']} {d['date']}: postgres={d['postgres']} "
                f"clickhouse={d['clickhouse']} (diff {d['difference']:+d})"
                for d in divergences[:20]
            )
            raise RuntimeError(
                f"CDC replica diverges from the source on {len(divergences)} "
                f"symbol-day(s):\n{detail}\n\n"
                "A negative difference means ClickHouse has MORE rows than Postgres "
                "(duplicate events that a merge has not yet collapsed - usually benign). "
                "A positive difference means change events were LOST, which is not."
            )

        return summary

    @task(task_id="record_result")
    def record_result(summary: dict, **context) -> None:
        client = clickhouse_client()
        try:
            client.insert(
                "analytics_ops.pipeline_runs",
                [
                    [
                        context["run_id"],
                        "cdc_reconciliation",
                        context["dag_run"].start_date or datetime.now(UTC),
                        datetime.now(UTC),
                        "success",
                        int(summary.get("rows_clickhouse", 0)),
                        0,
                        int(summary.get("days_compared", 0)),
                        int(summary.get("divergences", 0)),
                        f"postgres={summary.get('rows_postgres')} "
                        f"clickhouse={summary.get('rows_clickhouse')}",
                    ]
                ],
                column_names=[
                    "run_id",
                    "dag_id",
                    "started_at",
                    "finished_at",
                    "status",
                    "rows_ingested",
                    "models_built",
                    "tests_passed",
                    "tests_failed",
                    "notes",
                ],
            )
        finally:
            client.close()

    record_result(reconcile())
