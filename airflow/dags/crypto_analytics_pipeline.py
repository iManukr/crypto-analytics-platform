"""End-to-end pipeline DAG: ingestion -> CDC settle -> transformation -> modelling.

    preflight ──▶ ingest_backfill ──▶ wait_for_cdc ──▶ dbt staging ──▶ dbt marts
                                                            │              │
                                                            └──▶ publish results ──▶ record run

Division of labour between this DAG and the streaming ingester:

  * ``ingestion`` (the always-on container) is the low-latency path. It polls
    every 20s so the dashboards are live and CDC has something to stream.
  * This DAG is the *reconciliation and modelling* path. It heals whatever the
    streaming path missed, waits for CDC to actually settle, then rebuilds the
    models and tests them.

Both call the identical ``run_backfill``. A second implementation of "work out
what is missing and fetch it" would eventually disagree with the first, and the
disagreement would surface as data that is present in one path and absent in the
other - which is exceptionally unpleasant to debug.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pipeline_common import (
    DEFAULT_ARGS,
    assert_cdc_healthy,
    clickhouse_scalar,
    dbt_command,
    dbt_env,
    publish_dbt_results,
    record_pipeline_run,
)

log = logging.getLogger(__name__)

SCHEDULE = os.environ.get("AIRFLOW_SCHEDULE_CRON", "*/15 * * * *")
CDC_LAG_SLA = int(os.environ.get("CDC_LAG_SLA_SECONDS", "120"))


with DAG(
    dag_id="crypto_analytics_pipeline",
    description="REST API -> Postgres -> Debezium/Kafka -> ClickHouse -> dbt marts",
    default_args=DEFAULT_ARGS,
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Backfilling this makes no sense: the external API only serves recent
    # minutes and the CDC stream is inherently "now". A missed run is recovered
    # by the next one, because the ingester derives its window from the database
    # rather than from the logical date.
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "cdc", "dbt", "clickhouse"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    # ---------------------------------------------------------------- preflight
    with TaskGroup(group_id="preflight") as preflight:

        @task(task_id="check_cdc_connector")
        def check_cdc_connector():
            return assert_cdc_healthy()

        @task(task_id="check_clickhouse")
        def check_clickhouse():
            version = clickhouse_scalar("select version()")
            databases = clickhouse_scalar(
                "select count() from system.databases "
                "where name in ('raw','analytics_staging','analytics_marts','analytics_ops')"
            )
            if int(databases or 0) < 4:
                raise RuntimeError(
                    f"expected 4 pipeline databases in ClickHouse, found {databases}. "
                    "The init scripts did not complete."
                )
            log.info("ClickHouse %s ready with %s pipeline databases", version, databases)
            return {"version": version}

        check_cdc_connector()
        check_clickhouse()

    # ---------------------------------------------------------------- ingestion
    @task(task_id="ingest_backfill")
    def ingest_backfill() -> dict:
        """Heal any gap the streaming ingester left behind.

        Imports the ingestion package directly rather than shelling out, so a
        failure surfaces as a Python traceback in the task log instead of an
        exit code.
        """
        from ingestion.config import Config
        from ingestion.service import run_backfill

        config = Config.from_env()
        written = run_backfill(config)
        total = sum(written.values())
        log.info("batch reconciliation wrote %d rows: %s", total, written)
        return {"rows_ingested": total, "per_symbol": written}

    # -------------------------------------------------------------- CDC settle
    def _cdc_has_caught_up(**context) -> bool:
        """True once ClickHouse has seen everything Postgres has.

        Compares the newest candle in the CDC landing table against the SLA. A
        sensor here rather than a fixed sleep: a sleep is either too short (and
        the marts are built on data that has not arrived) or too long (and every
        run wastes the difference).
        """
        newest_lag = clickhouse_scalar(
            """
            select dateDiff('second', max(open_time), now())
            from raw.market_candles_1m
            """,
            default=None,
        )
        if newest_lag is None:
            log.info("no rows in raw.market_candles_1m yet; still waiting for the snapshot")
            return False

        pending = clickhouse_scalar(
            """
            select count()
            from raw.market_candles_1m
            where _cdc_arrived_at >= now() - toIntervalSecond(30)
            """,
            default=0,
        )
        log.info("newest candle is %ss old; %s rows arrived in the last 30s", newest_lag, pending)

        # Two minutes of slack over the SLA: the newest bar is by definition a
        # minute old the moment it closes, so demanding better than that would
        # never be satisfiable.
        return int(newest_lag) <= CDC_LAG_SLA + 120

    wait_for_cdc = PythonSensor(
        task_id="wait_for_cdc_propagation",
        python_callable=_cdc_has_caught_up,
        poke_interval=15,
        timeout=600,
        # reschedule frees the worker slot between pokes. With mode='poke' a
        # ten-minute wait would hold a slot for ten minutes and, at
        # max_active_runs=1, could deadlock the whole DAG.
        mode="reschedule",
        soft_fail=False,
    )

    # ------------------------------------------------------------ transformation
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        env=dbt_env(),
        append_env=True,
        # The project intentionally has no packages, so this is a no-op that
        # exists to keep the DAG correct the moment one is added.
        retries=1,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=dbt_command("source", extra="freshness") + " || true",
        env=dbt_env(),
        append_env=True,
        doc_md=(
            "Non-blocking on purpose. Freshness here duplicates what Prometheus "
            "already alerts on continuously; failing the DAG for it would mean a "
            "stale source blocks the rebuild of models that do not depend on the "
            "stale part."
        ),
    )

    # `dbt build` runs models and their tests interleaved in DAG order, so a
    # model whose test fails does not get built on top of. `dbt run` followed by
    # `dbt test` would build the entire layer first and only then discover the
    # failure.
    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=dbt_command("build", select="tag:staging"),
        env=dbt_env(),
        append_env=True,
        execution_timeout=timedelta(minutes=15),
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=dbt_command("build", select="tag:mart"),
        env=dbt_env(),
        append_env=True,
        execution_timeout=timedelta(minutes=25),
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=dbt_command("docs", extra="generate") + " || true",
        env=dbt_env(),
        append_env=True,
    )

    # -------------------------------------------------------------- publishing
    @task(task_id="publish_dbt_results", trigger_rule=TriggerRule.ALL_DONE)
    def publish_results(**context) -> dict:
        """Runs even when the dbt tasks failed - especially then.

        ALL_DONE rather than ALL_SUCCESS: a failed dbt build is exactly when
        somebody needs to see which test failed, and a publisher that only runs
        on success is a publisher that never runs when it matters.
        """
        return publish_dbt_results(run_id=context["run_id"])

    @task(task_id="record_run", trigger_rule=TriggerRule.ALL_DONE)
    def record_run(ingest_stats: dict, dbt_stats: dict, **context) -> None:
        dag_run = context["dag_run"]
        failed = [ti.task_id for ti in dag_run.get_task_instances() if ti.state == "failed"]
        started = (dag_run.start_date or datetime.now(UTC)).isoformat()

        record_pipeline_run(
            dag_id=dag.dag_id,
            run_id=context["run_id"],
            started_at=started,
            stats={
                "status": "failed" if failed else "success",
                "rows_ingested": (ingest_stats or {}).get("rows_ingested", 0),
                "models_built": (dbt_stats or {}).get("success", 0),
                "tests_passed": (dbt_stats or {}).get("pass", 0),
                "tests_failed": (dbt_stats or {}).get("fail", 0)
                + (dbt_stats or {}).get("error", 0),
                "notes": f"failed_tasks={failed}" if failed else "",
            },
        )

    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

    # ------------------------------------------------------------------ wiring
    ingest = ingest_backfill()
    dbt_results = publish_results()

    start >> preflight >> ingest >> wait_for_cdc >> dbt_deps
    dbt_deps >> dbt_source_freshness >> dbt_build_staging >> dbt_build_marts
    dbt_build_marts >> dbt_docs
    dbt_build_marts >> dbt_results >> record_run(ingest, dbt_results) >> finish
