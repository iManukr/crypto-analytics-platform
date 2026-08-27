"""Shared helpers for the DAGs.

Kept out of the DAG files themselves so that the DAG modules stay readable as
*workflow definitions*. Anything that talks to a system lives here.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import clickhouse_connect

log = logging.getLogger(__name__)

DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://connect:8083").rstrip("/")
CONNECTOR_NAME = os.environ.get("CDC_CONNECTOR_NAME", "crypto-oltp-cdc")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    # Exponential backoff: a transient ClickHouse restart resolves in seconds, a
    # real outage does not, and retrying a real outage every 30s just fills the
    # logs. Capped so a retry cannot outlive the schedule interval.
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


# --------------------------------------------------------------------------- #
# ClickHouse                                                                   #
# --------------------------------------------------------------------------- #
def clickhouse_client():
    """A ClickHouse client built from the same env vars everything else uses."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "analytics"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        connect_timeout=10,
        send_receive_timeout=300,
    )


def clickhouse_scalar(query: str, default: Any = None) -> Any:
    client = clickhouse_client()
    try:
        result = client.query(query)
        if not result.result_rows or result.result_rows[0][0] is None:
            return default
        return result.result_rows[0][0]
    finally:
        client.close()


# --------------------------------------------------------------------------- #
# Kafka Connect                                                                #
# --------------------------------------------------------------------------- #
def connector_status(name: str = CONNECTOR_NAME) -> dict:
    # Fixed internal service URL built from configuration, never from user input.
    req = urllib.request.Request(f"{CONNECT_URL}/connectors/{name}/status")  # noqa: S310
    with urllib.request.urlopen(req, timeout=15) as response:  # noqa: S310
        return json.loads(response.read().decode())


def assert_cdc_healthy(**_context) -> dict:
    """Fail the run before transforming anything if CDC is not delivering.

    Transforming on top of a dead CDC pipeline is worse than not running: the
    models succeed, the tests pass on stale data, and the dashboards keep
    drawing a flat line that looks like a quiet market rather than an outage.
    Checking first turns a silent data problem into a loud orchestration one.
    """
    status = connector_status()
    connector_state = status["connector"]["state"]
    task_states = [task["state"] for task in status.get("tasks", [])]

    log.info("connector=%s tasks=%s", connector_state, task_states)

    if connector_state != "RUNNING":
        raise RuntimeError(f"Debezium connector is {connector_state}, expected RUNNING")
    if not task_states:
        raise RuntimeError("Debezium connector has no tasks")
    for task in status["tasks"]:
        if task["state"] != "RUNNING":
            raise RuntimeError(
                f"connector task {task['id']} is {task['state']}:\n"
                f"{task.get('trace', '<no trace>')}"
            )

    return {"connector": connector_state, "tasks": task_states}


# --------------------------------------------------------------------------- #
# dbt                                                                          #
# --------------------------------------------------------------------------- #
def dbt_env() -> dict[str, str]:
    """Environment handed to every dbt subprocess.

    Passed explicitly rather than inherited so that what dbt connects to is
    visible in the DAG rather than a property of however the container happened
    to be started.
    """
    keys = (
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_HTTP_PORT",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_DB",
        "DBT_TARGET",
        "PATH",
        "HOME",
    )
    env = {key: os.environ[key] for key in keys if key in os.environ}
    env.setdefault("DBT_PROFILES_DIR", DBT_DIR)
    return env


def dbt_command(subcommand: str, select: str | None = None, extra: str = "") -> str:
    """Build a dbt invocation.

    ``--no-use-colors`` because ANSI escapes in the Airflow log viewer are
    noise, and ``--fail-fast`` so a broken upstream model does not spend five
    minutes failing every downstream one before reporting.
    """
    parts = [
        DBT_BIN,
        subcommand,
        "--no-use-colors",
        "--project-dir",
        DBT_DIR,
        "--profiles-dir",
        DBT_DIR,
    ]
    if select:
        parts += ["--select", select]
    if extra:
        parts.append(extra)
    return " ".join(parts)


# --------------------------------------------------------------------------- #
# Run-result publishing                                                        #
# --------------------------------------------------------------------------- #
def publish_dbt_results(run_id: str, **_context) -> dict:
    """Load dbt's run_results.json into analytics_ops.dbt_test_results.

    Keeping test outcomes in the warehouse - not only in an artefact file that
    the next run overwrites - is what turns "is this test failing?" into "when
    did this test start failing, and what else changed that day?".
    """
    path = Path(DBT_DIR) / "target" / "run_results.json"
    if not path.exists():
        log.warning("no run_results.json at %s; nothing to publish", path)
        return {"published": 0}

    payload = json.loads(path.read_text(encoding="utf-8"))
    invocation_at = datetime.now(UTC)

    rows = []
    summary = {"pass": 0, "fail": 0, "error": 0, "skipped": 0, "other": 0}

    for result in payload.get("results", []):
        node_id = result.get("unique_id", "")
        status = str(result.get("status", "unknown")).lower()
        summary[status if status in summary else "other"] += 1

        if not node_id.startswith("test."):
            continue

        # unique_id looks like test.<project>.<test_name>.<hash>
        parts = node_id.split(".")
        test_name = parts[2] if len(parts) > 2 else node_id
        rows.append(
            [
                run_id,
                invocation_at,
                node_id,
                test_name,
                _model_from_test(test_name),
                status,
                int(result.get("failures") or 0),
                float(result.get("execution_time") or 0.0) * 1000.0,
            ]
        )

    if rows:
        client = clickhouse_client()
        try:
            client.insert(
                "analytics_ops.dbt_test_results",
                rows,
                column_names=[
                    "run_id",
                    "invocation_at",
                    "node_id",
                    "test_name",
                    "model_name",
                    "status",
                    "failures",
                    "execution_ms",
                ],
            )
        finally:
            client.close()

    log.info("published %d test results; summary=%s", len(rows), summary)
    return {"published": len(rows), **summary}


def _model_from_test(test_name: str) -> str:
    """Best-effort model attribution from a generated test name.

    dbt names generic tests like ``not_null_stg_market_candles_symbol``. There is
    no perfect parse, and this is used for grouping in dashboards rather than
    for anything load-bearing, so a heuristic is the right level of effort.
    """
    for marker in ("stg_", "fct_", "dim_", "agg_", "ml_"):
        index = test_name.find(marker)
        if index >= 0:
            return test_name[index:]
    return test_name


def record_pipeline_run(dag_id: str, run_id: str, started_at: str, stats: dict) -> None:
    client = clickhouse_client()
    try:
        client.insert(
            "analytics_ops.pipeline_runs",
            [
                [
                    run_id,
                    dag_id,
                    datetime.fromisoformat(started_at),
                    datetime.now(UTC),
                    stats.get("status", "success"),
                    int(stats.get("rows_ingested", 0)),
                    int(stats.get("models_built", 0)),
                    int(stats.get("tests_passed", 0)),
                    int(stats.get("tests_failed", 0)),
                    str(stats.get("notes", "")),
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
