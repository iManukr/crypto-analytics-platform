#!/usr/bin/env python3
"""
Register (or update) the Debezium Postgres connector against Kafka Connect.

Deliberately stdlib-only so it can run inside any Python image without a
dependency install step, and so it is trivially unit-testable.

Behaviour:
  1. Wait for the Connect REST API to answer.
  2. Load the connector template, strip the ``__doc_*`` annotation keys, and
     substitute ``${VAR}`` placeholders from the environment.
  3. PUT the config. PUT on ``/connectors/<name>/config`` is create-or-update,
     which makes re-running this idempotent - important because compose will
     re-run it on every ``up``.
  4. Poll until the connector *and* every task report RUNNING, printing the
     Java stack trace if a task lands in FAILED.

Exit codes: 0 running, 1 failed/timed out, 2 configuration error.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://connect:8083").rstrip("/")
TEMPLATE = os.environ.get("CONNECTOR_TEMPLATE", "/opt/connect/debezium-postgres.json")
WAIT_SECONDS = int(os.environ.get("CONNECT_WAIT_SECONDS", "300"))
DOC_KEY_PREFIX = "__doc"
PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")


def log(msg: str) -> None:
    print(f"[register-connector] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Config preparation                                                           #
# --------------------------------------------------------------------------- #
def strip_doc_keys(config: dict) -> dict:
    """Drop the inline-documentation keys before the config goes over the wire.

    Kafka Connect only warns about unknown properties, but shipping them anyway
    pollutes the connector status and makes a real typo harder to spot.
    """
    return {k: v for k, v in config.items() if not k.startswith(DOC_KEY_PREFIX)}


def substitute(config: dict, env: dict[str, str]) -> dict:
    """Resolve ``${VAR}`` placeholders, failing loudly on anything unset.

    A connector that silently registers with the literal string ``${DEBEZIUM_PASSWORD}``
    as its password fails much later and much more confusingly than this does.
    """
    missing: set[str] = set()

    def replace(value: str) -> str:
        def one(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in env:
                missing.add(name)
                return match.group(0)
            return env[name]

        return PLACEHOLDER.sub(one, value)

    resolved = {k: (replace(v) if isinstance(v, str) else v) for k, v in config.items()}
    if missing:
        raise KeyError(f"unset environment variables referenced by the template: {sorted(missing)}")
    return resolved


def load_payload(path: str, env: dict[str, str]) -> tuple[str, dict]:
    with open(path, encoding="utf-8") as handle:
        template = json.load(handle)
    name = template["name"]
    config = substitute(strip_doc_keys(template["config"]), env)
    return name, config


# --------------------------------------------------------------------------- #
# Connect REST helpers                                                         #
# --------------------------------------------------------------------------- #
def request(method: str, path: str, body: dict | None = None, timeout: int = 15):
    data = json.dumps(body).encode() if body is not None else None
    # Fixed internal Connect REST endpoint from configuration, never user input.
    req = urllib.request.Request(  # noqa: S310
        f"{CONNECT_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    # Fixed internal Connect REST endpoint from configuration, not user input.
    with urllib.request.urlopen(req, timeout=timeout) as response:  # noqa: S310
        raw = response.read().decode()
    return json.loads(raw) if raw else None


def wait_for_connect(deadline: float) -> None:
    log(f"waiting for Kafka Connect at {CONNECT_URL} ...")
    while time.time() < deadline:
        try:
            info = request("GET", "/")
            log(f"Connect is up (kafka_cluster_id={info.get('kafka_cluster_id')})")
            return
        except (urllib.error.URLError, OSError, ValueError):
            time.sleep(3)
    raise TimeoutError(f"Kafka Connect did not become available within {WAIT_SECONDS}s")


def wait_for_running(name: str, deadline: float) -> None:
    """Poll until connector and all tasks are RUNNING, or a task fails."""
    log(f"waiting for connector '{name}' and its tasks to reach RUNNING ...")
    last = ""
    while time.time() < deadline:
        try:
            status = request("GET", f"/connectors/{name}/status")
        except (urllib.error.URLError, OSError, ValueError):
            time.sleep(2)
            continue

        connector_state = status["connector"]["state"]
        tasks = status.get("tasks", [])
        task_states = [t["state"] for t in tasks]

        summary = f"connector={connector_state} tasks={task_states or 'none yet'}"
        if summary != last:
            log(summary)
            last = summary

        failed = [t for t in tasks if t["state"] == "FAILED"]
        if connector_state == "FAILED" or failed:
            for task in failed:
                log(f"task {task['id']} FAILED:\n{task.get('trace', '<no trace>')}")
            raise RuntimeError(f"connector '{name}' failed to start")

        if connector_state == "RUNNING" and tasks and all(s == "RUNNING" for s in task_states):
            log(f"connector '{name}' is RUNNING with {len(tasks)} task(s)")
            return

        time.sleep(2)
    raise TimeoutError(f"connector '{name}' did not reach RUNNING within {WAIT_SECONDS}s")


def main() -> int:
    deadline = time.time() + WAIT_SECONDS
    try:
        name, config = load_payload(TEMPLATE, dict(os.environ))
    except (OSError, KeyError, ValueError) as exc:
        log(f"configuration error: {exc}")
        return 2

    try:
        wait_for_connect(deadline)
        # PUT .../config is create-or-update, so re-running on every `compose up`
        # converges rather than erroring with 409 Conflict.
        request("PUT", f"/connectors/{name}/config", config, timeout=60)
        log(f"submitted config for '{name}'")
        wait_for_running(name, deadline)
    except urllib.error.HTTPError as exc:
        log(f"HTTP {exc.code} from Connect: {exc.read().decode(errors='replace')}")
        return 1
    except (TimeoutError, RuntimeError, urllib.error.URLError, OSError) as exc:
        log(f"failed: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
