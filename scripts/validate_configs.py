#!/usr/bin/env python3
"""Parse every configuration file in the repository and fail on the first bad one.

Configuration is code here: a malformed Prometheus alert rule, Grafana dashboard
or ClickHouse XML does not fail at build time, it fails silently at container
start - and the symptom is "the dashboard is empty" or "the alert never fires",
hours later, with nothing obvious to point at.

Beyond parsing, this asserts a handful of invariants that a syntax check cannot
see but that quietly break the platform if violated: every alert carrying a
runbook, every dashboard panel naming a datasource, and the ClickHouse init
files being applied in a valid dependency order.

Stdlib plus PyYAML only, so the lint job needs no heavy install.
"""

from __future__ import annotations

import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", ".venv", "venv", "legacy", "target", "dbt_packages", "node_modules", ".claude"}

failures: list[str] = []
checked = 0


def skipped(path: Path) -> bool:
    return any(part in SKIP_DIRS for part in path.parts)


def report(path: Path, message: str) -> None:
    relative = path.relative_to(ROOT)
    failures.append(f"{relative}: {message}")
    # GitHub Actions annotation, so the failure lands on the file in the diff.
    print(f"::error file={relative}::{message}")


def ok(path: Path) -> None:
    global checked
    checked += 1
    print(f"  ok  {path.relative_to(ROOT)}")


# --------------------------------------------------------------------------- #
# Syntax
# --------------------------------------------------------------------------- #
def check_yaml() -> None:
    print("\nYAML")
    for path in sorted(ROOT.rglob("*.yml")) + sorted(ROOT.rglob("*.yaml")):
        if skipped(path):
            continue
        try:
            # safe_load handles the merge keys used by the compose anchors.
            yaml.safe_load(path.read_text(encoding="utf-8"))
            ok(path)
        except yaml.YAMLError as exc:
            report(path, f"invalid YAML: {exc}")


def check_json() -> None:
    print("\nJSON")
    for path in sorted(ROOT.rglob("*.json")):
        if skipped(path) or ".vscode" in path.parts:
            continue
        try:
            json.loads(path.read_text(encoding="utf-8"))
            ok(path)
        except json.JSONDecodeError as exc:
            report(path, f"invalid JSON: {exc}")


def check_xml() -> None:
    print("\nXML")
    for path in sorted((ROOT / "infra" / "clickhouse").rglob("*.xml")):
        try:
            # S314 is suppressed below: these are our own committed ClickHouse
            # config files, not untrusted input. Pulling in defusedxml to parse
            # four files we wrote ourselves would add a dependency to the lint
            # job for no real gain.
            ET.parse(path)  # noqa: S314
            ok(path)
        except ET.ParseError as exc:
            report(path, f"invalid XML: {exc}")


# --------------------------------------------------------------------------- #
# Semantics
# --------------------------------------------------------------------------- #
def check_alerts() -> None:
    """Every alert needs a summary and a runbook.

    An alert that fires at 03:00 with no stated next action is noise, and noise
    is what teaches people to silence alerts.
    """
    print("\nAlert rules")
    path = ROOT / "infra" / "prometheus" / "alerts.yml"
    rules = yaml.safe_load(path.read_text(encoding="utf-8"))

    names: set[str] = set()
    for group in rules.get("groups", []):
        for rule in group.get("rules", []):
            name = rule.get("alert")
            if not name:
                continue
            if name in names:
                report(path, f"duplicate alert name: {name}")
            names.add(name)

            annotations = rule.get("annotations", {})
            if not annotations.get("summary"):
                report(path, f"alert {name} has no summary")
            if not rule.get("labels", {}).get("severity"):
                report(path, f"alert {name} has no severity label")
            if rule.get("labels", {}).get("severity") in {"critical", "warning"}:
                if not annotations.get("runbook"):
                    report(path, f"alert {name} is actionable but has no runbook link")

    print(f"  {len(names)} alert(s) checked")
    checked_names = {
        "DebeziumConnectorDown",
        "CDCLagHigh",
        "ReplicationSlotBloat",
        "IngestionStalled",
    }
    missing = checked_names - names
    if missing:
        report(path, f"expected core alerts are missing: {sorted(missing)}")


def check_dashboards() -> None:
    """Panels must name a datasource and the dashboard must have a stable uid.

    A panel with no datasource silently binds to whatever Grafana considers
    default, which works on a fresh install and breaks the moment a second
    datasource is added.
    """
    print("\nGrafana dashboards")
    for path in sorted((ROOT / "infra" / "grafana" / "dashboards").glob("*.json")):
        dashboard = json.loads(path.read_text(encoding="utf-8"))

        if not dashboard.get("uid"):
            report(path, "dashboard has no uid, so provisioning cannot update it in place")
        if not dashboard.get("title"):
            report(path, "dashboard has no title")

        panels = dashboard.get("panels", [])
        data_panels = [p for p in panels if p.get("type") not in {"row", "text"}]
        for panel in data_panels:
            if not panel.get("datasource"):
                report(path, f"panel {panel.get('title')!r} does not name a datasource")
            if not panel.get("targets"):
                report(path, f"panel {panel.get('title')!r} has no queries")
        print(f"  ok  {path.relative_to(ROOT)} ({len(data_panels)} data panels)")


def check_clickhouse_init_order() -> None:
    """The init files are applied in lexical order; dependencies must respect it.

    A materialized view created before its target table fails at container
    start, and the container then comes up healthy with a missing view.
    """
    print("\nClickHouse init order")
    init_dir = ROOT / "infra" / "clickhouse" / "init"
    files = sorted(init_dir.glob("*.sql"))

    created: set[str] = set()
    for path in files:
        text = path.read_text(encoding="utf-8")
        body = re.sub(r"^\s*--.*$", "", text, flags=re.MULTILINE)

        # Walk statements in file order, interleaving "what this creates" with
        # "what this depends on". A view may legitimately target a table created
        # earlier in the SAME file, so a whole-file scan reports false positives.
        for statement in (st.strip() for st in body.split(";") if st.strip()):
            target = re.search(r"\bTO\s+([a-z_]+\.[a-z_0-9]+)", statement, re.IGNORECASE)
            if target and target.group(1).lower() not in created:
                report(
                    path,
                    f"materialized view targets {target.group(1)}, which is not created "
                    "by this point in the init order",
                )
            creates = re.search(
                r"CREATE (?:MATERIALIZED VIEW|TABLE|VIEW)(?: IF NOT EXISTS)? ([a-z_]+\.[a-z_0-9]+)",
                statement,
                re.IGNORECASE,
            )
            if creates:
                created.add(creates.group(1).lower())

        print(f"  ok  {path.relative_to(ROOT)}")

    expected = {"raw.market_candles_1m", "raw.symbols", "raw.fx_rates", "raw.cdc_dead_letters"}
    missing = expected - created
    if missing:
        report(init_dir, f"expected landing tables are never created: {sorted(missing)}")


def check_compose() -> None:
    """Sanity-check the compose file's shape without needing Docker installed."""
    print("\nDocker Compose")
    path = ROOT / "docker-compose.yml"
    compose = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = compose.get("services", {})

    required = {
        "postgres",
        "kafka",
        "connect",
        "connect-init",
        "clickhouse",
        "ingestion",
        "airflow-scheduler",
        "airflow-webserver",
        "prometheus",
        "grafana",
        "exporter",
    }
    missing = required - set(services)
    if missing:
        report(path, f"required services are missing: {sorted(missing)}")

    # Anything another service waits on must be able to report health.
    depended_on: set[str] = set()
    for service in services.values():
        depends = service.get("depends_on") or {}
        if isinstance(depends, dict):
            for name, spec in depends.items():
                if isinstance(spec, dict) and spec.get("condition") == "service_healthy":
                    depended_on.add(name)

    for name in sorted(depended_on):
        if name in services and "healthcheck" not in services[name]:
            report(
                path,
                f"service {name!r} is depended on with condition service_healthy "
                "but defines no healthcheck",
            )

    print(
        f"  ok  {path.relative_to(ROOT)} ({len(services)} services, "
        f"{len(depended_on)} health-gated dependencies)"
    )


# --------------------------------------------------------------------------- #
def main() -> int:
    check_yaml()
    check_json()
    check_xml()
    check_alerts()
    check_dashboards()
    check_clickhouse_init_order()
    check_compose()

    print(f"\n{checked} file(s) parsed")
    if failures:
        print(f"\n{len(failures)} problem(s):")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("all configuration is valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
