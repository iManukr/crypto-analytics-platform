# Observability Design

What is monitored, why those things and not others, and which tools were chosen.

**Contents**

1. [The principle](#1-the-principle)
2. [What is monitored](#2-what-is-monitored)
3. [The custom exporter, and why it exists](#3-the-custom-exporter-and-why-it-exists)
4. [Tool choices](#4-tool-choices)
5. [Dashboards](#5-dashboards)
6. [Alerting](#6-alerting)
7. [Gaps](#7-gaps)

---

## 1. The principle

Most pipeline monitoring measures whether the *software* is running. That is
necessary and insufficient. The failures that actually hurt in a data platform
are the ones where every process is healthy and the data is wrong:

- The connector is RUNNING but its task silently skipped a record.
- Kafka consumer lag is zero because the connector is dead and there is nothing
  to consume.
- The DAG succeeded and the tests passed — on data that stopped updating
  yesterday.
- A single future-dated row pinned the freshness metric at zero, so the staleness
  alert can never fire again.

Every one of those looks green on an infrastructure dashboard. So this design
splits monitoring into two questions and answers them separately:

| Question | Answered by |
|---|---|
| **Is the software running?** | ClickHouse native metrics, postgres_exporter, kafka_exporter, Airflow StatsD, cAdvisor |
| **Is the data correct and current?** | The custom pipeline exporter |

The second is what the pipeline exists to deliver, so it is the one the top row
of the main dashboard is dedicated to.

---

## 2. What is monitored

### Data freshness, per layer

`pipeline_oltp_freshness_seconds` and `pipeline_olap_freshness_seconds` measure
the age of the newest business timestamp at each stage. Tracked separately per
layer because that is what **localises** a stall rather than merely detecting it:

| Symptom | Diagnosis |
|---|---|
| OLTP climbing | Ingestion problem — the API, or the service |
| OLTP flat, `raw` climbing | CDC problem — connector, Kafka, or ClickHouse |
| Both flat, marts climbing | Orchestration problem — Airflow or dbt |

One panel, three lines, and the on-call engineer knows which of six systems to
open first.

### CDC lag — measured, not inferred

`cdc_end_to_end_lag_seconds{quantile="avg|p95|max"}`, computed from the rows
themselves as `_cdc_arrived_at - _source_ts_ms`: Postgres commit time to
ClickHouse visibility.

This is deliberately **not** Kafka consumer lag. Consumer lag is a proxy that
fails in the worst direction — it reads a comfortable zero when the connector is
down, because a topic nobody is producing to has nothing to lag on. Measuring
from the data cannot be fooled that way. Consumer lag is still collected, as a
*diagnostic* for why CDC lag is high, not as the headline number.

SLA: p95 under 120s. Typical observed: 1–2s.

### Row parity

`pipeline_row_parity_delta` — OLTP rows minus OLAP rows over the same settled
window (the last hour, excluding the last two minutes, which are legitimately in
flight).

This is the **only** signal that detects a silently dropped change event.
Everything else in this list would look perfect while the replica quietly runs
short a row forever. Sign matters:

- **Positive** — Postgres has more. Change events were *lost*. Serious.
- **Negative** — ClickHouse has more. Duplicate events awaiting a merge. Benign.

The `cdc_reconciliation` DAG performs the same comparison hourly at per-symbol,
per-day granularity and fails loudly on any divergence in a completed day.

### Replication slot lag

`cdc_replication_slot_lag_bytes` — WAL bytes Postgres is retaining because a
logical slot has not confirmed them.

This is the number that predicts an outage **in the source database**. Postgres
cannot recycle WAL a slot still needs; an unconsumed slot fills the disk and
stops the database accepting writes. That is a monitoring component taking down
the system it was only meant to observe, and it is the most common way a
Debezium deployment causes a production incident.

The Debezium heartbeat (`heartbeat.interval.ms = 10000`) exists specifically to
keep this flat during quiet periods, and `max_slot_wal_keep_size=2GB` bounds the
damage if it is not.

### Data quality

- `pipeline_ingest_rejects_total` — rows quarantined by the ingest-time
  validation gate, with `ingest_rows_rejected_total{reason}` for the breakdown.
- `cdc_dead_letters_total{topic}` — CDC messages ClickHouse could not parse.
  Any non-zero value means the replica is incomplete.
- `dq_dbt_test_failures{model}` — failing tests from the most recent dbt
  invocation, read from `analytics_ops.dbt_test_results`.

Keeping dbt test history in the warehouse, rather than only in a
`run_results.json` that the next run overwrites, turns "is this test failing?"
into "when did it start failing, and what else changed that day?".

### Source provenance

`ingest_active_source{source}` — 1 for the source currently in use.

Degradation has to be as loud as failure. A dashboard still drawing a line from
synthetic replay data is *worse* than a visibly empty one, because nobody
investigates a chart that looks fine. This metric, the `source` column carried
through to the marts, and the `IngestionUsingReplaySource` alert exist together
so that state cannot be silent.

### Warehouse health

`clickhouse_table_parts` is the ClickHouse metric most worth watching. Inserts
create parts; background merges collapse them. A sustained climb means merges
are losing, and ClickHouse **hard-rejects writes** at `parts_to_throw_insert`
(3000 by default) — a cliff, not a slope. The fix is fewer, larger inserts, not
a higher limit.

---

## 3. The custom exporter, and why it exists

Every metric in [§2](#2-what-is-monitored) that matters most is a **cross-system**
question. Postgres cannot tell you whether ClickHouse received a row. ClickHouse
cannot tell you whether Postgres sent one. Kafka knows about neither. No
off-the-shelf exporter can answer "is the replica complete", because answering
it requires querying both sides and comparing.

So `exporter/main.py` does exactly that, every 15 seconds, and nothing else. It
is ~350 lines with three dependencies.

Two design rules make it trustworthy:

**Failures are isolated per target.** Each collector is wrapped in `guarded()`.
One unreachable system degrades only its own metrics — it must not blind the
operator to the other four, precisely when they need them most.

**A failed scrape stops exporting rather than reporting a stale value.** The
gauge is cleared and `exporter_scrape_errors_total` increments. A comfortable-
looking old number is worse than a gap, because a gap is visible.

`exporter_target_up` is exposed so the monitoring's *own* health is visible: a
zero there means the metrics are missing, not necessarily that the system is.

---

## 4. Tool choices

### Prometheus and Grafana

Chosen for the reason they usually are: pull-based scraping means a dead target
is detectable (`up == 0`), the data model fits dimensional pipeline metrics
cleanly, PromQL expresses rate-of-change and ratio alerts directly, and both
run in one small container each with no external dependencies.

**Trade-off accepted:** Prometheus is not a long-term store. Retention is 15 days
locally. Anything needing quarter-over-quarter trend goes in
`analytics_ops`, in ClickHouse, which is a far better time-series store for that
purpose and is already running.

### ClickHouse native Prometheus endpoint

ClickHouse publishes its own metrics on `:9363/metrics`, so no sidecar exporter
is needed at all. One fewer container, and the metrics come from the source
rather than from a translation layer.

### Airflow via StatsD, not a Prometheus exporter plugin

Airflow emits StatsD natively; `statsd-exporter` translates it. The mapping file
(`infra/statsd/statsd-mapping.yml`) is the important part: Airflow embeds
`dag_id` and `task_id` **in the metric name**, so unmapped, a few hundred tasks
becomes a few hundred metric names that no query can aggregate over. The rules
pull those identifiers into labels, which is what makes
`sum by (dag_id) (...)` possible.

### No Alertmanager (locally)

Alert **rules** are fully defined and evaluate in Prometheus, visible in its UI
and in Grafana. Alertmanager itself is not deployed: with a single operator
watching a dashboard, routing, grouping and deduplication solve a problem that
does not yet exist, at the cost of another container to keep alive.

Adding it is a config change, not a rewrite — the `alerting.alertmanagers` block
in `prometheus.yml` is present with an empty target list, and every rule already
carries the `severity` label and `runbook` annotation that routing would key on.

### cAdvisor behind a profile

Container-level CPU/memory/IO needs privileged host mounts that not every
environment permits. It is behind `--profile full` so the stack comes up cleanly
without it, and Prometheus is configured not to fail on its absence.

---

## 5. Dashboards

Two, provisioned from version-controlled JSON.

**Pipeline Health** (`uid: pipeline-health`) — the operator view. The top row is
six stat panels that together answer "is the pipeline delivering correct,
current data right now": connector state, CDC p95 lag, data freshness, row
parity delta, failing dbt tests, active source. Everything below explains why
not — latency by layer, ingestion throughput and API outcomes, quality metrics,
orchestration outcomes and consumer lag.

**Platform Resources** (`uid: platform-resources`) — the infrastructure view,
consulted *after* Pipeline Health has said something is wrong. ClickHouse parts
and merge pressure, Postgres connections and replication slot lag, Kafka
throughput and per-partition lag, Airflow scheduler heartbeat and task
durations, and exporter target availability.

`allowUiUpdates: true` lets an operator tweak a panel live during an incident
without fighting the provisioner. The change is lost on restart, which is the
intended pressure to commit it.

A ClickHouse datasource is also provisioned for ad-hoc exploration of the data
itself. It needs a plugin downloaded on first boot; **no dashboard in this repo
depends on it**, so if that download fails everything still works.

---

## 6. Alerting

23 rules across five groups, in `infra/prometheus/alerts.yml`.

| Severity | Meaning |
|---|---|
| `critical` | Data is being lost or the pipeline has stopped. Act now. |
| `warning` | A budget is being consumed. Act today. |

Every actionable rule carries a `runbook` annotation pointing into
[RUNBOOK.md](RUNBOOK.md). This is enforced in CI — `scripts/validate_configs.py`
fails the lint job if a `critical` or `warning` alert has no runbook link. An
alert that fires at 03:00 with no stated next action is noise, and noise is what
teaches people to silence alerts.

The rules worth calling out:

- **`RowParityDivergence`** — the only alert that can detect a silently dropped
  change event. Its annotation explains the sign convention, because at 03:00
  nobody remembers which direction is benign.
- **`ReplicationSlotBloat`** — fires before the *source database* fills its disk.
- **`IngestionStalled`** — fires on *work completed*, not liveness. A healthy
  container that writes nothing is exactly what a simple up/down check misses.
- **`IngestionUsingReplaySource`** — makes silent degradation loud.
- **`PrometheusTargetDown`** — a scrape target that stops answering takes every
  alert depending on it with it. The monitoring failing silently is the most
  dangerous state, because the dashboard looks calm.

---

## 7. Gaps

Stated because a monitoring design that claims full coverage is not being
honest.

- **No distributed tracing.** A row cannot be followed by trace ID from API
  response to mart. At this depth (five hops) the per-layer freshness metrics
  localise a problem well enough; at greater depth OpenTelemetry would earn its
  place.
- **No log aggregation.** Logs are structured JSON and scrapable, but nothing
  scrapes them — diagnosis still means `docker compose logs`. Loki would slot in
  next to Grafana with no other changes.
- **No SLO error budgets.** Freshness and CDC lag have SLA thresholds and alerts,
  but no burn-rate tracking, so there is no measure of *how much* of a monthly
  budget an incident consumed.
- **No anomaly detection on the data itself.** A price that is valid but
  implausible (a genuine exchange glitch, correctly replicated) passes every
  check here. Statistical monitoring of value distributions, rather than only
  row counts and freshness, is the natural next layer.
