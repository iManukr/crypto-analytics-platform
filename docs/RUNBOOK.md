# Runbook

Every alert in `infra/prometheus/alerts.yml` links here. Each entry states what
the alert means, what it is safe to assume, and what to do — in that order,
because at 03:00 the ordering matters more than the content.

**First move, always:**

```bash
bash scripts/validate_stage.sh all
```

That walks all four stages and prints how far the data actually got. It is
almost always faster than reading dashboards.

---

## Quick triage

| Where freshness is climbing | Where the problem is |
|---|---|
| OLTP only | Ingestion — API or the service |
| OLTP flat, `raw` climbing | CDC — connector, Kafka, or ClickHouse |
| Both flat, marts climbing | Orchestration — Airflow or dbt |

The **Freshness by pipeline layer** panel on the Pipeline Health dashboard shows
all three lines at once and answers this in a glance.

---

## debezium-connector-down

**Alerts:** `DebeziumConnectorDown`, `DebeziumTaskFailed`

**What it means.** No change events are reaching Kafka. Postgres keeps accepting
writes, so this is silent data loss from the warehouse's point of view — the
replica falls further behind every second and nothing downstream complains,
because everything downstream is happily processing stale data.

**Do not** delete and recreate the connector as a first move. It works, and it
throws away the replication slot position, forcing a full re-snapshot.

### Diagnose

```bash
curl -s http://localhost:8083/connectors/crypto-oltp-cdc/status | python -m json.tool
```

The `trace` field on a failed task carries the Java exception. Then:

```bash
docker compose logs --tail=200 connect
```

### Common causes

**Postgres was unavailable.** The connector retries for 5 minutes
(`errors.retry.timeout`) then fails. Once Postgres is back, restart the task:

```bash
curl -X POST http://localhost:8083/connectors/crypto-oltp-cdc/tasks/0/restart
```

**The replication slot was dropped or invalidated.** If WAL retention exceeded
`max_slot_wal_keep_size`, Postgres invalidated the slot. Confirm:

```sql
SELECT slot_name, active, wal_status FROM pg_replication_slots;
```

`wal_status = 'lost'` means a re-snapshot is required. Delete the connector and
re-register; `snapshot.mode=initial` will rebuild. The upserts are idempotent,
so this is safe, just slow.

```bash
curl -X DELETE http://localhost:8083/connectors/crypto-oltp-cdc
docker compose up -d --force-recreate connect-init
```

**A record could not be converted.** `errors.tolerance` is deliberately `none` —
skipping a CDC record corrupts the replica permanently. The trace names the
offending record. Fix the cause, then restart the task.

### Verify

```bash
bash scripts/validate_stage.sh kafka
bash scripts/validate_stage.sh clickhouse
```

---

## cdc-lag-high

**Alerts:** `CDCLagHigh` (p95 > 120s), `CDCLagSevere` (p95 > 900s)

**What it means.** Data is arriving, just late. Measured from the rows
themselves, so unlike consumer lag it cannot be fooled by a dead producer.

### Diagnose, in this order

1. **Is it a backlog being worked through, or is it losing ground?** Watch the
   trend. Falling means it is catching up — usually after a restart replaying
   the snapshot, and no action is needed.

2. **Kafka consumer lag** on the Platform Resources dashboard. High lag with high
   CDC lag means ClickHouse cannot keep up. Low lag with high CDC lag means the
   delay is upstream, in Debezium or Postgres.

3. **ClickHouse merge pressure:**

   ```sql
   SELECT database, table, count() AS parts
   FROM system.parts WHERE active GROUP BY 1, 2 ORDER BY parts DESC;
   ```

   See [clickhouse-too-many-parts](#clickhouse-too-many-parts).

4. **Is the connector snapshotting?** A snapshot legitimately produces high lag
   until it completes. `docker compose logs connect | grep -i snapshot`.

### Fix

Sustained lag with no backlog usually means insert batching is too aggressive.
Raise `kafka_max_block_size` and `kafka_flush_interval_ms` on the Kafka engine
tables — fewer, larger inserts. See [SCALING.md](SCALING.md#stage-1--tuning-no-architecture-change).

---

## cdc-dead-letters

**Alert:** `CDCDeadLetters`

**What it means.** ClickHouse's Kafka engine could not parse a message. That
change event did not make it into the warehouse, so the replica is incomplete.
There is no acceptable non-zero count.

### Diagnose

```sql
SELECT topic, error, raw_message, received_at
FROM raw.cdc_dead_letters
ORDER BY received_at DESC LIMIT 10;
```

The most likely cause is an upstream schema change producing a shape the
materialized view's `JSONExtract` calls do not handle. The `raw_message` column
holds the full payload, so the actual shape is right there.

### Fix

1. Update the materialized view in `infra/clickhouse/init/04-materialized-views.sql`.
2. Recreate it (`DROP VIEW` + `CREATE MATERIALIZED VIEW`) — this does **not**
   touch the landing table.
3. Replay the affected offsets by detaching and re-attaching the Kafka table with
   an earlier offset, or accept the gap and backfill from Postgres.
4. Once resolved, `TRUNCATE TABLE raw.cdc_dead_letters` so the alert clears.

---

## row-parity-divergence

**Alert:** `RowParityDivergence`

**What it means.** The OLTP source and the OLAP replica disagree on row count in
a settled window. **The sign is the diagnosis:**

- **Positive** (Postgres has more) — change events were **lost**. Serious.
- **Negative** (ClickHouse has more) — duplicate events a merge has not yet
  collapsed. Usually benign.

The comparison window already excludes the last two minutes, so this is not
propagation delay.

### Diagnose

```bash
docker compose exec -T airflow-scheduler airflow dags trigger cdc_reconciliation
```

That reports the divergence per symbol per day, which localises it to a time
window immediately.

For a negative delta, confirm it is just unmerged duplicates:

```sql
SELECT count() AS raw_rows,
       (SELECT count() FROM raw.market_candles_1m FINAL WHERE _op != 'd') AS deduped
FROM raw.market_candles_1m;
```

If `deduped` matches Postgres, there is nothing wrong — force a merge if desired:

```sql
OPTIMIZE TABLE raw.market_candles_1m FINAL;
```

(Expensive on a large table. Merges happen on their own; this is impatience, not
a fix.)

### Fix a positive delta

Rows genuinely went missing. Backfill from the source:

```bash
docker compose exec -T airflow-scheduler python -c "
from ingestion.config import Config
from ingestion.service import run_backfill
print(run_backfill(Config.from_env(), minutes=1440))
"
```

Because the upsert only writes when a value actually changed, re-sending
unchanged rows produces no WAL and no CDC traffic. Only the genuinely missing
rows generate events.

---

## replication-slot-bloat

**Alerts:** `ReplicationSlotBloat`, `ReplicationSlotInactive`

**What it means.** Postgres is retaining WAL a slot has not confirmed. If it
keeps climbing, **the source database fills its disk and stops accepting
writes** — a monitoring component causing a production outage.

### Diagnose

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots;
```

### Fix

**If the connector is down:** restore it. See
[debezium-connector-down](#debezium-connector-down). The slot advances as soon
as consumption resumes.

**If the connector is up but the slot is not advancing:** the heartbeat is not
working. Confirm `heartbeat.interval.ms` is set on the connector; without it, a
quiet captured table means the slot never advances even while the rest of the
database is busy.

**If disk pressure is imminent and CDC cannot be restored quickly**, drop the
slot to protect the database. This forces a re-snapshot later — an acceptable
price for keeping the source alive:

```sql
SELECT pg_drop_replication_slot('dbz_crypto_slot');
```

Never restart Postgres to clear this. It does not help, and it loses the
diagnosis.

---

## ingestion-stalled

**Alerts:** `IngestionDown`, `IngestionStalled`, `DataFreshnessBreached`,
`IngestionErrorRateHigh`

**What it means.** `IngestionStalled` fires on *work completed*, not liveness —
so it catches the case a simple up/down check misses entirely: a perfectly
healthy container that is writing nothing.

### Diagnose

```bash
docker compose logs --tail=100 ingestion
curl -s http://localhost:8000/metrics | grep -E 'ingest_(api_requests|active_source|consecutive)'
```

### Common causes

**Geo-block (HTTP 451) or WAF (403).** Binance restricts several jurisdictions.
The host failover list handles this if an alternative is reachable; add hosts to
`BINANCE_HOSTS` in `.env`. If none are reachable, switch to the offline source
deliberately rather than waiting for the automatic fallback:

```bash
# in .env
INGEST_SOURCE=replay
```

Data produced this way is synthetic, stamped `source='replay'` and flagged
`is_synthetic` in the marts.

**Rate limited (429).** The service honours `Retry-After`. Sustained 429s mean
too many symbols for the interval — raise `INGEST_INTERVAL_SECONDS` or reduce
`SYMBOLS`.

**Postgres unreachable.** `ingest_errors_total{kind="database"}` climbing.
Check the OLTP container.

---

## replay-fallback-engaged

**Alert:** `IngestionUsingReplaySource`

**What it means.** The live API failed repeatedly and the service degraded to the
offline generator. **The pipeline is producing synthetic data.** Every row is
stamped `source = 'replay'` and surfaces as `dq_synthetic_source` /
`is_synthetic` downstream.

This alert exists because degradation must be as loud as failure. A dashboard
still drawing a line is worse than an empty one — nobody investigates a chart
that looks fine.

### Do

1. Diagnose the live source: see [ingestion-stalled](#ingestion-stalled).
2. Tell anyone consuming the marts that recent rows are synthetic.
3. The service returns to the live source automatically on the first success.
4. Once recovered, purge the synthetic rows if they must not persist:

   ```sql
   DELETE FROM crypto.market_candles_1m WHERE source = 'replay';
   ```

   Then backfill the real data and let CDC propagate the deletes.

---

## ingest-rejects

**Alert:** `IngestRejectsGrowing`

**What it means.** Rows are failing the ingest-time validation gate. This is the
system working — bad data is being stopped before it enters the pipeline — but a
rising rate means something upstream changed.

### Diagnose

```sql
SELECT reason, count(*), max(rejected_at)
FROM crypto.ingest_rejects
WHERE rejected_at > now() - interval '1 hour'
GROUP BY reason ORDER BY 2 DESC;

SELECT payload FROM crypto.ingest_rejects ORDER BY rejected_at DESC LIMIT 3;
```

The `reason` names the invariant that was broken and `payload` holds the
offending record, so this is usually diagnosable in one query. A change in the
upstream field order would show up as OHLC-consistency failures across the board.

---

## mart-stale

**Alert:** `MartStale`

**What it means.** The marts have not been rebuilt in over an hour, but the raw
layer is probably fine. The pipeline DAG runs every 15 minutes, so this is an
orchestration problem, not a data problem.

Confirm that first: if `pipeline_olap_freshness_seconds{database="raw"}` is low,
CDC is healthy and only the modelling has stopped.

### Diagnose

Open Airflow at <http://localhost:8080> and check `crypto_analytics_pipeline`.
Or:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list-runs -d crypto_analytics_pipeline -o table
```

### Common causes

**The scheduler is not running.** `docker compose ps airflow-scheduler`.

**The preflight task is failing.** It deliberately refuses to transform on top of
a dead CDC pipeline. Fix CDC first.

**The `wait_for_cdc_propagation` sensor is timing out.** CDC lag exceeds the SLA;
see [cdc-lag-high](#cdc-lag-high).

**dbt is failing.** Read the task log, or run it directly:

```bash
make dbt-build
```

---

## dbt-test-failure

**Alert:** `DbtTestsFailing`

**What it means.** A data-quality assertion failed. Tests run at
`severity: error`, so the DAG has already stopped — the marts are stale but not
wrong.

### Diagnose

Failing rows are persisted (`store_failures: true`):

```sql
SHOW TABLES FROM analytics_ops;
SELECT * FROM analytics_ops.<failing_test_name> LIMIT 20;
```

History across runs:

```sql
SELECT invocation_at, test_name, model_name, status, failures
FROM analytics_ops.dbt_test_results
WHERE status IN ('fail', 'error')
ORDER BY invocation_at DESC LIMIT 20;
```

### Interpreting specific failures

| Test | What it means |
|---|---|
| `unique_combination` on a fact | The `delete+insert` did not deduplicate — a real incremental bug |
| `ohlc_consistent` | The source produced an impossible bar, or a cast is wrong |
| `no_future_timestamps` | A clock is wrong. Fix urgently — it disables the freshness alert |
| `not_empty` | A model built successfully but is empty. Check the incremental filter |
| `assert_mart_reconciles_with_staging` | An incremental run skipped a window |
| `assert_rollup_reconciles` | The 5-minute rollup disagrees with the 1-minute fact — check for double-counted replays |
| `series_coverage` (warn) | Ingestion gaps. Normal in small numbers |

---

## clickhouse-too-many-parts

**Alerts:** `ClickHouseTooManyParts`, `ClickHouseRejectingInserts`

**What it means.** Inserts are outrunning background merges. This is a cliff, not
a slope: ClickHouse hard-rejects writes at `parts_to_throw_insert` (3000). If
`ClickHouseRejectingInserts` is firing, data is being lost **right now**.

### Diagnose

```sql
SELECT database, table, count() AS parts, sum(rows), formatReadableSize(sum(bytes_on_disk))
FROM system.parts WHERE active GROUP BY 1, 2 ORDER BY parts DESC;

SELECT * FROM system.merges;
SELECT event_time, table, error FROM system.part_log
WHERE event_time > now() - INTERVAL 1 HOUR AND error != 0 LIMIT 20;
```

### Fix

**Immediate relief:**

```sql
OPTIMIZE TABLE raw.market_candles_1m FINAL;
```

**The actual fix** is fewer, larger inserts. Raise `kafka_max_block_size` and
`kafka_flush_interval_ms` on the Kafka engine tables. Raising
`parts_to_throw_insert` treats the symptom and moves the cliff, it does not
remove it.

---

## kafka-consumer-lag

**Alert:** `KafkaConsumerLagHigh`

**What it means.** ClickHouse's Kafka engine is behind. Read it together with CDC
lag: sustained consumer lag with *healthy* CDC lag means a backlog is being
worked through; with *rising* CDC lag it means ground is being lost.

### Diagnose

```sql
SELECT * FROM system.kafka_consumers FORMAT Vertical;
```

`last_exception` there is usually the answer. Also check
[clickhouse-too-many-parts](#clickhouse-too-many-parts) — insert back-pressure is
the most common cause.

### Fix

At this scale a single partition is the ceiling. See
[SCALING.md](SCALING.md#2-single-kafka-partition--the-throughput-ceiling).

---

## airflow-dag-failure

**Alerts:** `AirflowDagFailing`, `AirflowSchedulerHeartbeatMissing`

**What it means.** Orchestration has stopped. The streaming path continues — CDC
keeps replicating into the raw layer — so this degrades the marts, not the raw
data.

### Diagnose

```bash
docker compose logs --tail=200 airflow-scheduler
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Import errors are the most common cause of a DAG that never runs at all: the
scheduler is healthy, the DAG simply is not there.

### Fix

**Scheduler not heartbeating:** `docker compose restart airflow-scheduler`.

**Metadata database unreachable:** the Airflow database lives in the same
Postgres container as the OLTP data, in a separate database. Check `postgres`.

**A task is stuck:** clear it and let it re-run.

```bash
docker compose exec -T airflow-scheduler \
  airflow tasks clear crypto_analytics_pipeline --yes
```

---

## observability-gaps

**Alerts:** `ExporterTargetDown`, `PrometheusTargetDown`

**What it means.** The *monitoring* is broken, not necessarily the pipeline. This
is the most dangerous state to be in unaware, because every alert that depends
on the missing metrics silently cannot fire, and the dashboard looks calm.

### Diagnose

```bash
curl -s http://localhost:9090/api/v1/targets | python -m json.tool | grep -A3 lastError
docker compose logs --tail=50 exporter
curl -s http://localhost:9101/metrics | grep exporter_target_up
```

`exporter_target_up` names which specific system the exporter cannot reach.

### Fix

Confirm the underlying system is genuinely healthy using its own interface
before assuming the pipeline is fine — then restart the exporter:

```bash
docker compose restart exporter
```

---

## Full reset

When the stack is in an unknown state and the data is disposable:

```bash
make clean          # down -v, deletes every volume
make up             # rebuild and restart
make wait           # block until CDC is flowing
make validate       # confirm all four stages
```

This is destructive. It drops the replication slot, all Kafka topics, every
ClickHouse table and all Postgres data. Everything rebuilds from the API within
one `BACKFILL_MINUTES` window.
