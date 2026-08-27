# Scaling and Extension Plan

How this pipeline handles increasing data volume, what breaks first, and what
changes at each stage.

The current stack is single-node throughout by design: one Kafka broker, one
ClickHouse node, one Postgres instance, `LocalExecutor`. Everything below is the
plan for when that is no longer appropriate.

---

## Where it stands today

Two symbols at 1-minute grain: **2,880 rows/day**, roughly 1 MB/day compressed
in ClickHouse. This is a demonstrable stack, not a loaded one. Everything below
is about what happens as that number grows, and the honest answer is that the
first several orders of magnitude need almost no change at all.

| Scale | Rows/day | Verdict |
|---|---|---|
| Today: 2 symbols, 1m | 2.9 K | Trivially handled |
| 100 symbols, 1m | 144 K | No change needed |
| 500 symbols, 1m | 720 K | Tuning only ([Stage 1](#stage-1--tuning-no-architecture-change)) |
| 500 symbols + trades | ~50 M | Architecture changes ([Stage 2](#stage-2--horizontal-scale)) |
| Multi-exchange, tick | 1 B+ | Rearchitecture ([Stage 3](#stage-3--rearchitecture)) |

---

## What breaks first, in order

Knowing the *order* matters more than knowing the list — it says where to spend
effort.

### 1. ClickHouse "too many parts" — the first cliff

**Symptom:** `clickhouse_table_parts` climbs steadily, then inserts are rejected
outright at `parts_to_throw_insert` (3000).

This is the failure that arrives without warning: everything is fine, then
writes stop. Every insert creates a part; background merges collapse them. More
symbols means more Kafka messages, which at a fixed `kafka_flush_interval_ms`
means *more, smaller* inserts — exactly the wrong direction.

**Fix, in order of preference:**

1. Raise `kafka_max_block_size` and `kafka_flush_interval_ms` so ClickHouse
   batches larger. Fewer, bigger inserts is always the right first answer.
2. Enable `async_insert = 1` with `wait_for_async_insert = 0` for the Kafka path,
   letting ClickHouse buffer server-side.
3. Only then raise the limits.

Already alerted on (`ClickHouseTooManyParts` at 300, well below the cliff).

### 2. Single Kafka partition — the throughput ceiling

**Symptom:** `kafka_consumergroup_lag` climbs and never recovers.

Topics are single-partition, so exactly one ClickHouse consumer can read each.
Consumption cannot be parallelised at all.

**Fix:** partition by `symbol` (the Debezium message key is already the primary
key, so ordering per key is preserved), raise `topic.creation.default.partitions`,
and set `kafka_num_consumers` on the ClickHouse Kafka tables to match. Ordering
guarantees are per-partition, and because the key is the primary key, per-key
ordering — the only ordering CDC actually needs — survives.

### 3. Postgres WAL volume from `REPLICA IDENTITY FULL`

**Symptom:** WAL generation rate climbs superlinearly with update volume;
`cdc_replication_slot_lag_bytes` grows.

`FULL` logs the entire old row on every UPDATE and DELETE. It buys the
before-image that makes tombstones usable.

**Fix:** move the high-volume tables to `REPLICA IDENTITY DEFAULT` (primary key
only) and handle deletes by key alone in ClickHouse. Keep `FULL` on the
low-volume dimension tables where the before-image is genuinely useful and the
cost is nothing. This is a real trade — the audit value of the before-image is
lost — and it should be made deliberately, not by default.

### 4. `FINAL` in the staging views

**Symptom:** dbt runs slow down proportionally to total table size rather than to
the incremental window.

`do_not_merge_across_partitions_select_final = 1` already bounds this, and the
marts push partition-pruning predicates through the views. It holds well past
100 M rows.

**Fix when it stops holding:** replace `FINAL` with an explicit
`argMax(...)` grouped by the dedup key, which lets ClickHouse use a plain
aggregation path; or materialise staging as an incremental table.

### 5. Airflow `LocalExecutor`

**Symptom:** `airflow_executor_queued_tasks` stays above zero.

**Fix:** `CeleryExecutor` with Redis and horizontally scaled workers, or
`KubernetesExecutor` for per-task pods. The DAGs need no changes.

---

## Stage 1 — Tuning (no architecture change)

**Up to roughly 1 M rows/day.** Everything stays single-node.

| Change | Why |
|---|---|
| `kafka_flush_interval_ms` 1000 → 5000, `kafka_max_block_size` 8192 → 65536 | Fewer, larger parts. Directly addresses the first cliff. |
| Topic partitions 1 → 8, `kafka_num_consumers` → 4 | Parallel consumption |
| `tasks.max` stays 1 | The Postgres connector is single-task by design; a second task cannot read the same slot |
| Symbol allow-list via `SYMBOLS` | Already supported; no code change |
| Ingestion concurrency: thread pool over symbols | Currently sequential; at 100+ symbols a cycle would exceed the interval |
| `PARTITION BY toYYYYMM` → `toMonday` | Only if monthly partitions exceed ~100 GB |
| Prometheus retention 15d → 90d, or remote-write to Thanos/Mimir | Trend analysis beyond two weeks |

The ingestion change is the only code change in this stage. The rest is
configuration.

---

## Stage 2 — Horizontal scale

**Roughly 1 M – 100 M rows/day.** Components become clustered.

### ClickHouse: cluster with replication

```
ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/raw/market_candles_1m', '{replica}', _lsn)
```

Shard by `cityHash64(symbol)` so all of one symbol's data lands on one shard and
the common `WHERE symbol = ...` query hits a single node. Add `Distributed`
tables over the shards for cross-symbol queries.

dbt-clickhouse already supports this: set `cluster_mode: True` and `cluster` in
the `prod` profile target (both already present in `dbt/profiles.yml`) and use
the `distributed_table` / `distributed_incremental` materializations. **The model
SQL does not change.**

### Kafka: multi-broker

Three brokers, `replication.factor=3`, `min.insync.replicas=2`. Topic partitions
sized to peak throughput divided by per-consumer capacity. Retention driven by
recovery objective — long enough to rebuild ClickHouse from Kafka without a
Postgres re-snapshot.

### Debezium: connector per table group

The Postgres connector is single-task per slot, so the scaling axis is *more
connectors*, each with its own publication and slot, grouped by table. Run
Connect as a multi-worker distributed cluster so connectors rebalance across
workers on failure.

### Avro plus Schema Registry

Now worth it. JSON costs roughly 3× the bytes, and at this volume that is real
network and disk. Confluent Schema Registry plus `AvroConverter` on the
connector; ClickHouse Kafka tables switch to `kafka_format = 'AvroConfluent'`.

**This is deliberately a small change** — a converter swap and a format setting
on three tables — which is precisely why the current design reads the envelope
as an opaque string rather than depending on Debezium's SMT layer.

### Postgres: read replica and partitioning

Partition `market_candles_1m` by range on `open_time` (monthly) so old partitions
detach cheaply. Note that logical replication must still read from the primary —
a read replica offloads analytics queries, not CDC.

---

## Stage 3 — Rearchitecture

**100 M+ rows/day, or tick-level, or multi-region.** The current shape stops
being the right one.

**Object storage as the source of truth.** Land raw CDC in Parquet on S3 (Iceberg
or Delta), with ClickHouse querying it via `s3()`/Iceberg table functions and
materialising only hot data locally. Decouples storage cost from query capacity
and makes the raw layer replayable indefinitely.

**Stream processing between Kafka and the warehouse.** Flink or Kafka Streams for
enrichment, windowed aggregation and joins before the warehouse, so ClickHouse
receives a narrower, pre-shaped stream.

**Tiered storage in ClickHouse.** Hot data on NVMe, warm on S3, via storage
policies and `TTL ... TO VOLUME`. The 24-month TTL already on the raw table
becomes a tiering rule instead of a deletion.

**Separate serving from analytics.** Point dashboards at pre-aggregated marts and
materialized views only; keep exploratory queries off the ingest path entirely so
one expensive ad-hoc query cannot back up CDC consumption.

---

## Cost and effort summary

| Stage | Volume | Effort | Nature of the work |
|---|---|---|---|
| Current | < 100 K/day | — | — |
| Stage 1 | < 1 M/day | ~1 day | Config, plus threading the ingester |
| Stage 2 | < 100 M/day | ~2 weeks | Clustering, Avro. Model SQL unchanged. |
| Stage 3 | 100 M+/day | ~2 months | Lakehouse, stream processing, tiering |

The important property is the shape of that table: **the dbt model SQL does not
change through Stage 2.** Physical layout, cluster topology and serialisation
format are all configuration. That is the payoff for keeping engine, ordering
and partitioning as dbt configs rather than baking them into model bodies, and
for keeping environment-specific settings in profile targets rather than in the
transformations.

---

## Extensions worth building next

Roughly in order of value per unit of effort.

1. **Alertmanager plus PagerDuty/Slack routing.** The rules already carry
   `severity` labels and runbook annotations; this is wiring, not design.
2. **Loki for log aggregation.** Logs are already structured JSON. Closes the
   largest gap in [OBSERVABILITY.md](OBSERVABILITY.md).
3. **A second data source.** The source registry in `ingestion/sources/` was
   built for exactly this — a new adapter plus a registry entry, no changes to
   the service loop, the DAG or the models.
4. **Backfill from Binance's historical archive.** Would give the ML mart years
   of contiguous history instead of hours, which is the single biggest limitation
   of that dataset today.
5. **A model serving loop.** Score `ml_features_1m`, write predictions to a
   `fct_predictions` table, resolve them against actual outcomes, and track
   rolling accuracy against a majority-class baseline — including honestly
   reporting when there is no edge.
6. **Data contracts on the sources.** dbt model contracts plus
   `on_schema_change: fail` for the mart layer, so an upstream schema change
   fails the build rather than silently propagating.
7. **SLO error budgets** on freshness and CDC lag, with burn-rate alerting.
