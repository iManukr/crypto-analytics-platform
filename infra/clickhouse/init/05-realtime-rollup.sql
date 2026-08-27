-- =============================================================================
-- REAL-TIME SERVING ROLLUP (ClickHouse-native, not dbt-managed)
--
-- This is the low-latency counterpart to the dbt mart analytics_marts.agg_candles_5m.
-- Materialized views chain in ClickHouse: an insert that arrives through
-- raw.mv_market_candles_1m into raw.market_candles_1m also fires this view, so
-- a 5-minute bucket updates within a second of the underlying minute landing,
-- with no orchestrator involvement at all.
--
-- Engine: AggregatingMergeTree holds partial aggregation states, merged in the
-- background and finalised at read time with the -Merge combinators. Storing
-- states rather than finals is what makes the rollup incrementally maintainable.
--
-- HONEST CAVEAT - read before trusting the volume column:
--   Debezium is at-least-once. If the connector restarts and replays, the same
--   minute can be inserted twice. argMin/argMax/max/min states are idempotent
--   under replay, but sumState is NOT: replayed rows double-count volume and
--   trade_count.
--
--   That is why this object is positioned as the *fast, approximate* serving
--   layer, and analytics_marts.agg_candles_5m - which deduplicates via FINAL
--   before aggregating - is the authoritative one. Dashboards that need a
--   number to reconcile with finance read the mart; dashboards that need the
--   last few seconds read this. The distinction is documented in
--   docs/DATA_MODEL.md and surfaced in the Grafana panel titles.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.candles_5m_rt
(
    symbol        LowCardinality(String),
    bucket_start  DateTime('UTC'),
    open_state    AggregateFunction(argMin, Decimal(20, 8), DateTime64(3, 'UTC')),
    high_state    AggregateFunction(max, Decimal(20, 8)),
    low_state     AggregateFunction(min, Decimal(20, 8)),
    close_state   AggregateFunction(argMax, Decimal(20, 8), DateTime64(3, 'UTC')),
    volume_state  AggregateFunction(sum, Decimal(30, 8)),
    trades_state  AggregateFunction(sum, Int64),
    minutes_state AggregateFunction(uniqExact, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (symbol, bucket_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_candles_5m_rt
TO raw.candles_5m_rt AS
SELECT
    symbol,
    toStartOfFiveMinute(open_time)            AS bucket_start,
    argMinState(open_price, open_time)        AS open_state,
    maxState(high_price)                      AS high_state,
    minState(low_price)                       AS low_state,
    argMaxState(close_price, open_time)       AS close_state,
    sumState(volume)                          AS volume_state,
    sumState(toInt64(trade_count))            AS trades_state,
    uniqExactState(open_time)                 AS minutes_state
FROM raw.market_candles_1m
WHERE _op != 'd'
GROUP BY symbol, bucket_start;

-- Read-time convenience view. minutes_covered is exposed deliberately: a bucket
-- reporting more than 5 distinct minutes is impossible, and one reporting fewer
-- is still filling. It is the cheapest completeness signal available here.
CREATE VIEW IF NOT EXISTS raw.candles_5m_rt_v AS
SELECT
    symbol,
    bucket_start,
    argMinMerge(open_state)     AS open_price,
    maxMerge(high_state)        AS high_price,
    minMerge(low_state)         AS low_price,
    argMaxMerge(close_state)    AS close_price,
    sumMerge(volume_state)      AS volume_approx,
    sumMerge(trades_state)      AS trade_count_approx,
    uniqExactMerge(minutes_state) AS minutes_covered
FROM raw.candles_5m_rt
GROUP BY symbol, bucket_start;

-- =============================================================================
-- OPS LAYER - pipeline metadata that the exporter and Grafana read
-- =============================================================================

-- One row per dbt test, appended by the Airflow DAG after every dbt build.
-- Keeping test history in the warehouse (rather than only in run_results.json)
-- makes "has this test ever failed, and when did it start" a SQL question.
CREATE TABLE IF NOT EXISTS analytics_ops.dbt_test_results
(
    run_id        String,
    invocation_at DateTime64(3, 'UTC'),
    node_id       String,
    test_name     String,
    model_name    String,
    status        LowCardinality(String),
    failures      UInt32,
    execution_ms  Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(invocation_at)
ORDER BY (invocation_at, node_id)
TTL toDateTime(invocation_at) + INTERVAL 90 DAY;

-- One row per orchestrated pipeline run, written by the Airflow DAG. Gives the
-- exporter a warehouse-side view of orchestration health that survives an
-- Airflow metadata-database reset.
CREATE TABLE IF NOT EXISTS analytics_ops.pipeline_runs
(
    run_id           String,
    dag_id           LowCardinality(String),
    started_at       DateTime64(3, 'UTC'),
    finished_at      DateTime64(3, 'UTC'),
    status           LowCardinality(String),
    rows_ingested    UInt64,
    models_built     UInt32,
    tests_passed     UInt32,
    tests_failed     UInt32,
    notes            String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (dag_id, started_at)
TTL toDateTime(started_at) + INTERVAL 180 DAY;
