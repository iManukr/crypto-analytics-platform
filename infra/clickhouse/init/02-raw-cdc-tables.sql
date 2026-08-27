-- =============================================================================
-- RAW LAYER - CDC landing tables
--
-- Engine choice: ReplacingMergeTree(_lsn)
--   Debezium is at-least-once, so the same change can arrive twice after a
--   connector restart. ReplacingMergeTree collapses rows sharing the ORDER BY
--   key, keeping the one with the highest version column. Postgres LSN is
--   strictly monotonic within an instance, which makes it the natural version:
--   a later UPDATE always wins over the snapshot row for the same key.
--
--   Merges are asynchronous, so duplicates are visible until a merge runs.
--   Every read path that needs exactness therefore either uses FINAL (staging
--   views) or an explicit argMax (marts). We never assume the merge happened.
--
-- Deletes: kept as tombstones (_op = 'd') rather than physically removed, so
--   the staging layer can decide the semantics and so a mistaken delete
--   upstream stays auditable. Staging filters them out.
--
-- Codecs: timestamps are near-monotonic, so Delta+ZSTD compresses them far
--   better than the default LZ4. Prices within a symbol move slowly, so
--   ZSTD(3) on the decimals buys a meaningful ratio for very little CPU.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.market_candles_1m
(
    symbol           LowCardinality(String),
    open_time        DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    close_time       DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    open_price       Decimal(20, 8)       CODEC(ZSTD(3)),
    high_price       Decimal(20, 8)       CODEC(ZSTD(3)),
    low_price        Decimal(20, 8)       CODEC(ZSTD(3)),
    close_price      Decimal(20, 8)       CODEC(ZSTD(3)),
    volume           Decimal(30, 8)       CODEC(ZSTD(3)),
    quote_volume     Decimal(30, 8)       CODEC(ZSTD(3)),
    trade_count      Int32                CODEC(T64, ZSTD(1)),
    taker_buy_base   Decimal(30, 8)       CODEC(ZSTD(3)),
    taker_buy_quote  Decimal(30, 8)       CODEC(ZSTD(3)),
    source           LowCardinality(String),
    ingested_at      DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),

    -- CDC metadata. Prefixed with _ so it never collides with a source column.
    _op              LowCardinality(String),              -- c | u | d | r (read/snapshot)
    _lsn             UInt64,                              -- version for dedup
    _source_ts_ms    UInt64,                              -- Postgres commit time
    _kafka_partition UInt32,
    _kafka_offset    UInt64,
    _cdc_arrived_at  DateTime64(3, 'UTC') DEFAULT now64(3)  -- for end-to-end lag
)
ENGINE = ReplacingMergeTree(_lsn)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
TTL toDateTime(open_time) + INTERVAL 24 MONTH
SETTINGS index_granularity = 8192;

-- Low-cardinality dimension. Same engine for the same reason, but no
-- partitioning: the table is a handful of rows and partitions would only add
-- metadata overhead.
CREATE TABLE IF NOT EXISTS raw.symbols
(
    symbol           String,
    base_asset       LowCardinality(String),
    quote_asset      LowCardinality(String),
    display_name     String,
    is_active        UInt8,
    created_at       DateTime64(3, 'UTC'),
    updated_at       DateTime64(3, 'UTC'),
    _op              LowCardinality(String),
    _lsn             UInt64,
    _source_ts_ms    UInt64,
    _kafka_partition UInt32,
    _kafka_offset    UInt64,
    _cdc_arrived_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY symbol;

-- Current-value FX table upstream, so this is the UPDATE-heavy stream: every
-- refresh is an UPDATE on the same primary key, which makes it the clearest
-- demonstration that the version column is doing its job.
--
-- Note the ORDER BY includes as_of. That is deliberate and it changes the
-- semantics: Postgres keeps only the *current* rate, but ClickHouse keeps one
-- row per distinct as_of, so the warehouse accumulates the rate history the
-- OLTP store throws away. Dedup still applies within an as_of, which is what
-- collapses replayed CDC events. Without this, a mart could only ever reprice
-- history at today's rate - which would silently rewrite the past every day.
CREATE TABLE IF NOT EXISTS raw.fx_rates
(
    base             LowCardinality(String),
    quote            LowCardinality(String),
    rate             Decimal(20, 8),
    as_of            DateTime64(3, 'UTC'),
    source           String,
    updated_at       DateTime64(3, 'UTC'),
    _op              LowCardinality(String),
    _lsn             UInt64,
    _source_ts_ms    UInt64,
    _kafka_partition UInt32,
    _kafka_offset    UInt64,
    _cdc_arrived_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (base, quote);

-- Dead-letter table. The Kafka engines run with kafka_handle_error_mode
-- = 'stream', so a message that cannot be parsed lands here instead of
-- stalling the consumer. Alerted on in Prometheus.
CREATE TABLE IF NOT EXISTS raw.cdc_dead_letters
(
    topic        LowCardinality(String),
    partition    UInt32,
    offset       UInt64,
    error        String,
    raw_message  String,
    received_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(received_at)
ORDER BY (topic, received_at)
TTL toDateTime(received_at) + INTERVAL 30 DAY;
