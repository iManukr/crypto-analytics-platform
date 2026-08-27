-- =============================================================================
-- KAFKA ENGINE TABLES - the CDC ingress into ClickHouse
--
-- Why the Kafka engine instead of the ClickHouse Kafka Connect sink?
--   * One fewer moving part: no sink connector process to size, monitor or
--     restart, and no second place where schema mapping can drift.
--   * Offsets are committed by ClickHouse itself, so "what has ClickHouse
--     consumed" has exactly one answer, visible in system.kafka_consumers.
--   * Back-pressure is handled inside ClickHouse's own insert path.
--
-- Why kafka_format = 'JSONAsString' rather than JSONEachRow?
--   The Debezium envelope is nested (before / after / source / op / ts_ms) and
--   evolves with connector upgrades. Reading the whole message as one String
--   column and projecting fields in the materialized view means:
--     * a new or renamed field upstream cannot break the consumer,
--     * no dependency on Debezium's ExtractNewRecordState SMT, whose option
--       names changed across 1.x/2.x,
--     * before-images stay available for DELETE handling.
--   The cost is explicit JSONExtract calls in the MV, which is a fair trade for
--   a consumer that does not fall over on a connector upgrade.
--
-- kafka_handle_error_mode = 'stream' routes unparseable messages to the
-- _error / _raw_message virtual columns instead of stalling the consumer;
-- a second set of materialized views drains those into raw.cdc_dead_letters.
--
-- kafka_num_consumers is 1 here because the local broker runs single-partition
-- topics. Scaling this is covered in docs/SCALING.md.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.kafka_market_candles_1m (raw String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list         = 'kafka:29092',
    kafka_topic_list          = 'cdc.crypto.market_candles_1m',
    kafka_group_name          = 'clickhouse-cdc-candles',
    kafka_format              = 'JSONAsString',
    kafka_num_consumers       = 1,
    kafka_max_block_size      = 8192,
    kafka_poll_max_batch_size = 1000,
    kafka_flush_interval_ms   = 1000,
    kafka_handle_error_mode   = 'stream';

CREATE TABLE IF NOT EXISTS raw.kafka_symbols (raw String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list       = 'kafka:29092',
    kafka_topic_list        = 'cdc.crypto.symbols',
    kafka_group_name        = 'clickhouse-cdc-symbols',
    kafka_format            = 'JSONAsString',
    kafka_num_consumers     = 1,
    kafka_flush_interval_ms = 1000,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS raw.kafka_fx_rates (raw String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list       = 'kafka:29092',
    kafka_topic_list        = 'cdc.crypto.fx_rates',
    kafka_group_name        = 'clickhouse-cdc-fx',
    kafka_format            = 'JSONAsString',
    kafka_num_consumers     = 1,
    kafka_flush_interval_ms = 1000,
    kafka_handle_error_mode = 'stream';
