-- =============================================================================
-- CDC PROJECTION MATERIALIZED VIEWS
--
-- Each view is an insert trigger on its Kafka table: as messages are polled the
-- SELECT runs over the new block and the result is pushed TO the corresponding
-- ReplacingMergeTree landing table.
--
-- Envelope handling:
--   op = c | u | r  -> the current row state lives in the "after" object
--   op = d          -> "after" is null; the before-image lives in "before"
--                      (available because the upstream tables are declared
--                      REPLICA IDENTITY FULL)
--
-- Type notes:
--   * decimal.handling.mode=string on the connector means Postgres numerics
--     arrive as JSON strings. toDecimal*OrZero parses them exactly; a float
--     round-trip would not preserve 8 decimal places reliably.
--   * timestamptz columns arrive as ISO-8601 strings (Debezium ZonedTimestamp),
--     parsed with parseDateTime64BestEffortOrZero.
--   * source.lsn is the dedup version. It is absent only for records that did
--     not originate in the WAL, in which case 0 sorts below every real change,
--     which is the behaviour we want.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_market_candles_1m
TO raw.market_candles_1m AS
WITH
    if(JSONExtractString(raw, 'op') = 'd',
       JSONExtractRaw(raw, 'before'),
       JSONExtractRaw(raw, 'after')) AS row_json
SELECT
    JSONExtractString(row_json, 'symbol')                                  AS symbol,
    parseDateTime64BestEffortOrZero(JSONExtractString(row_json, 'open_time'), 3, 'UTC')   AS open_time,
    parseDateTime64BestEffortOrZero(JSONExtractString(row_json, 'close_time'), 3, 'UTC')  AS close_time,
    toDecimal64OrZero(JSONExtractString(row_json, 'open_price'), 8)        AS open_price,
    toDecimal64OrZero(JSONExtractString(row_json, 'high_price'), 8)        AS high_price,
    toDecimal64OrZero(JSONExtractString(row_json, 'low_price'), 8)         AS low_price,
    toDecimal64OrZero(JSONExtractString(row_json, 'close_price'), 8)       AS close_price,
    toDecimal128OrZero(JSONExtractString(row_json, 'volume'), 8)           AS volume,
    toDecimal128OrZero(JSONExtractString(row_json, 'quote_volume'), 8)     AS quote_volume,
    JSONExtractInt(row_json, 'trade_count')                                AS trade_count,
    toDecimal128OrZero(JSONExtractString(row_json, 'taker_buy_base'), 8)   AS taker_buy_base,
    toDecimal128OrZero(JSONExtractString(row_json, 'taker_buy_quote'), 8)  AS taker_buy_quote,
    JSONExtractString(row_json, 'source')                                  AS source,
    parseDateTime64BestEffortOrZero(JSONExtractString(row_json, 'ingested_at'), 3, 'UTC') AS ingested_at,
    JSONExtractString(raw, 'op')                                           AS _op,
    JSONExtractUInt(raw, 'source', 'lsn')                                  AS _lsn,
    JSONExtractUInt(raw, 'source', 'ts_ms')                                AS _source_ts_ms,
    _partition                                                             AS _kafka_partition,
    _offset                                                                AS _kafka_offset,
    now64(3)                                                               AS _cdc_arrived_at
FROM raw.kafka_market_candles_1m
WHERE length(_error) = 0 AND notEmpty(row_json) AND row_json != 'null';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_symbols
TO raw.symbols AS
WITH
    if(JSONExtractString(raw, 'op') = 'd',
       JSONExtractRaw(raw, 'before'),
       JSONExtractRaw(raw, 'after')) AS row_json
SELECT
    JSONExtractString(row_json, 'symbol')           AS symbol,
    JSONExtractString(row_json, 'base_asset')       AS base_asset,
    JSONExtractString(row_json, 'quote_asset')      AS quote_asset,
    JSONExtractString(row_json, 'display_name')     AS display_name,
    toUInt8(JSONExtractBool(row_json, 'is_active')) AS is_active,
    parseDateTime64BestEffortOrZero(JSONExtractString(row_json, 'created_at'), 3, 'UTC') AS created_at,
    parseDateTime64BestEffortOrZero(JSONExtractString(row_json, 'updated_at'), 3, 'UTC') AS updated_at,
    JSONExtractString(raw, 'op')                    AS _op,
    JSONExtractUInt(raw, 'source', 'lsn')           AS _lsn,
    JSONExtractUInt(raw, 'source', 'ts_ms')         AS _source_ts_ms,
    _partition                                      AS _kafka_partition,
    _offset                                         AS _kafka_offset,
    now64(3)                                        AS _cdc_arrived_at
FROM raw.kafka_symbols
WHERE length(_error) = 0 AND notEmpty(row_json) AND row_json != 'null';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_fx_rates
TO raw.fx_rates AS
WITH
    if(JSONExtractString(raw, 'op') = 'd',
       JSONExtractRaw(raw, 'before'),
       JSONExtractRaw(raw, 'after')) AS row_json
SELECT
    JSONExtractString(row_json, 'base')                      AS base,
    JSONExtractString(row_json, 'quote')                     AS quote,
    toDecimal64OrZero(JSONExtractString(row_json, 'rate'), 8) AS rate,
    parseDateTime64BestEffortOrZero(JSONExtractString(row_json, 'as_of'), 3, 'UTC')      AS as_of,
    JSONExtractString(row_json, 'source')                    AS source,
    parseDateTime64BestEffortOrZero(JSONExtractString(row_json, 'updated_at'), 3, 'UTC') AS updated_at,
    JSONExtractString(raw, 'op')                             AS _op,
    JSONExtractUInt(raw, 'source', 'lsn')                    AS _lsn,
    JSONExtractUInt(raw, 'source', 'ts_ms')                  AS _source_ts_ms,
    _partition                                               AS _kafka_partition,
    _offset                                                  AS _kafka_offset,
    now64(3)                                                 AS _cdc_arrived_at
FROM raw.kafka_fx_rates
WHERE length(_error) = 0 AND notEmpty(row_json) AND row_json != 'null';

-- --------------------------------------------------------------- dead letters
-- One drain per Kafka table. These fire on exactly the messages the projection
-- views skip, so nothing is silently dropped.

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_dlq_candles TO raw.cdc_dead_letters AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message, now64(3) AS received_at
FROM raw.kafka_market_candles_1m WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_dlq_symbols TO raw.cdc_dead_letters AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message, now64(3) AS received_at
FROM raw.kafka_symbols WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_dlq_fx TO raw.cdc_dead_letters AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message, now64(3) AS received_at
FROM raw.kafka_fx_rates WHERE length(_error) > 0;
