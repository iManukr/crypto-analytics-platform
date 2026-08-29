{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['symbol', 'bucket_start'],
        engine='MergeTree()',
        order_by='(symbol, bucket_start)',
        partition_by='toYYYYMM(bucket_start)',
        tags=['mart', 'candles', 'rollup']
    )
}}

/*
    Mart: 5-minute OHLCV rollup - the AUTHORITATIVE one.

    ClickHouse also maintains raw.candles_5m_rt, a materialized view over the
    same data that updates within a second of a minute landing. That one is
    fast and approximate: it aggregates every insert it sees, so a Debezium
    replay double-counts its volume. This model aggregates from the deduplicated
    fact table instead, so its numbers reconcile. Both exist on purpose; the
    docs and the Grafana panel titles say which is which.

    open/close use argMin/argMax over open_time rather than min/max over price.
    That distinction is the whole point of an OHLC bar: the "open" is the price
    at the earliest minute, not the smallest price in the window.

    minutes_covered is carried through as a completeness signal. A bucket with
    fewer than 5 minutes is either still filling or has a gap, and a consumer
    that averages across incomplete buckets without noticing gets a subtly wrong
    answer that no test would otherwise catch.
*/

with source_candles as (

    select *
    from {{ ref('fct_candles_1m') }}
    where {{ incremental_since('open_time', watermark='bucket_start') }}
      and is_valid

),

aggregated as (

    /* Aggregation happens here and ONLY here. Deriving a ratio in the same
       SELECT as the sums it divides would make ClickHouse resolve `volume` to
       the output alias rather than the source column, and reject the query
       with "aggregate function found inside another aggregate function". */
    select
        symbol,
        toStartOfFiveMinute(open_time)          as bucket_start,

        any(base_asset)                         as base_asset,
        any(quote_asset)                        as quote_asset,

        /* open/close are argMin/argMax over TIME, not min/max over price: the
           open of a bar is the price at its earliest minute, not its lowest. */
        argMin(open_price, open_time)           as open_price,
        max(high_price)                         as high_price,
        min(low_price)                          as low_price,
        argMax(close_price, open_time)          as close_price,

        sum(volume)                             as volume,
        sum(quote_volume)                       as quote_volume,
        sum(trade_count)                        as trade_count,
        sum(taker_buy_base)                     as taker_buy_base,
        argMax(fx_rate, open_time)              as fx_rate,

        count(*)                                as minutes_covered,
        countIf(data_source = 'replay') > 0     as contains_synthetic

    from source_candles
    group by symbol, bucket_start

)

select
    symbol,
    bucket_start,
    bucket_start + toIntervalMinute(5)          as bucket_end,

    base_asset,
    quote_asset,

    open_price,
    high_price,
    low_price,
    close_price,

    volume,
    quote_volume,
    trade_count,
    taker_buy_base,

    if(volume > 0, toFloat64(taker_buy_base) / toFloat64(volume), 0.0)
                                                as taker_buy_ratio,
    if(volume > 0, quote_volume / volume, close_price)
                                                as vwap,

    close_price - open_price                    as price_change,
    if(open_price > 0,
       (close_price - open_price) / open_price,
       toDecimal64(0, 8))                       as price_change_pct,

    fx_rate,
    if(fx_rate > 0, close_price * fx_rate, null) as close_price_fx,

    /* Carried through as a completeness signal. A bucket with fewer than five
       minutes is either still filling or has a gap, and averaging across
       incomplete buckets without noticing gives a subtly wrong answer that no
       other test would catch. */
    minutes_covered,
    minutes_covered = 5                         as is_complete,
    contains_synthetic,
    now64(3)                                    as dbt_updated_at

from aggregated
