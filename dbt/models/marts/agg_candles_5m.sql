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

)

select
    symbol,
    toStartOfFiveMinute(open_time)                          as bucket_start,
    toStartOfFiveMinute(open_time) + toIntervalMinute(5)    as bucket_end,

    any(base_asset)                                         as base_asset,
    any(quote_asset)                                        as quote_asset,

    argMin(open_price, open_time)                           as open_price,
    max(high_price)                                         as high_price,
    min(low_price)                                          as low_price,
    argMax(close_price, open_time)                          as close_price,

    sum(volume)                                             as volume,
    sum(quote_volume)                                       as quote_volume,
    sum(trade_count)                                        as trade_count,
    sum(taker_buy_base)                                     as taker_buy_base,

    if(sum(volume) > 0,
       toFloat64(sum(taker_buy_base)) / toFloat64(sum(volume)),
       0.0)                                                 as taker_buy_ratio,
    if(sum(volume) > 0, sum(quote_volume) / sum(volume),
       argMax(close_price, open_time))                      as vwap,

    argMax(close_price, open_time) - argMin(open_price, open_time)
                                                            as price_change,
    if(argMin(open_price, open_time) > 0,
       (argMax(close_price, open_time) - argMin(open_price, open_time))
         / argMin(open_price, open_time),
       toDecimal64(0, 8))                                   as price_change_pct,

    argMax(fx_rate, open_time)                              as fx_rate,
    if(argMax(fx_rate, open_time) > 0,
       argMax(close_price, open_time) * argMax(fx_rate, open_time),
       null)                                                as close_price_fx,

    count(*)                                                as minutes_covered,
    count(*) = 5                                            as is_complete,
    countIf(data_source = 'replay') > 0                     as contains_synthetic,
    now64(3)                                                as dbt_updated_at

from source_candles
group by symbol, bucket_start
