{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['symbol', 'trade_date'],
        engine='MergeTree()',
        order_by='(symbol, trade_date)',
        partition_by='toYYYYMM(trade_date)',
        tags=['mart', 'reporting']
    )
}}

/*
    Mart: daily market summary - the table a BI tool or a weekly report reads.

    Built from agg_candles_5m rather than fct_candles_1m. Rolling up from the
    already-aggregated layer is 1/5th the rows for an identical result on
    min/max/sum, and argMin/argMax over bucket_start reproduces the day's true
    open and close. The one place this matters is `minutes_covered`, which is
    carried up rather than recounted so that a day built from incomplete
    5-minute buckets reports honestly.

    The lookback here is 3 days rather than 3 hours: a late CDC update to a
    minute anywhere in a day changes that whole day's aggregate, and the current
    day is by definition still being written.
*/

with buckets as (

    select *
    from {{ ref('agg_candles_5m') }}
    where {{ incremental_since('bucket_start', hours=72, watermark='trade_date') }}

),

daily as (

    select
        symbol,
        toDate(bucket_start)                                as trade_date,

        any(base_asset)                                     as base_asset,
        any(quote_asset)                                    as quote_asset,

        argMin(open_price, bucket_start)                    as open_price,
        max(high_price)                                     as high_price,
        min(low_price)                                      as low_price,
        argMax(close_price, bucket_start)                   as close_price,

        sum(volume)                                         as volume,
        sum(quote_volume)                                   as quote_volume,
        sum(trade_count)                                    as trade_count,
        /* vwap is derived in the outer SELECT, not here. Referencing an
           aggregate's own output alias inside another aggregate in the same
           SELECT makes ClickHouse bind `volume` to the alias rather than the
           column, and it rejects the query with ILLEGAL_AGGREGATION. */

        argMax(fx_rate, bucket_start)                       as fx_rate_eod,
        sum(minutes_covered)                                as minutes_covered,
        countIf(not is_complete)                            as incomplete_buckets,
        max(contains_synthetic)                             as contains_synthetic

    from buckets
    group by symbol, trade_date

)

select
    symbol,
    trade_date,
    base_asset,
    quote_asset,

    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    quote_volume,
    trade_count,
    if(volume > 0, quote_volume / volume, close_price)      as vwap,

    close_price - open_price                                as price_change,
    if(open_price > 0,
       (close_price - open_price) / open_price,
       toDecimal64(0, 8))                                   as price_change_pct,
    high_price - low_price                                  as daily_range,

    /* Parkinson's estimator: uses the high-low range rather than only the
       close, so it extracts more information from the same day. The 4*ln(2)
       constant is what makes it an unbiased estimator of variance.

       Computed here, outside the aggregation, from the already-aggregated
       high/low. Written inside the GROUP BY it would nest min()/max() inside
       another aggregate and ClickHouse would reject it. */
    if(low_price > 0,
       sqrt(pow(log(toFloat64(high_price) / toFloat64(low_price)), 2) / (4.0 * log(2.0))),
       0.0)                                                 as realised_volatility,

    fx_rate_eod                                             as fx_rate,
    if(fx_rate_eod > 0, close_price * fx_rate_eod, null)    as close_price_fx,

    minutes_covered,
    incomplete_buckets,
    /* 1440 minutes in a day. Coverage below 100% is normal for the current
       (still-running) day and abnormal for any completed one - which is exactly
       the distinction the completeness test asserts on. */
    round(100.0 * minutes_covered / 1440.0, 2)              as coverage_pct,
    minutes_covered >= 1440                                 as is_complete_day,
    contains_synthetic,
    now64(3)                                                as dbt_updated_at

from daily
