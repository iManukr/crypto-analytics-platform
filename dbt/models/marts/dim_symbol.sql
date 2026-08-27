{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(symbol)',
        tags=['mart', 'reference']
    )
}}

/*
    Mart: tradable pair dimension.

    Materialised as a full table rather than incrementally: it holds a handful
    of rows and rebuilding it costs milliseconds, so incremental logic would be
    pure risk with no payoff. No PARTITION BY for the same reason - partitions
    on a ten-row table are metadata overhead and nothing else.

    Activity statistics are joined in from the fact so that "which pairs do we
    actually have data for, and how much" is answerable without touching the
    large table.
*/

with symbols as (

    select * from {{ ref('stg_symbols') }}

),

activity as (

    select
        symbol,
        count(*)                                            as candle_count,
        min(open_time)                                      as first_candle_at,
        max(open_time)                                      as last_candle_at,
        countIf(data_source = 'replay')                     as synthetic_candle_count,
        countIf(not is_valid)                               as invalid_candle_count
    from {{ ref('fct_candles_1m') }}
    group by symbol

)

select
    s.symbol,
    s.base_asset,
    s.quote_asset,
    s.display_name,
    s.is_active,
    s.is_deleted,
    s.created_at,
    s.updated_at,

    coalesce(a.candle_count, 0)                             as candle_count,
    a.first_candle_at,
    a.last_candle_at,
    coalesce(a.synthetic_candle_count, 0)                   as synthetic_candle_count,
    coalesce(a.invalid_candle_count, 0)                     as invalid_candle_count,

    /* A symbol that is active upstream but has no recent candles is a broken
       ingestion path, not an inactive pair. Surfacing it as a column means the
       distinction is queryable rather than a judgement call. */
    s.is_active
        and (a.last_candle_at is null
             or a.last_candle_at < now64(3) - toIntervalHour(1))              as is_stale,

    now64(3)                                                as dbt_updated_at
from symbols as s
left join activity as a
  on s.symbol = a.symbol
