{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['symbol', 'open_time'],
        engine='MergeTree()',
        order_by='(symbol, open_time)',
        partition_by='toYYYYMM(open_time)',
        settings={'index_granularity': 8192},
        tags=['mart', 'candles']
    )
}}

/*
    Mart: the canonical 1-minute fact table.

    ClickHouse physical design, and why:

    ORDER BY (symbol, open_time)
        Every real query filters by symbol and then by time range - a chart, a
        backtest and a feature build all do. Putting symbol first means the
        sparse primary index prunes to one symbol's granules immediately, and
        the time predicate then prunes within it. The reverse order would force
        every symbol's data to be scanned for any time range.

    PARTITION BY toYYYYMM(open_time)
        Monthly, not daily. Partitions are a pruning and lifecycle unit, not an
        index: too many of them creates a metadata part per partition per insert
        and pushes the table toward the "too many parts" failure. At ~86k rows
        per symbol per month this keeps partitions large enough to merge well
        while still letting a "last 90 days" query skip everything older, and
        letting a retention policy drop a whole month atomically.

    Engine: plain MergeTree
        Deduplication already happened in staging. Using ReplacingMergeTree here
        would pay for merge-time dedup work on data that has no duplicates.
        Uniqueness is instead enforced by the delete+insert incremental strategy
        plus a dbt test, which fails loudly rather than silently collapsing rows.

    FX enrichment uses ASOF LEFT JOIN
        This is the ClickHouse feature that makes point-in-time correctness
        cheap. It matches each candle to the most recent FX rate published at or
        before that candle's open_time - a single pass, no correlated subquery.
        A plain join to "the latest rate" would retroactively reprice all of
        history every time the rate moved, which is the classic way a revenue
        number changes overnight for no reason anybody can explain.
*/

with candles as (

    select
        *,
        '{{ var("fx_base") }}' as fx_base_key
    from {{ ref('stg_market_candles') }}
    where {{ incremental_since('open_time') }}

),

fx as (

    select base, quote, as_of, rate
    from {{ ref('stg_fx_rates') }}
    where base = '{{ var("fx_base") }}'
      and quote = '{{ var("fx_quote") }}'

),

priced as (

    /* ASOF requires exactly one inequality and it must come last. The equality
       is on the constant fx_base_key column carried by `candles`, because
       ClickHouse needs a genuine two-sided equality key, not a literal. */
    select
        c.*,
        f.rate  as fx_rate,
        f.as_of as fx_rate_as_of
    from candles as c
    asof left join fx as f
      on c.fx_base_key = f.base
     and c.open_time >= f.as_of

),

symbols as (

    select symbol, base_asset, quote_asset, display_name, is_active
    from {{ ref('stg_symbols') }}
    where not is_deleted

)

select
    -- ---------------------------------------------------------------- keys
    p.symbol,
    p.open_time,
    p.close_time,

    -- ------------------------------------------------------------ dimension
    s.base_asset,
    s.quote_asset,
    s.display_name                                          as symbol_display_name,
    s.is_active                                             as symbol_is_active,

    -- --------------------------------------------------------------- OHLCV
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.volume,
    p.quote_volume,
    p.trade_count,
    p.taker_buy_base,
    p.taker_buy_quote,

    -- ------------------------------------------------------------- derived
    p.price_range,
    p.price_change,
    p.price_change_pct,
    p.taker_buy_ratio,
    if(p.volume > 0, p.quote_volume / p.volume, p.close_price) as vwap_approx,

    -- ------------------------------------------- point-in-time FX conversion
    p.fx_rate,
    p.fx_rate_as_of,
    '{{ var("fx_quote") }}'                                 as fx_quote_currency,
    if(p.fx_rate > 0, p.close_price * p.fx_rate, null)      as close_price_fx,

    -- ------------------------------------------------ calendar (UTC always)
    toDate(p.open_time)                                     as trade_date,
    toHour(p.open_time)                                     as trade_hour,
    toMinute(p.open_time)                                   as trade_minute,
    toDayOfWeek(p.open_time)                                as day_of_week,

    -- -------------------------------------------------- provenance & quality
    p.source                                                as data_source,
    p.is_valid,
    p.dq_zero_volume,
    p.dq_synthetic_source,
    p.ingested_at,
    p.cdc_arrived_at,
    p.cdc_lag_seconds,
    now64(3)                                                as dbt_updated_at

from priced as p
left join symbols as s
  on p.symbol = s.symbol
