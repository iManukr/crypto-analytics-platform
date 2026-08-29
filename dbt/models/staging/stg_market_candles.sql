{{
    config(
        materialized='view',
        tags=['staging', 'candles']
    )
}}

/*
    Staging: 1-minute OHLCV bars.

    Responsibilities, in order:
      1. Collapse the CDC stream to one row per (symbol, open_time).
      2. Drop tombstones, so downstream models never see deleted rows.
      3. Name and type things the way analysts expect, not the way the WAL
         happened to serialise them.
      4. Attach data-quality flags rather than silently dropping suspicious
         rows - the mart decides what to exclude, and the flags make the
         exclusion measurable.

    On FINAL: ReplacingMergeTree only guarantees deduplication *eventually*,
    when a background merge runs. FINAL forces it at read time. It is the
    expensive part of this view, which is exactly why the dedup happens here,
    once, instead of being re-derived in five different marts. The
    do_not_merge_across_partitions_select_final setting (see profiles.yml)
    keeps the cost proportional to the partitions actually scanned.
*/

with deduplicated as (

    select
        symbol,
        open_time,
        close_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_volume,
        trade_count,
        taker_buy_base,
        taker_buy_quote,
        source,
        ingested_at,
        _op,
        _lsn,
        _source_ts_ms,
        _cdc_arrived_at
    from {{ source('cdc', 'market_candles_1m') }} final
    where _op != 'd'   -- tombstones: the row no longer exists upstream

    {% if is_incremental() %}
      /* Views are not incremental, but this branch keeps the file honest if the
         materialisation is ever switched. Left as a no-op deliberately rather
         than deleted, so the switch is a one-line change. */
    {% endif %}

),

flagged as (

    select
        symbol,
        open_time,
        close_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_volume,
        trade_count,
        taker_buy_base,
        taker_buy_quote,
        source,
        ingested_at,
        _op                                     as cdc_operation,
        _lsn                                    as cdc_lsn,
        _source_ts_ms                           as cdc_source_ts_ms,
        _cdc_arrived_at                         as cdc_arrived_at,

        /* End-to-end CDC latency for this specific row: Postgres commit ->
           visible in ClickHouse. Averaging this in the mart gives a real
           number rather than an inference from queue depth. */
        (toUnixTimestamp64Milli(_cdc_arrived_at) - toInt64(_source_ts_ms)) / 1000.0
                                                as cdc_lag_seconds,

        /* ---------------------------------------------------------------
           Derived measures every consumer would otherwise recompute.
           --------------------------------------------------------------- */
        high_price - low_price                  as price_range,
        close_price - open_price                as price_change,
        if(open_price > 0,
           (close_price - open_price) / open_price,
           toDecimal64(0, 8))                   as price_change_pct,
        if(volume > 0,
           toFloat64(taker_buy_base) / toFloat64(volume),
           0.0)                                 as taker_buy_ratio,

        /* ---------------------------------------------------------------
           Data-quality flags. Nothing is dropped on the strength of these;
           they are counted in the mart and asserted on by dbt tests, so a
           degradation shows up as a trend rather than as missing rows.
           --------------------------------------------------------------- */
        high_price < low_price                                  as dq_inverted_range,
        high_price < greatest(open_price, close_price)          as dq_high_below_body,
        low_price  > least(open_price, close_price)             as dq_low_above_body,
        volume = 0                                              as dq_zero_volume,
        taker_buy_base > volume                                 as dq_taker_exceeds_volume,
        -- CAST(.. AS String) strips LowCardinality (toString does NOT); comparing
        -- the raw LC column yields LowCardinality(UInt8), which cannot be a column.
        CAST(source AS String) = 'replay'                             as dq_synthetic_source

    from deduplicated

)

select
    *,
    /* One column that answers "can I trust this row". Cheaper for a consumer
       than remembering the five conditions, and the definition lives here. */
    not (dq_inverted_range
         or dq_high_below_body
         or dq_low_above_body
         or dq_taker_exceeds_volume) as is_valid
from flagged
