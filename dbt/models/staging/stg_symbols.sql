{{
    config(
        materialized='view',
        tags=['staging', 'reference']
    )
}}

/*
    Staging: tradable pair dimension.

    Tiny table, same dedup discipline as everything else. Deletes are kept here
    rather than filtered, exposed as is_deleted: a symbol being removed upstream
    is information the mart wants (it should stop appearing in new facts but its
    history must remain joinable), whereas a deleted candle is simply noise.
*/

select
    symbol,
    base_asset,
    quote_asset,
    display_name,
    is_active = 1                       as is_active,
    -- CAST(.. AS String) strips LowCardinality (toString does NOT); see stg_market_candles.
    CAST(_op AS String) = 'd'                 as is_deleted,
    created_at,
    updated_at,
    _lsn                                as cdc_lsn,
    _cdc_arrived_at                     as cdc_arrived_at
from {{ source('cdc', 'symbols') }} final
