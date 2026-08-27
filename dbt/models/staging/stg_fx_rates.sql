{{
    config(
        materialized='view',
        tags=['staging', 'reference']
    )
}}

/*
    Staging: FX rate history.

    The upstream Postgres table holds only the current rate per pair. The
    ClickHouse landing table is ordered by (base, quote, as_of), so every
    published rate survives as its own row and this view is a genuine history.
    That is what makes the point-in-time ASOF join in fct_candles_1m possible -
    without it, every historical KES figure would silently be repriced at
    today's rate every time the mart rebuilt.

    as_of is the PROVIDER's publish timestamp, not our fetch time. Using the
    fetch time would make every poll look like a new rate and destroy the
    ability to tell whether the rate actually moved.
*/

select
    base,
    quote,
    concat(base, quote)                 as pair,
    rate,
    as_of,
    source                              as provider,
    updated_at,
    _lsn                                as cdc_lsn,
    _cdc_arrived_at                     as cdc_arrived_at
from {{ source('cdc', 'fx_rates') }} final
where _op != 'd'
  and rate > 0
