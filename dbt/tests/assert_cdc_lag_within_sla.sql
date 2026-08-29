/*
    CDC delivery latency, scoped to rows that arrived recently.

    This is the assertion the column-level bound on cdc_lag_seconds used to
    attempt and got wrong. The distinction matters:

      * "no row has ever had a lag above one hour" is false for any pipeline
        that has ever replayed a backlog, and stays false forever afterwards.
      * "rows arriving right now are arriving promptly" is the property an
        operator actually cares about, and it recovers on its own once the
        backlog drains.

    Scoped to a 15-minute window and p95 rather than max, so a single slow
    record - a merge pause, a GC hit - does not fail the build while a genuine
    sustained regression still does.

    Returns no rows when healthy. Skips silently when nothing has arrived in
    the window, because "no recent data" is a freshness problem that
    fresher_than already reports; failing here too would just double-report the
    same incident with a misleading name.
*/

with recent as (

    select cdc_lag_seconds
    from {{ ref('stg_market_candles') }}
    where cdc_arrived_at > now() - interval 15 minute

)

select
    count(*)                            as rows_in_window,
    round(avg(cdc_lag_seconds), 2)      as avg_lag_seconds,
    round(quantile(0.95)(cdc_lag_seconds), 2) as p95_lag_seconds,
    round(max(cdc_lag_seconds), 2)      as max_lag_seconds
from recent
having count(*) > 0
   and quantile(0.95)(cdc_lag_seconds) > 120
