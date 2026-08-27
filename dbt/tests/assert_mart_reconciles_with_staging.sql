/*
    Row-count parity between staging and the mart, per symbol per day.

    This is the test that catches the failure modes the per-column tests cannot:
    an incremental run that silently skipped a window, a delete+insert that
    deleted more than it re-inserted, or a partition that never got rebuilt.
    Every one of those leaves the mart internally consistent and simply missing
    data, which no not_null or range check would ever notice.

    Today is excluded because staging is live and the mart is rebuilt on a
    schedule, so a difference there is expected rather than a defect.
*/

with staged as (
    select symbol, toDate(open_time) as d, count(*) as n
    from {{ ref('stg_market_candles') }}
    where open_time >= now64(3) - toIntervalDay(7)
      and toDate(open_time) < today()
      and is_valid
    group by symbol, d
),

marted as (
    select symbol, toDate(open_time) as d, count(*) as n
    from {{ ref('fct_candles_1m') }}
    where open_time >= now64(3) - toIntervalDay(7)
      and toDate(open_time) < today()
      and is_valid
    group by symbol, d
)

select
    coalesce(s.symbol, m.symbol)    as symbol,
    coalesce(s.d, m.d)              as trade_date,
    coalesce(s.n, 0)                as staging_rows,
    coalesce(m.n, 0)                as mart_rows,
    coalesce(s.n, 0) - coalesce(m.n, 0) as row_difference
from staged s
full outer join marted m
    on s.symbol = m.symbol and s.d = m.d
where coalesce(s.n, 0) != coalesce(m.n, 0)
