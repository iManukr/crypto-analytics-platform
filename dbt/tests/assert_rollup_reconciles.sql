/*
    The 5-minute rollup must agree with the 1-minute fact it was built from.

    Volume is the column that catches real bugs here, because it is the only
    one that is not idempotent under a replay: if the aggregation ever consumed
    duplicated rows, the sum diverges while every min/max stays correct.

    Only complete buckets are compared - an in-flight bucket legitimately holds
    fewer minutes than the fact table will eventually give it. A small absolute
    tolerance absorbs Decimal rounding across the two aggregation paths.
*/

with from_minutes as (
    select
        symbol,
        toStartOfFiveMinute(open_time) as bucket_start,
        sum(volume)                    as volume,
        sum(trade_count)               as trade_count,
        count(*)                       as minutes
    from {{ ref('fct_candles_1m') }}
    where open_time >= now64(3) - toIntervalHour(6)
      and is_valid
    group by symbol, bucket_start
    having minutes = 5
),

from_rollup as (
    select symbol, bucket_start, volume, trade_count, minutes_covered
    from {{ ref('agg_candles_5m') }}
    where bucket_start >= now64(3) - toIntervalHour(6)
      and is_complete
)

select
    m.symbol,
    m.bucket_start,
    m.volume        as minute_volume,
    r.volume        as rollup_volume,
    m.trade_count   as minute_trades,
    r.trade_count   as rollup_trades
from from_minutes m
inner join from_rollup r
    on m.symbol = r.symbol and m.bucket_start = r.bucket_start
where abs(toFloat64(m.volume) - toFloat64(r.volume)) > 0.00001
   or m.trade_count != r.trade_count
