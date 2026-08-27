{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['symbol', 'open_time'],
        engine='MergeTree()',
        order_by='(symbol, open_time)',
        partition_by='toYYYYMM(open_time)',
        tags=['mart', 'ml']
    )
}}

/*
    Mart: machine-learning-ready feature set at 1-minute grain.

    ============================ LEAKAGE POLICY ============================
    Every FEATURE column is computed over `ROWS BETWEEN n PRECEDING AND CURRENT
    ROW`. None of them can see a future row. That is the whole reason the frame
    is written out explicitly on every window instead of relying on a default:
    ClickHouse's default frame for an ordered window is RANGE UNBOUNDED PRECEDING
    TO CURRENT ROW, which is *usually* fine but silently includes peer rows with
    an equal ORDER BY value. Being explicit removes the ambiguity.

    Exactly two columns look forward, both prefixed `target_`, both documented as
    labels. Anyone selecting `* EXCEPT target_*` gets a leakage-free feature
    matrix. `is_label_resolved` marks rows whose future has not happened yet -
    train on those and the model learns from a zero that means "unknown".
    ========================================================================

    Incremental warm-up: a 60-minute moving average needs 60 minutes of history
    before the first row it emits. So this model READS from
    (watermark - feature_warmup_hours) but only EMITS from
    (watermark - incremental_lookback_hours). Without the wider read, the left
    edge of every batch would be computed from a partial window and be quietly,
    plausibly wrong - the worst kind of data bug.

    Gap awareness: `has_contiguous_history` is false when the preceding minute is
    missing. A return computed across a 40-minute hole is not a 1-minute return,
    and a model trained on those learns from an artefact of our uptime.
*/

with base as (

    select
        symbol,
        open_time,
        toFloat64(open_price)   as open_price,
        toFloat64(high_price)   as high_price,
        toFloat64(low_price)    as low_price,
        toFloat64(close_price)  as close_price,
        toFloat64(volume)       as volume,
        toFloat64(quote_volume) as quote_volume,
        trade_count,
        taker_buy_ratio,
        data_source,
        is_valid
    from {{ ref('fct_candles_1m') }}
    where {{ feature_read_since('open_time') }}
      and is_valid

),

lagged as (

    select
        *,
        any(close_price) over w1 as prev_close,
        any(open_time)   over w1 as prev_open_time
    from base
    window w1 as (
        partition by symbol order by open_time
        rows between 1 preceding and 1 preceding
    )

),

deltas as (

    select
        *,
        prev_close > 0                                              as has_prev,
        prev_open_time = open_time - toIntervalMinute(1)             as has_contiguous_history,
        if(prev_close > 0, log(close_price / prev_close), 0.0)       as log_return_1m,
        if(prev_close > 0, close_price - prev_close, 0.0)            as price_delta,
        greatest(if(prev_close > 0, close_price - prev_close, 0.0), 0.0) as gain,
        greatest(if(prev_close > 0, prev_close - close_price, 0.0), 0.0) as loss,
        /* True range: the classic definition, which accounts for gaps between
           the previous close and the current bar rather than only the bar's own
           high-low spread. */
        greatest(
            high_price - low_price,
            if(prev_close > 0, abs(high_price - prev_close), 0.0),
            if(prev_close > 0, abs(low_price - prev_close), 0.0)
        )                                                            as true_range
    from lagged

),

features as (

    select
        *,

        -- ------------------------------------------------- trend / momentum
        avg(close_price)   over w5   as sma_5,
        avg(close_price)   over w15  as sma_15,
        avg(close_price)   over w60  as sma_60,

        -- ---------------------------------------------------- volatility
        stddevPop(log_return_1m) over w15 as volatility_15,
        stddevPop(log_return_1m) over w60 as volatility_60,
        avg(true_range)          over w15 as atr_15,

        -- ------------------------------------------- relative strength (RSI)
        avg(gain) over w14 as avg_gain_14,
        avg(loss) over w14 as avg_loss_14,

        -- ---------------------------------------------------------- volume
        avg(volume)       over w60 as volume_sma_60,
        stddevPop(volume) over w60 as volume_std_60,
        avg(taker_buy_ratio) over w15 as taker_buy_ratio_15,

        -- -------------------------------------------------------- position
        max(high_price) over w60 as high_60,
        min(low_price)  over w60 as low_60,

        -- ------------------------------------------------------- cumulative
        sum(log_return_1m) over w15 as cum_return_15,
        sum(log_return_1m) over w60 as cum_return_60,

        -- --------------------------------------------------------- horizon
        max(open_time) over (partition by symbol) as symbol_max_open_time

    from deltas
    window
        w5  as (partition by symbol order by open_time rows between 4  preceding and current row),
        w14 as (partition by symbol order by open_time rows between 13 preceding and current row),
        w15 as (partition by symbol order by open_time rows between 14 preceding and current row),
        w60 as (partition by symbol order by open_time rows between 59 preceding and current row)

),

labelled as (

    select
        *,
        /* ---- LABELS. These are the only forward-looking columns here. ---- */
        any(close_price) over (
            partition by symbol order by open_time
            rows between {{ var('label_horizon_minutes', 1) }} following
                     and {{ var('label_horizon_minutes', 1) }} following
        ) as next_close
    from features

)

select
    -- ------------------------------------------------------------- keys
    symbol,
    open_time,

    -- ------------------------------------------------------- raw context
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    trade_count,

    -- --------------------------------------------------------- FEATURES
    log_return_1m,
    cum_return_15,
    cum_return_60,

    sma_5,
    sma_15,
    sma_60,
    if(sma_15 > 0, close_price / sma_15 - 1, 0.0)              as close_to_sma15,
    if(sma_60 > 0, close_price / sma_60 - 1, 0.0)              as close_to_sma60,
    if(sma_60 > 0, sma_5 / sma_60 - 1, 0.0)                    as sma_ratio_5_60,

    volatility_15,
    volatility_60,
    atr_15,
    if(close_price > 0, atr_15 / close_price, 0.0)             as atr_15_pct,

    /* Wilder's RSI. The avg_loss = 0 branch is not an edge case to ignore: an
       unbroken run of up-minutes is common in thin markets, and dividing by
       zero there would emit inf and poison the whole feature column. */
    if(avg_loss_14 > 0,
       100.0 - (100.0 / (1.0 + avg_gain_14 / avg_loss_14)),
       if(avg_gain_14 > 0, 100.0, 50.0))                       as rsi_14,

    volume,
    volume_sma_60,
    if(volume_std_60 > 0, (volume - volume_sma_60) / volume_std_60, 0.0)
                                                               as volume_zscore_60,
    taker_buy_ratio,
    taker_buy_ratio_15,

    /* Where the close sits inside the last hour's range: 0 = at the low,
       1 = at the high. Scale-free, so it transfers across symbols. */
    if(high_60 > low_60, (close_price - low_60) / (high_60 - low_60), 0.5)
                                                               as pct_of_60m_range,

    /* Cyclical time encodings. Minute-of-day as a raw integer would tell a
       model that 23:59 and 00:00 are maximally distant; sin/cos pairs preserve
       the wrap-around. */
    toMinute(open_time) + toHour(open_time) * 60               as minute_of_day,
    sin(2 * pi() * (toHour(open_time) * 60 + toMinute(open_time)) / 1440)  as minute_of_day_sin,
    cos(2 * pi() * (toHour(open_time) * 60 + toMinute(open_time)) / 1440)  as minute_of_day_cos,
    toDayOfWeek(open_time)                                     as day_of_week,
    sin(2 * pi() * toDayOfWeek(open_time) / 7)                 as day_of_week_sin,
    cos(2 * pi() * toDayOfWeek(open_time) / 7)                 as day_of_week_cos,

    -- ----------------------------------------------------------- LABELS
    next_close                                                 as target_next_close,
    if(open_time < symbol_max_open_time and close_price > 0,
       log(next_close / close_price),
       0.0)                                                    as target_log_return_1m,
    toUInt8(open_time < symbol_max_open_time and next_close > close_price)
                                                               as target_direction_up,

    -- ------------------------------------------------- usability metadata
    /* Do not train on rows where any of these is false. Kept as columns rather
       than filtered away so that the counts are observable and a consumer can
       make its own call. */
    toUInt8(open_time < symbol_max_open_time)                  as is_label_resolved,
    toUInt8(has_prev and has_contiguous_history)               as has_contiguous_history,
    toUInt8(data_source = 'replay')                            as is_synthetic,
    data_source,
    now64(3)                                                   as dbt_updated_at

from labelled
where {{ feature_emit_since('open_time') }}
