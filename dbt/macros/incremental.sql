{#
    Incremental-window helpers.

    Every incremental mart in this project reprocesses a trailing window rather
    than only strictly-new rows. That is not laziness - it is required for
    correctness with CDC:

      * Debezium is at-least-once and can replay after a restart, so a row that
        was already loaded may arrive again with a corrected value.
      * An UPDATE upstream changes a row whose open_time is in the past. A
        `where open_time > max(open_time)` filter would never see it.
      * The ingester heals gaps by backfilling, which lands old timestamps at
        a new wall-clock time.

    The window is `incremental_lookback_hours` (default 3). Combined with the
    `delete+insert` strategy on the natural key, a reprocessed row overwrites
    its previous version instead of duplicating it.

    The trade-off is stated plainly: a wider window costs more compute per run
    and tolerates later-arriving corrections. Three hours is sized to absorb a
    connector outage of an hour or two without a full refresh.
#}

{#
    `column`    - the timestamp column being filtered in the SOURCE relation.
    `watermark` - the timestamp column holding the high-water mark in {{ this }}.
                  Defaults to `column`, and differs whenever a model aggregates
                  to a coarser grain (e.g. filtering on open_time while the
                  target's watermark lives in bucket_start).
#}
{% macro incremental_since(column, hours=none, watermark=none) -%}
    {%- set lookback = hours if hours is not none else var('incremental_lookback_hours', 3) -%}
    {%- set mark = watermark if watermark is not none else column -%}
    {%- if is_incremental() -%}
        {{ column }} >= (
            select coalesce(max({{ mark }}), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))
            from {{ this }}
        ) - toIntervalHour({{ lookback }})
    {%- else -%}
        1 = 1
    {%- endif -%}
{%- endmacro %}


{#
    Same idea, but for models with window functions.

    A moving average over the last 60 minutes needs 60 minutes of history
    *before* the first row it emits, or the left edge of every incremental batch
    is computed from a partial window and is quietly wrong. So the model reads a
    wider slice than it writes: read from (watermark - warmup), emit from
    (watermark - lookback).

    `feature_warmup_hours` must exceed the longest window in the model. It is a
    project var so that adding a 4-hour feature is a config change, not a
    silently-broken column.
#}
{% macro feature_read_since(column) -%}
    {%- if is_incremental() -%}
        {{ column }} >= (
            select coalesce(max({{ column }}), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))
            from {{ this }}
        ) - toIntervalHour({{ var('feature_warmup_hours', 6) }})
    {%- else -%}
        1 = 1
    {%- endif -%}
{%- endmacro %}


{% macro feature_emit_since(column) -%}
    {%- if is_incremental() -%}
        {{ column }} >= (
            select coalesce(max({{ column }}), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))
            from {{ this }}
        ) - toIntervalHour({{ var('incremental_lookback_hours', 3) }})
    {%- else -%}
        1 = 1
    {%- endif -%}
{%- endmacro %}
