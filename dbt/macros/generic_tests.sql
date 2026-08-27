{#
    Custom generic tests.

    Written by hand rather than pulled from dbt_utils on purpose: `dbt deps`
    needs network access at run time, and an orchestrated pipeline that cannot
    start because a package registry is briefly unavailable is a self-inflicted
    outage. Everything here is a few lines of SQL; the dependency is not worth
    the failure mode.

    dbt's convention is that a test passes when it returns zero rows, so each
    of these selects the offending rows.
#}


{#
    Bounds check. Either bound may be omitted.
    Usage:
      tests:
        - value_between:
            min_value: 0
            inclusive: false
#}
{% test value_between(model, column_name, min_value=none, max_value=none, inclusive=true) %}

    {%- set lower = '>=' if inclusive else '>' -%}
    {%- set upper = '<=' if inclusive else '<' -%}

    select {{ column_name }} as offending_value, count(*) as occurrences
    from {{ model }}
    where {{ column_name }} is not null
      and (
        1 = 0
        {% if min_value is not none %} or not ({{ column_name }} {{ lower }} {{ min_value }}) {% endif %}
        {% if max_value is not none %} or not ({{ column_name }} {{ upper }} {{ max_value }}) {% endif %}
      )
    group by 1
    order by occurrences desc
    limit 100

{% endtest %}


{#
    Compound uniqueness. dbt's built-in `unique` only handles a single column,
    and every fact in this project is keyed on (symbol, timestamp).
#}
{% test unique_combination(model, combination_of_columns) %}

    select
        {{ combination_of_columns | join(', ') }},
        count(*) as duplicate_rows
    from {{ model }}
    group by {{ combination_of_columns | join(', ') }}
    having count(*) > 1
    limit 100

{% endtest %}


{#
    Nothing may be stamped in the future.

    This is not pedantry. The freshness metric is `now() - max(open_time)`, and
    it is what the on-call alert fires on. A single future-dated row pins that
    metric at zero and the alert can never fire again - the monitoring fails
    silently and permanently. Cheap test, expensive failure.
#}
{% test no_future_timestamps(model, column_name, tolerance_minutes=2) %}

    select {{ column_name }} as offending_timestamp
    from {{ model }}
    where {{ column_name }} > now64(3) + toIntervalMinute({{ tolerance_minutes }})
    limit 100

{% endtest %}


{#
    OHLC internal consistency: high is the maximum and low is the minimum of the
    bar. A bar that violates this makes every derived range, ATR and volatility
    figure downstream wrong, in a way that looks plausible on a chart.
#}
{% test ohlc_consistent(model, open_column='open_price', high_column='high_price',
                        low_column='low_price', close_column='close_price') %}

    select
        {{ open_column }}  as open_price,
        {{ high_column }}  as high_price,
        {{ low_column }}   as low_price,
        {{ close_column }} as close_price
    from {{ model }}
    where {{ high_column }} < {{ low_column }}
       or {{ high_column }} < greatest({{ open_column }}, {{ close_column }})
       or {{ low_column }}  > least({{ open_column }}, {{ close_column }})
    limit 100

{% endtest %}


{#
    A model that builds successfully but is empty is a silent failure: every
    dashboard reads zero and every other test trivially passes on no rows.
    This is the guard against that.
#}
{% test not_empty(model, min_rows=1) %}

    select count(*) as row_count
    from {{ model }}
    having count(*) < {{ min_rows }}

{% endtest %}


{#
    Freshness assertion on a model (as opposed to `dbt source freshness`, which
    only covers sources). Fails when the newest row is older than the SLA.
#}
{% test fresher_than(model, column_name, max_age_minutes=30) %}

    select
        max({{ column_name }})                                       as newest_row,
        dateDiff('minute', max({{ column_name }}), now64(3))         as age_minutes
    from {{ model }}
    having age_minutes > {{ max_age_minutes }}

{% endtest %}


{#
    Gap detection for a regular time series.

    A 1-minute series should have exactly one row per minute per symbol. Missing
    minutes are normal in small numbers (the exchange has quiet periods, our
    ingester has restarts), so the test allows a tolerance and only fails when
    coverage degrades past it. Set max_missing_pct to 0 to require completeness.
#}
{% test series_coverage(model, partition_column, time_column,
                        interval_seconds=60, max_missing_pct=5, lookback_hours=6) %}

    with bounds as (
        select
            {{ partition_column }}                                  as series_key,
            min({{ time_column }})                                  as first_seen,
            max({{ time_column }})                                  as last_seen,
            count(*)                                                as actual_rows
        from {{ model }}
        where {{ time_column }} >= now64(3) - toIntervalHour({{ lookback_hours }})
        group by 1
    )

    select
        series_key,
        actual_rows,
        expected_rows,
        round(100.0 * (expected_rows - actual_rows) / expected_rows, 2) as missing_pct
    from (
        select
            series_key,
            actual_rows,
            greatest(
                1,
                intDiv(dateDiff('second', first_seen, last_seen), {{ interval_seconds }}) + 1
            ) as expected_rows
        from bounds
    )
    where 100.0 * (expected_rows - actual_rows) / expected_rows > {{ max_missing_pct }}

{% endtest %}
