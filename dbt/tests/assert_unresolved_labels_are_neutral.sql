/*
    Guards the ML contract.

    The most recent minute for each symbol has no "next minute" yet, so its
    label cannot exist. Those rows carry placeholder zeros and is_label_resolved
    = 0. If a placeholder ever became a non-zero value, a model trained without
    filtering would learn from a fabricated label - and it would look like a
    perfectly ordinary training row while doing it.

    This asserts the placeholder really is neutral.
*/

select
    symbol,
    open_time,
    is_label_resolved,
    target_direction_up,
    target_log_return_1m
from {{ ref('ml_features_1m') }}
where is_label_resolved = 0
  and (target_direction_up != 0 or target_log_return_1m != 0)
