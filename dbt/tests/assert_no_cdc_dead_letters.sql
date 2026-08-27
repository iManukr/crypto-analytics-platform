/*
    Any row in the dead-letter table is a message the ClickHouse Kafka engine
    could not parse. There is no acceptable non-zero count: it means a change
    event from Postgres did not make it into the warehouse, so the replica is
    silently incomplete.

    Scoped to the last 24h so that a historical incident, once investigated,
    does not permanently red-light every future run.
*/

select
    topic,
    count(*)        as dead_letter_count,
    min(received_at) as first_seen,
    max(received_at) as last_seen,
    any(error)      as sample_error
from {{ source('cdc', 'cdc_dead_letters') }}
where received_at >= now64(3) - toIntervalHour(24)
group by topic
