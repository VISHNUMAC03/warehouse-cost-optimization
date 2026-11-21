-- One row per session.
--
-- Every session metric can be merged (sums, min/max, earliest event), so an
-- incremental run never re-reads a session's history:
--   1. aggregate the fct_events rows newer than our watermark per session,
--   2. union in the stored row of each session that got new events,
--   3. merge the two into the new row and delete+insert it (lightweight
--      DELETE on user_session, then INSERT).
-- This stays exact even for sessions that run across several days, which
-- happens in this dataset (the same user_session shows up days apart).
-- It works because fct_events is append-only: rows past the watermark can't
-- already be counted in a stored session.
--
-- A full rebuild groups ~9M sessions for a month of data. With all 16 threads the
-- external-aggregation merge needs >3 GB, which doesn't fit next to Kafka on a
-- laptop; 4 threads peaks at ~1.8 GB and is only a few seconds slower.
{{ config(
    incremental_strategy='delete+insert',
    unique_key='user_session',
    partition_by='toYYYYMM(session_date)',
    order_by='(session_date, user_session)',
    query_settings={'max_threads': 4, 'max_memory_usage': 3000000000},
) }}

with new_events as (
    select
        user_session,
        min(user_id) as user_id,
        min(event_time) as session_start,
        max(event_time) as session_end,
        count() as events,
        countIf(event_type = 'view') as views,
        countIf(event_type = 'cart') as carts,
        countIf(event_type = 'remove_from_cart') as removes,
        countIf(event_type = 'purchase') as purchases,
        sum(revenue) as revenue,
        -- first event and its category; ties on event_time broken by name so
        -- a rebuild always gives the same answer
        min((event_time, category_l1)) as entry,
        max(_loaded_at) as max_loaded_at
    from {{ ref('fct_events') }}
    where user_session != ''
    {% if is_incremental() %}
      and {{ loaded_after_watermark() }}
    {% endif %}
    group by user_session
)

{% if is_incremental() %}
, sessions as (
    select * from new_events

    union all

    select
        user_session,
        user_id,
        session_start,
        session_end,
        events,
        views,
        carts,
        removes,
        purchases,
        revenue,
        (session_start, entry_category_l1) as entry,
        max_loaded_at
    from {{ this }}
    where user_session in (select user_session from new_events)
)
{% endif %}

select
    user_session,
    min(user_id) as user_id,
    min(session_start) as session_start,
    max(session_end) as session_end,
    toDate(session_start) as session_date,
    toUInt32(dateDiff('second', session_start, session_end)) as duration_s,
    toUInt32(sum(events)) as events,
    toUInt32(sum(views)) as views,
    toUInt32(sum(carts)) as carts,
    toUInt32(sum(removes)) as removes,
    toUInt32(sum(purchases)) as purchases,
    sum(revenue) as revenue,
    toUInt8(purchases > 0) as converted,
    min(entry).2 as entry_category_l1,
    max(max_loaded_at) as max_loaded_at
from {% if is_incremental() %}sessions{% else %}new_events{% endif %}
group by user_session
