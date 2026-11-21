-- Hourly view -> cart -> purchase funnel per top-level category.
-- Same strategy as agg_daily_category: touched days are rebuilt and swapped
-- in whole (the delete+insert key is the day, not the hour).
{{ config(
    incremental_strategy='delete+insert',
    unique_key='toDate(event_hour)',
    partition_by='toDate(event_hour)',
    order_by='(event_hour, category_l1)',
) }}

select
    toStartOfHour(event_time) as event_hour,
    category_l1,
    toUInt64(countIf(event_type = 'view')) as views,
    toUInt64(countIf(event_type = 'cart')) as carts,
    toUInt64(countIf(event_type = 'purchase')) as purchases,
    sum(revenue) as revenue,
    toUInt64(uniqExactIf(user_session, user_session != '')) as sessions,
    toUInt64(uniqExactIf(user_id, event_type = 'view')) as viewers,
    toUInt64(uniqExactIf(user_id, event_type = 'purchase')) as buyers,
    max(_loaded_at) as max_loaded_at
from {{ ref('fct_events') }}
{% if is_incremental() %}
where event_date in (
    select distinct event_date from {{ ref('fct_events') }} where {{ loaded_after_watermark() }}
)
{% endif %}
group by event_hour, category_l1
