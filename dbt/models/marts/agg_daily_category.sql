-- Daily rollup by category and brand. Distinct counts (buyers, sessions)
-- can't be patched by adding new rows on top, so a run recomputes every day
-- that received new events and swaps those days in whole: delete+insert keyed
-- on the day deletes every stored row of a recomputed day, then inserts the
-- fresh ones. (insert_overwrite would do the same with REPLACE PARTITION, but
-- dbt-clickhouse needs SELECT on system.parts for that, which the dbt user
-- doesn't have.)
{{ config(
    incremental_strategy='delete+insert',
    unique_key='event_date',
    partition_by='event_date',
    order_by='(event_date, category_l1, brand)',
) }}

select
    event_date,
    category_l1,
    brand,
    toUInt64(countIf(event_type = 'view')) as views,
    toUInt64(countIf(event_type = 'cart')) as carts,
    toUInt64(countIf(event_type = 'remove_from_cart')) as removes,
    toUInt64(countIf(event_type = 'purchase')) as purchases,
    sum(revenue) as revenue,
    toUInt64(uniqExactIf(user_id, event_type = 'purchase')) as buyers,
    toUInt64(uniqExactIf(user_session, user_session != '')) as sessions,
    max(_loaded_at) as max_loaded_at
from {{ ref('fct_events') }}
{% if is_incremental() %}
where event_date in (
    select distinct event_date from {{ ref('fct_events') }} where {{ loaded_after_watermark() }}
)
{% endif %}
group by event_date, category_l1, brand
