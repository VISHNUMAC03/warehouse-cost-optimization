-- Latest known attributes per product.
--
-- ReplacingMergeTree keyed on product_id, versioned by last_seen_at: an
-- incremental run inserts one merged row per touched product and ClickHouse
-- drops the older version on its next merge. Until then a product can have
-- two rows, so readers should query `dim_products final`.
--
-- "Latest" = the event with the greatest (event_time, category_id, ...) tuple,
-- so ties within the same second resolve the same way on every rebuild.
-- The merged row combines the stored row with the new events, which keeps
-- first_seen_at right without re-reading the product's history.
{{ config(
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_seen_at)',
    order_by='product_id',
) }}

with new_products as (
    select
        product_id,
        min(event_time) as first_seen_at,
        max((event_time, category_id, category_code, category_l1, category_l2, brand, price)) as latest,
        max(_loaded_at) as max_loaded_at
    from {{ ref('fct_events') }}
    {% if is_incremental() %}
    where {{ loaded_after_watermark() }}
    {% endif %}
    group by product_id
)

{% if is_incremental() %}
, merged as (
    select product_id, first_seen_at, latest, max_loaded_at
    from new_products

    union all

    select
        product_id,
        first_seen_at,
        (last_seen_at, category_id, category_code, category_l1, category_l2, brand, last_price) as latest,
        max_loaded_at
    from {{ this }} final
    where product_id in (select product_id from new_products)
)
{% endif %}

select
    product_id,
    max(latest).2 as category_id,
    max(latest).3 as category_code,
    max(latest).4 as category_l1,
    max(latest).5 as category_l2,
    max(latest).6 as brand,
    max(latest).7 as last_price,
    min(first_seen_at) as first_seen_at,
    max(latest).1 as last_seen_at,
    max(max_loaded_at) as max_loaded_at
from {% if is_incremental() %}merged{% else %}new_products{% endif %}
group by product_id
