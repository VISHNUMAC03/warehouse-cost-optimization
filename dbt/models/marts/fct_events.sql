-- The big fact table. Partitioned by month and sorted by the columns the
-- dashboards filter on, so a "last 7 days of purchases" panel reads a few
-- granules instead of the whole table.
--
-- Append-only: each run takes the raw rows loaded since the newest _loaded_at
-- already here. Rows younger than 30s are left for the next run, so an insert
-- the consumer is committing right now can't land behind the watermark.
--
-- The table isn't sorted by _loaded_at, so two helpers keep the watermark
-- lookups from scanning it:
--   idx_loaded_at: minmax skip index, lets downstream models find "rows since
--                  my watermark" in a few granules (new rows land together)
--   p_watermark:   tiny aggregate projection that answers max(_loaded_at)
--                  for this model's own watermark without reading the column
{{ config(
    incremental_strategy='append',
    partition_by='toYYYYMM(event_date)',
    order_by='(event_date, event_type, category_l1, brand)',
    indexes=[{'name': 'idx_loaded_at', 'definition': '_loaded_at TYPE minmax GRANULARITY 1'}],
    projections=[{'name': 'p_watermark', 'query': 'select max(_loaded_at)'}],
) }}

select
    event_time,
    event_date,
    event_type,
    product_id,
    category_id,
    category_code,
    category_l1,
    category_l2,
    brand,
    price,
    if(event_type = 'purchase', price, 0) as revenue,
    user_id,
    user_session,
    _source,
    _loaded_at
from {{ ref('stg_events') }}
where _loaded_at <= now64(3) - interval 30 second
{% if is_incremental() %}
  and {{ loaded_after_watermark('_loaded_at') }}
{% endif %}
