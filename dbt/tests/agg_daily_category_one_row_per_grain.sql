-- insert_overwrite must replace day partitions, never stack a second copy.
select event_date, category_l1, brand, count() as rows
from {{ ref('agg_daily_category') }}
group by event_date, category_l1, brand
having rows > 1
