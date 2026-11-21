-- insert_overwrite must replace day partitions, never stack a second copy.
select event_hour, category_l1, count() as rows
from {{ ref('agg_hourly_funnel') }}
group by event_hour, category_l1
having rows > 1
