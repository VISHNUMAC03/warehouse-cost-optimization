-- The daily rollup must account for every event in the fact table, day by day.
-- Catches a day that an incremental run should have rebuilt but didn't.
with fct as (
    select event_date, count() as events, sum(revenue) as revenue
    from {{ ref('fct_events') }}
    group by event_date
),

agg as (
    select event_date, sum(views + carts + removes + purchases) as events, sum(revenue) as revenue
    from {{ ref('agg_daily_category') }}
    group by event_date
)

select fct.event_date, fct.events, agg.events as agg_events, fct.revenue, agg.revenue as agg_revenue
from fct
full outer join agg on agg.event_date = fct.event_date
where fct.events != agg.events or fct.revenue != agg.revenue
