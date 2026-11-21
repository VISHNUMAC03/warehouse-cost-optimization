-- Duplicates are expected before a merge; after FINAL there must be one row per product.
select product_id, count() as rows
from {{ ref('dim_products') }} final
group by product_id
having rows > 1
