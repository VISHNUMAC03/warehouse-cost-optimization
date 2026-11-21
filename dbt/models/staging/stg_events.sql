-- Light cleanup on top of raw.events. A view, so it costs nothing until a
-- mart reads it, and filters on _loaded_at push down to raw's sort key.
select
    event_time,
    toDate(event_time) as event_date,
    event_type,
    product_id,
    category_id,
    category_code,
    toLowCardinality(if(category_code = '', 'unknown', splitByChar('.', category_code)[1])) as category_l1,
    toLowCardinality(if(length(splitByChar('.', category_code)) >= 2, splitByChar('.', category_code)[2], 'unknown')) as category_l2,
    toLowCardinality(if(brand = '', 'unknown', brand)) as brand,
    price,
    user_id,
    user_session,
    _source,
    _loaded_at
from {{ source('raw', 'events') }}
