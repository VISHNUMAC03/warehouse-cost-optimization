-- One row per storefront event (view / cart / remove_from_cart / purchase).
-- Same columns as the Kaggle CSV plus a few load-metadata columns.
--
-- Sorted by _loaded_at so dbt's incremental models can cheaply pick up
-- "everything since the last run" without scanning the whole table.
CREATE TABLE IF NOT EXISTS raw.events
(
    event_time        DateTime('UTC'),
    event_type        LowCardinality(String),
    product_id        UInt64,
    category_id       UInt64,
    category_code     LowCardinality(String),
    brand             LowCardinality(String),
    price             Decimal(10, 2),
    user_id           UInt64,
    user_session      String,

    _source           LowCardinality(String),           -- 'backfill' or 'stream'
    _kafka_partition  Int32 DEFAULT -1,
    _kafka_offset     Int64 DEFAULT -1,
    _loaded_at        DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (_loaded_at, event_type);
