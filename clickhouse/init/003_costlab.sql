-- Every dbt invocation (from the transformer loop or the benchmark),
-- with the bytes it read/wrote taken from system.query_log.
CREATE TABLE IF NOT EXISTS costlab.dbt_runs
(
    run_id         String,
    started_at     DateTime64(3, 'UTC'),
    finished_at    DateTime64(3, 'UTC'),
    mode           LowCardinality(String),   -- 'incremental' | 'full-refresh'
    triggered_by   LowCardinality(String),   -- 'transformer' | 'bench'
    status         LowCardinality(String),   -- 'success' | 'error'
    query_count    UInt32,
    read_rows      UInt64,
    read_bytes     UInt64,
    written_rows   UInt64,
    written_bytes  UInt64,
    duration_s     Float64
)
ENGINE = MergeTree
ORDER BY started_at;

-- One row per dashboard panel per table layout, from `costlab bench-scan`.
CREATE TABLE IF NOT EXISTS costlab.bench_queries
(
    run_id        String,
    run_at        DateTime('UTC'),
    layout        LowCardinality(String),    -- 'unpartitioned' | 'partitioned'
    dashboard     LowCardinality(String),
    panel         String,
    read_rows     UInt64,
    read_bytes    UInt64,
    duration_ms   Float64,
    result_rows   UInt64
)
ENGINE = MergeTree
ORDER BY (run_at, dashboard, panel);

-- Modeled daily warehouse cost per scenario step, from `costlab simulate`.
-- step 0 = baseline, then each lever from the post applied cumulatively.
CREATE TABLE IF NOT EXISTS costlab.scenario_costs
(
    run_id          String,
    run_at          DateTime('UTC'),
    step            UInt8,
    scenario        String,
    warehouse       LowCardinality(String),
    queries         UInt32,
    bytes_scanned   UInt64,
    busy_seconds    Float64,
    billed_seconds  Float64,
    credits         Float64,
    usd             Float64,          -- Snowflake-style: billed warehouse time
    scan_usd        Float64           -- same day priced per TiB scanned
)
ENGINE = MergeTree
ORDER BY (run_at, step, warehouse);
