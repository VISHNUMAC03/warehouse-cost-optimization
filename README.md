# warehouse-cost-optimization

Most "we cut our warehouse bill by X%" posts don't show their work. This repo is my attempt to show it:
a small but real analytics stack, deliberately set up the expensive way, and then fixed one lever at a
time. Every lever is measured from the warehouse's own query log.

The stack is a streaming e-commerce analytics setup, all local in Docker:

- **Kafka** carries a live stream of storefront events (views, carts, purchases).
- **ClickHouse** is the warehouse: raw landing table, dbt-built marts, and `system.query_log` as the "billing export".
- **dbt** (dbt-clickhouse) builds the fact table, sessions, product dimension and rollups, all incremental.
- **Grafana** has a live store dashboard, three BI dashboards and a warehouse-cost dashboard.
- **costlab** (Python) replays the dashboards' queries, benchmarks dbt, and turns the measurements into a modeled Snowflake-style bill.

The data is real: the Kaggle [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
(REES46), October 2019, which has **42M events**. 30 days are backfilled as history; the 31st day is replayed through Kafka in real time as "today".

## Results

From one run on my laptop (full October file, 41.2M-row fact table). Rerun `python -m costlab report` to get your own numbers.

| | Before | After | |
|---|---:|---:|---|
| BI dashboard refreshes per day | 96 (every 15 min) | 2 | lever 1 |
| Bytes read by a "last 7 days of purchases" panel | 154 MB | 2.2 MB (1.4%) | lever 2 |
| Bytes read by all BI panels on `fct_events`, per refresh | 2.03 GB | 0.54 GB (26.7%) | lever 2 |
| Hourly dbt run: bytes read | 18.4 GB (full rebuild) | 534 MB (2.9%) incremental | lever 3 |
| Hourly dbt run: warehouse busy | 150 s | 3.5 s | lever 3 |
| Auto-suspend | 600 s | 60 s | lever 4 |
| **Bytes scanned per day** | **640 GB** | **15 GB (-97.7%)** | all |
| **Modeled bill, Snowflake-style** | **$206/day** | **$79/day (-61.9%)** | all |

Two things surprised me, and they're the reason this repo exists:

1. **Which lever pays depends on how you're billed.** Snowflake bills warehouse *time*. With queries this
   fast, almost all of the billed time is the idle tail before auto-suspend plus the 60 s minimum per resume.
   So on the Snowflake model the refresh schedule (-36.5%) and auto-suspend (-22.6%) do the work, and
   partitioning saves exactly $0. Priced per byte scanned (BigQuery-style) it's the other way round:
   incremental dbt is -66.9% of the bill and auto-suspend does nothing. Same warehouse, same queries.
2. **The always-on live dashboard is the floor.** It costs $72/day in every scenario. Leave it out
   and the Snowflake-style cut is 95%, not 62%. In real life that's usually the line item nobody
   questions.

The bytes and timings are measured. The dollars are a model, and every assumption behind them is in
[costlab/workload.yml](costlab/workload.yml): credit price, warehouse sizes, refresh schedules, analyst
query volume, dbt cadence. `reports/cost_report.md` has the full breakdown, per panel and per warehouse.

## How it fits together

```
  Kaggle CSV (Oct 2019)
     |                \
     | backfill        \ replay "today" (producer)
     | (30 days)        v
     |              Kafka  shop.events (3 partitions)
     |                |
     |                | consumer: micro-batches, at-least-once
     v                v
  ClickHouse  raw.events  (ORDER BY _loaded_at)
     |
     | dbt (every 5 min, incremental)
     v
  analytics.fct_events      partitioned by month, ORDER BY (event_date, event_type, category_l1, brand)
  analytics.fct_sessions    merge-on-update, delete+insert
  analytics.dim_products    ReplacingMergeTree
  analytics.agg_daily_category / agg_hourly_funnel   recompute touched days
     |
     v
  Grafana  live-store | sales-overview | conversion-funnel | product-performance | warehouse-cost

  system.query_log  --->  costlab  --->  costlab.* tables + reports/cost_report.md
```

Every tool connects as its own ClickHouse user (`loader`, `dbt`, `grafana`, `costlab`), and every Grafana
query starts with a `/* grafana dashboard=... panel=... */` tag. That makes `query_log` answer "who spent
these bytes" the same way a Snowflake warehouse-per-workload setup would.

## The four levers, and where they live

**1. Find the expensive queries.** `python -m costlab top-queries` ranks query patterns by bytes read, per
user and per dashboard, and shows what each dashboard costs on a 15-minute refresh compared with how often
it actually ran. The "before" schedule lives in `workload.yml`. The Grafana BI dashboards ship with
auto-refresh off.

**2. Partition and cluster the fact table.** `fct_events` is partitioned by month and sorted by
`(event_date, event_type, category_l1, brand)`. Every BI panel filters on `event_date`, which is what lets
ClickHouse skip granules. `python -m costlab bench-scan` copies the table into an unpartitioned
`ORDER BY tuple()` version and replays every panel's SQL (read straight from the dashboard JSON, with
Grafana macros expanded) against both layouts. Purchase panels drop to 1–4% of the bytes. Panels that
need every event type in the range read 22% of the rows (the 7 days in range), but 40–50% of the bytes.

**3. Incremental models.** Every mart is `materialized='incremental'`, each with its own watermark
(`macros/loaded_after_watermark.sql`):
- `fct_events` appends rows loaded since its newest `_loaded_at`, with a 30 s settle window so a batch the consumer is still committing can't land behind the watermark.
- `fct_sessions` merges new events into the stored session rows instead of re-reading history. Sessions in this dataset can span days, which a "re-read yesterday too" approach got wrong.
- The daily and hourly rollups recompute only the days that got new rows.

`python -m costlab bench-dbt` runs a full refresh and then an incremental run, and logs both to
`costlab.dbt_runs`.

**4. Right-size compute.** There is no auto-suspend to measure on a laptop, so this one is purely the
billing model in [costlab/billing.py](costlab/billing.py): per-second billing, 60 s minimum per resume,
the idle tail until auto-suspend, and overlapping queries sharing the warehouse. It's unit tested.

## Running it

You need Docker Desktop (give it 8 GB of memory), Python 3.10+, and about 8 GB of free disk for the
full month. A 250 MB sample works too.

```bash
git clone <this repo> && cd warehouse-cost-optimization
cp .env.example .env

python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# data: pick one
python -m ingest.download                    # 2019-Oct.csv from Kaggle (needs a Kaggle API token)
python -m ingest.download --source mirror    # same file from an unofficial Hugging Face copy, no account
python -m ingest.download --sample-mb 250    # just Oct 1-5 (~6.5M events), for a quick try
```

If you use the sample, set `REPLAY_FILE=data/raw/2019-Oct-sample.csv` and `REPLAY_DAY=2019-10-05` in `.env`.

```bash
docker compose up -d clickhouse kafka grafana
python -m ingest.backfill                    # history: every day before REPLAY_DAY, ends "yesterday"
python -m costlab.dbt_runner --full-refresh  # build analytics.*
docker compose up -d --build                 # producer, consumer, transformer (dbt every 5 min), kafka-ui
```

- Grafana: http://localhost:3000 (admin / admin). The home dashboard is the live store.
- Kafka UI: http://localhost:8080
- ClickHouse: http://localhost:8123 (user `admin`, password from `.env`)

The producer first has to read through the file to reach `REPLAY_DAY`, about 3 minutes for the gzipped
month. It then bursts everything from midnight until now, and keeps streaming in real time.

The cost report:

```bash
docker compose stop transformer       # bench-dbt does its own full refresh
python -m costlab report --wait 300   # bench-scan + bench-dbt + simulate -> reports/cost_report.md
docker compose start transformer
```

Tests (pure logic, no Docker needed): `pytest`

## Layout

```
ingest/        download.py, backfill.py, producer.py, consumer.py
dbt/           models/staging, models/marts, macros, tests, profiles.yml
costlab/       billing.py, workload.yml, bench_scan.py, bench_dbt.py, dbt_runner.py, top_queries.py, simulate.py, report.py
grafana/       provisioning/ (datasource, dashboard provider), dashboards/*.json
clickhouse/    init/ (databases, raw.events, costlab tables, users), config.d/, users.d/
tests/         pytest
```

## Things that bit me

- **ClickHouse ate the Docker VM.** A full rebuild of `fct_sessions` groups about 9M sessions. With 16
  threads, the external-aggregation merge alone wanted more than 3 GB, and ClickHouse's default memory
  limit (90% of RAM) sits above what Docker gives the VM, so the server got killed instead of the query
  failing. The fix: a server cap in `clickhouse/config.d/memory.xml`, per-query caps with spill-to-disk in
  `users.d/memory.xml`, and `max_threads=4` on just that model.
- **`insert_overwrite` needs `SELECT` on `system.parts`** in dbt-clickhouse, so the rollups use
  `delete+insert` keyed on the day instead. The result is the same, and it doesn't need the extra grant.
- **The same `user_session` id shows up days apart** in this dataset, so a session model that only
  re-reads "yesterday plus today" quietly disagrees with a full rebuild. I caught it by diffing the
  incremental output against a full refresh.
- The consumer subscribes before the producer creates the topic, and librdkafka only re-checks missing
  topics every 5 minutes by default. That's now 10 s.

## Caveats

- ClickHouse is standing in for Snowflake. Bytes scanned and query times are real; credits and dollars come from a model of Snowflake's billing rules, not an invoice.
- Laptop queries are much faster than a cloud warehouse's. Raise `duration_scale` in `workload.yml` to see how the levers behave when queries take longer.
- The consumer is at-least-once. Stream rows carry `_kafka_partition` and `_kafka_offset`, so duplicates can be checked with a single `GROUP BY`.
- Event dates are shifted so the last backfilled day is yesterday. The original timestamps are from October 2019.

## Data

The dataset is from REES46 via Kaggle: [mkechinov/ecommerce-behavior-data-from-multi-category-store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
Check the dataset page for its terms before redistributing it. Nothing from it is committed here.
`--source mirror` downloads from a third-party copy on Hugging Face; it's handy without a Kaggle account,
but it isn't an official source.
