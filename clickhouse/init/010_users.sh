#!/bin/bash
# One ClickHouse user per tool. Nothing fancy security-wise (shared dev password),
# the point is that system.query_log.user tells you *who* spent the bytes -
# same idea as a separate Snowflake warehouse/role per workload.
set -e

clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE USER IF NOT EXISTS loader  IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PASSWORD}';
CREATE USER IF NOT EXISTS dbt     IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PASSWORD}';
CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PASSWORD}';
CREATE USER IF NOT EXISTS costlab IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PASSWORD}';

-- loader: Kafka consumer + CSV backfill
GRANT SELECT, INSERT, TRUNCATE ON raw.* TO loader;
GRANT FILE, CREATE TEMPORARY TABLE ON *.* TO loader;

-- dbt: owns analytics, reads raw, logs its own runs
GRANT ALL ON analytics.* TO dbt;
GRANT SELECT ON raw.* TO dbt;
GRANT CREATE TEMPORARY TABLE ON *.* TO dbt;
GRANT SELECT ON system.query_log TO dbt;
GRANT SYSTEM FLUSH LOGS ON *.* TO dbt;
GRANT INSERT, SELECT ON costlab.dbt_runs TO dbt;

-- grafana: read-only dashboards, including the cost dashboard over query_log
GRANT SELECT ON raw.* TO grafana;
GRANT SELECT ON analytics.* TO grafana;
GRANT SELECT ON costlab.* TO grafana;
GRANT SELECT ON system.query_log TO grafana;
GRANT SELECT ON system.parts TO grafana;
GRANT SELECT ON system.tables TO grafana;

-- costlab: benchmarks + report. Builds its own copies of the fact table in costlab.*
-- and drives dbt runs through the dbt user.
GRANT ALL ON costlab.* TO costlab;
GRANT SELECT ON raw.* TO costlab;
GRANT SELECT ON analytics.* TO costlab;
GRANT SELECT ON system.* TO costlab;
GRANT SYSTEM FLUSH LOGS ON *.* TO costlab;
GRANT CREATE TEMPORARY TABLE ON *.* TO costlab;
SQL
