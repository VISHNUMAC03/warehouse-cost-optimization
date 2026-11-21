-- raw:       landing zone, written by the backfill job and the Kafka consumer
-- analytics: everything dbt builds
-- costlab:   benchmark results and dbt run log used by the cost report/dashboard
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS costlab;
