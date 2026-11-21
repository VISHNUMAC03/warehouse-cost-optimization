"""Settings for the costlab toolkit. Same env vars as ingest/config.py, but
costlab connects as its own ClickHouse user so its benchmark queries are
easy to tell apart in system.query_log.

Inside the transformer container CLICKHOUSE_USER=dbt (dbt_runner only needs
the grants the dbt user has).
"""
import os
from pathlib import Path

import clickhouse_connect
from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "costlab")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "changeme")

DBT_DIR = PROJECT_DIR / "dbt"
DASHBOARDS_DIR = PROJECT_DIR / "grafana" / "dashboards"
REPORTS_DIR = PROJECT_DIR / "reports"
WORKLOAD_FILE = Path(__file__).resolve().parent / "workload.yml"

# Tag on every benchmark query (system.query_log.log_comment)
BENCH_TAG = "costlab-bench"


def clickhouse(timeout=300, database=None, **settings):
    """ClickHouse client; `timeout` is the HTTP read timeout in seconds, kwargs are server settings."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=database,
        send_receive_timeout=timeout,
        settings=settings or None,
    )
