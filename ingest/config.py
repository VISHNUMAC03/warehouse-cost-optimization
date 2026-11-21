"""Settings shared by the ingest jobs. Everything comes from env vars / .env.

Defaults are for running on the host against `docker compose up`;
inside the containers docker-compose.yml overrides the hostnames.
"""
import os
from datetime import date
from pathlib import Path

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

PROJECT_DIR = Path(__file__).resolve().parent.parent

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "loader")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "changeme")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shop.events")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "warehouse-loader")

REPLAY_FILE = os.getenv("REPLAY_FILE", "data/raw/2019-Oct.csv")
REPLAY_DAY = date.fromisoformat(os.getenv("REPLAY_DAY", "2019-10-31"))
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "1"))

# Column order of the Kaggle CSV (and of raw.events, minus the _ columns)
CSV_COLUMNS = [
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
]


def clickhouse(timeout=300, **settings):
    """ClickHouse client; `timeout` is the HTTP read timeout in seconds, kwargs are server settings."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        send_receive_timeout=timeout,
        settings=settings or None,
    )
