"""Load historical events from a Kaggle CSV into raw.events.

ClickHouse reads the file itself (./data is mounted into its user_files dir),
so this script only builds and runs one INSERT ... SELECT.

Dates are shifted so the last loaded day becomes *yesterday*. The Kafka
producer replays the next day (REPLAY_DAY) as *today*, so history and the
live stream line up and the dashboards look like a shop that's open right now.

    python -m ingest.backfill                      # all days before REPLAY_DAY
    python -m ingest.backfill --days 14            # just the last 14 of them
    python -m ingest.backfill --truncate           # wipe raw.events first
"""
import argparse
import time
from datetime import date, datetime, timedelta, timezone

from ingest import config

CSV_STRUCTURE = (
    "event_time String, event_type String, product_id UInt64, category_id UInt64, "
    "category_code String, brand String, price Decimal(10, 2), user_id UInt64, user_session String"
)


def source(path):
    # path relative to the project dir == path relative to ClickHouse user_files
    return f"file('{path}', 'CSVWithNames', '{CSV_STRUCTURE}')"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", default=config.REPLAY_FILE, help="CSV under data/ (default: REPLAY_FILE)")
    parser.add_argument("--last-day", type=date.fromisoformat,
                        help="last original day to load (default: the day before REPLAY_DAY)")
    parser.add_argument("--days", type=int, help="how many days to load, counting back from --last-day")
    parser.add_argument("--truncate", action="store_true", help="empty raw.events before loading")
    args = parser.parse_args()

    path = args.file.replace("\\", "/")
    if not (config.PROJECT_DIR / path).exists():
        raise SystemExit(f"{path} not found - run `python -m ingest.download` first")

    ch = config.clickhouse(timeout=3600)

    existing = ch.command("SELECT count() FROM raw.events WHERE _source = 'backfill'")
    if existing and not args.truncate:
        raise SystemExit(f"raw.events already has {existing:,} backfill rows. Re-run with --truncate to reload.")
    if args.truncate:
        ch.command("TRUNCATE TABLE raw.events")
        print("raw.events truncated")

    last_day = args.last_day or (config.REPLAY_DAY - timedelta(days=1))
    if args.days:
        first_day = last_day - timedelta(days=args.days - 1)
    else:
        print(f"scanning {path} for the first day ...")
        first_day = ch.command(f"SELECT toDate(min(parseDateTimeBestEffort(event_time, 'UTC'))) FROM {source(path)}")
        first_day = date.fromisoformat(str(first_day))

    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    shift = (yesterday - last_day).days
    print(f"loading {first_day} .. {last_day} from {path}, shifted +{shift} days "
          f"(-> {first_day + timedelta(days=shift)} .. {yesterday})")

    started = time.monotonic()
    ch.command(f"""
        INSERT INTO raw.events
            (event_time, event_type, product_id, category_id, category_code,
             brand, price, user_id, user_session, _source)
        SELECT
            parseDateTimeBestEffort(event_time, 'UTC') + INTERVAL {shift} DAY,
            event_type, product_id, category_id, category_code,
            brand, price, user_id, user_session, 'backfill'
        FROM {source(path)}
        WHERE toDate(parseDateTimeBestEffort(event_time, 'UTC')) BETWEEN '{first_day}' AND '{last_day}'
        SETTINGS max_insert_threads = 4
    """)
    elapsed = time.monotonic() - started

    rows = ch.query("""
        SELECT toDate(event_time) AS day, count() AS events, countIf(event_type = 'purchase') AS purchases
        FROM raw.events WHERE _source = 'backfill'
        GROUP BY day ORDER BY day
    """).result_rows
    for day, events, purchases in rows:
        print(f"  {day}  {events:>10,} events  {purchases:>8,} purchases")
    print(f"done: {sum(r[1] for r in rows):,} rows in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
