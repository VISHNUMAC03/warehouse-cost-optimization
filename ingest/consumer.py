"""Kafka -> ClickHouse loader: reads shop.events and appends micro-batches to raw.events.

    python -m ingest.consumer                 # runs until SIGTERM / Ctrl+C
    python -m ingest.consumer --batch-size 5000 --batch-seconds 1

Delivery is at-least-once: a batch is inserted first and its offsets are committed
afterwards. If the process dies between the two, the batch is inserted again on
restart. Every stream row carries _kafka_partition/_kafka_offset, so duplicates
are easy to spot:

    SELECT _kafka_partition, _kafka_offset, count() FROM raw.events
    WHERE _source = 'stream' GROUP BY 1, 2 HAVING count() > 1

Messages that aren't valid events are logged, counted and skipped (their offsets
are still committed). ClickHouse errors are retried with backoff; nothing is
committed until the insert succeeds.
"""
import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from confluent_kafka import Consumer, KafkaError, TopicPartition

from ingest import config

log = logging.getLogger("consumer")

COLUMNS = [
    "event_time", "event_type", "product_id", "category_id", "category_code", "brand",
    "price", "user_id", "user_session", "_source", "_kafka_partition", "_kafka_offset",
]
EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
UINT64_MAX = 2**64 - 1
MAX_PRICE = Decimal("99999999.99")  # Decimal(10, 2)
CENT = Decimal("0.01")
STATUS_EVERY = 10.0


# --- pure helpers (unit tested) -----------------------------------------------------------

def parse_event_time(value):
    """ISO-8601 string -> timezone-aware UTC datetime. A missing offset means UTC."""
    if not isinstance(value, str):
        raise ValueError("event_time must be a string")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _uint(event, field):
    value = event[field]
    # bool is an int subclass; a float id would already have lost precision
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= UINT64_MAX:
        raise ValueError(f"{field} must be an unsigned 64-bit integer, got {value!r}")
    return value


def _text(event, field):
    value = event.get(field)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string, got {value!r}")
    return value


def message_to_row(value, partition, offset):
    """Kafka message value (JSON bytes) -> tuple in COLUMNS order. Raises ValueError if malformed."""
    try:
        event = json.loads(value)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"not JSON: {exc}") from None
    if not isinstance(event, dict):
        raise ValueError("not a JSON object")
    try:
        event_type = event["event_type"]
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unknown event_type {event_type!r}")
        try:
            price = Decimal(str(event["price"]))
        except InvalidOperation:
            raise ValueError(f"bad price {event['price']!r}") from None
        if not price.is_finite() or price < 0 or price > MAX_PRICE:
            raise ValueError(f"price out of range: {event['price']!r}")
        return (
            parse_event_time(event["event_time"]),
            event_type,
            _uint(event, "product_id"),
            _uint(event, "category_id"),
            _text(event, "category_code"),
            _text(event, "brand"),
            price.quantize(CENT),
            _uint(event, "user_id"),
            _text(event, "user_session"),
            "stream",
            partition,
            offset,
        )
    except KeyError as exc:
        raise ValueError(f"missing field {exc}") from None


def commit_offsets(messages):
    """Offsets to commit after `messages`: highest offset + 1 per partition."""
    highest = {}
    for topic, partition, offset in messages:
        key = (topic, partition)
        highest[key] = max(highest.get(key, -1), offset)
    return [TopicPartition(t, p, o + 1) for (t, p), o in sorted(highest.items())]


def backoff(attempt, cap=30.0):
    return min(cap, 2.0 ** attempt)


# --- main loop ----------------------------------------------------------------------------

class Loader:
    def __init__(self, args):
        self.args = args
        self.stopping = False
        self.rows = 0
        self.malformed = 0
        self.batches = 0
        self._last_status = time.monotonic()
        self._last_rows = 0

    def stop(self, signum, frame):
        log.info("signal %s received, finishing the current batch", signum)
        self.stopping = True

    def read_batch(self, consumer):
        """Up to batch_size messages or batch_seconds, whichever comes first."""
        messages = []
        deadline = time.monotonic() + self.args.batch_seconds
        while len(messages) < self.args.batch_size and not self.stopping:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            got = consumer.consume(min(1000, self.args.batch_size - len(messages)), timeout=min(remaining, 1.0))
            for msg in got:
                err = msg.error()
                if err is None:
                    messages.append(msg)
                elif err.code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", err)
        return messages

    def to_rows(self, messages):
        rows = []
        for msg in messages:
            try:
                rows.append(message_to_row(msg.value(), msg.partition(), msg.offset()))
            except ValueError as exc:
                self.malformed += 1
                log.warning("skipping malformed message p%d@%d (%s): %r",
                            msg.partition(), msg.offset(), exc, (msg.value() or b"")[:200])
        return rows

    def insert(self, rows):
        """Insert with retries. Returns False if we gave up because of a shutdown."""
        attempt = 0
        while True:
            try:
                self.ch.insert("raw.events", rows, column_names=COLUMNS)
                return True
            except Exception as exc:  # network, server restart, ... - all worth a retry
                if self.stopping:
                    log.error("insert failed during shutdown, leaving %d rows uncommitted: %s", len(rows), exc)
                    return False
                wait = backoff(attempt)
                attempt += 1
                log.error("insert of %d rows failed (attempt %d), retrying in %.0fs: %s", len(rows), attempt, wait, exc)
                time.sleep(wait)
                if attempt % 5 == 0:
                    self.ch = config.clickhouse()  # fresh client in case the connection pool is wedged

    def status(self, consumer):
        now = time.monotonic()
        if now - self._last_status < STATUS_EVERY:
            return
        rate = (self.rows - self._last_rows) / (now - self._last_status)
        log.info("loaded %d rows (%.0f rows/s) in %d batches, malformed %d, lag %s",
                 self.rows, rate, self.batches, self.malformed, self.lag(consumer))
        self._last_status, self._last_rows = now, self.rows

    @staticmethod
    def lag(consumer):
        try:
            assignment = consumer.assignment()
            if not assignment:
                return "n/a (no partitions assigned)"
            total = 0
            for tp in consumer.position(assignment):
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                total += high - (tp.offset if tp.offset >= 0 else low)
            return str(total)
        except Exception as exc:  # lag is only informational
            return f"unknown ({exc})"

    def run(self):
        consumer = Consumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": config.KAFKA_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            # on a fresh stack the producer creates the topic after we subscribe;
            # the default 5 min metadata refresh would leave us idle that long
            "topic.metadata.refresh.interval.ms": 10_000,
        })
        consumer.subscribe([config.KAFKA_TOPIC])
        self.ch = config.clickhouse()
        log.info("consuming %s @ %s as group %s -> raw.events on %s (user %s)",
                 config.KAFKA_TOPIC, config.KAFKA_BOOTSTRAP, config.KAFKA_GROUP,
                 config.CLICKHOUSE_HOST, config.CLICKHOUSE_USER)
        exit_code = 0
        try:
            while not self.stopping:
                messages = self.read_batch(consumer)
                if messages:
                    rows = self.to_rows(messages)
                    if rows and not self.insert(rows):
                        exit_code = 1
                        break
                    # malformed messages are committed too: retrying them won't make them valid
                    consumer.commit(offsets=commit_offsets(
                        (m.topic(), m.partition(), m.offset()) for m in messages), asynchronous=False)
                    self.rows += len(rows)
                    self.batches += 1
                self.status(consumer)
        finally:
            log.info("closing: %d rows loaded, %d malformed", self.rows, self.malformed)
            consumer.close()
        return exit_code


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-size", type=int, default=10_000, help="max messages per insert (default: %(default)s)")
    parser.add_argument("--batch-seconds", type=float, default=2.0, help="max seconds to collect a batch (default: %(default)s)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    loader = Loader(args)
    signal.signal(signal.SIGINT, loader.stop)
    signal.signal(signal.SIGTERM, loader.stop)
    return loader.run()


if __name__ == "__main__":
    sys.exit(main())
