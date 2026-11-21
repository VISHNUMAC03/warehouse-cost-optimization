"""Replay one day of the Kaggle CSV into Kafka as if it were happening right now.

    python -m ingest.producer                         # REPLAY_DAY of REPLAY_FILE, real time, forever
    python -m ingest.producer --max-events 20000      # quick test
    python -m ingest.producer --speed 60 --no-loop    # one day in 24 minutes, then exit

Timing. Every event is scheduled at

    due = anchor + (day_index * 86400 + seconds_since_replay_midnight) / speed

and its event_time in Kafka is exactly that `due` (so dashboards see a live shop).

* speed 1 (default): anchor = today's midnight UTC, so an event from 14:03:12 on
  REPLAY_DAY is sent at 14:03:12 today with today's date. On start-up everything
  between midnight and now is sent as fast as possible ("catch-up"), then the
  replay continues in real time. --no-catchup skips that part instead.
  If the topic already has events from today (producer restart), the replay
  resumes after the newest one rather than sending the morning twice.
* speed != 1: there's no sensible way to map a compressed day onto today's
  clock, so the replay starts at the replay day's 00:00 *now* (anchor = start
  time) and event_time is the scheduled send time. No catch-up, no resume.

When the day is done the same day is replayed again shifted by one day (day_index
+1), i.e. tomorrow at speed 1. That's the default (the container should keep
streaming); --no-loop exits instead.

The input is read line by line (.csv or .csv.gz), never loaded into memory.
Rows before REPLAY_DAY are skipped with a cheap byte-level scan; the byte offset
of the day is remembered so later loops seek straight to it.
"""
import argparse
import csv
import gzip
import io
import json
import logging
import signal
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from ingest import config

log = logging.getLogger("producer")

DAY = 86400
STATUS_EVERY = 10.0
PARTITIONS = 3
CENT = Decimal("0.01")


# --- pure helpers (unit tested) -----------------------------------------------------------

def parse_csv_time(value):
    """'2019-10-05 14:03:12 UTC' -> aware UTC datetime."""
    return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def day_midnight(day):
    return datetime(day.year, day.month, day.day, tzinfo=timezone.utc)


def offset_in_day(row_time, replay_day):
    """Seconds between the replay day's midnight and the event (0 .. 86399)."""
    return (row_time - day_midnight(replay_day)).total_seconds()


def plan_anchor(speed, now):
    """Wall-clock epoch seconds that the replay day's 00:00 maps to."""
    if speed == 1:
        today = now.astimezone(timezone.utc).date()
        return day_midnight(today).timestamp()
    return now.timestamp()


def due_time(anchor, offset, day_index, speed):
    """Epoch seconds when an event `offset` seconds into the replay day should be sent."""
    return anchor + (day_index * DAY + offset) / speed


def format_event_time(epoch):
    """Epoch seconds -> '2026-11-20T14:03:12.123Z'."""
    ms = round(epoch * 1000)
    dt = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{ms % 1000:03d}Z"


def row_to_message(row, event_epoch):
    """CSV row (list of 9 strings, Kaggle column order) -> (key bytes, value bytes)."""
    if len(row) != len(config.CSV_COLUMNS):
        raise ValueError(f"expected {len(config.CSV_COLUMNS)} columns, got {len(row)}")
    _, event_type, product_id, category_id, category_code, brand, price, user_id, user_session = row
    try:
        price = str(Decimal(price).quantize(CENT))
    except InvalidOperation:
        raise ValueError(f"bad price {price!r}") from None
    event = {
        "event_time": format_event_time(event_epoch),
        "event_type": event_type,
        "product_id": int(product_id),
        "category_id": int(category_id),
        "category_code": category_code,
        "brand": brand,
        "price": price,
        "user_id": int(user_id),
        "user_session": user_session,
    }
    # key by session so one session's events stay in order on one partition
    key = user_session or str(event["user_id"])
    return key.encode(), json.dumps(event, separators=(",", ":")).encode()


# --- reading the file ---------------------------------------------------------------------

def open_binary(path):
    return gzip.open(path, "rb") if str(path).endswith(".gz") else open(path, "rb")


def find_day_offset(path, day):
    """Byte offset (in the decompressed stream) of the first row on or after `day`.

    Rows are sorted by event_time, so comparing the first 10 bytes of each line is
    enough - much faster than csv-parsing tens of millions of rows we don't need.
    Returns None if the file has no row on or after `day`.
    """
    target = day.isoformat().encode()
    offset = 0
    with open_binary(path) as f:
        for n, line in enumerate(f):
            if n and line[:10] >= target:
                return offset
            offset += len(line)
            if n and n % 5_000_000 == 0:
                log.info("skipping to %s: %dM rows read, at %s", day, n // 1_000_000, line[:19].decode())
    return None


def iter_day_rows(path, start_offset, day):
    """Yield csv rows of `day` starting at `start_offset`; stops at the first row of a later day."""
    prefix = day.isoformat()
    with open_binary(path) as raw:
        raw.seek(start_offset)
        with io.TextIOWrapper(raw, encoding="utf-8", newline="") as text:
            for row in csv.reader(text):
                if not row:
                    continue
                if row[0][:10] != prefix:
                    return
                yield row


# --- kafka --------------------------------------------------------------------------------

def ensure_topic(bootstrap, topic):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=15).topics:
        return
    log.info("creating topic %s (%d partitions)", topic, PARTITIONS)
    futures = admin.create_topics([NewTopic(topic, num_partitions=PARTITIONS, replication_factor=1)])
    try:
        futures[topic].result(timeout=30)
    except KafkaException as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):  # another producer won the race
            raise
    # a brand-new topic has no partition leaders for a moment
    for _ in range(30):
        partitions = admin.list_topics(topic, timeout=15).topics[topic].partitions.values()
        if partitions and all(p.leader >= 0 and p.error is None for p in partitions):
            return
        time.sleep(1)
    log.warning("topic %s created but its partitions have no leader yet", topic)


def newest_event_time(bootstrap, topic):
    """Epoch seconds of the newest event_time on the topic (last message of each partition), or None."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "producer-resume-probe",
                         "enable.auto.commit": False})
    newest = None
    try:
        partitions = consumer.list_topics(topic, timeout=15).topics[topic].partitions
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
            if high <= low:
                continue
            consumer.assign([TopicPartition(topic, p, high - 1)])
            msg = consumer.poll(10)
            if msg is None or msg.error():
                continue
            try:
                stamp = json.loads(msg.value())["event_time"]
                epoch = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
            except (ValueError, KeyError, TypeError):
                continue
            newest = epoch if newest is None else max(newest, epoch)
    finally:
        consumer.close()
    return newest


def make_producer(bootstrap):
    return Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 50,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
        "queue.buffering.max.messages": 200_000,
    })


# --- main loop ----------------------------------------------------------------------------

class Replay:
    def __init__(self, args):
        self.args = args
        self.path = args.file
        self.stopping = False
        self.sent = 0           # handed to the producer
        self.delivered = 0      # acked by the broker
        self.failed = 0
        self.bad_rows = 0
        self.clock = None       # (original row time, live event time) of the last event
        self._last_status = time.monotonic()
        self._last_sent = 0

    def on_delivery(self, err, msg):
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            if self.failed <= 10 or self.failed % 1000 == 0:
                log.error("delivery failed (%d so far): %s", self.failed, err)

    def stop(self, signum, frame):
        log.info("signal %s received, flushing and exiting", signum)
        self.stopping = True

    def status(self, force=False):
        now = time.monotonic()
        if not force and now - self._last_status < STATUS_EVERY:
            return
        rate = (self.sent - self._last_sent) / max(now - self._last_status, 1e-9)
        if self.clock:
            original, live = self.clock
            clock = f"replay clock {original} -> {format_event_time(live)[:19]}"
        else:
            clock = "no events yet"
        log.info("sent %d (%.0f/s), delivered %d, failed %d | %s",
                 self.sent, rate, self.delivered, self.failed, clock)
        self._last_status, self._last_sent = now, self.sent

    def wait_until(self, producer, due):
        """Sleep until `due` (epoch s), serving delivery callbacks and staying responsive to signals."""
        while not self.stopping:
            remaining = due - time.time()
            if remaining <= 0:
                return
            producer.poll(min(remaining, 0.5))
            self.status()

    def produce(self, producer, key, value):
        while True:
            try:
                producer.produce(config.KAFKA_TOPIC, value=value, key=key, on_delivery=self.on_delivery)
                break
            except BufferError:  # local queue full during catch-up: let it drain
                producer.poll(0.5)
        producer.poll(0)
        self.sent += 1

    def run(self):
        args = self.args
        day = args.day
        log.info("replaying %s of %s at speed %g into %s @ %s",
                 day, self.path, args.speed, config.KAFKA_TOPIC, config.KAFKA_BOOTSTRAP)

        # topic first: the skip below can take minutes and the consumer is waiting for it
        ensure_topic(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC)

        start_offset = find_day_offset(self.path, day)
        if start_offset is None:
            log.error("%s has no rows for %s", self.path, day)
            return 1

        producer = make_producer(config.KAFKA_BOOTSTRAP)

        anchor = plan_anchor(args.speed, datetime.now(timezone.utc))
        skip_until = None  # events due at or before this epoch are not sent
        if args.speed == 1:
            newest = newest_event_time(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC)
            # ignore anything in the future (left over from a --speed run)
            if newest and anchor <= newest <= time.time() + 60:
                log.info("topic already has events up to %s - resuming after that", format_event_time(newest))
                skip_until = newest
            if not args.catchup:
                skip_until = max(skip_until or 0, time.time())
                log.info("no catch-up: skipping events before %s", format_event_time(skip_until))

        day_index = 0
        caught_up = args.speed != 1  # only speed 1 has a catch-up phase worth reporting
        while not self.stopping:
            for row in iter_day_rows(self.path, start_offset, day):
                if self.stopping:
                    break
                try:
                    row_time = parse_csv_time(row[0])
                    due = due_time(anchor, offset_in_day(row_time, day), day_index, args.speed)
                    if skip_until is not None and due <= skip_until:
                        continue
                    key, value = row_to_message(row, due)
                except ValueError as exc:
                    self.bad_rows += 1
                    log.warning("skipping bad row %r: %s", row, exc)
                    continue

                if due > time.time():
                    if not caught_up:
                        caught_up = True
                        log.info("caught up after %d events, now streaming in real time", self.sent)
                    self.wait_until(producer, due)
                    if self.stopping:
                        break
                self.produce(producer, key, value)
                self.clock = (row[0][:19], due)
                self.status()
                if args.max_events and self.sent >= args.max_events:
                    log.info("reached --max-events %d", args.max_events)
                    self.stopping = True
                    break

            if self.stopping or not args.loop:
                break
            day_index += 1
            log.info("end of replay day, starting it again as day +%d", day_index)

        log.info("flushing ...")
        remaining = producer.flush(30)
        self.status(force=True)
        if remaining:
            log.error("%d messages still undelivered after flush", remaining)
            return 1
        return 1 if self.failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", default=config.REPLAY_FILE, help="CSV or CSV.GZ to replay (default: REPLAY_FILE=%(default)s)")
    parser.add_argument("--day", type=date.fromisoformat, default=config.REPLAY_DAY,
                        help="original day to replay (default: REPLAY_DAY=%(default)s)")
    parser.add_argument("--speed", type=float, default=config.REPLAY_SPEED,
                        help="1 = real time, 60 = a day in 24 minutes (default: REPLAY_SPEED=%(default)s)")
    parser.add_argument("--no-catchup", dest="catchup", action="store_false",
                        help="speed 1 only: don't burst the events between midnight and now, start from now")
    parser.add_argument("--loop", action=argparse.BooleanOptionalAction, default=True,
                        help="replay the day again (shifted +1 day) when it ends (default: on)")
    parser.add_argument("--max-events", type=int, help="stop after sending this many events (testing)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.speed <= 0:
        parser.error("--speed must be > 0")
    path = Path(args.file)
    if not path.is_absolute():
        path = config.PROJECT_DIR / path
    if not path.exists():
        log.error("replay file %s not found - run `python -m ingest.download` (or set REPLAY_FILE)", path)
        return 2
    args.file = path

    replay = Replay(args)
    signal.signal(signal.SIGINT, replay.stop)
    signal.signal(signal.SIGTERM, replay.stop)
    return replay.run()


if __name__ == "__main__":
    sys.exit(main())
