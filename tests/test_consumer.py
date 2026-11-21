import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ingest.consumer import COLUMNS, backoff, commit_offsets, message_to_row, parse_event_time
from ingest.producer import row_to_message

EVENT = {
    "event_time": "2026-11-20T14:03:12.123Z", "event_type": "purchase", "product_id": 44600062,
    "category_id": 2103807459595387724, "category_code": "", "brand": "shiseido", "price": "35.79",
    "user_id": 541312140, "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
}


def encode(**changes):
    event = {**EVENT, **changes}
    return json.dumps({k: v for k, v in event.items() if v is not ...}).encode()


def test_message_to_row():
    row = message_to_row(encode(), 2, 1234)
    assert len(row) == len(COLUMNS)
    values = dict(zip(COLUMNS, row))
    assert values["event_time"] == datetime(2026, 11, 20, 14, 3, 12, 123000, tzinfo=timezone.utc)
    assert values["event_time"].utcoffset().total_seconds() == 0
    assert values["price"] == Decimal("35.79") and isinstance(values["price"], Decimal)
    assert values["category_id"] == 2103807459595387724
    assert values["_source"] == "stream"
    assert (values["_kafka_partition"], values["_kafka_offset"]) == (2, 1234)


def test_round_trip_from_csv_row():
    csv_row = ["2019-10-05 14:03:12 UTC", "cart", "3900821", "2053013552326770905",
               "appliances.environment.water_heater", "aqua", "33.2", "554748717", ""]
    epoch = datetime(2026, 11, 20, 14, 3, 12, tzinfo=timezone.utc).timestamp()
    _, value = row_to_message(csv_row, epoch)
    row = dict(zip(COLUMNS, message_to_row(value, 0, 0)))
    assert row["event_time"].timestamp() == epoch
    assert row["price"] == Decimal("33.20")
    assert row["category_code"] == "appliances.environment.water_heater"
    assert row["user_session"] == ""


def test_parse_event_time_variants():
    assert parse_event_time("2026-11-20T14:03:12Z") == datetime(2026, 11, 20, 14, 3, 12, tzinfo=timezone.utc)
    # an offset is converted to UTC, no offset means UTC
    assert parse_event_time("2026-11-20T16:03:12.000+02:00") == datetime(2026, 11, 20, 14, 3, 12, tzinfo=timezone.utc)
    assert parse_event_time("2026-11-20T14:03:12").tzinfo == timezone.utc


def test_missing_optional_text_becomes_empty():
    row = dict(zip(COLUMNS, message_to_row(encode(brand=None, category_code=...), 0, 0)))
    assert row["brand"] == "" and row["category_code"] == ""


@pytest.mark.parametrize("value", [
    b"not json",
    b"\xff\xfe",
    b"[1, 2]",
    None,
    encode(event_time=...),
    encode(event_time="yesterday"),
    encode(event_time=1763647392),
    encode(event_type="click"),
    encode(product_id="44600062"),
    encode(product_id=-1),
    encode(user_id=2**64),
    encode(user_id=True),
    encode(category_id=1.5),
    encode(price="abc"),
    encode(price="NaN"),
    encode(price="-1.00"),
    encode(price="100000000.00"),
    encode(brand=5),
])
def test_malformed_messages_raise_value_error(value):
    with pytest.raises(ValueError):
        message_to_row(value, 0, 0)


def test_commit_offsets_are_highest_plus_one_per_partition():
    offsets = commit_offsets([("t", 1, 5), ("t", 0, 7), ("t", 1, 9), ("t", 0, 3)])
    assert [(tp.topic, tp.partition, tp.offset) for tp in offsets] == [("t", 0, 8), ("t", 1, 10)]


def test_backoff_is_capped():
    assert [backoff(a) for a in range(4)] == [1, 2, 4, 8]
    assert backoff(10) == 30
