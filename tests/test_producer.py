import gzip
import json
from datetime import date, datetime, timezone

import pytest

from ingest.producer import (
    due_time, find_day_offset, format_event_time, iter_day_rows, offset_in_day,
    parse_csv_time, plan_anchor, row_to_message,
)

HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"
ROWS = [
    "2019-10-04 23:59:59 UTC,view,1,10,,,1.00,100,s-a\n",
    "2019-10-05 00:00:00 UTC,view,44600062,2103807459595387724,,shiseido,35.79,541312140,72d76fde\n",
    "2019-10-05 14:03:12 UTC,purchase,3900821,2053013552326770905,appliances.environment.water_heater,aqua,33.2,554748717,\n",
    "2019-10-06 00:00:00 UTC,cart,2,20,,,2.00,200,s-b\n",
]


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def test_row_to_message_matches_the_contract():
    row = ROWS[1].strip().split(",")
    key, value = row_to_message(row, utc(2026, 11, 20, 14, 3, 12).timestamp() + 0.123)
    assert key == b"72d76fde"
    assert json.loads(value) == {
        "event_time": "2026-11-20T14:03:12.123Z",
        "event_type": "view",
        "product_id": 44600062,
        "category_id": 2103807459595387724,  # > 2**53, must stay an exact int
        "category_code": "",
        "brand": "shiseido",
        "price": "35.79",
        "user_id": 541312140,
        "user_session": "72d76fde",
    }


def test_row_to_message_empty_session_keys_by_user_and_pads_price():
    row = ROWS[2].strip().split(",")
    key, value = row_to_message(row, 0)
    event = json.loads(value)
    assert key == b"554748717"
    assert event["user_session"] == ""
    assert event["price"] == "33.20"
    assert event["category_code"] == "appliances.environment.water_heater"


@pytest.mark.parametrize("row", [
    ["2019-10-05 00:00:00 UTC", "view", "1"],                                      # too few columns
    ["2019-10-05 00:00:00 UTC", "view", "x", "1", "", "", "1.00", "1", "s"],       # bad product_id
    ["2019-10-05 00:00:00 UTC", "view", "1", "1", "", "", "abc", "1", "s"],        # bad price
])
def test_row_to_message_rejects_malformed_rows(row):
    with pytest.raises(ValueError):
        row_to_message(row, 0)


def test_format_event_time_rounds_to_milliseconds():
    assert format_event_time(utc(2026, 11, 20).timestamp()) == "2026-11-20T00:00:00.000Z"
    assert format_event_time(utc(2026, 11, 20, 23, 59, 59).timestamp() + 0.9996) == "2026-11-21T00:00:00.000Z"
    assert format_event_time(utc(2026, 11, 20, 1, 2, 3).timestamp() + 0.0504) == "2026-11-20T01:02:03.050Z"


def test_parse_csv_time_and_offset():
    t = parse_csv_time("2019-10-05 14:03:12 UTC")
    assert t == utc(2019, 10, 5, 14, 3, 12)
    assert offset_in_day(t, date(2019, 10, 5)) == 14 * 3600 + 3 * 60 + 12


def test_speed_one_maps_replay_day_onto_today():
    now = utc(2026, 11, 20, 9, 30)
    anchor = plan_anchor(1, now)
    assert anchor == utc(2026, 11, 20).timestamp()
    offset = offset_in_day(parse_csv_time("2019-10-05 14:03:12 UTC"), date(2019, 10, 5))
    assert format_event_time(due_time(anchor, offset, 0, 1)) == "2026-11-20T14:03:12.000Z"
    # events before "now" are already due -> sent immediately (catch-up)
    early = offset_in_day(parse_csv_time("2019-10-05 08:00:00 UTC"), date(2019, 10, 5))
    assert due_time(anchor, early, 0, 1) <= now.timestamp()
    # second loop = the same day replayed tomorrow
    assert format_event_time(due_time(anchor, offset, 1, 1)) == "2026-11-21T14:03:12.000Z"


def test_plan_anchor_uses_the_utc_date_of_an_aware_local_time():
    from datetime import timedelta
    # 00:30 in UTC+2 is still the previous day in UTC
    local = datetime(2026, 11, 21, 0, 30, tzinfo=timezone(timedelta(hours=2)))
    assert plan_anchor(1, local) == utc(2026, 11, 20).timestamp()


def test_faster_speed_compresses_the_day_from_start_time():
    now = utc(2026, 11, 20, 9, 30)
    anchor = plan_anchor(60, now)
    assert anchor == now.timestamp()               # replay day 00:00 starts right away
    assert due_time(anchor, 3600, 0, 60) == anchor + 60       # one hour -> one minute
    assert due_time(anchor, 0, 1, 60) == anchor + 1440        # next loop starts after 24 minutes
    assert format_event_time(due_time(anchor, 30, 0, 60)) == "2026-11-20T09:30:00.500Z"


@pytest.mark.parametrize("suffix", [".csv", ".csv.gz"])
def test_find_day_offset_and_iter_day_rows(tmp_path, suffix):
    path = tmp_path / f"events{suffix}"
    data = (HEADER + "".join(ROWS)).encode()
    if suffix.endswith(".gz"):
        path.write_bytes(gzip.compress(data))
    else:
        path.write_bytes(data)

    offset = find_day_offset(path, date(2019, 10, 5))
    assert offset == len((HEADER + ROWS[0]).encode())
    rows = list(iter_day_rows(path, offset, date(2019, 10, 5)))
    assert [r[0] for r in rows] == ["2019-10-05 00:00:00 UTC", "2019-10-05 14:03:12 UTC"]
    assert rows[1][-1] == ""

    assert find_day_offset(path, date(2019, 10, 1)) == len(HEADER)
    assert find_day_offset(path, date(2019, 10, 7)) is None
