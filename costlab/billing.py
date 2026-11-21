"""Snowflake-style warehouse billing, simulated over one day.

The rules (as documented by Snowflake for virtual warehouses):
  - a suspended warehouse resumes when a query arrives;
  - it stays up while any query runs, then keeps running idle for
    `auto_suspend` seconds before it suspends;
  - a query that arrives during that idle tail reuses the running warehouse
    (no new resume);
  - billing is per second, with a 60 second minimum every time it resumes;
  - credits per hour depend on the size (XS=1, S=2, M=4, L=8, XL=16).

So each resume session costs max(60, last_busy_end + auto_suspend - session_start).
Sessions are cut at midnight: the model looks at one 24h day and does not bill
the idle tail of a 23:59 query into tomorrow.

Pure functions only - no ClickHouse here, so it is easy to unit test.
"""
from dataclasses import dataclass

CREDITS_PER_HOUR = {"XS": 1, "S": 2, "M": 4, "L": 8, "XL": 16}
DAY_SECONDS = 86400


@dataclass
class Bill:
    billed_seconds: float
    busy_seconds: float
    resumes: int


def busy_blocks(queries):
    """Merge (start_s, duration_s) query intervals into non-overlapping (start, end) blocks."""
    blocks = []
    for start, duration in sorted(queries):
        end = start + max(duration, 0.0)
        if blocks and start <= blocks[-1][1]:
            blocks[-1][1] = max(blocks[-1][1], end)
        else:
            blocks.append([start, end])
    return [(s, e) for s, e in blocks]


def simulate(queries, auto_suspend_s, min_billed_s=60, always_on=False, day_s=DAY_SECONDS):
    """Bill one warehouse for one day.

    queries: iterable of (start_s, duration_s), start measured from midnight.
    Returns Bill(billed_seconds, busy_seconds, resumes).
    """
    blocks = busy_blocks(queries)
    busy = sum(e - s for s, e in blocks)

    if always_on:
        return Bill(billed_seconds=float(day_s), busy_seconds=busy, resumes=0)

    billed = 0.0
    resumes = 0
    session_start = None
    busy_until = None
    for start, end in blocks:
        # still up (busy or inside the idle tail)? then no resume
        if session_start is not None and start <= busy_until + auto_suspend_s:
            busy_until = max(busy_until, end)
            continue
        if session_start is not None:
            billed += _session_cost(session_start, busy_until, auto_suspend_s, min_billed_s, day_s)
        session_start, busy_until = start, end
        resumes += 1
    if session_start is not None:
        billed += _session_cost(session_start, busy_until, auto_suspend_s, min_billed_s, day_s)

    return Bill(billed_seconds=billed, busy_seconds=busy, resumes=resumes)


def _session_cost(start, busy_until, auto_suspend_s, min_billed_s, day_s):
    end = min(busy_until + auto_suspend_s, day_s)
    return max(float(min_billed_s), end - start)


def credits(billed_seconds, size):
    return billed_seconds / 3600.0 * CREDITS_PER_HOUR[size]
