"""Lever 2: what partitioning + clustering the fact table buys.

Copies analytics.fct_events (partitioned by month, ordered by
(event_date, event_type, category_l1, brand)) into
costlab.fct_events_unpartitioned (no partition key, ORDER BY tuple() - the
"before" layout), then replays every BI dashboard panel's SQL against both
tables and records rows/bytes read and run time from system.query_log into
costlab.bench_queries.

Panel SQL comes straight from grafana/dashboards/<uid>.json. Grafana macros
are expanded the way grafana-clickhouse-datasource does it, using the
dashboard's default time range ending now.

    python -m costlab bench-scan              # 5 timed runs per query (+1 warm-up)
    python -m costlab bench-scan --runs 3 --drop-table
"""
import argparse
import json
import re
import statistics
import time
import uuid
from datetime import datetime, timezone

from costlab import config, workload

FACT_TABLE = "analytics.fct_events"
UNPARTITIONED_TABLE = "costlab.fct_events_unpartitioned"
LAYOUTS = ("unpartitioned", "partitioned")

# Grafana's default for a wide panel is roughly one point per pixel; ~1000 is typical.
MAX_DATA_POINTS = 1000

TAG_RE = re.compile(r"/\*\s*grafana\s+dashboard=([\w-]+)\s+panel=([\w-]+)\s*\*/")
RELATIVE_RE = re.compile(r"^now-(\d+)([smhdw])$")
UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


# --- dashboards -> panel SQL ------------------------------------------------

def load_panels(dashboard_uids, dashboards_dir=None):
    """[{dashboard, panel, sql, time_from}, ...] for every SQL target of the given dashboards."""
    dashboards_dir = dashboards_dir or config.DASHBOARDS_DIR
    out = []
    for uid in dashboard_uids:
        path = dashboards_dir / f"{uid}.json"
        with open(path, encoding="utf-8") as f:
            dash = json.load(f)
        time_from = dash.get("time", {}).get("from", "now-7d")
        for panel in _walk_panels(dash.get("panels", [])):
            for target in panel.get("targets", []):
                sql = target.get("rawSql")
                if not sql:
                    continue
                tag = TAG_RE.search(sql)
                slug = tag.group(2) if tag else _slugify(f"{panel.get('title', 'panel')}-{target.get('refId', '')}")
                out.append({"dashboard": uid, "panel": slug, "sql": sql, "time_from": time_from})
    return out


def _walk_panels(panels):
    # collapsed rows keep their children in a nested "panels" list
    for panel in panels:
        yield panel
        yield from _walk_panels(panel.get("panels", []))


def _slugify(text):
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# --- Grafana macros ----------------------------------------------------------

def relative_seconds(time_from):
    """'now-7d' -> 604800. Only the simple now-<n><unit> form the dashboards use."""
    m = RELATIVE_RE.match(time_from.strip())
    if not m:
        raise ValueError(f"unsupported dashboard time.from: {time_from!r}")
    return int(m.group(1)) * UNIT_SECONDS[m.group(2)]


# Grafana's kbn.roundInterval steps (ms), as (upper bound of raw interval, rounded value)
_ROUND_STEPS_MS = [
    (15, 10), (35, 20), (75, 50), (150, 100), (350, 200), (750, 500), (1500, 1000),
    (3500, 2000), (7500, 5000), (12500, 10000), (17500, 15000), (25000, 20000),
    (45000, 30000), (90000, 60000), (210000, 120000), (450000, 300000), (750000, 600000),
    (1050000, 900000), (1500000, 1200000), (2700000, 1800000), (5400000, 3600000),
    (9000000, 7200000), (16200000, 10800000), (32400000, 21600000), (86400000, 43200000),
    (604800000, 86400000), (1814400000, 604800000), (3628800000, 2592000000),
]


def grafana_interval_s(range_s, max_data_points=MAX_DATA_POINTS):
    """The $__interval Grafana would pick: range / max data points, rounded to a
    'nice' step the same way Grafana's roundInterval does, at least 1 second
    (the macros work in whole seconds). 7d -> 600s, 24h -> 60s, 30m -> 2s."""
    raw_ms = range_s * 1000.0 / max_data_points
    rounded = 31536000000
    for bound, value in _ROUND_STEPS_MS:
        if raw_ms < bound:
            rounded = value
            break
    return max(1, rounded // 1000)


def expand_macros(sql, from_epoch, to_epoch, interval_s):
    """Expand the Grafana macros the dashboards are allowed to use."""
    frm = f"toDateTime({int(from_epoch)})"
    to = f"toDateTime({int(to_epoch)})"
    sql = re.sub(r"\$__timeFilter\(\s*([^()]+?)\s*\)",
                 lambda m: f"{m.group(1)} >= {frm} AND {m.group(1)} <= {to}", sql)
    sql = re.sub(r"\$__timeInterval\(\s*([^()]+?)\s*\)",
                 lambda m: f"toStartOfInterval({m.group(1)}, INTERVAL {int(interval_s)} second)", sql)
    sql = re.sub(r"\$__fromTime(?!\w)", frm, sql)
    sql = re.sub(r"\$__toTime(?!\w)", to, sql)
    sql = re.sub(r"\$__interval_s(?!\w)", str(int(interval_s)), sql)
    leftover = re.search(r"\$__\w+|\$\{?\w+", sql)
    if leftover:
        raise ValueError(f"unsupported macro or variable {leftover.group(0)!r}")
    return sql


def rewrite_fact_table(sql, target=UNPARTITIONED_TABLE):
    """Point analytics.fct_events at the unpartitioned copy. Leaves other tables
    alone (analytics.fct_events_x, fct_sessions, ...). A bare `fct_events` is
    rewritten too, since Grafana's default database is analytics."""
    return re.sub(r"(?<![\w.`])(?:analytics\.)?fct_events(?![\w`])", target, sql)


def prepare_sql(panel, layout, now_epoch):
    """Panel SQL ready to run: macros expanded for the dashboard's default range ending at now_epoch."""
    range_s = relative_seconds(panel["time_from"])
    sql = expand_macros(panel["sql"], now_epoch - range_s, now_epoch, grafana_interval_s(range_s))
    if layout == "unpartitioned":
        sql = rewrite_fact_table(sql)
    return sql.strip().rstrip(";")


# --- benchmark --------------------------------------------------------------

def build_unpartitioned_copy(ch):
    ch.command(f"DROP TABLE IF EXISTS {UNPARTITIONED_TABLE}")
    ch.command(f"CREATE TABLE {UNPARTITIONED_TABLE} ENGINE = MergeTree ORDER BY tuple() "
               f"AS SELECT * FROM {FACT_TABLE}")
    return ch.command(f"SELECT count() FROM {UNPARTITIONED_TABLE}")


def fetch_query_stats(ch, query_ids):
    """{query_id: (read_rows, read_bytes, duration_ms, result_rows)} for finished queries."""
    ch.command("SYSTEM FLUSH LOGS")
    rows = ch.query(
        """
        SELECT query_id, read_rows, read_bytes,
               (toUnixTimestamp64Micro(event_time_microseconds)
                - toUnixTimestamp64Micro(query_start_time_microseconds)) / 1000.0 AS duration_ms,
               result_rows
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND query_id IN {ids:Array(String)}
        """,
        parameters={"ids": list(query_ids)},
    ).result_rows
    return {r[0]: tuple(r[1:]) for r in rows}


def run(runs=5, keep_table=True, dashboards=None):
    """Run the benchmark; returns (run_id, rows written to costlab.bench_queries)."""
    dashboards = dashboards or workload.load()["bi_dashboards"]
    panels = load_panels(dashboards)
    if not panels:
        raise SystemExit("no panel SQL found in " + ", ".join(dashboards))

    # database=analytics so bare table names resolve the way they do in Grafana
    ch = config.clickhouse(timeout=600, database="analytics")
    started = time.monotonic()
    copied = build_unpartitioned_copy(ch)
    print(f"{UNPARTITIONED_TABLE}: {copied:,} rows copied from {FACT_TABLE} in {time.monotonic() - started:.1f}s")

    run_id = "bench-scan-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_at = datetime.now(timezone.utc).replace(microsecond=0)
    now_epoch = int(time.time())
    settings = {"use_query_cache": 0, "log_comment": config.BENCH_TAG}

    # queue: (layout, panel, [query_id, ...])
    planned = []
    failures = 0
    for panel in panels:
        for layout in LAYOUTS:
            sql = prepare_sql(panel, layout, now_epoch)
            ids = []
            try:
                # one warm-up so both layouts are measured with warm caches
                ch.query(sql, settings=settings)
                for _ in range(runs):
                    qid = f"{run_id}-{uuid.uuid4().hex[:12]}"
                    ch.query(sql, settings=dict(settings, query_id=qid))
                    ids.append(qid)
            except Exception as e:  # keep going - one broken panel shouldn't sink the run
                failures += 1
                print(f"  FAILED {panel['dashboard']}/{panel['panel']} [{layout}]: {str(e).splitlines()[0][:200]}")
                continue
            planned.append((layout, panel, ids))

    stats = fetch_query_stats(ch, [qid for _, _, ids in planned for qid in ids])
    rows = []
    for layout, panel, ids in planned:
        measured = [stats[q] for q in ids if q in stats]
        if not measured:
            print(f"  no query_log rows for {panel['dashboard']}/{panel['panel']} [{layout}]")
            continue
        rows.append([
            run_id, run_at, layout, panel["dashboard"], panel["panel"],
            int(statistics.median(m[0] for m in measured)),
            int(statistics.median(m[1] for m in measured)),
            float(statistics.median(m[2] for m in measured)),
            int(statistics.median(m[3] for m in measured)),
        ])
    if rows:
        ch.insert("costlab.bench_queries", rows, column_names=[
            "run_id", "run_at", "layout", "dashboard", "panel",
            "read_rows", "read_bytes", "duration_ms", "result_rows"])

    if not keep_table:
        ch.command(f"DROP TABLE IF EXISTS {UNPARTITIONED_TABLE}")

    print_summary(rows)
    print(f"{run_id}: {len(rows)} rows -> costlab.bench_queries ({failures} failed queries)")
    return run_id, rows


def print_summary(rows):
    by_key = {}
    for r in rows:
        by_key.setdefault((r[3], r[4]), {})[r[2]] = r
    print(f"{'panel':<48} {'unpart MB':>10} {'part MB':>10} {'read %':>7} {'unpart ms':>10} {'part ms':>8}")
    totals = {layout: 0 for layout in LAYOUTS}
    for (dash, panel), by_layout in sorted(by_key.items()):
        u, p = by_layout.get("unpartitioned"), by_layout.get("partitioned")
        if not (u and p):
            continue
        totals["unpartitioned"] += u[6]
        totals["partitioned"] += p[6]
        pct = 100.0 * p[6] / u[6] if u[6] else 0.0
        print(f"{(dash + '/' + panel)[:48]:<48} {u[6] / 1e6:>10.1f} {p[6] / 1e6:>10.1f} {pct:>6.1f}% "
              f"{u[7]:>10.1f} {p[7]:>8.1f}")
    if totals["unpartitioned"]:
        print(f"{'total':<48} {totals['unpartitioned'] / 1e6:>10.1f} {totals['partitioned'] / 1e6:>10.1f} "
              f"{100.0 * totals['partitioned'] / totals['unpartitioned']:>6.1f}%")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="costlab bench-scan", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_arguments(parser)
    args = parser.parse_args(argv)
    run(runs=args.runs, keep_table=args.keep_table)


def add_arguments(parser):
    parser.add_argument("--runs", type=int, default=5, help="timed runs per query and layout (median is kept)")
    parser.add_argument("--drop-table", dest="keep_table", action="store_false",
                        help=f"drop {UNPARTITIONED_TABLE} afterwards (default: keep it for the cost dashboard)")


if __name__ == "__main__":
    main()
