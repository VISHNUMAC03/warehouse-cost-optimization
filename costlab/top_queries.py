"""Lever 1, discovery: which queries are spending the bytes?

Reads system.query_log for the last N hours and prints
  - the top query patterns (normalized_query_hash) by bytes read, per user
    and per Grafana dashboard (from the /* grafana dashboard=... */ tag);
  - per Grafana dashboard: bytes per refresh, how often it actually refreshed,
    and what a 15-minute 24/7 refresh schedule would scan per day.

    python -m costlab top-queries
    python -m costlab top-queries --hours 6 --limit 20
"""
import argparse

from costlab import config

DASHBOARD_RE = "dashboard=([a-z0-9-]+)"
PANEL_RE = "panel=([a-z0-9-]+)"


def ascii(text):
    return str(text).encode("ascii", "replace").decode("ascii")


def top_patterns(ch, hours, limit):
    return list(ch.query(
        f"""
        SELECT user,
               extract(query, '{DASHBOARD_RE}') AS dashboard,
               toString(normalized_query_hash) AS pattern,
               count() AS queries,
               sum(read_rows) AS read_rows,
               sum(read_bytes) AS read_bytes,
               sum(query_duration_ms) / 1000.0 AS seconds,
               any(substring(replaceRegexpAll(query, '\\\\s+', ' '), 1, 70)) AS sample
        FROM system.query_log
        WHERE event_date >= toDate(now() - INTERVAL {{hours:UInt32}} HOUR)
          AND event_time >= now() - INTERVAL {{hours:UInt32}} HOUR
          AND type = 'QueryFinish'
        GROUP BY user, dashboard, pattern
        ORDER BY read_bytes DESC
        LIMIT {{limit:UInt32}}
        """,
        parameters={"hours": hours, "limit": limit},
    ).named_results())


def dashboard_refreshes(ch, hours):
    """Per Grafana dashboard: queries, panels, refreshes and bytes in the window."""
    rows = ch.query(
        f"""
        SELECT extract(query, '{DASHBOARD_RE}') AS dashboard,
               count() AS queries,
               uniqExact(extract(query, '{PANEL_RE}')) AS panels,
               sum(read_bytes) AS read_bytes
        FROM system.query_log
        WHERE event_date >= toDate(now() - INTERVAL {{hours:UInt32}} HOUR)
          AND event_time >= now() - INTERVAL {{hours:UInt32}} HOUR
          AND type = 'QueryFinish' AND user = 'grafana' AND dashboard != ''
        GROUP BY dashboard
        ORDER BY read_bytes DESC
        """,
        parameters={"hours": hours},
    ).named_results()
    out = []
    for r in rows:
        refreshes = r["queries"] / max(r["panels"], 1)
        per_refresh = r["read_bytes"] / refreshes if refreshes else 0
        out.append(dict(
            r,
            refreshes=refreshes,
            bytes_per_refresh=per_refresh,
            refreshes_per_day=refreshes * 24.0 / hours,
            bytes_per_day_actual=r["read_bytes"] * 24.0 / hours,
            bytes_per_day_15min=per_refresh * 96,
        ))
    return out


def run(hours=24, limit=15):
    ch = config.clickhouse()
    ch.command("SYSTEM FLUSH LOGS")
    patterns = top_patterns(ch, hours, limit)
    dashboards = dashboard_refreshes(ch, hours)

    print(f"Top {limit} query patterns by bytes read, last {hours}h (system.query_log)")
    print(f"{'user':<9} {'dashboard':<20} {'queries':>8} {'MB read':>10} {'sec':>8}  sample")
    for r in patterns:
        print(f"{ascii(r['user'])[:9]:<9} {ascii(r['dashboard'] or '-')[:20]:<20} {r['queries']:>8,} "
              f"{r['read_bytes'] / 1e6:>10.1f} {r['seconds']:>8.1f}  {ascii(r['sample'])}")

    print()
    if not dashboards:
        print("No Grafana dashboard queries in the window - open some dashboards first.")
    else:
        print(f"Grafana dashboards, last {hours}h")
        print(f"{'dashboard':<22} {'panels':>6} {'refreshes':>9} {'MB/refresh':>10} {'refresh/day':>11} "
              f"{'MB/day now':>10} {'MB/day @15min':>13}")
        for d in dashboards:
            print(f"{ascii(d['dashboard'])[:22]:<22} {d['panels']:>6} {d['refreshes']:>9.0f} "
                  f"{d['bytes_per_refresh'] / 1e6:>10.1f} {d['refreshes_per_day']:>11.1f} "
                  f"{d['bytes_per_day_actual'] / 1e6:>10.1f} {d['bytes_per_day_15min'] / 1e6:>13.1f}")
        print("(refreshes = queries / distinct panels; @15min = bytes per refresh x 96 refreshes a day)")
    return {"hours": hours, "patterns": patterns, "dashboards": dashboards}


def add_arguments(parser):
    parser.add_argument("--hours", type=int, default=24, help="look back this many hours (default 24)")
    parser.add_argument("--limit", type=int, default=15, help="how many query patterns to show")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="costlab top-queries", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_arguments(parser)
    args = parser.parse_args(argv)
    run(args.hours, args.limit)


if __name__ == "__main__":
    main()
