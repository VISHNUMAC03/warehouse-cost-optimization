"""Run `dbt run` and log what it cost to costlab.dbt_runs.

Each run gets a run_id that dbt puts in the log_comment of every query it
sends (the profile reads it from DBT_RUN_TAG), so afterwards the rows/bytes
it read and wrote can be summed straight from system.query_log.

The transformer container runs this in a loop as ClickHouse user `dbt`;
bench-dbt reuses run_dbt() for a full-refresh vs incremental comparison.

    python -m costlab.dbt_runner                    # one incremental run
    python -m costlab.dbt_runner --full-refresh
    python -m costlab.dbt_runner --every 300        # forever, one run every 5 min
    python -m costlab.dbt_runner --select fct_events+
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from costlab import config


def dbt_executable():
    # prefer the dbt installed next to this interpreter (venv Scripts/ on Windows, bin/ in the image)
    here = Path(sys.executable).parent
    for name in ("dbt.exe", "dbt"):
        if (here / name).exists():
            return str(here / name)
    return shutil.which("dbt") or "dbt"


def query_log_stats(ch, run_id, since):
    """Sum the query_log rows tagged with run_id. `since` (a datetime) only prunes the scan.

    busy_s is the span from the first query start to the last query end - the time a
    warehouse would actually be running for this dbt run (dbt's own parse/startup
    time on the client is not warehouse time).
    """
    if since.tzinfo is None:  # ClickHouse hands back naive UTC datetimes
        since = since.replace(tzinfo=timezone.utc)
    row = ch.query(
        """
        SELECT count(), sum(read_rows), sum(read_bytes), sum(written_rows), sum(written_bytes),
               (max(toUnixTimestamp64Micro(event_time_microseconds))
                - min(toUnixTimestamp64Micro(query_start_time_microseconds))) / 1e6
        FROM system.query_log
        WHERE event_date >= toDate(toDateTime({since:UInt32})) - 1
          AND event_time >= toDateTime({since:UInt32}) - INTERVAL 1 MINUTE
          AND log_comment = {run_id:String}
          AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
        """,
        parameters={"run_id": run_id, "since": int(since.timestamp())},
    ).result_rows[0]
    count, read_rows, read_bytes, written_rows, written_bytes, busy_s = row
    return {
        "query_count": int(count),
        "read_rows": int(read_rows),
        "read_bytes": int(read_bytes),
        "written_rows": int(written_rows),
        "written_bytes": int(written_bytes),
        "busy_s": float(busy_s) if count else 0.0,
    }


def run_dbt(full_refresh=False, triggered_by="transformer", select=None, verbose=False):
    """One `dbt run`; returns the costlab.dbt_runs row as a dict (plus busy_s and output)."""
    mode = "full-refresh" if full_refresh else "incremental"
    started_at = datetime.now(timezone.utc)
    run_id = f"dbt-{mode}-{started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"

    cmd = [dbt_executable(), "run", "--no-use-colors",
           "--project-dir", str(config.DBT_DIR), "--profiles-dir", str(config.DBT_DIR)]
    if full_refresh:
        cmd.append("--full-refresh")
    if select:
        cmd += ["--select", *select]

    env = dict(os.environ, DBT_RUN_TAG=run_id)
    env.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")

    t0 = time.monotonic()
    try:
        proc = subprocess.run(cmd, env=env, cwd=str(config.PROJECT_DIR), capture_output=True,
                              text=True, encoding="utf-8", errors="replace")
        ok, output = proc.returncode == 0, proc.stdout + proc.stderr
    except OSError as e:
        ok, output = False, f"could not start dbt: {e}"
    duration_s = time.monotonic() - t0
    finished_at = datetime.now(timezone.utc)

    if verbose or not ok:
        lines = output.strip().splitlines()
        shown = lines if verbose else lines[-25:]
        print("\n".join(shown).encode("ascii", "replace").decode("ascii"))

    # fresh client: a keep-alive connection opened before a long dbt run would have gone stale
    ch = config.clickhouse()
    ch.command("SYSTEM FLUSH LOGS")
    stats = query_log_stats(ch, run_id, started_at)

    row = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "mode": mode,
        "triggered_by": triggered_by,
        "status": "success" if ok else "error",
        "query_count": stats["query_count"],
        "read_rows": stats["read_rows"],
        "read_bytes": stats["read_bytes"],
        "written_rows": stats["written_rows"],
        "written_bytes": stats["written_bytes"],
        "duration_s": duration_s,
    }
    ch.insert("costlab.dbt_runs", [list(row.values())], column_names=list(row))
    return dict(row, busy_s=stats["busy_s"], output=output)


def describe(run):
    return (f"{run['started_at']:%Y-%m-%d %H:%M:%S} {run['run_id']} {run['status']} "
            f"{run['duration_s']:.1f}s (warehouse busy {run['busy_s']:.1f}s) {run['query_count']} queries, "
            f"read {run['read_rows']:,} rows / {run['read_bytes'] / 1e6:.1f} MB, "
            f"wrote {run['written_rows']:,} rows / {run['written_bytes'] / 1e6:.1f} MB")


def loop(every, full_refresh=False, select=None):
    print(f"dbt runner: one run every {every}s (Ctrl+C to stop)", flush=True)
    while True:
        t0 = time.monotonic()
        try:
            print(describe(run_dbt(full_refresh, "transformer", select)), flush=True)
        except Exception as e:  # ClickHouse restarting, network blip... try again next round
            print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} run failed: {e}", flush=True)
        time.sleep(max(0.0, every - (time.monotonic() - t0)))


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m costlab.dbt_runner", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--every", type=float, help="run forever, starting a run every N seconds")
    parser.add_argument("--full-refresh", action="store_true", help="rebuild every model from scratch")
    parser.add_argument("--select", nargs="+", help="dbt node selection, passed through to dbt")
    parser.add_argument("--triggered-by", default="transformer", choices=["transformer", "bench"])
    parser.add_argument("-v", "--verbose", action="store_true", help="print dbt's output")
    args = parser.parse_args(argv)

    if args.every:
        loop(args.every, args.full_refresh, args.select)
    else:
        run = run_dbt(args.full_refresh, args.triggered_by, args.select, verbose=args.verbose)
        print(describe(run))
        if run["status"] != "success":
            sys.exit(1)


if __name__ == "__main__":
    main()
