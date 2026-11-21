"""Lever 3: full rebuild vs incremental dbt run.

Runs `dbt run --full-refresh` and then a plain incremental `dbt run` through
dbt_runner (triggered_by='bench'), so both land in costlab.dbt_runs with the
bytes they read and wrote according to system.query_log.

With the producer + consumer running, --wait gives the stream time to land
new rows before the incremental run, so it has real work to do.

Note: the full refresh rebuilds everything in analytics.* (it is left fully
built afterwards). Stop the transformer container first, or its runs will
overlap with the benchmark.

    python -m costlab bench-dbt
    python -m costlab bench-dbt --wait 120
"""
import argparse
import subprocess
import sys
import time

from costlab import config, dbt_runner


def transformer_running():
    """True/False if docker can tell us, None if docker isn't available (e.g. inside a container)."""
    try:
        out = subprocess.run(["docker", "compose", "ps", "--status", "running", "--services"],
                             cwd=str(config.PROJECT_DIR), capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return "transformer" in out.stdout.split()


def recent_dbt_queries(ch):
    ch.command("SYSTEM FLUSH LOGS")
    return ch.command("""
        SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 1 MINUTE AND user = 'dbt'
    """)


def pending_rows(ch):
    """raw.events rows that the next incremental run will pick up."""
    return ch.command("""
        SELECT count() FROM raw.events
        WHERE _loaded_at > (SELECT max(_loaded_at) FROM analytics.fct_events)
    """)


def run(wait_s=0):
    if transformer_running():
        print("WARNING: the transformer container is running - its dbt runs will overlap with this benchmark.\n"
              "         Stop it first: docker compose stop transformer")
    elif recent_dbt_queries(config.clickhouse()):
        print("WARNING: user dbt ran queries in the last minute - another dbt run may be in progress.")

    print("full refresh ...", flush=True)
    full = dbt_runner.run_dbt(full_refresh=True, triggered_by="bench")
    print("  " + dbt_runner.describe(full))
    if full["status"] != "success":
        raise SystemExit("full refresh failed, not running the incremental step")

    if wait_s:
        print(f"waiting {wait_s}s for new stream rows ...", flush=True)
        time.sleep(wait_s)
    new_rows = pending_rows(config.clickhouse())
    print(f"incremental run ({new_rows:,} new raw rows to pick up) ...", flush=True)
    inc = dbt_runner.run_dbt(full_refresh=False, triggered_by="bench")
    inc["new_raw_rows"] = new_rows
    print("  " + dbt_runner.describe(inc))
    if inc["status"] != "success":
        raise SystemExit("incremental run failed")

    def ratio(key):
        return f"{100.0 * inc[key] / full[key]:.1f}%" if full[key] else "n/a"
    print(f"incremental vs full refresh: duration {ratio('duration_s')}, warehouse busy {ratio('busy_s')}, "
          f"bytes read {ratio('read_bytes')}, bytes written {ratio('written_bytes')}")
    return full, inc


def add_arguments(parser):
    parser.add_argument("--wait", type=int, default=0,
                        help="seconds to wait between the full refresh and the incremental run")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="costlab bench-dbt", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_arguments(parser)
    args = parser.parse_args(argv)
    run(args.wait)


if __name__ == "__main__":
    sys.exit(main())
