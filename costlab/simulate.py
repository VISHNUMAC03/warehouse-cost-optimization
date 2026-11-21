"""Model the daily warehouse bill for each scenario step.

Inputs are measured: per-panel bytes and run time from the latest bench-scan
run (costlab.bench_queries) and dbt run cost from the latest successful
full-refresh and incremental runs (costlab.dbt_runs, preferring bench runs).
workload.yml says how often each of those happens in a day; billing.py turns
that into Snowflake-style credits. Results go to costlab.scenario_costs.

    python -m costlab simulate
"""
import argparse
from datetime import datetime, timezone

from costlab import billing, config, dbt_runner, workload


def latest_bench(ch):
    """(run_id, {layout: [panel dicts]}) from the newest bench-scan run.

    Only panels measured on both layouts are kept, so every step uses the same panel set.
    """
    rows = ch.query("""
        SELECT run_id, layout, dashboard, panel, read_rows, read_bytes, duration_ms
        FROM costlab.bench_queries
        WHERE run_id = (SELECT argMax(run_id, run_at) FROM costlab.bench_queries)
    """).result_rows
    if not rows:
        raise SystemExit("costlab.bench_queries is empty - run `python -m costlab bench-scan` first")
    panels = {}
    for run_id, layout, dashboard, panel, read_rows, read_bytes, duration_ms in rows:
        panels.setdefault(layout, {})[(dashboard, panel)] = {
            "dashboard": dashboard, "panel": panel, "read_rows": read_rows,
            "read_bytes": read_bytes, "duration_s": duration_ms / 1000.0,
        }
    common = set.intersection(*(set(p) for p in panels.values()))
    return rows[0][0], {layout: [p[k] for k in sorted(common)] for layout, p in panels.items()}


def latest_dbt_runs(ch):
    """{mode: run dict} - newest successful run per mode, bench runs first.

    busy_s (first query start to last query end, from query_log) is what the
    transform warehouse is billed for; falls back to the wall-clock duration
    if the query_log rows have expired.
    """
    out = {}
    for mode in ("full-refresh", "incremental"):
        rows = ch.query("""
            SELECT run_id, started_at, triggered_by, duration_s, query_count,
                   read_rows, read_bytes, written_rows, written_bytes
            FROM costlab.dbt_runs
            WHERE status = 'success' AND mode = {mode:String} AND query_count > 0
            ORDER BY triggered_by = 'bench' DESC, started_at DESC
            LIMIT 1
        """, parameters={"mode": mode}).named_results()
        rows = list(rows)
        if not rows:
            raise SystemExit(f"no successful {mode} dbt run in costlab.dbt_runs - "
                             f"run `python -m costlab bench-dbt` first")
        run = rows[0]
        stats = dbt_runner.query_log_stats(ch, run["run_id"], run["started_at"])
        run["busy_s"] = stats["busy_s"] if stats["query_count"] else run["duration_s"]
        run["wall_s"] = run["duration_s"]
        run["duration_s"] = run["busy_s"]   # what workload.build_day bills
        out[mode] = run
    return out


def price_step(wl, step, day):
    """Bill one simulated day: one dict per warehouse."""
    pricing = wl["pricing"]
    out = []
    for name, wh in wl["warehouses"].items():
        queries = day.get(name, [])
        bill = billing.simulate(
            [(q.start_s, q.duration_s) for q in queries],
            auto_suspend_s=step["auto_suspend_seconds"],
            min_billed_s=pricing["min_billed_seconds"],
            always_on=wh.get("always_on", False),
        )
        credits = billing.credits(bill.billed_seconds, wh["size"])
        scanned = sum(q.read_bytes for q in queries)
        out.append({
            "warehouse": name,
            "size": wh["size"],
            "always_on": bool(wh.get("always_on", False)),
            "queries": len(queries),
            "bytes_scanned": scanned,
            "busy_seconds": bill.busy_seconds,
            "billed_seconds": bill.billed_seconds,
            "resumes": bill.resumes,
            "credits": credits,
            "usd": credits * pricing["usd_per_credit"],
            "scan_usd": scanned / 2**40 * pricing.get("usd_per_tib_scanned", 0.0),
        })
    return out


def simulate_all(wl, panels, dbt_runs):
    """[(step, [warehouse dicts]), ...] for every scenario step. Pure."""
    results = []
    for step in workload.scenario_steps(wl):
        day = workload.build_day(wl, step, panels, dbt_runs)
        results.append((step, price_step(wl, step, day)))
    return results


def run(write=True):
    wl = workload.load()
    ch = config.clickhouse()
    bench_run_id, panels = latest_bench(ch)
    dbt_runs = latest_dbt_runs(ch)
    results = simulate_all(wl, panels, dbt_runs)

    run_id = "simulate-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_at = datetime.now(timezone.utc).replace(microsecond=0)
    if write:
        rows = [[run_id, run_at, step["step"], step["name"], w["warehouse"], w["queries"], w["bytes_scanned"],
                 w["busy_seconds"], w["billed_seconds"], w["credits"], w["usd"], w["scan_usd"]]
                for step, warehouses in results for w in warehouses]
        ch.insert("costlab.scenario_costs", rows, column_names=[
            "run_id", "run_at", "step", "scenario", "warehouse", "queries", "bytes_scanned",
            "busy_seconds", "billed_seconds", "credits", "usd", "scan_usd"])

    print(f"Modeled daily bill (bench-scan {bench_run_id}; dbt runs "
          f"{dbt_runs['full-refresh']['run_id']}, {dbt_runs['incremental']['run_id']})")
    print(f"{'step':<4} {'scenario':<20} {'queries':>8} {'GB scanned':>11} {'billed h':>9} {'credits':>8} "
          f"{'USD/day':>9} {'scan USD':>9}")
    for step, warehouses in results:
        print(f"{step['step']:<4} {step['name'][:20]:<20} {sum(w['queries'] for w in warehouses):>8,} "
              f"{sum(w['bytes_scanned'] for w in warehouses) / 1e9:>11.2f} "
              f"{sum(w['billed_seconds'] for w in warehouses) / 3600:>9.2f} "
              f"{sum(w['credits'] for w in warehouses):>8.2f} {sum(w['usd'] for w in warehouses):>9.2f} "
              f"{sum(w['scan_usd'] for w in warehouses):>9.2f}")
    if write:
        print(f"{run_id}: {len(results)} steps -> costlab.scenario_costs")
    return {"run_id": run_id, "bench_run_id": bench_run_id, "panels": panels,
            "dbt_runs": dbt_runs, "results": results, "workload": wl}


def main(argv=None):
    parser = argparse.ArgumentParser(prog="costlab simulate", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args(argv)
    run()


if __name__ == "__main__":
    main()
