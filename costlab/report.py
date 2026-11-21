"""The whole cost story in one command: measure, model, write reports/cost_report.md.

    python -m costlab report               # bench-scan + bench-dbt + simulate + markdown
    python -m costlab report --skip-dbt    # reuse the latest dbt runs in costlab.dbt_runs

bench-dbt does a full refresh of analytics.* - stop the transformer container
first (docker compose stop transformer) or use --skip-dbt.
"""
import argparse
from datetime import datetime, timezone

from costlab import bench_dbt, bench_scan, config, simulate, top_queries, workload


def fmt_bytes(n):
    for unit, size in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if abs(n) >= size:
            return f"{n / size:,.2f} {unit}"
    return f"{n:,.0f} B"


def fmt_usd(x):
    return f"${x:,.2f}"


def fmt_duration(seconds):
    if seconds < 120:
        return f"{seconds:,.1f} s"
    if seconds < 7200:
        return f"{seconds / 60:,.1f} min"
    return f"{seconds / 3600:,.2f} h"


def pct(part, whole):
    return f"{100.0 * part / whole:.1f}%" if whole else "n/a"


def changed_at(results, key):
    """Index of the first step that changes `key`, or None."""
    for i in range(1, len(results)):
        if results[i][0][key] != results[i - 1][0][key]:
            return i
    return None


def fact_table_panels(dashboards):
    """{(dashboard, panel)} whose SQL reads analytics.fct_events."""
    return {(p["dashboard"], p["panel"]) for p in bench_scan.load_panels(dashboards)
            if bench_scan.rewrite_fact_table(p["sql"]) != p["sql"]}


def render(sim, fact_panels, top=None, now=None):
    """Markdown for the report. Everything comes from simulate.run()'s result."""
    now = now or datetime.now(timezone.utc)
    wl, results, panels, dbt_runs = sim["workload"], sim["results"], sim["panels"], sim["dbt_runs"]
    totals = [(step, sum(w["usd"] for w in ws), sum(w["bytes_scanned"] for w in ws)) for step, ws in results]
    scan_totals = [sum(w["scan_usd"] for w in ws) for _, ws in results]
    base_usd, final_usd = totals[0][1], totals[-1][1]
    live_usd = sum(w["usd"] for w in results[0][1] if w["always_on"])
    lines = []
    add = lines.append

    add("# Warehouse cost report")
    add("")
    add(f"Generated {now:%Y-%m-%d %H:%M} UTC by `python -m costlab report` "
        f"(bench-scan `{sim['bench_run_id']}`, simulate `{sim['run_id']}`).")
    add("")
    add("ClickHouse on a laptop stands in for Snowflake here, so be clear about what is what:")
    add("")
    add("- **Measured** (ClickHouse `system.query_log`): rows and bytes every query read, query run times, "
        "and the run time, bytes read and bytes written of real `dbt run`s.")
    add("- **Modeled** (`costlab/workload.yml` + `costlab/billing.py`): how many times a day each of those "
        "queries runs, warehouse sizes, auto-suspend and Snowflake's credit billing "
        "(per second, 60 s minimum per resume). Credits and dollars are modeled, not billed by anyone.")
    add("")

    add("## The bill, lever by lever")
    add("")
    add("| Step | Change | Bytes scanned/day | Modeled USD/day | vs previous | vs baseline | Scan-priced USD/day | vs baseline |")
    add("|---:|---|---:|---:|---:|---:|---:|---:|")
    prev = None
    for (step, usd, scanned), scan_usd in zip(totals, scan_totals):
        delta = "" if prev is None else f"{'-' if usd < prev else '+'}{fmt_usd(abs(usd - prev))}"
        add(f"| {step['step']} | **{step['name']}**: {step['change']} | {fmt_bytes(scanned)} | {fmt_usd(usd)} | "
            f"{delta} | {pct(usd - base_usd, base_usd) if step['step'] else ''} | {fmt_usd(scan_usd)} | "
            f"{pct(scan_usd - scan_totals[0], scan_totals[0]) if step['step'] else ''} |")
        prev = usd
    add("")
    add("*Modeled USD* is Snowflake-style (billed warehouse time). *Scan-priced USD* is the same day at "
        f"{fmt_usd(wl['pricing'].get('usd_per_tib_scanned', 0))}/TiB scanned (BigQuery on-demand style), "
        "reporting + transform only.")
    add("")
    saving = base_usd - final_usd
    add(f"Modeled bill: **{fmt_usd(base_usd)}/day -> {fmt_usd(final_usd)}/day**, "
        f"a {pct(saving, base_usd)} cut (about {fmt_usd(saving * 30)} a month). "
        f"Bytes scanned per day: {fmt_bytes(totals[0][2])} -> {fmt_bytes(totals[-1][2])} "
        f"({pct(totals[-1][2], totals[0][2])} of baseline).")
    add("")
    add(f"The always-on live dashboard warehouse costs {fmt_usd(live_usd)}/day in every step - no lever touches it - "
        f"so it caps the achievable saving. Without it the cut would be "
        f"{pct(saving, base_usd - live_usd)}.")
    add("")
    add("```")
    width = 50
    for step, usd, _ in totals:
        bar = "#" * max(1, round(width * usd / base_usd)) if base_usd else ""
        add(f"{step['step']} {step['name'][:20]:<20} {bar:<{width}} {fmt_usd(usd):>10}")
    add("```")
    add("")

    add("Snowflake bills warehouse *time*, not bytes. A lever that cuts bytes scanned saves money only to the extent "
        "it shortens the time a warehouse is up; when queries take seconds, most of the billed time is the idle "
        "tail before auto-suspend and the 60 s minimums, so the byte savings of levers 2 and 3 show up far more in "
        "the bytes column than in dollars. Raise `duration_scale` in workload.yml to see the same day with "
        "slower, cloud-like query times.")
    add("")
    steps_scan = ", ".join(
        f"{results[i][0]['name']} {pct(scan_totals[i] - scan_totals[i - 1], scan_totals[0])}"
        for i in range(1, len(results)))
    biggest = max(range(1, len(results)), key=lambda i: scan_totals[i - 1] - scan_totals[i])
    add(f"Priced per byte scanned the picture changes ({fmt_usd(scan_totals[0])}/day -> "
        f"{fmt_usd(scan_totals[-1])}/day, {pct(scan_totals[0] - scan_totals[-1], scan_totals[0])} lower). "
        f"Each step's share of the baseline: {steps_scan}. Auto-suspend can't save anything there; "
        f"the biggest step is **{results[biggest][0]['name']}**. Which lever matters depends on how you're "
        "billed - which is the point of measuring before cutting.")
    add("")
    add("### Per warehouse")
    add("")
    add("| Step | Warehouse | Size | Queries/day | Bytes/day | Busy | Billed | Resumes | Credits | USD/day |")
    add("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for step, ws in results:
        for w in ws:
            resumes = "always on" if w["always_on"] else f"{w['resumes']}"
            add(f"| {step['step']} | {w['warehouse']} | {w['size']} | {w['queries']:,} | {fmt_bytes(w['bytes_scanned'])} | "
                f"{fmt_duration(w['busy_seconds'])} | {w['billed_seconds'] / 3600:,.2f} h | {resumes} | "
                f"{w['credits']:,.2f} | {fmt_usd(w['usd'])} |")
    add("")
    add("Busy = time at least one query was running (measured durations x the day's schedule). "
        "Billed = what Snowflake would charge for, including the idle tail before auto-suspend and the 60 s minimums.")
    add("")

    # Lever 1
    sched = wl["schedules"]
    base_step = results[0][0]
    after_step = results[changed_at(results, "dashboard_schedule") or 0][0]
    before_n = len(workload.expand_schedule(sched[base_step["dashboard_schedule"]]))
    after_n = len(workload.expand_schedule(sched[after_step["dashboard_schedule"]]))
    add("## 1. Dashboards refreshing every 15 minutes")
    add("")
    add(f"Before: each BI dashboard refreshes {before_n} times a day (`{base_step['dashboard_schedule']}`). "
        f"After: {after_n} times a day (`{after_step['dashboard_schedule']}`). "
        "Each refresh runs every panel; bytes per refresh are measured (bench-scan, unpartitioned layout, as in the baseline).")
    add("")
    add("| Dashboard | Panels | Bytes per refresh | Bytes/day before | Bytes/day after |")
    add("|---|---:|---:|---:|---:|")
    for dash in wl["bi_dashboards"]:
        ps = [p for p in panels.get("unpartitioned", []) if p["dashboard"] == dash]
        per_refresh = sum(p["read_bytes"] for p in ps)
        add(f"| {dash} | {len(ps)} | {fmt_bytes(per_refresh)} | {fmt_bytes(per_refresh * before_n)} | "
            f"{fmt_bytes(per_refresh * after_n)} |")
    add("")
    if top and top.get("dashboards"):
        add(f"What Grafana actually ran in the last {top['hours']} h (from `query_log`, user `grafana`; "
            "refreshes = queries / distinct panels):")
        add("")
        add("| Dashboard | Panels | Refreshes/day | Bytes per refresh | Bytes/day | Bytes/day at 15 min |")
        add("|---|---:|---:|---:|---:|---:|")
        for d in top["dashboards"]:
            add(f"| {d['dashboard']} | {d['panels']} | {d['refreshes_per_day']:,.1f} | {fmt_bytes(d['bytes_per_refresh'])} | "
                f"{fmt_bytes(d['bytes_per_day_actual'])} | {fmt_bytes(d['bytes_per_day_15min'])} |")
        add("")
    else:
        add("(No Grafana dashboard queries in `query_log` for the last 24 h, so there is no observed usage table. "
            "Run `python -m costlab top-queries` after using the dashboards.)")
        add("")

    # Lever 2
    add("## 2. Partitioning and clustering `fct_events`")
    add("")
    add("Every BI panel replayed against `analytics.fct_events` (partitioned by month, ordered by "
        "`(event_date, event_type, category_l1, brand)`) and against `costlab.fct_events_unpartitioned` "
        "(same rows, no partition key, `ORDER BY tuple()`), with Grafana macros expanded for the dashboard's "
        "default time range. Medians of the timed runs, query cache off.")
    add("")
    add("| Panel | Reads fct_events | Bytes before | Bytes after | Read after/before | ms before | ms after |")
    add("|---|:---:|---:|---:|---:|---:|---:|")
    by_key = {layout: {(p["dashboard"], p["panel"]): p for p in ps} for layout, ps in panels.items()}
    fact_before = fact_after = 0
    for key in sorted(by_key.get("unpartitioned", {})):
        u, p = by_key["unpartitioned"][key], by_key["partitioned"][key]
        is_fact = key in fact_panels
        if is_fact:
            fact_before += u["read_bytes"]
            fact_after += p["read_bytes"]
        add(f"| {key[0]} / {key[1]} | {'yes' if is_fact else ''} | {fmt_bytes(u['read_bytes'])} | "
            f"{fmt_bytes(p['read_bytes'])} | {pct(p['read_bytes'], u['read_bytes'])} | "
            f"{u['duration_s'] * 1000:,.1f} | {p['duration_s'] * 1000:,.1f} |")
    add("")
    add(f"Panels on `fct_events` read **{pct(fact_after, fact_before)}** of the bytes they read before "
        f"({fmt_bytes(fact_before)} -> {fmt_bytes(fact_after)} per refresh of all dashboards).")
    add("")
    bi = set(wl["bi_dashboards"])
    per_refresh = {layout: sum(p["read_bytes"] for p in ps if p["dashboard"] in bi) for layout, ps in panels.items()}
    if per_refresh.get("unpartitioned"):
        add(f"In the waterfall this lever comes after the refresh schedule fix, so it only acts on {after_n} "
            f"refreshes a day. On the baseline {before_n}-a-day schedule the same change would take the BI "
            f"dashboards from {fmt_bytes(per_refresh['unpartitioned'] * before_n)} to "
            f"{fmt_bytes(per_refresh['partitioned'] * before_n)} scanned per day.")
        add("")

    # Lever 3
    add("## 3. Incremental dbt")
    add("")
    add("Two real `dbt run`s, logged in `costlab.dbt_runs`; bytes come from `query_log` rows tagged with the run id. "
        "Warehouse busy = first query start to last query end (dbt's own start-up time is not warehouse time); "
        "that is what the model bills for each hourly run.")
    add("")
    add("| Mode | Run | Wall clock | Warehouse busy | Queries | Rows read | Bytes read | Rows written | Bytes written |")
    add("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for mode in ("full-refresh", "incremental"):
        r = dbt_runs[mode]
        add(f"| {mode} | `{r['run_id']}` | {r['wall_s']:.1f} s | {r['busy_s']:.1f} s | {r['query_count']} | "
            f"{r['read_rows']:,} | {fmt_bytes(r['read_bytes'])} | {r['written_rows']:,} | {fmt_bytes(r['written_bytes'])} |")
    full, inc = dbt_runs["full-refresh"], dbt_runs["incremental"]
    add("")
    add(f"Incremental reads {pct(inc['read_bytes'], full['read_bytes'])} of the bytes and keeps the warehouse busy "
        f"{pct(inc['busy_s'], full['busy_s'])} as long as a full rebuild. How much new data the incremental run had "
        "to process depends on the stream rate during the benchmark; the aggregate models recompute whole days, "
        "so even a quiet incremental run re-reads today's events.")
    add("")

    # Lever 4
    i = changed_at(results, "auto_suspend_seconds") or 0
    s3, s4 = results[max(i - 1, 0)], results[i]
    add("## 4. Auto-suspend 10 min -> 60 s")
    add("")
    add("| Warehouse | Billed before | Billed after | Resumes before | Resumes after | USD/day before | USD/day after |")
    add("|---|---:|---:|---:|---:|---:|---:|")
    for before, after in zip(s3[1], s4[1]):
        if before["always_on"]:
            continue
        add(f"| {before['warehouse']} | {before['billed_seconds'] / 3600:,.2f} h | {after['billed_seconds'] / 3600:,.2f} h | "
            f"{before['resumes']} | {after['resumes']} | {fmt_usd(before['usd'])} | {fmt_usd(after['usd'])} |")
    add("")
    add("With queries this short, most of what's billed is idle time waiting to suspend; a shorter auto-suspend "
        "means more resumes, each billed at least 60 s.")
    add("")

    add("## Assumptions (all in `costlab/workload.yml`)")
    add("")
    pricing = wl["pricing"]
    add(f"- Price: {fmt_usd(pricing['usd_per_credit'])} per credit; per-second billing with a "
        f"{pricing['min_billed_seconds']} s minimum per resume; credits/hour XS=1, S=2, M=4, L=8, XL=16.")
    for name, wh in wl["warehouses"].items():
        add(f"- Warehouse `{name}`: {wh['size']}{', always on' if wh.get('always_on') else ''}.")
    add(f"- BI dashboards: {', '.join(wl['bi_dashboards'])}; each refresh runs all panels at the same moment.")
    for name, sch in sched.items():
        when = ", ".join(sch["at"]) if "at" in sch else f"every {sch['every_minutes']} min {sch.get('start', '00:00')}-{sch.get('end', '24:00')}"
        add(f"- Schedule `{name}`: {when} ({len(workload.expand_schedule(sch))} refreshes/day).")
    aq = wl["analyst_queries"]
    add(f"- Analysts: {aq['per_day']} ad-hoc queries a day between {aq['working_hours'][0]} and {aq['working_hours'][1]} "
        f"(seed {aq['seed']}), each one a random measured BI panel query. Same in every step.")
    add(f"- dbt: one run every {wl['dbt']['every_minutes']} min at :{wl['dbt'].get('offset_minutes', 0):02d}, "
        "with the measured full-refresh or incremental cost.")
    add(f"- Auto-suspend: {base_step['auto_suspend_seconds']} s before, {results[-1][0]['auto_suspend_seconds']} s after "
        "(reporting and transform).")
    add(f"- Duration scale: {wl.get('duration_scale', 1.0)} (measured laptop durations are used as they are; "
        "a cloud warehouse would be slower, which makes the idle-tail and minimum-billing effects relatively smaller).")
    add("- The live dashboard warehouse is billed as always on; its bytes are not modeled.")
    add("- Days are billed as a 24 h window: a warehouse session is cut at midnight.")
    add("")
    return "\n".join(lines) + "\n"


def run(skip_dbt=False, runs=5, wait=0):
    wl = workload.load()
    print("== bench-scan")
    bench_scan.run(runs=runs)
    if not skip_dbt:
        print("== bench-dbt")
        bench_dbt.run(wait)
    print("== top-queries")
    try:
        top = top_queries.run(hours=24, limit=10)
        # only dashboards that are provisioned from grafana/dashboards (not ad-hoc test ones)
        known = {p.stem for p in config.DASHBOARDS_DIR.glob("*.json")}
        top["dashboards"] = [d for d in top["dashboards"] if d["dashboard"] in known]
    except Exception as e:  # the report is still useful without it
        print(f"top-queries failed, leaving it out: {e}")
        top = None
    print("== simulate")
    sim = simulate.run()

    config.REPORTS_DIR.mkdir(exist_ok=True)
    path = config.REPORTS_DIR / "cost_report.md"
    path.write_text(render(sim, fact_table_panels(wl["bi_dashboards"]), top), encoding="utf-8")
    print(f"wrote {path}")
    return path


def add_arguments(parser):
    parser.add_argument("--skip-dbt", action="store_true", help="don't run bench-dbt, use the latest dbt runs")
    parser.add_argument("--runs", type=int, default=5, help="bench-scan timed runs per query")
    parser.add_argument("--wait", type=int, default=0, help="bench-dbt: seconds between full refresh and incremental")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="costlab report", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_arguments(parser)
    args = parser.parse_args(argv)
    run(args.skip_dbt, args.runs, args.wait)


if __name__ == "__main__":
    main()
