"""python -m costlab <command>

    top-queries   which query patterns / dashboards spend the bytes (lever 1)
    bench-scan    partitioned vs unpartitioned fact table, per BI panel (lever 2)
    bench-dbt     dbt full refresh vs incremental run (lever 3)
    simulate      model the daily bill for every scenario step (all levers)
    report        bench-scan + bench-dbt + simulate -> reports/cost_report.md

The dbt loop the transformer container runs is `python -m costlab.dbt_runner`.
"""
import argparse

from costlab import bench_dbt, bench_scan, report, simulate, top_queries


def main():
    parser = argparse.ArgumentParser(prog="python -m costlab", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True, metavar="command")

    p = sub.add_parser("top-queries", help="top query patterns and dashboard refresh cost from query_log")
    top_queries.add_arguments(p)
    p = sub.add_parser("bench-scan", help="replay BI panels on partitioned vs unpartitioned fct_events")
    bench_scan.add_arguments(p)
    p = sub.add_parser("bench-dbt", help="dbt full refresh vs incremental run")
    bench_dbt.add_arguments(p)
    sub.add_parser("simulate", help="model the daily bill per scenario step")
    p = sub.add_parser("report", help="run the benchmarks + simulation and write reports/cost_report.md")
    report.add_arguments(p)

    args = parser.parse_args()
    if args.command == "top-queries":
        top_queries.run(args.hours, args.limit)
    elif args.command == "bench-scan":
        bench_scan.run(runs=args.runs, keep_table=args.keep_table)
    elif args.command == "bench-dbt":
        bench_dbt.run(args.wait)
    elif args.command == "simulate":
        simulate.run()
    elif args.command == "report":
        report.run(args.skip_dbt, args.runs, args.wait)


if __name__ == "__main__":
    main()
