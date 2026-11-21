"""The daily workload model: turns workload.yml plus measured query costs into
one simulated day of queries per warehouse, for each scenario step.

Pure functions; simulate.py feeds in the measurements and bills the result.

    panels   = {"partitioned": [{"dashboard":..., "panel":..., "duration_s":..., "read_bytes":...}, ...],
                "unpartitioned": [...]}
    dbt_runs = {"full-refresh": {"duration_s":..., "read_bytes":...}, "incremental": {...}}
"""
import random
from collections import namedtuple

import yaml

from costlab import config

# kind: 'dashboard' | 'analyst' | 'dbt'
Query = namedtuple("Query", "start_s duration_s read_bytes kind label")

DAY_SECONDS = 86400


def load(path=None):
    with open(path or config.WORKLOAD_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


def clock_to_seconds(text):
    """'08:30' -> 30600. '24:00' is allowed and means end of day."""
    hours, minutes = str(text).split(":")
    seconds = int(hours) * 3600 + int(minutes) * 60
    if not 0 <= seconds <= DAY_SECONDS:
        raise ValueError(f"bad time of day: {text!r}")
    return seconds


def expand_schedule(schedule):
    """Refresh start times (seconds after midnight) for one schedule entry.

    Either {"at": ["08:00", ...]} or {"every_minutes": 15, "start": "00:00", "end": "24:00"}
    (end is exclusive).
    """
    if "at" in schedule:
        return sorted(clock_to_seconds(t) for t in schedule["at"])
    step = int(schedule["every_minutes"]) * 60
    if step <= 0:
        raise ValueError("every_minutes must be positive")
    start = clock_to_seconds(schedule.get("start", "00:00"))
    end = clock_to_seconds(schedule.get("end", "24:00"))
    return list(range(start, end, step))


def dbt_start_times(dbt_cfg):
    step = int(dbt_cfg["every_minutes"]) * 60
    offset = int(dbt_cfg.get("offset_minutes", 0)) * 60
    return list(range(offset, DAY_SECONDS, step))


def analyst_plan(cfg, n_choices):
    """Deterministic ad-hoc queries: [(start_s, choice_index), ...] sorted by start.

    choice_index picks one of n_choices measured panel queries.
    """
    rng = random.Random(cfg.get("seed", 0))
    first, last = (clock_to_seconds(t) for t in cfg["working_hours"])
    plan = [(rng.uniform(first, last), rng.randrange(n_choices)) for _ in range(int(cfg["per_day"]))]
    return sorted(plan)


def scenario_steps(workload):
    """Baseline plus each lever applied on top of the previous step.

    Returns a list of dicts with step, name, change, dashboard_schedule, layout,
    dbt_mode, auto_suspend_seconds.
    """
    current = dict(workload["baseline"])
    steps = [dict(current, step=0)]
    for i, lever in enumerate(workload.get("steps", []), start=1):
        current = dict(current, **lever)
        steps.append(dict(current, step=i))
    return steps


def build_day(workload, step, panels, dbt_runs):
    """One day of queries per warehouse for a scenario step: {warehouse: [Query, ...]}."""
    scale = float(workload.get("duration_scale", 1.0))
    layout_panels = sorted(panels[step["layout"]], key=lambda p: (p["dashboard"], p["panel"]))
    day = {name: [] for name in workload["warehouses"]}

    # Every refresh fires all of a dashboard's panels at the same moment.
    refresh_times = expand_schedule(workload["schedules"][step["dashboard_schedule"]])
    for t in refresh_times:
        for p in layout_panels:
            if p["dashboard"] in workload["bi_dashboards"]:
                day["reporting"].append(Query(t, p["duration_s"] * scale, p["read_bytes"], "dashboard",
                                              f"{p['dashboard']}/{p['panel']}"))

    if layout_panels:
        for t, choice in analyst_plan(workload["analyst_queries"], len(layout_panels)):
            p = layout_panels[choice]
            day["reporting"].append(Query(t, p["duration_s"] * scale, p["read_bytes"], "analyst",
                                          f"{p['dashboard']}/{p['panel']}"))

    run = dbt_runs[step["dbt_mode"]]
    for t in dbt_start_times(workload["dbt"]):
        day["transform"].append(Query(t, run["duration_s"] * scale, run["read_bytes"], "dbt", step["dbt_mode"]))

    for queries in day.values():
        queries.sort()
    return day
