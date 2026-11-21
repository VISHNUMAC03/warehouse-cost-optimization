import pytest

from costlab import simulate, workload


def test_clock_to_seconds():
    assert workload.clock_to_seconds("00:00") == 0
    assert workload.clock_to_seconds("08:30") == 30600
    assert workload.clock_to_seconds("24:00") == 86400
    with pytest.raises(ValueError):
        workload.clock_to_seconds("25:00")


def test_every_15_min_all_day():
    times = workload.expand_schedule({"every_minutes": 15, "start": "00:00", "end": "24:00"})
    assert len(times) == 96
    assert times[:2] == [0, 900]
    assert times[-1] == 86400 - 900


def test_every_n_within_window_end_exclusive():
    assert workload.expand_schedule({"every_minutes": 60, "start": "09:00", "end": "12:00"}) == [32400, 36000, 39600]


def test_fixed_times_sorted():
    assert workload.expand_schedule({"at": ["13:00", "08:00"]}) == [28800, 46800]


def test_dbt_hourly_at_five_past():
    times = workload.dbt_start_times({"every_minutes": 60, "offset_minutes": 5})
    assert len(times) == 24
    assert times[0] == 300 and times[1] == 3900


def test_analyst_plan_is_deterministic_and_in_working_hours():
    cfg = {"per_day": 30, "working_hours": ["09:00", "18:00"], "seed": 7}
    a = workload.analyst_plan(cfg, 10)
    assert a == workload.analyst_plan(cfg, 10)
    assert len(a) == 30
    assert all(32400 <= t <= 64800 for t, _ in a)
    assert all(0 <= c < 10 for _, c in a)
    assert a == sorted(a)
    assert a != workload.analyst_plan(dict(cfg, seed=8), 10)


def test_steps_apply_cumulatively():
    steps = workload.scenario_steps(workload.load())
    assert [s["step"] for s in steps] == [0, 1, 2, 3, 4]
    base, last = steps[0], steps[-1]
    assert (base["dashboard_schedule"], base["layout"], base["dbt_mode"], base["auto_suspend_seconds"]) == \
        ("every_15_min", "unpartitioned", "full-refresh", 600)
    assert (last["dashboard_schedule"], last["layout"], last["dbt_mode"], last["auto_suspend_seconds"]) == \
        ("twice_a_day", "partitioned", "incremental", 60)
    # step 2 changes the layout but keeps step 1's schedule
    assert steps[2]["layout"] == "partitioned" and steps[2]["dashboard_schedule"] == "twice_a_day"
    assert steps[2]["dbt_mode"] == "full-refresh"


def fake_measurements():
    panels = {}
    for layout, scale in (("unpartitioned", 1.0), ("partitioned", 0.2)):
        panels[layout] = [
            {"dashboard": d, "panel": f"p{i}", "duration_s": 0.5 * scale, "read_bytes": int(1e8 * scale)}
            for d in ("sales-overview", "conversion-funnel", "product-performance") for i in range(5)
        ]
    dbt_runs = {"full-refresh": {"duration_s": 40.0, "read_bytes": int(2e9)},
                "incremental": {"duration_s": 6.0, "read_bytes": int(3e8)}}
    return panels, dbt_runs


def test_build_day_baseline_counts():
    wl = workload.load()
    panels, dbt_runs = fake_measurements()
    day = workload.build_day(wl, workload.scenario_steps(wl)[0], panels, dbt_runs)
    n_analyst = wl["analyst_queries"]["per_day"]
    assert len([q for q in day["reporting"] if q.kind == "dashboard"]) == 96 * 15
    assert len([q for q in day["reporting"] if q.kind == "analyst"]) == n_analyst
    assert len(day["transform"]) == 24
    assert all(q.duration_s == 40.0 and q.read_bytes == int(2e9) for q in day["transform"])
    assert day["live"] == []
    # every panel of a refresh starts at the same moment
    assert len({q.start_s for q in day["reporting"] if q.kind == "dashboard"}) == 96


def test_duration_scale_is_applied():
    wl = dict(workload.load(), duration_scale=3.0)
    panels, dbt_runs = fake_measurements()
    day = workload.build_day(wl, workload.scenario_steps(wl)[0], panels, dbt_runs)
    assert day["transform"][0].duration_s == 120.0


def test_every_lever_lowers_the_modeled_bill():
    wl = workload.load()
    panels, dbt_runs = fake_measurements()
    results = simulate.simulate_all(wl, panels, dbt_runs)
    usd = [sum(w["usd"] for w in ws) for _, ws in results]
    scanned = [sum(w["bytes_scanned"] for w in ws) for _, ws in results]
    assert all(b <= a for a, b in zip(usd, usd[1:]))
    assert usd[-1] < usd[0]
    assert scanned[-1] < scanned[0]
    # live warehouse: XS always on = 24 credits a day in every step
    for _, ws in results:
        live = next(w for w in ws if w["warehouse"] == "live")
        assert live["credits"] == 24
        assert live["usd"] == 24 * wl["pricing"]["usd_per_credit"]


def test_scan_priced_bill_follows_bytes():
    wl = workload.load()
    panels, dbt_runs = fake_measurements()
    results = simulate.simulate_all(wl, panels, dbt_runs)
    per_tib = wl["pricing"]["usd_per_tib_scanned"]
    for _, ws in results:
        for w in ws:
            assert w["scan_usd"] == w["bytes_scanned"] / 2**40 * per_tib
    scan = [sum(w["scan_usd"] for w in ws) for _, ws in results]
    # auto-suspend (last step) changes billed time, not bytes
    assert scan[-1] == scan[-2]
