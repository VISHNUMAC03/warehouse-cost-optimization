import json

import pytest

from costlab import bench_scan


def test_time_filter_expands_like_the_plugin():
    sql = bench_scan.expand_macros("SELECT 1 FROM t WHERE $__timeFilter(event_time)", 100, 200, 60)
    assert sql == "SELECT 1 FROM t WHERE event_time >= toDateTime(100) AND event_time <= toDateTime(200)"


def test_from_to_interval_macros():
    sql = bench_scan.expand_macros(
        "SELECT $__timeInterval(event_time) AS t, count() / $__interval_s FROM x "
        "WHERE event_date >= toDate($__fromTime) AND event_date <= toDate($__toTime) GROUP BY t",
        1700000000, 1700604800, 600)
    assert sql == (
        "SELECT toStartOfInterval(event_time, INTERVAL 600 second) AS t, count() / 600 FROM x "
        "WHERE event_date >= toDate(toDateTime(1700000000)) AND event_date <= toDate(toDateTime(1700604800)) GROUP BY t")


def test_several_time_filters_and_spaces():
    sql = bench_scan.expand_macros("$__timeFilter( a ) AND $__timeFilter(b)", 1, 2, 1)
    assert sql == "a >= toDateTime(1) AND a <= toDateTime(2) AND b >= toDateTime(1) AND b <= toDateTime(2)"


def test_unknown_macro_or_variable_is_an_error():
    with pytest.raises(ValueError):
        bench_scan.expand_macros("SELECT $__timeFrom()", 1, 2, 1)
    with pytest.raises(ValueError):
        bench_scan.expand_macros("SELECT * FROM t WHERE brand = '$brand'", 1, 2, 1)
    with pytest.raises(ValueError):
        bench_scan.expand_macros("SELECT $__fromTime_ms", 1, 2, 1)


def test_relative_seconds():
    assert bench_scan.relative_seconds("now-7d") == 7 * 86400
    assert bench_scan.relative_seconds("now-24h") == 86400
    assert bench_scan.relative_seconds("now-30m") == 1800
    with pytest.raises(ValueError):
        bench_scan.relative_seconds("now/d")


def test_grafana_interval_rounding():
    assert bench_scan.grafana_interval_s(7 * 86400) == 600
    assert bench_scan.grafana_interval_s(86400) == 60
    assert bench_scan.grafana_interval_s(6 * 3600) == 20
    assert bench_scan.grafana_interval_s(1800) == 2
    assert bench_scan.grafana_interval_s(60) == 1  # never below a second


def test_rewrite_fact_table():
    sql = ("SELECT * FROM analytics.fct_events e JOIN analytics.fct_sessions s USING user_session "
           "WHERE e.event_type = 'purchase'")
    assert bench_scan.rewrite_fact_table(sql) == (
        "SELECT * FROM costlab.fct_events_unpartitioned e JOIN analytics.fct_sessions s USING user_session "
        "WHERE e.event_type = 'purchase'")


def test_rewrite_is_word_boundary_safe():
    untouched = "SELECT * FROM analytics.fct_events_daily, my_fct_events, raw.fct_events, analytics.fct_eventsx"
    assert bench_scan.rewrite_fact_table(untouched) == untouched
    assert bench_scan.rewrite_fact_table("FROM fct_events\nWHERE 1") == "FROM costlab.fct_events_unpartitioned\nWHERE 1"
    assert bench_scan.rewrite_fact_table("FROM analytics.fct_events)") == "FROM costlab.fct_events_unpartitioned)"


def write_dashboard(tmp_path, uid, panels, time_from="now-7d"):
    (tmp_path / f"{uid}.json").write_text(json.dumps({"uid": uid, "time": {"from": time_from, "to": "now"},
                                                       "panels": panels}), encoding="utf-8")


def target(sql, ref="A"):
    return {"refId": ref, "rawSql": sql}


def test_load_panels_reads_tags_and_nested_rows(tmp_path):
    write_dashboard(tmp_path, "sales-overview", [
        {"type": "stat", "title": "Revenue", "targets": [
            target("/* grafana dashboard=sales-overview panel=revenue */ SELECT sum(revenue) FROM analytics.fct_events")]},
        {"type": "row", "title": "More", "panels": [
            {"type": "table", "title": "Top Brands", "targets": [target("SELECT brand FROM analytics.dim_products")]},
        ]},
        {"type": "text", "title": "notes"},
    ])
    panels = bench_scan.load_panels(["sales-overview"], tmp_path)
    assert [(p["dashboard"], p["panel"]) for p in panels] == [("sales-overview", "revenue"),
                                                             ("sales-overview", "top-brands-a")]
    assert panels[0]["time_from"] == "now-7d"


def test_prepare_sql_for_each_layout():
    panel = {"dashboard": "d", "panel": "p", "time_from": "now-24h",
             "sql": "/* grafana dashboard=d panel=p */ SELECT count() FROM analytics.fct_events "
                    "WHERE event_date >= toDate($__fromTime) AND $__timeFilter(event_time);"}
    part = bench_scan.prepare_sql(panel, "partitioned", 1_000_000)
    unpart = bench_scan.prepare_sql(panel, "unpartitioned", 1_000_000)
    assert "toDate(toDateTime(913600))" in part
    assert "event_time <= toDateTime(1000000)" in part
    assert "FROM analytics.fct_events " in part
    assert "FROM costlab.fct_events_unpartitioned " in unpart
    assert not part.endswith(";")
