import pytest

from costlab import billing


def test_no_queries_costs_nothing():
    bill = billing.simulate([], auto_suspend_s=600)
    assert bill == billing.Bill(billed_seconds=0.0, busy_seconds=0, resumes=0)


def test_single_short_query_hits_the_60s_minimum_when_suspend_is_short():
    bill = billing.simulate([(1000, 2.0)], auto_suspend_s=30)
    # 2s busy + 30s idle = 32s, but every resume is billed at least 60s
    assert bill.billed_seconds == 60
    assert bill.busy_seconds == 2.0
    assert bill.resumes == 1


def test_single_query_pays_the_idle_tail():
    bill = billing.simulate([(1000, 5.0)], auto_suspend_s=600)
    assert bill.billed_seconds == 605
    assert bill.resumes == 1


def test_long_query_billed_by_the_second():
    bill = billing.simulate([(0, 300.0)], auto_suspend_s=60)
    assert bill.billed_seconds == 360


def test_overlapping_queries_share_the_warehouse():
    # three queries starting together, the longest is 10s
    bill = billing.simulate([(100, 4.0), (100, 10.0), (102, 3.0)], auto_suspend_s=60)
    assert bill.busy_seconds == 10.0
    assert bill.resumes == 1
    assert bill.billed_seconds == 70


def test_query_during_idle_tail_extends_the_session():
    # second query arrives 300s after the first ended, while still inside the 600s tail
    bill = billing.simulate([(0, 10.0), (310, 10.0)], auto_suspend_s=600)
    assert bill.resumes == 1
    assert bill.billed_seconds == pytest.approx(320 + 600)


def test_query_after_suspend_resumes_and_pays_another_minimum():
    # second query arrives after the warehouse suspended (10 + 60 = 70 < 500)
    bill = billing.simulate([(0, 10.0), (500, 1.0)], auto_suspend_s=60)
    assert bill.resumes == 2
    assert bill.billed_seconds == pytest.approx(70 + 61)


def test_query_exactly_at_suspend_time_still_finds_warehouse_up():
    bill = billing.simulate([(0, 10.0), (70, 1.0)], auto_suspend_s=60)
    assert bill.resumes == 1
    assert bill.billed_seconds == pytest.approx(131)


def test_shorter_auto_suspend_bills_less_for_sparse_queries():
    queries = [(h * 3600, 2.0) for h in range(24)]
    before = billing.simulate(queries, auto_suspend_s=600)
    after = billing.simulate(queries, auto_suspend_s=60)
    assert before.billed_seconds == pytest.approx(24 * 602)
    assert after.billed_seconds == pytest.approx(24 * 62)


def test_frequent_refresh_keeps_warehouse_up_all_day():
    # every 5 minutes with a 10 minute auto-suspend: never suspends
    queries = [(t, 1.0) for t in range(0, 86400, 300)]
    bill = billing.simulate(queries, auto_suspend_s=600)
    assert bill.resumes == 1
    assert bill.billed_seconds == 86400  # cut at midnight


def test_session_cut_at_midnight_keeps_minimum():
    bill = billing.simulate([(86390, 5.0)], auto_suspend_s=600)
    assert bill.billed_seconds == 60


def test_always_on_bills_the_whole_day():
    bill = billing.simulate([(0, 1.0), (50, 1.0)], auto_suspend_s=60, always_on=True)
    assert bill.billed_seconds == 86400
    assert bill.busy_seconds == 2.0
    assert bill.resumes == 0
    assert billing.simulate([], auto_suspend_s=60, always_on=True).billed_seconds == 86400


def test_busy_blocks_merges_touching_and_overlapping():
    assert billing.busy_blocks([(10, 5), (0, 3), (12, 1), (15, 2)]) == [(0, 3), (10, 17)]


def test_credits_by_size():
    assert billing.credits(3600, "XS") == 1
    assert billing.credits(3600, "S") == 2
    assert billing.credits(1800, "M") == 2
    assert billing.credits(86400, "XL") == 16 * 24
