import re
from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.fixer.postcheck import decide, run_postcheck
from tests.fakes import FakeSql

NOW = datetime(2026, 10, 28, tzinfo=timezone.utc)


def test_confirmed_when_finding_resolved_and_queries_not_slower():
    assert decide("resolved", 10.0, 10.5, 0.1, 0.1, 500)[0] == "confirmed"


def test_regressed_when_queries_slower_or_lock_share_up():
    assert decide("resolved", 10.0, 11.5, 0.1, 0.1, 500)[0] == "regressed"
    assert decide("resolved", 10.0, 10.0, 0.1, 0.3, 500)[0] == "regressed"
    assert decide("open", 10.0, 10.0, 0.1, 0.1, 500)[0] == "regressed"


def test_low_traffic_is_confirmed_with_low_confidence():
    assert decide("resolved", None, None, None, None, 3)[:2] == ("confirmed", "low")


DUE = {"action_id": "a1", "fingerprint": "fp", "applied_at": NOW, "finding_status": "resolved",
       "project_id": "p", "endpoint_id": "e", "current_measure": None, "prior_measure": None, "checked_runs": 2,
       "checked_now": True}
SLOW = {"before_ms": 10.0, "after_ms": 12.0, "calls_after": 500}
UPD_OK = (r"^\s*UPDATE \S*proposed_actions", [{"num_affected_rows": 1}])
T = "cat.lakebase_health."


def make(due, stats=SLOW, locks=None, upd=UPD_OK):
    return FakeSql(responses=[(r"FROM \S*proposed_actions a JOIN", due), upd,
                              (r"pg_stat_statements_counters", [stats]),
                              (r"wait_event_counters", [locks or {"b": None, "a": None}])])


def test_run_postcheck_records_outcome_ledger_and_score():
    sql = make([DUE])
    [res] = run_postcheck(sql, EngineConfig(catalog="cat"), NOW)
    assert res["outcome"] == "regressed" and res["reasons"]
    [run] = sql.tables[T + "action_runs"]
    assert run["kind"] == "postcheck" and run["outcome"] == "regressed" and run["approval_id"] is None
    assert sql.tables[T + "ledger"][0]["event_type"] == "regressed"
    assert any("outcome_score" in s and a["s"] == 0.0 for s, a in sql.statements)


def test_lock_share_rising_from_zero_regresses():
    sql = make([DUE], {"before_ms": 10.0, "after_ms": 10.0, "calls_after": 500}, {"b": 0.0, "a": 0.3})
    run_postcheck(sql, EngineConfig(catalog="cat"), NOW)
    stmt = [s for s, _ in sql.statements if "wait_event_counters" in s][0]
    assert "coalesce(sum(CASE WHEN ts < :t AND wait_class_name = 'Lock'" in stmt.replace("COALESCE", "coalesce")
    assert decide("resolved", 10.0, 10.0, 0.0, 0.3, 500)[0] == "regressed"


def test_lost_state_race_skips_all_writes():
    sql = make([DUE], upd=(r"^\s*UPDATE \S*proposed_actions", [{"num_affected_rows": 0}]))
    assert run_postcheck(sql, EngineConfig(catalog="cat"), NOW) == []
    assert T + "action_runs" not in sql.tables and T + "ledger" not in sql.tables
    assert not any("outcome_score" in s for s, _ in sql.statements)


def test_one_action_raising_does_not_stop_the_next():
    bad = {**DUE, "action_id": "bad"}
    good = {**DUE, "action_id": "good"}

    def stats(stmt, args):
        if args["ep"] == "boom":
            raise RuntimeError("x")
        return [SLOW]
    bad["endpoint_id"] = "boom"
    sql = make([bad, good])
    sql.responses.insert(0, (r"pg_stat_statements_counters", stats))
    errors = []
    out = run_postcheck(sql, EngineConfig(catalog="cat"), NOW, errors)
    assert [r["action_id"] for r in out] == ["good"] and "bad" in errors[0]


def test_zero_before_ms_is_low_confidence_not_a_crash():
    assert decide("resolved", 0.0, 5.0, None, None, 500)[:2] == ("confirmed", "low")


def test_improving_open_finding_is_not_a_regression():
    assert decide("open", 10.0, 10.0, 0.1, 0.1, 500, improving=True)[0] == "confirmed"
    assert decide("open", 10.0, 10.0, 0.1, 0.1, 500)[0] == "regressed"
    due = {**DUE, "finding_status": "open", "current_measure": 70.0, "prior_measure": 100.0}
    sql = make([due], {"before_ms": 10.0, "after_ms": 10.0, "calls_after": 500})
    [res] = run_postcheck(sql, EngineConfig(catalog="cat"), NOW)
    assert res["outcome"] == "confirmed"


def test_the_run_in_progress_counts_as_the_second_run():
    # Live 0e47c80a: applied 02:01, analysis runs at 02:03 and 02:06 (this one, still 'running' while the postcheck
    # runs). Its check_results are written before the postcheck, so it is the second run with check data.
    first_run = make([{**DUE, "checked_runs": 1}])
    assert run_postcheck(first_run, EngineConfig(catalog="cat"), NOW, run_id="r2") == []
    assert T + "action_runs" not in first_run.tables
    second_run = make([DUE])
    [res] = run_postcheck(second_run, EngineConfig(catalog="cat"), NOW, run_id="r2")
    assert res["action_id"] == "a1"


# Final review I3: only runs in which the finding's check produced data count; without current data the action
# stays applied (pending) and is re-evaluated later; after 7 days without data it is confirmed with low confidence.
def test_the_due_query_counts_runs_where_the_check_ran_for_the_endpoint_after_the_apply():
    sql = make([DUE])
    run_postcheck(sql, EngineConfig(catalog="cat"), NOW, run_id="r2")
    [(stmt, args)] = [(s, a) for s, a in sql.statements if re.search(r"FROM \S*proposed_actions a JOIN", s)]
    assert "check_results" in stmt and "c.fingerprint IS NULL" in stmt and "c.check_id = f.check_id" in stmt
    assert "c.status IN ('ok', 'ok_stale_source')" in stmt and "c.ts > a.updated_at" in stmt
    assert "c.endpoint_id = f.endpoint_id" in stmt and args["run_id"] == "r2" and "runs_after" not in stmt


def test_no_current_check_data_leaves_the_action_applied():
    # compute scaled to zero: the check was skipped this run, so "finding still open" says nothing
    for due in ({**DUE, "finding_status": "open", "checked_now": False},
                {**DUE, "finding_status": "open", "checked_runs": 1, "checked_now": True}):
        sql = make([due], {"before_ms": None, "after_ms": None, "calls_after": 0})
        assert run_postcheck(sql, EngineConfig(catalog="cat"), NOW, run_id="r2") == []
        assert not sql.ran(r"^\s*UPDATE \S*proposed_actions") and T + "ledger" not in sql.tables


def test_seven_days_without_check_data_confirms_with_low_confidence():
    due = {**DUE, "finding_status": "open", "checked_runs": 0, "checked_now": False,
           "applied_at": NOW - timedelta(days=7, minutes=1)}
    sql = make([due])
    [res] = run_postcheck(sql, EngineConfig(catalog="cat"), NOW, run_id="r2")
    assert (res["outcome"], res["confidence"]) == ("confirmed", "low")
    assert "no check data" in res["reasons"][0]
    assert sql.tables[T + "ledger"][0]["event_type"] == "confirmed"
    assert not sql.ran("pg_stat_statements_counters")
    recent = make([{**due, "applied_at": NOW - timedelta(days=6)}])
    assert run_postcheck(recent, EngineConfig(catalog="cat"), NOW, run_id="r2") == []
