from datetime import datetime, timedelta, timezone
from lakebase_health.config import EngineConfig
from lakebase_health.fixer.queue import Work, claim, sql_hash, staleness
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
SQL = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);"


def work(request="verify", state="proposed", approved_sql=SQL, current_sql=SQL, expires=NOW + timedelta(days=1), finding="open"):
    return Work({"approval_id": "ap1", "action_id": "a1", "request": request, "sql_hash": sql_hash(approved_sql)},
                {"action_id": "a1", "state": state, "sql": current_sql, "rollback_sql": "DROP INDEX CONCURRENTLY IF EXISTS x;",
                 "verify_on_branch": True, "expires_at": expires}, finding)


def test_fresh_approval_is_not_stale():
    assert staleness(work(), NOW) is None


def test_stale_reasons():
    assert staleness(work(current_sql=SQL.replace("customer_id", "status")), NOW) == "sql_changed"
    assert staleness(work(expires=NOW - timedelta(seconds=1)), NOW) == "expired"
    assert staleness(work(finding="resolved"), NOW) == "finding_resolved"
    assert staleness(work(request="apply", state="proposed"), NOW) == "illegal_state"


def test_rollback_hashes_the_rollback_sql_and_ignores_finding_state():
    w = work(request="rollback", state="regressed", approved_sql="DROP INDEX CONCURRENTLY IF EXISTS x;", finding="resolved")
    assert staleness(w, NOW) is None


def test_claim_is_compare_and_set():
    sql = FakeSql(responses=[(r"^UPDATE cat\.lakebase_health\.proposed_actions SET state = :to_state", [{"num_affected_rows": 1}])])
    assert claim(sql, CFG, "a1", "proposed", "verifying", NOW)
    stmt, args = sql.statements[-1]
    assert "WHERE action_id = :id AND state = :from_state" in stmt and args["from_state"] == "proposed"
    lost = FakeSql(responses=[(r"^UPDATE", [{"num_affected_rows": 0}])])
    assert not claim(lost, CFG, "a1", "proposed", "verifying", NOW)


def test_a_rollback_that_cannot_run_is_consumed_not_executed():
    """Nothing to undo (empty or a comment such as '-- VACUUM changes no data'), or SQL outside the allowlist."""
    for rb in (None, "", "-- VACUUM changes no data; nothing to roll back", "CREATE INDEX i ON hc_test.t (a);"):
        w = work(request="rollback", state="applied", approved_sql=rb or "")
        w.action["rollback_sql"] = rb
        assert staleness(w, NOW) == "no_rollback", rb
