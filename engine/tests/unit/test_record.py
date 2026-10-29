from datetime import datetime, timedelta, timezone

from lakebase_health.checkspec import parse_check
from lakebase_health.config import EngineConfig
from lakebase_health.fingerprint import fingerprint
from lakebase_health.ledger import verify
from lakebase_health.models import FLEET, CheckOutcome, CheckResult
from lakebase_health.record import load_last_ok, persist
from tests.fakes import FakeSql
from tests.unit.test_executors import ep

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
FIX = {"kind": "ddl", "template": "CREATE INDEX {n};", "rollback": "DROP INDEX {n};", "risk": "moderate",
       "reversible": True, "verify_on_branch": True}
SPEC = parse_check({"id": "c1", "title": "T", "category": "c", "source": "delta", "explain": "E", "sql": "SELECT 1",
                    "thresholds": {"warning": 0}, "fix": FIX}, stem="c1")
SPECS = {"c1": SPEC}
T = CFG.table


def outcome(with_result=True, status="ok"):
    res = [CheckResult("c1", fingerprint("c1", "p", "e", "o"), "p", "b", "e", "o", 1.0, "warning", "high",
                       {"k": 1}, "SELECT 1", "CREATE INDEX CONCURRENTLY ON s.t (x);", "DROP INDEX CONCURRENTLY IF EXISTS s.x;")] if with_result else []
    return CheckOutcome("c1", FLEET, status, results=res)


def test_persist_writes_everything_and_is_idempotent():
    sql = FakeSql()
    c1 = persist(sql, CFG, "r1", NOW, [outcome()], SPECS, [ep()])
    assert c1["findings_new"] == 1 and c1["actions_proposed"] == 1
    assert len(sql.tables[T("inventory")]) == 1
    assert {r["status"] for r in sql.tables[T("check_results")]} == {"ok"}
    assert sql.tables[T("findings")][0]["query_text"] == "SELECT 1"
    assert '"k": 1' in sql.tables[T("findings")][0]["evidence"]
    assert sql.tables[T("proposed_actions")][0]["state"] == "proposed"
    assert sql.tables[T("shadow_log")][0]["would_allow_level"] == "L2"
    c2 = persist(sql, CFG, "r2", NOW + timedelta(hours=1), [outcome()], SPECS, [ep()])
    assert c2["findings_new"] == 0 and c2["findings_open"] == 1
    assert len(sql.tables[T("findings")]) == 1 and len(sql.tables[T("proposed_actions")]) == 1
    assert len(sql.tables[T("ledger")]) == 1 and verify(sql.tables[T("ledger")])


def test_resolution_expires_action_and_extends_ledger_chain():
    sql = FakeSql(responses=[(r"max_by\(hash", lambda s, a: [{"action_id": r["action_id"], "hash": r["hash"]} for r in sql.tables.get(T("ledger"), [])])])
    persist(sql, CFG, "r1", NOW, [outcome()], SPECS, [])
    persist(sql, CFG, "r2", NOW + timedelta(hours=1), [outcome(False)], SPECS, [])
    c = persist(sql, CFG, "r3", NOW + timedelta(hours=2), [outcome(False)], SPECS, [])
    assert c["findings_resolved"] == 1
    assert sql.tables[T("proposed_actions")][0]["state"] == "expired"
    ledger = sql.tables[T("ledger")]
    assert [e["event_type"] for e in ledger] == ["proposed", "expired"] and verify(ledger)


def test_status_rows_for_non_ok_outcomes():
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [CheckOutcome("c1", ("p", "e"), "skipped:compute_inactive")], SPECS, [])
    [row] = sql.tables[T("check_results")]
    assert row["status"] == "skipped:compute_inactive" and row["fingerprint"] is None


def test_check_broken_after_three_error_runs():
    history = [{"check_id": "c1", "status": "error", "run_ts": NOW - timedelta(hours=h)} for h in (0, 1, 2)]
    sql = FakeSql(responses=[(r"AS run_ts", history)])
    c = persist(sql, CFG, "r1", NOW, [CheckOutcome("c1", FLEET, "error", "boom")], SPECS, [])
    broken = [f for f in sql.tables[T("findings")] if f["check_id"] == "check_broken"]
    assert broken and broken[0]["object"] == "c1" and c["findings_new"] == 1


def test_metrics_daily_merge_runs():
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [], SPECS, [])
    assert sql.ran(r"MERGE INTO cat\.lakebase_health\.metrics_daily")


def test_sequence_metric_and_check_results_retention():
    seq = parse_check({"id": "sequence_exhaustion", "title": "S", "category": "c", "source": "postgres",
                       "explain": "E", "sql": "SELECT 1", "thresholds": {"warning": 50}}, stem="sequence_exhaustion")
    r = CheckResult("sequence_exhaustion", "fp", "p", "b", "e", "db:t.id", 74.5, "warning", "high", {})
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [CheckOutcome("sequence_exhaustion", ("p", "e"), "ok", results=[r])],
            {"sequence_exhaustion": seq}, [])
    [m] = sql.tables[T("metrics_daily")]
    assert m["metric"] == "sequence_pct:db:t.id" and m["value"] == 74.5
    assert sql.ran(r"DELETE FROM cat\.lakebase_health\.check_results WHERE ts < current_timestamp\(\) - INTERVAL 90 DAYS")


def test_load_last_ok():
    sql = FakeSql(responses=[(r"max\(ts\) AS last_ok", [{"check_id": "c1", "last_ok": NOW}])])
    assert load_last_ok(sql, CFG) == {"c1": NOW}


# Final review I4: findings on a branch that no longer exists (or a verification branch, never inventoried) resolve,
# and their proposed actions expire with reason branch_gone. A project whose listing failed is left alone.
def _on(branch, project="p"):
    res = CheckResult("c1", fingerprint("c1", project, f"e-{branch}", "o"), project, branch, f"e-{branch}", "o", 1.0,
                      "warning", "high", {"k": 1}, None, "CREATE INDEX CONCURRENTLY ON s.t (x);",
                      "DROP INDEX CONCURRENTLY IF EXISTS s.x;")
    return CheckOutcome("c1", FLEET, "ok", results=[res])


def test_findings_on_a_gone_branch_resolve_and_their_actions_expire_branch_gone():
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [_on("b-old")], SPECS, [], branches={"p": {"b-old"}})
    assert sql.tables[T("proposed_actions")][0]["state"] == "proposed"
    c = persist(sql, CFG, "r2", NOW + timedelta(hours=1), [], SPECS, [], branches={"p": {"b-main"}})
    [f] = sql.tables[T("findings")]
    assert f["status"] == "resolved" and f["resolved_at"] == NOW + timedelta(hours=1)
    assert '"resolved_reason": "branch_gone"' in f["evidence"] and c["findings_resolved"] == 1
    assert sql.tables[T("proposed_actions")][0]["state"] == "expired"
    [_, expired] = sql.tables[T("ledger")]
    assert expired["event_type"] == "expired" and "branch_gone" in str(expired["payload"])


def test_results_on_a_branch_missing_from_the_inventory_are_dropped():
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [_on("br-verify"), _on("b-main")], SPECS, [], branches={"p": {"b-main"}})
    assert [f["branch_id"] for f in sql.tables[T("findings")]] == ["b-main"]


def test_a_project_whose_listing_failed_is_not_gone():
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [_on("b-old")], SPECS, [], branches={"p": {"b-old"}})
    persist(sql, CFG, "r2", NOW + timedelta(hours=1), [], SPECS, [], branches={"p": None})
    assert sql.tables[T("findings")][0]["status"] == "new"


def test_a_project_the_engine_does_not_list_is_left_alone():
    # telemetry can name projects the engine cannot list (another workspace, no permission): unknown, not gone
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [_on("b-old")], SPECS, [], branches={"other": {"b"}})
    persist(sql, CFG, "r2", NOW + timedelta(hours=1), [], SPECS, [], branches={"other": {"b"}})
    assert sql.tables[T("findings")][0]["status"] == "new"
