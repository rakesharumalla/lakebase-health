from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.fixer.draft.states import draft_token
from lakebase_health.fixer.draft.store import (DraftWork, new_draft, pending_drafts, rewrite_action, staleness,
                                               stuck_drafts)
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
FP = "f" * 32


def work(attempt=1, last=0, last_state=None, status="open", check="top_db_time_query"):
    return DraftWork({"approval_id": "ap1", "fingerprint": FP, "sql_hash": draft_token(FP, attempt),
                      "approver": "owner@x", "requested_at": NOW},
                     {"fingerprint": FP, "status": status, "check_id": check, "project_id": "p1", "branch_id": "b1",
                      "endpoint_id": "ep-a", "object": "42", "evidence": '{"queryid": "42"}', "measure": 60.0},
                     last, last_state)


def test_pending_drafts_reads_unconsumed_draft_approvals_with_the_latest_attempt():
    row = {"approval_id": "ap1", "fingerprint": FP, "sql_hash": "h", "approver": "owner@x", "requested_at": NOW,
           "finding_status": "open", "check_id": "plan_flip", "project_id": "p1", "branch_id": "b1",
           "endpoint_id": "ep-a", "object": "42", "evidence": "{}", "measure": 3.0, "last_attempt": 2,
           "last_state": "verify_failed"}
    sql = FakeSql([(r"FROM cat\.lakebase_health\.approvals ap", [row])])
    [w] = pending_drafts(sql, CFG)
    assert w.approval["approval_id"] == "ap1" and w.finding["status"] == "open" and w.finding["check_id"] == "plan_flip"
    assert (w.last_attempt, w.last_state) == (2, "verify_failed")
    stmt = sql.statements[0][0]
    assert "ap.request = 'draft'" in stmt and "FROM cat.lakebase_health.action_runs WHERE approval_id IS NOT NULL" in stmt
    assert "max_by(state, attempt)" in stmt


def test_staleness_rules():
    assert staleness(work(), True) is None
    assert staleness(work(attempt=3, last=2, last_state="verified"), True) is None  # Retry after a final attempt
    assert staleness(work(), False) == "not_set_up"
    assert staleness(work(status="resolved"), True) == "finding_resolved"
    assert staleness(work(status="superseded"), True) == "finding_superseded"
    assert staleness(work(status=None), True) == "finding_resolved"  # the finding row is gone
    assert staleness(work(check="missing_fk_index"), True) == "not_rewritable"
    assert staleness(work(attempt=2, last=1, last_state="verifying"), True) == "in_progress"
    assert staleness(work(attempt=1, last=1, last_state="draft_failed"), True) == "attempt_changed"  # double click


def test_new_draft_and_its_advisory_action_share_the_id():
    d = new_draft(work(attempt=3, last=2, last_state="verified"), CFG, NOW)
    assert d["attempt"] == 3 and d["state"] == "drafting" and d["endpoint"] == "databricks-claude-opus-5-5"
    assert d["requested_by"] == "owner@x" and d["approval_id"] == "ap1" and len(d["draft_id"]) == 32
    a = rewrite_action(d, work().finding, "SELECT 1", NOW)
    assert a["action_id"] == a["draft_id"] == d["draft_id"] and a["origin"] == "llm" and a["kind"] == "query_rewrite"
    assert a["state"] == "verifying" and a["rollback_sql"] is None and "application code" in a["rollback_note"]
    assert a["expires_at"] == NOW + timedelta(days=7)


def test_stuck_drafts_are_the_running_ones_older_than_the_branch_ttl():
    rows = [{"draft_id": "old", "state": "drafting", "updated_at": (NOW - timedelta(hours=5)).replace(tzinfo=None)},
            {"draft_id": "new", "state": "verifying", "updated_at": NOW - timedelta(hours=1)}]
    sql = FakeSql([(r"FROM cat\.lakebase_health\.drafts WHERE state IN \('drafting', 'verifying'\)", rows)])
    assert [r["draft_id"] for r in stuck_drafts(sql, CFG, NOW)] == ["old"]
