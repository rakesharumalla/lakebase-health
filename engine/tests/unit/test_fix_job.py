import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.fixer.queue import Work, sql_hash
from lakebase_health.jobs import fix
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
SQL = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);"


def work(request="verify", state="proposed", sql=SQL, project="p1", aid="a1", apid="ap1"):
    return Work({"approval_id": apid, "action_id": aid, "request": request, "sql_hash": sql_hash(sql), "approver": "owner@x"},
                {"action_id": aid, "state": state, "sql": sql, "rollback_sql": "DROP INDEX CONCURRENTLY IF EXISTS hc_test.ix;",
                 "verify_on_branch": True, "expires_at": NOW + timedelta(days=1), "project_id": project,
                 "branch_id": "br1", "kind": "ddl", "check_id": "missing_fk_index"}, "open")


class Recorder:
    def __init__(self, items, claim_ok=True, stuck=()):
        self.items, self.claim_ok, self.stuck = items, claim_ok, list(stuck)
        self.states, self.runs, self.events, self.event_calls = [], [], [], []
        self.sessions, self.recovered, self.executed, self.branch_closed = [], [], [], 0


def deps(rec, judged=True, branch_error=None, execute=None):
    @contextmanager
    def branch(w, project, source, aid):
        if branch_error:
            raise branch_error
        try:
            yield {"branch": "b", "endpoint": "e", "host": "h"}
        finally:
            rec.branch_closed += 1

    @contextmanager
    def session(endpoint, host, db, kind):
        rec.sessions.append(endpoint)
        yield object()

    def events(sql, cfg, rows, now):
        rec.event_calls.append(list(rows))
        rec.events.extend(rows)

    def do_execute(conn, s, aid):
        rec.executed.append((s, aid))
        return (execute or {"outcome": "applied", "error": None, "index": "hc_test.ix"})

    def recover(conn, s, aid):
        rec.recovered.append((s, aid))
        return "applied"

    return {
        "pending": lambda sql, cfg: [i for i in rec.items if i.approval["approval_id"] not in {r["approval_id"] for r in rec.runs}],
        "stuck": lambda sql, cfg, now: rec.stuck,
        "claim": lambda sql, cfg, aid, f, t, now: rec.claim_ok and (rec.states.append((aid, f, t)) or True),
        "set_state": lambda sql, cfg, aid, t, now: rec.states.append((aid, None, t)),
        "record_run": lambda sql, cfg, row: rec.runs.append(row),
        "events": events,
        "branch": branch, "session": session,
        "target": lambda w, w_action: {"project": "projects/x", "branch": "projects/x/branches/production",
                                       "endpoint": "projects/x/branches/production/endpoints/primary", "host": "h", "database": "db"},
        "samples": lambda sql, cfg, project_id, table: [{"queryid": "1", "query": "SELECT secret FROM t WHERE x = $1",
                                                          "params": [["4111"]], "is_write": False}],
        "measure": lambda conn, q, params: {"ms": 10.0, "nodes": [], "indexes": []},
        "judge": lambda kind, ix, before, after, **kw: rec.__dict__.setdefault("judged_with", []).append(
            (kind, kw, before)) or {"passed": judged, "rules": []},
        "execute": do_execute,
        "recover": recover,
    }


def test_verify_success_records_report_and_events():
    rec = Recorder([work()])
    out = fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert ("a1", "proposed", "verifying") in rec.states and ("a1", None, "verified") in rec.states
    assert rec.runs[0]["outcome"] == "claimed" and rec.runs[0]["approval_id"] == "ap1"
    assert rec.runs[-1]["outcome"] == "verified" and rec.runs[-1]["approval_id"] is None
    assert [e["event_type"] for e in rec.events] == ["verify_started", "verified"]
    assert out["verified"] == 1


def test_lost_claim_does_nothing_destructive():
    rec = Recorder([work()], claim_ok=False)
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.runs[0]["outcome"] == "skipped:claim_lost" and not any(t == "verified" for _, _, t in rec.states)


def test_stale_approval_is_consumed_not_executed():
    w = work()
    w.action["sql"] = SQL.replace("customer_id", "status")
    rec = Recorder([w])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.runs[0]["outcome"] == "stale:sql_changed" and rec.states == []
    assert rec.events[0]["event_type"] == "stale_approval"


def test_disallowed_sql_is_blocked():
    rec = Recorder([work(sql="DROP TABLE hc_test.orders")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert ("a1", None, "blocked") in rec.states and rec.runs[-1]["outcome"] == "blocked"


def test_branch_failure_marks_verify_failed():
    rec = Recorder([work()])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec, branch_error=RuntimeError("branch limit")), now=NOW)
    assert ("a1", None, "verify_failed") in rec.states
    assert rec.runs[-1]["error"] == "branch_unavailable: RuntimeError: branch limit"


def test_one_action_per_project_per_sweep():
    rec = Recorder([work(aid="a1", apid="ap1"), work(aid="a2", apid="ap2")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert [r["approval_id"] for r in rec.runs if r["approval_id"]] == ["ap1"] and len(rec.runs) == 2


def test_apply_runs_on_target():
    rec = Recorder([work(request="apply", state="verified")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert ("a1", "verified", "applying") in rec.states and ("a1", None, "applied") in rec.states
    assert rec.sessions == ["projects/x/branches/production/endpoints/primary"]
    assert rec.executed == [(SQL, "a1")]


def test_rollback_runs_rollback_sql_with_action_id():
    rec = Recorder([work(request="rollback", state="applied")])
    rec.items[0].approval["sql_hash"] = sql_hash(rec.items[0].action["rollback_sql"])
    out = fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.executed == [("DROP INDEX CONCURRENTLY IF EXISTS hc_test.ix;", "a1")]
    assert ("a1", None, "rolled_back") in rec.states and out == {"rolled_back": 1}


# Ruling 2: the fix runs on the branch with the action id; a branch apply failure is verify_failed,
# never an exception past the branch context; measurement never touches the target.
def test_verify_applies_on_branch_endpoint_only_with_action_id():
    rec = Recorder([work()])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.sessions == ["e"] and rec.executed == [(SQL, "a1")] and rec.branch_closed == 1


def test_branch_apply_failure_is_verify_failed_with_error():
    rec = Recorder([work()])
    d = deps(rec, execute={"outcome": "apply_failed", "error": "index lbh_a1 already exists", "index": None})
    d["judge"] = lambda *a: (_ for _ in ()).throw(AssertionError("judge must not run after a failed branch apply"))
    out = fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert ("a1", None, "verify_failed") in rec.states and out == {"verify_failed": 1}
    assert "already exists" in rec.runs[-1]["error"] and rec.branch_closed == 1
    assert rec.events[-1]["event_type"] == "verify_failed"


def test_measure_errors_reach_judge_and_fail():
    rec = Recorder([work()])
    seen = {}
    d = deps(rec)
    d["measure"] = lambda conn, q, params: {"ms": None, "nodes": [], "indexes": [], "error": "UndefinedTable"}

    def judge(kind, ix, before, after, **kw):
        seen.update(before=before, after=after)
        return {"passed": False, "rules": [{"rule": "query_error", "passed": False, "detail": "query 1 failed: UndefinedTable"}]}

    d["judge"] = judge
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert seen["before"]["1"]["error"] == "UndefinedTable"
    assert rec.runs[-1]["outcome"] == "verify_failed"
    assert json.loads(rec.runs[-1]["report"])["rules"][0]["rule"] == "query_error"


# Ruling 6: reports carry ids, timings, rules and branch only.
def test_report_has_no_query_text_or_parameters():
    rec = Recorder([work()])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    report = rec.runs[-1]["report"]
    assert "secret" not in report and "4111" not in report
    parsed = json.loads(report)
    assert parsed["branch"] == "b" and parsed["queries"][0]["queryid"] == "1"
    assert parsed["queries"][0]["before_ms"] == 10.0 and rec.runs[-1]["branch"] == "b"


# Ruling 3: stuck verifying becomes verify_failed; applying/rolling_back use recover().
def stuck_row(state, age, aid="a9"):
    return {"action_id": aid, "state": state, "sql": SQL, "rollback_sql": "DROP INDEX CONCURRENTLY IF EXISTS hc_test.ix;",
            "updated_at": (NOW - age).replace(tzinfo=None), "project_id": "p9", "branch_id": "br9"}


def test_stuck_query_selects_old_rows_per_state():
    rows = [stuck_row("verifying", timedelta(hours=4, minutes=16), "v_old"),
            stuck_row("verifying", timedelta(hours=3), "v_new"),
            stuck_row("applying", timedelta(hours=2, minutes=16), "a_old"),
            stuck_row("applying", timedelta(hours=1), "a_new"),
            stuck_row("rolling_back", timedelta(hours=3), "r_old")]
    sql = FakeSql([(r"FROM cat\.lakebase_health\.proposed_actions", rows)])
    got = [r["action_id"] for r in fix._stuck(sql, CFG, NOW)]
    assert got == ["v_old", "a_old", "r_old"]
    assert "'verifying'" in sql.statements[0][0]


def test_stuck_verifying_recovers_to_verify_failed_without_touching_postgres():
    rec = Recorder([], stuck=[stuck_row("verifying", timedelta(hours=5))])
    out = fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.states == [("a9", None, "verify_failed")] and rec.sessions == [] and rec.recovered == []
    assert rec.events == [{"action_id": "a9", "event_type": "verify_failed", "payload": {"recovered": True}}]
    assert out == {"recovered:verify_failed": 1}


def test_stuck_applying_and_rolling_back_use_recover_with_action_id():
    rec = Recorder([], stuck=[stuck_row("applying", timedelta(hours=3), "a1"),
                              stuck_row("rolling_back", timedelta(hours=3), "a2")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.recovered == [(SQL, "a1"), ("DROP INDEX CONCURRENTLY IF EXISTS hc_test.ix;", "a2")]
    assert ("a1", None, "applied") in rec.states and ("a2", None, "rolled_back") in rec.states


def test_recovery_failure_does_not_stop_the_sweep():
    rec = Recorder([work()], stuck=[stuck_row("applying", timedelta(hours=3))])
    d = deps(rec)
    d["recover"] = lambda conn, s, aid: (_ for _ in ()).throw(RuntimeError("endpoint down"))
    out = fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert not any(aid == "a9" for aid, _, _ in rec.states) and out["verified"] == 1


# Ruling 4: one events() call per action per sweep, on every path.
def test_each_path_writes_events_once_per_action():
    stale = work(aid="s1", apid="x1", project="p1")
    stale.action["sql"] = SQL.replace("customer_id", "status")
    items = [stale, work(aid="r1", apid="x2", project="p2", request="reject"),
             work(aid="b1", apid="x3", project="p3", sql="DROP TABLE hc_test.orders"),
             work(aid="v1", apid="x4", project="p4"),
             work(aid="f1", apid="x5", project="p5", request="apply", state="verified")]
    rec = Recorder(items)
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    per_call = [{e["action_id"] for e in c} for c in rec.event_calls]
    assert per_call == [{"s1"}, {"r1"}, {"b1"}, {"v1"}, {"f1"}]
    assert [e["event_type"] for e in rec.event_calls[2]] == ["verify_started", "blocked"]
    assert [e["event_type"] for e in rec.event_calls[4]] == ["apply_started", "applied"]


# Ruling 5: a lost claim records a skipped run, no state change, no ledger event.
def test_lost_claim_writes_no_event_and_no_state():
    rec = Recorder([work()], claim_ok=False)
    out = fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.states == [] and rec.event_calls == [] and rec.executed == []
    assert len(rec.runs) == 1 and out == {"skipped:claim_lost": 1}


# Fix round 1, ruling 1: the approval is consumed by a "claimed" row before anything executes.
def test_claimed_row_is_written_before_execution():
    rec = Recorder([work(request="apply", state="verified")])
    d = deps(rec)
    seen = []
    d["execute"] = lambda conn, s, aid: seen.append([dict(r) for r in rec.runs]) or {"outcome": "applied", "error": None, "index": "i"}
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert [(r["approval_id"], r["kind"], r["outcome"]) for r in seen[0]] == [("ap1", "apply", "claimed")]
    final = rec.runs[-1]
    assert final["approval_id"] is None and final["kind"] == "apply" and final["outcome"] == "applied"
    assert json.loads(final["report"])["approval_id"] == "ap1"


class Crash(BaseException):
    pass


def test_interrupted_apply_is_recovered_not_rerun():
    rec = Recorder([work(request="apply", state="verified")])
    d = deps(rec)

    def crash(conn, s, aid):
        rec.executed.append((s, aid))
        raise Crash  # the job dies mid-apply

    d["execute"] = crash
    try:
        fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    except Crash:
        pass
    later = NOW + timedelta(hours=3)
    rec.stuck = [{**stuck_row("applying", timedelta(hours=3), "a1"), "updated_at": NOW}]
    rec.items[0].action["state"] = "apply_failed"  # what pending() would see after recovery, were the approval unconsumed
    out = fix.run_fixer(None, None, CFG, "fixer", deps=d, now=later)
    assert len(rec.executed) == 1 and rec.recovered == [(SQL, "a1")]
    assert out == {"recovered:applied": 1}
    assert [r["approval_id"] for r in rec.runs if r["approval_id"]] == ["ap1"]


def test_recovery_records_a_run_without_approval():
    rec = Recorder([], stuck=[stuck_row("applying", timedelta(hours=3))])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert [(r["approval_id"], r["action_id"], r["outcome"]) for r in rec.runs] == [(None, "a9", "recovered:applied")]


# Fix round 1, ruling 3: missing privileges or failed authentication block the action.
class PgError(Exception):
    def __init__(self, sqlstate):
        super().__init__("permission denied")
        self.sqlstate = sqlstate


def test_insufficient_privilege_on_apply_blocks():
    rec = Recorder([work(request="apply", state="verified")])
    d = deps(rec)
    d["execute"] = lambda conn, s, aid: (_ for _ in ()).throw(PgError("42501"))
    out = fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert ("a1", None, "blocked") in rec.states and out == {"blocked": 1}
    assert rec.runs[-1]["error"] == ("fixer not enrolled or missing privileges on this project: 42501 "
                                     "(PgError: permission denied)")
    assert rec.events[-1]["event_type"] == "blocked"


def test_authentication_failure_in_cause_blocks_verify():
    rec = Recorder([work()])
    d = deps(rec)

    @contextmanager
    def session(endpoint, host, db, kind):
        try:
            raise PgError("28P01")
        except PgError as e:
            raise RuntimeError("connect failed") from e
        yield

    d["session"] = session
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert ("a1", None, "blocked") in rec.states and rec.runs[-1]["outcome"] == "blocked"
    assert rec.runs[-1]["error"].endswith(": 28P01")


def test_privilege_error_returned_by_execute_blocks():
    rec = Recorder([work(request="apply", state="verified")])
    d = deps(rec, execute={"outcome": "apply_failed", "error": "InsufficientPrivilege: denied", "index": None, "sqlstate": "42501"})
    out = fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert out == {"blocked": 1}


def test_branch_error_inside_body_is_not_branch_unavailable():
    rec = Recorder([work()])
    d = deps(rec)
    d["samples"] = lambda *a: [{"queryid": "1", "query": "SELECT 1", "params": [[]], "is_write": False}]
    d["measure"] = lambda *a: (_ for _ in ()).throw(ValueError("boom"))
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert rec.runs[-1]["error"] == "ValueError: boom" and rec.branch_closed == 1


def test_blocked_error_keeps_the_postgres_message_from_execute():
    rec = Recorder([work(request="apply", state="verified")])
    d = deps(rec, execute={"outcome": "apply_failed", "error": "InsufficientPrivilege: must be owner of table orders",
                           "index": None, "sqlstate": "42501"})
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert rec.runs[-1]["error"] == ("fixer not enrolled or missing privileges on this project: 42501 "
                                     "(InsufficientPrivilege: must be owner of table orders)")


def _blocked_work(request="verify", kind="verify", reason="privileges"):
    w = work(request=request, state="blocked")
    w.action.update(blocked_kind=kind, blocked_report=json.dumps({"blocked_reason": reason}), blocked_error=None)
    return w


def test_privilege_block_is_retried_with_an_unblocked_event():
    rec = Recorder([_blocked_work()])
    out = fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert out == {"verified": 1}
    assert ("a1", "blocked", "verifying") in rec.states
    assert [e["event_type"] for e in rec.events][:2] == ["unblocked", "verify_started"]


def test_allowlist_block_stays_terminal():
    rec = Recorder([_blocked_work(reason="not_allowed")])
    out = fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert out == {"stale": 1}
    assert not any(t == "verifying" for _, _, t in rec.states)


def test_block_reports_say_why():
    rec = Recorder([work(request="apply", state="verified")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec, execute={"outcome": "apply_failed", "error": "x", "index": None,
                                                                    "sqlstate": "42501"}), now=NOW)
    assert json.loads(rec.runs[-1]["report"])["blocked_reason"] == "privileges"
    rec = Recorder([work(sql="DROP TABLE hc_test.audit_events")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.runs[-1]["outcome"] == "blocked"
    assert json.loads(rec.runs[-1]["report"])["blocked_reason"] == "not_allowed"


def test_credential_permission_denied_blocks():
    from databricks.sdk.errors import PermissionDenied
    from lakebase_health.fixer.pg import FixerPg
    w = type("W", (), {})()
    w.postgres = type("P", (), {"generate_database_credential": lambda self, endpoint: (_ for _ in ()).throw(
        PermissionDenied("no credential"))})()
    rec = Recorder([work(request="apply", state="verified")])
    d = deps(rec)
    d["session"] = FixerPg(w, "fixer", connect=lambda **k: None).session
    out = fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert out == {"blocked": 1} and json.loads(rec.runs[-1]["report"])["blocked_reason"] == "privileges"


# Live criterion 4: a DROP INDEX verify looked for the index name in query text and found nothing.
def test_drop_index_verify_samples_the_indexed_table_and_explains_empty_samples():
    drop = "DROP INDEX CONCURRENTLY hc_test.customers_region_idx;"
    w = work(sql=drop)
    w.action["rollback_sql"] = "CREATE INDEX CONCURRENTLY customers_region_idx ON hc_test.customers USING btree (region);"
    rec = Recorder([w])
    d = deps(rec)
    asked = []
    d["samples"] = lambda sql, cfg, project_id, table: asked.append(table) or []
    d["no_samples"] = lambda sql, cfg, project_id, table: f"0 plans on {table}; 812 redacted"
    d["judge"] = fix.verify.judge
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert asked == ["hc_test.customers"]
    report = json.loads(rec.runs[-1]["report"])
    [rule] = [r for r in report["rules"] if r["rule"] == "affected_queries"]
    assert not rule["passed"] and "812 redacted" in rule["detail"] and report["samples"] == "0 plans on hc_test.customers; 812 redacted"


def test_write_queries_go_through_the_alternating_probe_and_the_report_keeps_the_samples():
    rec = Recorder([work()])
    d = deps(rec)
    d["samples"] = lambda sql, cfg, p, t: [
        {"queryid": "r", "query": "SELECT 1", "params": [[]], "is_write": False},
        {"queryid": "w", "query": "INSERT INTO t VALUES (1)", "params": [[]], "is_write": True}]
    seen = []
    d["measure"] = lambda conn, q, params, repeats=3: seen.append((q, repeats)) or {"ms": 4.0, "nodes": [], "indexes": []}
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    assert seen.count(("INSERT INTO t VALUES (1)", 1)) == 6 and seen.count(("SELECT 1", 3)) == 2
    report = json.loads(rec.runs[-1]["report"])
    [w] = [q for q in report["queries"] if q["queryid"] == "w"]
    assert w["before_samples_ms"] == [4.0, 4.0, 4.0] and w["after_samples_ms"] == [4.0, 4.0, 4.0]


def test_create_index_verify_judges_writes_against_the_new_index_columns_without_storing_query_text():
    rec = Recorder([work()])
    d = deps(rec)
    d["samples"] = lambda sql, cfg, p, t: [
        {"queryid": "w", "query": "UPDATE hc_test.orders SET status = 'x'", "params": [[]], "is_write": True}]
    d["measure"] = lambda conn, q, params, repeats=3: {"ms": 4.0, "nodes": [], "indexes": []}
    fix.run_fixer(None, None, CFG, "fixer", deps=d, now=NOW)
    [(kind, kw, before)] = rec.judged_with
    assert kind == "create_index" and kw == {"index_columns": ["customer_id"]}
    assert before["w"]["query"].startswith("UPDATE")  # the judge needs the statement kind and SET list
    assert "UPDATE" not in rec.runs[-1]["report"]  # the report never carries query text
