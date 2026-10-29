import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from lakebase_health import ledger
from lakebase_health.config import EngineConfig
from lakebase_health.fixer.draft.context import NoQueryText
from lakebase_health.fixer.draft.llm import ModelError
from lakebase_health.fixer.params import Unsampled
from lakebase_health.fixer.draft.states import draft_token
from lakebase_health.fixer.draft.store import DraftWork
from lakebase_health.fixer.queue import Work, sql_hash, staleness
from lakebase_health.jobs import fix
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
T = CFG.table
NOW = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
FP = "f" * 32
ORIGINAL = ("SELECT o.id, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 AND o.status <> 'void' "
            "AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 WHERE o2.id BETWEEN $1 AND $2 "
            "AND o2.status = o.status) ORDER BY o.id")
REWRITE = ("WITH a AS (SELECT status, avg(amount) AS m FROM hc_test.orders WHERE id BETWEEN $1 AND $2 GROUP BY status) "
           "SELECT o.id, o.amount FROM hc_test.orders o JOIN a ON a.status = o.status WHERE o.id BETWEEN $1 AND $2 "
           "AND o.status <> $3 AND o.amount > a.m ORDER BY o.id")
GOOD = json.dumps({"rewrite": REWRITE, "rationale": "Aggregates once per status instead of once per row.",
                   "expected_plan_change": "SubPlan replaced by a Hash Join over a HashAggregate"})
SETS = [["1600004111", "1600006111"], ["1600200000", "1600202000"]]


def work(attempt=1, last=0, last_state=None, project="p1", apid="ap1"):
    return DraftWork({"approval_id": apid, "fingerprint": FP, "sql_hash": draft_token(FP, attempt),
                      "approver": "owner@x", "requested_at": NOW - timedelta(minutes=1)},
                     {"fingerprint": FP, "status": "open", "check_id": "top_db_time_query", "project_id": project,
                      "branch_id": "br1", "endpoint_id": "ep-a", "object": "42", "evidence": '{"queryid": "42"}',
                      "measure": 60.0}, last, last_state)


class Model:
    def __init__(self, reply=GOOD, error=None):
        self.reply, self.error, self.sent = reply, error, []

    def complete(self, msgs, draft_id, record):
        self.sent.append(msgs)
        record({"call_id": "c1", "draft_id": draft_id, "endpoint": "ep", "prompt_hash": "p" * 64,
                "response_hash": None if self.error else "r" * 64, "input_tokens": 900, "output_tokens": 80,
                "latency_ms": 1500.0, "status": "429" if self.error else "ok", "request_id": "req", "called_at": None})
        if self.error:
            raise self.error
        return self.reply


def deps(items, model=None, rewrite_rows=None, branch_error=None, source_error=None, bind_error=None, draw_error=None,
         ready=True, fixes=(), source_query=None, original_rows=1803, original_ms=512.0):
    seen = {"branch_closed": 0, "sessions": [], "checksums": [], "drawn": []}

    @contextmanager
    def branch(w, project, source, did):
        if branch_error:
            raise branch_error
        try:
            yield {"branch": f"{project}/branches/lbh-verify-{did[:8]}", "endpoint": "e-branch", "host": "h"}
        finally:
            seen["branch_closed"] += 1

    @contextmanager
    def session(endpoint, host, db, kind):
        seen["sessions"].append((endpoint, db, kind))
        yield object()

    def source(sql, cfg, session, tgt, finding):
        if source_error:
            raise source_error
        return {"queryid": "42", "query": source_query or ORIGINAL, "database": "app", "dbid": 16407, "mean_rows": 900.0,
                "origin": "pg_stat_statements"}

    def bindings(conn, q):
        if bind_error:
            raise bind_error
        return []

    def draw(conn, q, mean_rows):
        seen["drawn"].append((q, mean_rows))
        if draw_error:
            raise draw_error
        return [list(x) for x in SETS]

    def checksum(conn, q, values, ordered, timeout_ms=None):
        seen["checksums"].append((q, tuple(values), ordered))
        seen.setdefault("timeouts", []).append(("checksum", q == ORIGINAL, timeout_ms))
        rows = (rewrite_rows or {}).get(tuple(values), original_rows) if q != ORIGINAL else original_rows
        return {"rows": rows, "checksum": f"c{rows}"}

    return seen, {
        "pending": lambda sql, cfg: list(fixes), "stuck": lambda sql, cfg, now: [],
        "pending_drafts": lambda sql, cfg: list(items), "stuck_drafts": fix.dstore.stuck_drafts,
        "probe": lambda w, cfg, now: {"endpoint": cfg.llm_endpoint, "enabled": True, "ready": ready,
                                      "reason": None if ready else "turned off", "checked_at": now},
        "record_run": fix.queue.record_run, "events": fix.queue.events,
        "draft_target": lambda w, f: {"project": "projects/x", "branch": "projects/x/branches/production",
                                      "endpoint": "projects/x/branches/production/endpoints/primary", "host": "h",
                                      "databases": ["postgres", "app"]},
        "branch": branch, "session": session, "source": source, "bindings": bindings, "params": draw,
        "generic_plan": lambda conn, q: {"Plan": {"Node Type": "Index Scan", "Index Cond": "(id >= $1)"}},
        "catalog": lambda conn, tables: [{"name": "hc_test.orders", "row_estimate": 900000}],
        "model": model or Model(), "checksum": checksum,
        "measure": lambda conn, q, params, read_only=False, timeout_ms=None: (
            seen.setdefault("timeouts", []).append(("measure", q == ORIGINAL, timeout_ms, read_only)),
            {"ms": original_ms if q == ORIGINAL else 2.0, "nodes": [], "indexes": []})[1],
        "plan_shape": lambda conn, q, values: ({"nodes": ["Index Scan", "SubPlan"], "queryid": "42"} if q == ORIGINAL
                                               else {"nodes": ["Hash Join", "HashAggregate"], "queryid": "-77"}),
    }


def run(items, **kw):
    sql = FakeSql()
    seen, d = deps(items, **kw)
    out = fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    return sql, seen, d, out


def drafts(sql):
    return sorted(sql.tables.get(T("drafts"), []), key=lambda r: r["attempt"])


def test_a_verified_rewrite_end_to_end():
    sql, seen, d, out = run([work()])
    assert out == {"verified": 1}
    [draft] = drafts(sql)
    did = draft["draft_id"]
    assert draft["state"] == "verified" and draft["attempt"] == 1 and draft["reason"] is None
    assert draft["action_id"] == did and draft["prompt_version"] == "rewrite_v2" and len(draft["context_hash"]) == 64
    assert draft["rationale"].startswith("Aggregates once") and "Hash Join" in draft["expected_plan_change"]
    [action] = sql.tables[T("proposed_actions")]
    assert action["action_id"] == did and action["origin"] == "llm" and action["state"] == "verified"
    assert action["sql"] == REWRITE.replace("$3", "'void'")  # the original's literal put back, ready to copy
    claimed, verify_run = sql.tables[T("action_runs")]
    assert (claimed["approval_id"], claimed["kind"], claimed["outcome"]) == ("ap1", "draft", "claimed")
    assert (verify_run["approval_id"], verify_run["kind"], verify_run["outcome"]) == (None, "verify", "verified")
    report = json.loads(verify_run["report"])
    assert [r["rule"] for r in report["rules"]] == ["equivalence", "speed", "plan"] and report["ordered"] is True
    assert report["sets"][0] == {"set": 1, "rows_original": 1803, "rows_rewrite": 1803, "same_checksum": True,
                                 "original_ms": 512.0, "rewrite_ms": 2.0}
    assert report["rewrite_queryid"] == "-77" and report["original_queryid"] == "42" and len(report["rewrite_key"]) == 64
    assert report["branch"] == f"projects/x/branches/lbh-verify-{did[:8]}" and seen["branch_closed"] == 1
    for text in ("1600004111", "void", "hc_test.orders", "SELECT"):
        assert text not in verify_run["report"], text
    events = sql.tables[T("ledger")]
    assert [e["event_type"] for e in events] == ["draft_requested", "draft_started", "drafted", "verify_started",
                                                "verified"]
    assert {e["action_id"] for e in events} == {did} and ledger.verify(events)
    [call] = sql.tables[T("llm_calls")]
    assert call["draft_id"] == did and call["called_at"] == NOW
    payload = json.dumps(d["model"].sent)
    assert not any(v in payload for v in ("1600004111", "1600006111", "1600200000", "'void'"))
    assert seen["sessions"] == [("projects/x/branches/production/endpoints/primary", "app", "draft_context"),
                                ("e-branch", "app", "query_rewrite")]
    assert seen["drawn"] == [(ORIGINAL, 900.0)]  # values drawn on the verification branch, from the data
    assert json.loads(d["model"].sent[0][1]["content"])["plan"] == {"Plan": {"Node Type": "Index Scan",
                                                                             "Index Cond": "(id >= $1)"}}
    assert sql.tables[T("llm_status")][0]["ready"] is True


def test_retry_after_a_final_attempt_creates_the_next_attempt_and_keeps_the_first():
    sql = FakeSql()
    seen, d = deps([work()], model=Model(reply="not json"))
    fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    d["pending_drafts"] = lambda s, c: [work(attempt=2, last=1, last_state="draft_failed", apid="ap2")]
    d["model"] = Model()
    fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW + timedelta(minutes=5))
    first, second = drafts(sql)
    assert (first["attempt"], first["state"], first["reason"]) == (1, "draft_failed", "invalid_response: not JSON")
    assert (second["attempt"], second["state"]) == (2, "verified")
    assert first["draft_id"] != second["draft_id"] and len(sql.tables[T("proposed_actions")]) == 1


def test_requests_that_cannot_draft_are_consumed_with_the_reason():
    for item, ready, reason in ((work(attempt=2, last=1, last_state="verifying"), True, "in_progress"),
                                (work(attempt=1, last=1, last_state="verified"), True, "attempt_changed"),
                                (work(), False, "not_set_up")):
        sql, seen, d, out = run([item], ready=ready)
        [r] = sql.tables[T("action_runs")]
        assert (r["approval_id"], r["action_id"], r["outcome"]) == ("ap1", None, f"stale:{reason}")
        assert T("drafts") not in sql.tables and T("ledger") not in sql.tables and d["model"].sent == []
        assert out == {"stale": 1}


def failed(**kw):
    sql, seen, d, out = run([work()], **kw)
    [draft] = drafts(sql)
    events = [e["event_type"] for e in sql.tables[T("ledger")]]
    return draft, events, sql, out


def test_model_failures_end_draft_failed_with_the_reason():
    draft, events, sql, out = failed(model=Model(error=ModelError("model_unavailable", "429")))
    assert (draft["state"], draft["reason"]) == ("draft_failed", "model_unavailable") and out == {"draft_failed": 1}
    assert events == ["draft_requested", "draft_started", "draft_failed"]
    assert sql.tables[T("llm_calls")][0]["status"] == "429" and T("proposed_actions") not in sql.tables
    assert failed(model=Model(reply='{"rewrite": null, "reason": "already optimal"}'))[0]["reason"] == \
        "no_safe_rewrite: already optimal"
    assert failed(model=Model(error=ModelError("not_set_up", "403")))[0]["reason"] == "not_set_up"


def test_static_check_failures_never_reach_a_branch():
    bad = json.dumps({"rewrite": f"WITH d AS (DELETE FROM hc_test.orders RETURNING 1) {ORIGINAL.replace(chr(39) + 'void' + chr(39), '$3')}",
                      "rationale": "x", "expected_plan_change": "x"})
    draft, events, sql, out = failed(model=Model(reply=bad))
    assert draft["state"] == "draft_failed" and draft["reason"].startswith("data_modifying_cte")
    assert T("proposed_actions") not in sql.tables


def test_untokenizable_source_text_records_the_error_type_and_never_a_literal():
    """Final review I-2: sqlglot's TokenError quotes the surrounding SQL, so a literal reached drafts.reason, the
    ledger and every project viewer."""
    bad = "SELECT o.id FROM hc_test.orders o WHERE o.ssn = '123-45-6789' AND o.note = $$unterminated"
    draft, events, sql, out = failed(source_query=bad)
    assert (draft["state"], draft["reason"]) == ("draft_failed", "not_rewritable: SqlTextError")
    assert "123-45-6789" not in json.dumps(sql.tables, default=str)
    assert T("llm_calls") not in sql.tables


def test_an_unexpected_draft_error_is_recorded_by_type_only():
    draft, events, sql, out = failed(source_error=ValueError("invalid input syntax for type integer: \"123-45-6789\""))
    assert draft["reason"] == "error: ValueError"
    assert "123-45-6789" not in json.dumps(sql.tables, default=str)


def test_a_rewrite_is_not_verified_when_no_parameter_set_returns_rows():
    """Final review I-3: equal empty results proved nothing, yet read as verified."""
    sql, seen, d, out = run([work()], original_rows=0)
    assert out == {"verify_failed": 1}
    [draft] = drafts(sql)
    assert draft["state"] == "verify_failed" and draft["reason"].startswith("no_informative_samples")
    assert sql.tables[T("proposed_actions")][0]["state"] == "verify_failed"


def test_the_rewrite_runs_read_only_under_ten_times_the_originals_median_clamped_to_5_to_60_s():
    """Final review I-4: model SQL ran up to 10 min per execution, for hours in all, blocking every fix."""
    for original_ms, want in ((512.0, 5120), (100.0, 5000), (20000.0, 60000)):
        sql, seen, d, out = run([work()], original_ms=original_ms)
        rewrite = [t for t in seen["timeouts"] if not t[1]]
        assert rewrite and all(t[2] == want for t in rewrite), (original_ms, rewrite)
        assert all(t[3] is True for t in seen["timeouts"] if t[0] == "measure")
        assert all(t[2] == 60000 for t in seen["timeouts"] if t[1]), "the original runs under the 60 s ceiling"
    assert ("e-branch", "app", "query_rewrite") in seen["sessions"]


def test_verification_stops_at_the_15_minute_budget():
    sql, seen, d = FakeSql(), *deps([work()])
    ticks = iter([0.0, 10.0, 901.0, 901.0, 901.0, 901.0, 901.0, 901.0])
    d["clock"] = lambda: next(ticks, 901.0)
    out = fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    assert out == {"verify_failed": 1}
    [draft] = drafts(sql)
    assert draft["reason"].startswith("timeout") and len(seen["checksums"]) < 2 * 3


def test_a_rewrite_that_hits_its_statement_timeout_fails_as_timeout():
    sql, seen, d = FakeSql(), *deps([work()])
    d["checksum"] = lambda conn, q, values, ordered, timeout_ms=None: (
        {"rows": 1803, "checksum": "c"} if q == ORIGINAL else {"rows": None, "checksum": None, "error": "QueryCanceled"})
    fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    [draft] = drafts(sql)
    assert draft["state"] == "verify_failed" and draft["reason"].startswith("timeout")


def test_no_query_text_fails_before_the_model_is_called():
    draft, events, sql, out = failed(source_error=NoQueryText("no pg_stat_statements entry for queryid 42"))
    assert (draft["state"], draft["reason"]) == ("draft_failed", "no_query_text: no pg_stat_statements entry for queryid 42")
    assert T("llm_calls") not in sql.tables and events == ["draft_requested", "draft_started", "draft_failed"]


def test_a_parameter_bound_to_no_column_is_no_samples_before_the_model_is_called():
    draft, events, sql, out = failed(bind_error=Unsampled("parameter $2 is not bound to a column"))
    assert (draft["state"], draft["reason"]) == ("draft_failed", "no_samples: parameter $2 is not bound to a column")
    assert T("llm_calls") not in sql.tables and T("proposed_actions") not in sql.tables


def test_no_rows_to_draw_values_from_on_the_branch_is_verify_failed_no_samples():
    draft, events, sql, out = failed(draw_error=Unsampled("no rows with values in hc_test.orders"))
    assert (draft["state"], draft["reason"]) == ("verify_failed", "no_samples: no rows with values in hc_test.orders")
    assert events[-1] == "verify_failed"


def test_sampled_values_never_reach_the_model_the_reports_the_drafts_or_the_ledger():
    sql, seen, d, out = run([work()])
    assert out == {"verified": 1}
    values = [v for s in SETS for v in s]
    written = json.dumps({k: v for k, v in sql.tables.items() if k != T("proposed_actions")}, default=str)
    payload = json.dumps(d["model"].sent)
    for v in values:
        assert v not in payload and v not in written, v
    assert not any(v in json.dumps(a, default=str) for a in sql.tables[T("proposed_actions")] for v in values)


def test_a_non_equivalent_rewrite_is_verify_failed_with_the_rule():
    draft, events, sql, out = failed(rewrite_rows={tuple(SETS[1]): 1700})
    assert draft["state"] == "verify_failed" and draft["reason"] == "equivalence: set 2: 1803 rows vs 1700"
    assert sql.tables[T("proposed_actions")][0]["state"] == "verify_failed"
    assert events[-1] == "verify_failed" and out == {"verify_failed": 1}


def test_branch_unavailable_is_verify_failed():
    draft, events, sql, out = failed(branch_error=RuntimeError("branch limit"))
    assert draft["state"] == "verify_failed" and draft["reason"] == "branch_unavailable: RuntimeError: branch limit"


def test_one_branch_per_project_per_sweep_across_fixes_and_drafts():
    fixw = Work({"approval_id": "apx", "action_id": "a" * 32, "request": "reject", "sql_hash": "", "approver": "o"},
                {"action_id": "a" * 32, "state": "proposed", "sql": "x", "project_id": "p1", "kind": "ddl"}, "open")
    sql = FakeSql()
    seen, d = deps([work(project="p1"), work(project="p2", apid="ap2")], fixes=[fixw])
    d.update(claim=lambda *a: True, set_state=lambda *a: None)
    out = fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    assert out == {"rejected": 1, "verified": 1}
    assert [r["approval_id"] for r in sql.tables[T("action_runs")] if r["kind"] == "draft"] == ["ap2"]


def test_an_interrupted_draft_is_recovered_so_retry_is_offered():
    stuck = {"draft_id": "d" * 32, "finding_fingerprint": FP, "attempt": 1, "state": "drafting",
             "updated_at": NOW - timedelta(hours=5)}
    sql = FakeSql([(r"FROM cat\.lakebase_health\.drafts WHERE state IN", [stuck])])
    seen, d = deps([])
    out = fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    [row] = sql.tables[T("drafts")]
    assert (row["state"], row["reason"]) == ("draft_failed", "interrupted: the fixer job stopped")
    assert out == {"recovered:draft_failed": 1}
    assert [e["event_type"] for e in sql.tables[T("ledger")]] == ["draft_failed"]


def test_fix_requests_on_a_rewrite_action_are_refused_as_advisory():
    w = Work({"approval_id": "ap", "action_id": "d" * 32, "request": "apply", "sql_hash": sql_hash("SELECT 1"),
              "approver": "o"}, {"action_id": "d" * 32, "state": "verified", "sql": "SELECT 1", "kind": "query_rewrite",
                                 "verify_on_branch": True, "expires_at": NOW + timedelta(days=1)}, "open")
    assert staleness(w, NOW) == "advisory"


def test_a_sweep_without_drafts_records_the_endpoint_status_once_so_the_first_request_is_accepted():
    """Live acceptance: the app refuses a draft until llm_status has a row, and the fixer probed only when a draft was
    pending, so no first request could ever be recorded."""
    sql = FakeSql()
    seen, d = deps([])
    d["status_due"] = fix.dstore.status_due
    assert fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW) == {}
    assert sql.tables[T("llm_status")][0]["ready"] is True
    assert sql.ran(r"SELECT ready, checked_at FROM cat\.lakebase_health\.llm_status WHERE endpoint = :e")
    known = FakeSql([(r"FROM cat\.lakebase_health\.llm_status", [{"ready": True, "checked_at": NOW - timedelta(minutes=59)}])])
    d["probe"] = lambda *a: (_ for _ in ()).throw(AssertionError("no probe while a ready status is under an hour old"))
    fix.run_fixer(known, None, CFG, "fixer", deps=d, now=NOW)
    assert T("llm_status") not in known.tables


def test_a_not_ready_status_is_probed_again_on_the_next_sweep_without_any_draft_pending():
    """Final review I-1: once a probe recorded ready=false (e.g. no CAN_QUERY yet) the app refused every request and
    the fixer never probed again, so granting CAN_QUERY changed nothing."""
    for row in ({"ready": False, "checked_at": NOW - timedelta(minutes=1)},
                {"ready": "false", "checked_at": NOW - timedelta(minutes=1)},
                {"ready": True, "checked_at": NOW - timedelta(minutes=61)},
                {"ready": True, "checked_at": None}):
        sql = FakeSql([(r"FROM cat\.lakebase_health\.llm_status", [row])])
        seen, d = deps([])
        d["status_due"] = fix.dstore.status_due
        assert fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW) == {}
        assert sql.tables[T("llm_status")][0]["ready"] is True, row
        assert sql.tables[T("llm_status")][0]["checked_at"] == NOW
