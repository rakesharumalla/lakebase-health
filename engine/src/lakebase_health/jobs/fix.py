import argparse
import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

from ..config import EngineConfig, pii_masking_enabled
from ..fixer import apply as applymod
from ..fixer import branch as branchmod
from ..fixer import params as fparams
from ..fixer import pgss, queue, samples, verify
from ..fixer.allowlist import NotAllowed, parse
from ..fixer.draft import context as dcontext
from ..fixer.draft import llm as dllm
from ..fixer.draft import prompt as dprompt
from ..fixer.draft import static as dstatic
from ..fixer.draft import store as dstore
from ..fixer.draft import verify as dverify
from ..fixer.draft.sqltext import SqlTextError, denormalize, normalize, space_params, text_key
from ..fixer.pg import CREDENTIAL_DENIED, FixerPg
from ..fixer.states import NO_DATABASE, NOT_ALLOWED, PRIVILEGES, START

log = logging.getLogger(__name__)

# statement timeout (2h) + margin for applies; branch TTL (4h) + margin for verification
STUCK_AFTER = {"verifying": timedelta(hours=4, minutes=15),
               "applying": timedelta(hours=2, minutes=15),
               "rolling_back": timedelta(hours=2, minutes=15)}
FAILED = {"verify": "verify_failed", "apply": "apply_failed", "rollback": "rollback_failed"}
REQUEST_OF = {"verifying": "verify", "applying": "apply", "rolling_back": "rollback"}
DENIED = {"42501", "28000", "28P01", CREDENTIAL_DENIED}  # insufficient_privilege, invalid authorization / password,
# or no Lakebase credential for the fixer


def _utc(dt):
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _stuck(sql, cfg, now):
    states = ", ".join(f"'{s}'" for s in STUCK_AFTER)
    rows = sql.query(f"SELECT a.*, f.project_id, f.branch_id, f.object, f.evidence FROM {cfg.table('proposed_actions')} a "
                     f"LEFT JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint "
                     f"WHERE a.state IN ({states})")
    return [r for r in rows if r.get("updated_at") and now - _utc(r["updated_at"]) > STUCK_AFTER[r["state"]]]


class NoDatabase(Exception):
    """The finding does not say which database its object is in (or that database is gone): never guess."""


def finding_database(action, databases):
    """The finding's database: the evidence's datname, else the object's "<db>:" prefix (per-database checks),
    matched against the branch's databases. None when neither names an existing database."""
    ev = action.get("evidence")
    if isinstance(ev, str):
        try:
            ev = json.loads(ev)
        except ValueError:
            ev = None
    datname = ev.get("datname") if isinstance(ev, dict) else None
    if datname:
        return datname if datname in databases else None
    obj = action.get("object") or ""
    matches = [d for d in databases if obj.startswith(f"{d}:")]
    return max(matches, key=len) if matches else None


def _target(w, action):
    proj = next(p for p in w.postgres.list_projects() if p.uid == action["project_id"])
    branch = next(b for b in w.postgres.list_branches(parent=proj.name) if b.uid == action["branch_id"])
    ep = next(e for e in w.postgres.list_endpoints(parent=branch.name) if str(e.status.endpoint_type).endswith("READ_WRITE"))
    dbs = [d.status.postgres_database for d in w.postgres.list_databases(parent=branch.name) if d.status]
    db = finding_database(action, dbs)
    if db is None:
        raise NoDatabase(f"cannot tell which database the finding is in (object {action.get('object')!r}); "
                         "refusing to guess")
    return {"project": proj.name, "branch": branch.name, "endpoint": ep.name, "host": ep.status.hosts.host, "database": db}


def _draft_target(w, finding):
    """Where a draft reads its statement: the finding's own compute (pg_stat_statements is per compute), else the
    branch's read-write compute, and every database of the branch."""
    proj = next(p for p in w.postgres.list_projects() if p.uid == finding["project_id"])
    branch = next(b for b in w.postgres.list_branches(parent=proj.name) if b.uid == finding["branch_id"])
    eps = list(w.postgres.list_endpoints(parent=branch.name))
    ep = next((e for e in eps if e.uid == finding.get("endpoint_id")), None) or \
        next(e for e in eps if str(e.status.endpoint_type).endswith("READ_WRITE"))
    dbs = [d.status.postgres_database for d in w.postgres.list_databases(parent=branch.name) if d.status]
    return {"project": proj.name, "branch": branch.name, "endpoint": ep.name, "host": ep.status.hosts.host,
            "databases": dbs}


def default_deps(sql, w, user, cfg=None, llm_w=None):
    pg = FixerPg(w, user)
    cfg = cfg or EngineConfig(catalog="unused")
    model = dllm.ModelClient(dllm.sdk_query(llm_w or w), cfg.llm_endpoint, test_response=cfg.llm_test_response)
    return {"pending": queue.pending, "stuck": _stuck, "claim": queue.claim, "set_state": queue.set_state,
            "record_run": queue.record_run, "events": queue.events, "branch": branchmod.verification_branch,
            "session": pg.session, "target": _target, "samples": samples.affected_queries, "measure": verify.measure,
            "judge": verify.judge, "execute": applymod.execute, "recover": applymod.recover,
            "pending_drafts": dstore.pending_drafts, "stuck_drafts": dstore.stuck_drafts, "probe": dllm.probe,
            "status_due": dstore.status_due,
            "draft_target": _draft_target, "source": dcontext.load_source, "bindings": fparams.check,
            "generic_plan": pgss.generic_plan, "params": fparams.sample, "catalog": dcontext.load_catalog,
            "model": model, "checksum": dverify.checksum, "plan_shape": dverify.plan_shape}


def _run_row(work, kind, now):
    return {"run_id": uuid.uuid4().hex, "approval_id": work.approval["approval_id"], "action_id": work.action["action_id"],
            "kind": kind, "started_at": now, "ended_at": None, "outcome": None, "report": None, "branch": None, "error": None}


class Denied(Exception):
    def __init__(self, sqlstate, detail=None):
        super().__init__(sqlstate)
        self.sqlstate, self.detail = sqlstate, detail


def _denied_sqlstate(e):
    """sqlstate when the fixer lacks privileges or cannot authenticate (on the error or its cause), else None."""
    for err in (e, getattr(e, "__cause__", None)):
        state = getattr(err, "sqlstate", None)
        if state in DENIED:
            return state
    return None


def _raise_if_denied(res):
    if res.get("outcome") != "applied" and res.get("sqlstate") in DENIED:
        raise Denied(res["sqlstate"], res.get("error"))  # the Postgres message names the missing privilege


def _measure_all(d, conn, queries):
    """Reads: 3 warm repetitions each (median). Writes: the alternating write probe (verify.measure_writes)."""
    out = {q["queryid"]: {**d["measure"](conn, q["query"], q["params"]), "is_write": False}
           for q in queries if not q["is_write"]}
    writes = [q for q in queries if q["is_write"]]
    if writes:
        text = {q["queryid"]: q["query"] for q in writes}  # kept for the judge only; the report holds ids and timings
        out.update({k: {**m, "is_write": True, "query": text[k]}
                    for k, m in verify.measure_writes(d["measure"], conn, writes).items()})
    return out


def index_columns(create_sql):
    """Column names of a CREATE INDEX CONCURRENTLY ... (cols) statement (the allowlist accepts plain columns only)."""
    m = re.search(r"\(([^()]*)\)\s*;?\s*$", create_sql or "")
    if not m:
        return None
    cols = [re.sub(r"(?i)\s+(ASC|DESC)$", "", c.strip()) for c in m.group(1).split(",")]
    return [c[1:-1].replace('""', '"') if c.startswith('"') else c.lower() for c in cols if c]


def _sampled(d, conn, candidates):
    """The candidates whose parameters could be drawn from the branch's data, and why the others could not."""
    queries, unsampled = [], []
    for q in candidates:
        try:
            queries.append({**q, "params": d["params"](conn, q["query"], q.get("mean_rows"))})
        except fparams.Unsampled as e:
            unsampled.append(f"query {q['queryid']}: {e}")
    return queries, unsampled


def _verify(d, sql, w, cfg, work, st, tgt):
    """Returns (passed, report, error). The report holds query ids, timings, rules and the branch name only.
    Statements come from pg_stat_statements on the source branch (read-only), parameter values from the data on
    the verification branch, before the fix runs there."""
    a = work.action
    table = samples.sample_table(st, a)
    candidates = []
    if table:
        with d["session"](tgt["endpoint"], tgt["host"], tgt["database"], "stats") as conn:
            candidates = d["samples"](conn, table, tgt["database"])
    report = {"branch": None, "queries": [], "rules": []}
    entered = False
    try:
        with d["branch"](w, tgt["project"], tgt["branch"], a["action_id"]) as b:
            entered = True
            report["branch"] = b["branch"]
            with d["session"](b["endpoint"], b["host"], tgt["database"], st.kind) as conn:
                queries, unsampled = _sampled(d, conn, candidates)
                if not queries and st.kind in ("create_index", "drop_index"):
                    report["samples"] = samples.no_samples_reason(len(candidates), unsampled, table, tgt["database"])
                before = _measure_all(d, conn, queries)
                res = d["execute"](conn, a["sql"], a["action_id"])
                _raise_if_denied(res)
                if res["outcome"] != "applied":
                    return False, report, f"fix failed on the branch: {res['error']}"
                after = _measure_all(d, conn, queries)
    except Exception as e:
        if entered or _denied_sqlstate(e):
            raise
        return False, report, f"branch_unavailable: {type(e).__name__}: {e}"[:1000]  # no retry; approver may re-request
    cols = index_columns(a["sql"]) if st.kind == "create_index" else None
    verdict = d["judge"](st.kind, res.get("index") or st.target, before, after,
                         **({"index_columns": cols} if cols else {}))
    report["rules"] = verdict["rules"]
    if report.get("samples"):
        for r in report["rules"]:
            if r.get("rule") == "affected_queries" and not r.get("passed"):
                r["detail"] = f"{r['detail']}: {report['samples']}"
    report["queries"] = [{"queryid": k, "before_ms": before[k].get("ms"), "after_ms": after.get(k, {}).get("ms"),
                          "indexes_after": after.get(k, {}).get("indexes", []),
                          **({"is_write": True, "before_samples_ms": before[k].get("samples_ms"),
                              "after_samples_ms": after.get(k, {}).get("samples_ms")} if before[k].get("is_write") else {})}
                         for k in before]
    return verdict["passed"], report, None


def _bump(counts, key):
    counts[key] = counts.get(key, 0) + 1


def _handle(d, sql, w, cfg, work, now, counts):
    """Processes one approval. Every path that changes state writes its ledger events in one events() call."""
    a, ap = work.action, work.approval
    aid, request = a["action_id"], ap["request"]
    run = _run_row(work, request, now)
    events = []

    def event(event_type, payload):
        events.append({"action_id": aid, "event_type": event_type, "payload": payload})

    def finish(outcome, report=None):
        # After a claim the approval row is already consumed; the outcome row is approval-less and names it in the report.
        if claimed:
            run.update(approval_id=None, report=json.dumps({**(report or {}), "approval_id": ap["approval_id"]}, default=str))
        run.update(outcome=outcome, ended_at=now)
        d["record_run"](sql, cfg, run)
        if events:
            d["events"](sql, cfg, events, now)
        _bump(counts, outcome.split(":")[0] if outcome.startswith("stale:") else outcome)

    claimed = False
    stale = queue.staleness(work, now)
    if stale:
        event("stale_approval", {"approval_id": ap["approval_id"], "reason": stale})
        return finish(f"stale:{stale}")
    if not d["claim"](sql, cfg, aid, a["state"], START[request], now):
        return finish("skipped:claim_lost")  # another sweep owns it: no state change, no event
    # Consume the approval before anything executes, so a crash can never let it run twice.
    d["record_run"](sql, cfg, {**run, "outcome": "claimed", "ended_at": now})
    claimed = True
    run = {**run, "run_id": uuid.uuid4().hex}
    if request == "reject":
        event("rejected", {"approval_id": ap["approval_id"], "approver": ap.get("approver")})
        return finish("rejected")
    if a["state"] == "blocked":  # a privilege/enrolment block; staleness() already refused any other block
        event("unblocked", {"approval_id": ap["approval_id"], "request": request})
    event(f"{request}_started", {"approval_id": ap["approval_id"], "approver": ap.get("approver")})
    stmt_sql = a["rollback_sql"] if request == "rollback" else a["sql"]
    try:
        st = parse(stmt_sql)
    except NotAllowed as e:
        d["set_state"](sql, cfg, aid, "blocked", now)
        run.update(error=str(e))
        event("blocked", {"reason": str(e)})
        return finish("blocked", {"blocked_reason": NOT_ALLOWED})
    report = {}
    try:
        tgt = d["target"](w, a)
    except NoDatabase as e:
        d["set_state"](sql, cfg, aid, "blocked", now)
        run.update(error=str(e)[:1000])
        event("blocked", {"reason": str(e)[:1000]})
        return finish("blocked", {"blocked_reason": NO_DATABASE})
    try:
        if request == "verify":
            passed, report, error = _verify(d, sql, w, cfg, work, st, tgt)
            outcome = "verified" if passed else "verify_failed"
            run.update(branch=report.get("branch"), error=error)
        else:
            with d["session"](tgt["endpoint"], tgt["host"], tgt["database"], st.kind) as conn:
                res = d["execute"](conn, stmt_sql, aid)
            _raise_if_denied(res)
            ok = res["outcome"] == "applied"
            outcome = ("applied" if ok else "apply_failed") if request == "apply" else ("rolled_back" if ok else "rollback_failed")
            run.update(error=res["error"])
            report = {"index": res.get("index")}
    except Exception as e:
        denied = _denied_sqlstate(e)
        outcome = "blocked" if denied else FAILED[request]
        # The Postgres message names the object and privilege; only its first line (no query text or values)
        raw = getattr(e, "detail", None) or (f"{type(e).__name__}: {e}" if denied and hasattr(e, "sqlstate") else None)
        detail = f" ({str(raw).splitlines()[0][:500]})" if denied and raw else ""
        run.update(error=f"fixer not enrolled or missing privileges on this project: {denied}{detail}"[:1500] if denied
                   else f"{type(e).__name__}: {e}"[:1000])
    if outcome == "blocked":
        report = {**report, "blocked_reason": PRIVILEGES}
    d["set_state"](sql, cfg, aid, outcome, now)
    event(outcome, {"run_id": run["run_id"], "error": run["error"]})
    finish(outcome, report)


def _recover(d, sql, w, cfg, row, now):
    if row["state"] == "verifying":
        outcome = "verify_failed"  # the branch has expired by now; the verification cannot be resumed
    else:
        stmt_sql = row["rollback_sql"] if row["state"] == "rolling_back" else row["sql"]
        try:
            tgt = d["target"](w, row)
        except NoDatabase:
            tgt = None  # never inspect another database's objects
        if tgt is None:
            outcome = "blocked"
        else:
            with d["session"](tgt["endpoint"], tgt["host"], tgt["database"], parse(stmt_sql).kind) as conn:
                res = d["recover"](conn, stmt_sql, row["action_id"])
            outcome = res if row["state"] == "applying" else ("rolled_back" if res == "applied" else "rollback_failed")
    d["set_state"](sql, cfg, row["action_id"], outcome, now)
    d["record_run"](sql, cfg, {"run_id": uuid.uuid4().hex, "approval_id": None, "action_id": row["action_id"],
                               "kind": REQUEST_OF[row["state"]], "started_at": now, "ended_at": now,
                               "outcome": f"recovered:{outcome}", "report": None, "branch": None, "error": None})
    d["events"](sql, cfg, [{"action_id": row["action_id"], "event_type": outcome, "payload": {"recovered": True}}], now)
    return outcome


def _draft_event(did, event_type, payload):
    return {"action_id": did, "event_type": event_type, "payload": payload}


def _first_failure(report) -> str:
    bad = next((r for r in report.get("rules", []) if not r.get("passed")), None)
    if bad and str(bad.get("detail", "")).startswith("no_informative_samples"):
        return bad["detail"][:1000]  # a reason of its own: nothing showed the rewrite wrong, nothing showed it right
    return f"{bad['rule']}: {bad['detail']}"[:1000] if bad else "verification failed"


def _verify_rewrite(d, w, tgt, src, did, rewrite, ordered, expected):
    """Returns (passed, report, error). Parameter values are drawn from the data on the verification branch and
    stay in this function: the report holds counts, checksums' verdicts, timings, plan node names and query ids
    only, never query text or values."""
    original = src["query"]
    report = {"branch": None, "ordered": ordered, "original_queryid": src["queryid"], "sets": [], "rules": []}
    sets, entered = [], False
    clock = d.get("clock", time.monotonic)
    started = clock()

    def over_budget():
        return clock() - started > dverify.BUDGET_S

    budget_error = f"timeout: verification exceeded its {dverify.BUDGET_S // 60} min budget"
    try:
        with d["branch"](w, tgt["project"], tgt["branch"], did) as b:
            entered = True
            report["branch"] = b["branch"]
            with d["session"](b["endpoint"], b["host"], src["database"], "query_rewrite") as conn:
                try:
                    param_sets = d["params"](conn, original, src.get("mean_rows"))
                except fparams.Unsampled as e:
                    return False, report, f"no_samples: {e}"[:1000]
                ceiling = dverify.TIMEOUT_MAX_MS
                for i, values in enumerate(param_sets, 1):
                    # Read only throughout; the original first, so the rewrite's timeout follows from its median.
                    if over_budget():
                        return False, report, budget_error
                    om = d["measure"](conn, original, [values], read_only=True, timeout_ms=ceiling)
                    limit = dverify.rewrite_timeout_ms(om.get("ms"))
                    if over_budget():
                        return False, report, budget_error
                    o = d["checksum"](conn, original, values, ordered, timeout_ms=ceiling)
                    if over_budget():
                        return False, report, budget_error
                    r = d["checksum"](conn, rewrite, values, ordered, timeout_ms=limit)
                    if over_budget():
                        return False, report, budget_error
                    rm = d["measure"](conn, rewrite, [values], read_only=True, timeout_ms=limit)
                    sets.append({"set": i, "original": o, "rewrite": r, "original_ms": om.get("ms"),
                                 "rewrite_ms": rm.get("ms"), "timeout_ms": limit,
                                 "canceled": [k for k, m in (("original", om), ("original", o), ("rewrite", r),
                                                             ("rewrite", rm)) if m.get("error") == "QueryCanceled"]})
                before = d["plan_shape"](conn, original, param_sets[0])
                after = d["plan_shape"](conn, rewrite, param_sets[0])
    except Exception as e:
        if entered or _denied_sqlstate(e):
            raise
        return False, report, f"branch_unavailable: {type(e).__name__}: {e}"[:1000]
    verdict = dverify.judge(sets, ordered, before, after, expected)
    report.update(rules=verdict["rules"], plan=verdict["plan"], rewrite_queryid=after.get("queryid"),
                  rewrite_key=text_key(rewrite),
                  sets=[{"set": x["set"], "rows_original": x["original"].get("rows"),
                         "rows_rewrite": x["rewrite"].get("rows"),
                         "same_checksum": x["original"].get("checksum") is not None
                         and x["original"].get("checksum") == x["rewrite"].get("checksum"),
                         "original_ms": x["original_ms"], "rewrite_ms": x["rewrite_ms"]} for x in sets])
    canceled = next((x for x in sets if x["canceled"]), None)
    if canceled:  # a statement timeout is a timeout, whatever else the rules say
        who = canceled["canceled"][0]
        return False, report, (f"timeout: the {who} query exceeded its statement timeout of "
                               f"{(canceled['timeout_ms'] if who == 'rewrite' else dverify.TIMEOUT_MAX_MS) / 1000:g} s "
                               f"(parameter set {canceled['set']})")
    return verdict["passed"], report, None


def _handle_draft(d, sql, w, cfg, work, now, counts, ready):
    """One draft approval: consume it, draft with the model, check statically, verify on a branch. Every path
    ends the attempt in a final state and writes its ledger events in one events() call."""
    ap, f = work.approval, work.finding
    stale = dstore.staleness(work, ready)
    if stale:  # no draft row and no ledger chain exist for a refused request: the run row records why
        d["record_run"](sql, cfg, {"run_id": uuid.uuid4().hex, "approval_id": ap["approval_id"], "action_id": None,
                                   "kind": "draft", "started_at": now, "ended_at": now, "outcome": f"stale:{stale}",
                                   "report": None, "branch": None, "error": None})
        return _bump(counts, "stale")
    draft = dstore.new_draft(work, cfg, now)
    did = draft["draft_id"]
    # Consume the approval before anything runs, so a crash can never draft it twice.
    d["record_run"](sql, cfg, {"run_id": uuid.uuid4().hex, "approval_id": ap["approval_id"], "action_id": did,
                               "kind": "draft", "started_at": now, "ended_at": now, "outcome": "claimed",
                               "report": None, "branch": None, "error": None})
    dstore.save_draft(sql, cfg, draft)
    events = [_draft_event(did, "draft_requested", {"approval_id": ap["approval_id"], "requested_by": ap.get("approver"),
                                                     "requested_at": ap.get("requested_at"), "attempt": draft["attempt"],
                                                     "fingerprint": f["fingerprint"]}),
              _draft_event(did, "draft_started", {"attempt": draft["attempt"], "endpoint": cfg.llm_endpoint})]

    def finish(state, reason=None):
        draft.update(state=state, reason=None if reason is None else str(reason)[:1000], updated_at=now, ended_at=now)
        dstore.save_draft(sql, cfg, draft)
        events.append(_draft_event(did, state, {"reason": draft["reason"]}))
        d["events"](sql, cfg, events, now)
        _bump(counts, state)

    try:
        tgt = d["draft_target"](w, f)
        src = d["source"](sql, cfg, d["session"], tgt, f)  # pg_stat_statements on the finding's compute
        norm, literal_values = normalize(src["query"])
        original = dstatic.shape(norm)
        with d["session"](tgt["endpoint"], tgt["host"], src["database"], "draft_context") as conn:
            d["bindings"](conn, src["query"])  # every $n bound to a column, or no_samples before the model call
            plan = dcontext.plan_context(d["generic_plan"](conn, norm))
            tables = d["catalog"](conn, original.tables)
        context = dcontext.build_context(f, norm, original.ordered, plan, tables)
        draft.update(prompt_version=dprompt.PROMPT_VERSION, context_hash=dprompt.context_hash(context))
        content = d["model"].complete(dprompt.messages(context), did,
                                      lambda row: dstore.record_llm_call(sql, cfg, {**row, "called_at": now}))
    except dcontext.NoQueryText as e:
        return finish("draft_failed", f"no_query_text: {e}")
    except fparams.Unsampled as e:
        return finish("draft_failed", f"no_samples: {e}")
    except dstatic.StaticError as e:
        return finish("draft_failed", f"not_rewritable: {e.detail}")  # the original is not one SELECT
    except dllm.ModelError as e:
        return finish("draft_failed", e.reason)
    except SqlTextError as e:  # by type only: the tokenizer's message quotes the source text, literals included
        return finish("draft_failed", f"not_rewritable: {type(e).__name__}")
    except Exception as e:  # by type only: driver and parser messages can quote SQL and values
        denied = _denied_sqlstate(e)
        return finish("draft_failed", f"privileges: fixer not enrolled or missing privileges ({denied})" if denied
                      else f"error: {type(e).__name__}")
    parsed = dprompt.parse_response(content)
    if not parsed["ok"]:
        return finish("draft_failed", f"{parsed['reason']}: {parsed['detail']}" if parsed["detail"] else parsed["reason"])
    rewrite = space_params(parsed["rewrite"])
    try:
        dstatic.check(norm, rewrite, [t.get("name") for t in tables])
    except dstatic.StaticError as e:
        return finish("draft_failed", f"{e.check}: {e.detail}")
    rewrite_sql = denormalize(rewrite, literal_values)
    draft.update(state="verifying", action_id=did, rationale=parsed["rationale"],
                 expected_plan_change=parsed["expected_plan_change"], updated_at=now)
    dstore.save_draft(sql, cfg, draft)
    action = dstore.rewrite_action(draft, f, rewrite_sql, now)
    dstore.save_action(sql, cfg, action)
    events += [_draft_event(did, "drafted", {"attempt": draft["attempt"], "prompt_version": draft["prompt_version"],
                                              "context_hash": draft["context_hash"], "sql_hash": queue.sql_hash(rewrite_sql)}),
               _draft_event(did, "verify_started", {"approval_id": ap["approval_id"], "approver": ap.get("approver")})]
    run = {"run_id": uuid.uuid4().hex, "approval_id": None, "action_id": did, "kind": "verify", "started_at": now,
           "ended_at": None, "outcome": None, "report": None, "branch": None, "error": None}
    try:
        passed, report, error = _verify_rewrite(d, w, tgt, src, did, rewrite_sql, original.ordered,
                                                parsed["expected_plan_change"])
    except Exception as e:
        denied = _denied_sqlstate(e)
        passed, report = False, {"branch": None, "rules": []}
        error = (f"fixer not enrolled or missing privileges on this project: {denied}" if denied
                 else type(e).__name__)  # by type only: a driver message can quote the SQL and its values
    outcome = "verified" if passed else "verify_failed"
    run.update(outcome=outcome, ended_at=now, report=json.dumps(report, default=str), branch=report.get("branch"),
               error=error)
    d["record_run"](sql, cfg, run)
    action.update(state=outcome, updated_at=now)
    dstore.save_action(sql, cfg, action)
    draft.update(state=outcome, reason=None if passed else (error or _first_failure(report)), updated_at=now,
                 ended_at=now)
    dstore.save_draft(sql, cfg, draft)
    events.append(_draft_event(did, outcome, {"run_id": run["run_id"], "error": error}))
    d["events"](sql, cfg, events, now)
    _bump(counts, outcome)


def _recover_draft(d, sql, cfg, row, now) -> str:
    """A draft left drafting or verifying by a job that died: final, so Retry is offered again."""
    state = "draft_failed" if row["state"] == "drafting" else "verify_failed"
    dstore.save_draft(sql, cfg, {**row, "state": state, "reason": "interrupted: the fixer job stopped",
                                 "updated_at": now, "ended_at": now})
    d["events"](sql, cfg, [_draft_event(row["draft_id"], state, {"recovered": True})], now)
    return state


def _llm_ready(d, sql, w, cfg, now) -> bool:
    try:
        row = d["probe"](w, cfg, now)
        dstore.write_status(sql, cfg, row)
        return bool(row["ready"])
    except Exception:
        log.exception("could not record the model endpoint status; drafting is skipped this sweep")
        return False


def run_fixer(sql, w, cfg, user, deps=None, now=None, llm_w=None) -> dict:
    now = now or datetime.now(timezone.utc)
    d = {**(default_deps(sql, w, user, cfg, llm_w) if deps is None else {}), **(deps or {})}
    counts: dict = {}
    for row in d["stuck"](sql, cfg, now):
        try:
            _bump(counts, f"recovered:{_recover(d, sql, w, cfg, row, now)}")
        except Exception:
            log.exception("recovery of action %s failed; it stays %s for the next sweep", row["action_id"], row["state"])
            _bump(counts, "recover_error")
    for row in d["stuck_drafts"](sql, cfg, now):
        try:
            _bump(counts, f"recovered:{_recover_draft(d, sql, cfg, row, now)}")
        except Exception:
            log.exception("recovery of draft %s failed", row["draft_id"])
            _bump(counts, "recover_error")
    busy = set()
    for work in d["pending"](sql, cfg):
        project = work.action.get("project_id")
        if project in busy:
            continue
        busy.add(project)
        _handle(d, sql, w, cfg, work, now, counts)
    drafts = d["pending_drafts"](sql, cfg)
    # Probe when a draft waits; otherwise every sweep while the status is missing or not ready (the app accepts no
    # draft request until it is ready), and hourly while ready.
    try:
        due = not drafts and "status_due" in d and d["status_due"](sql, cfg, now)
    except Exception:
        log.exception("could not read llm_status; the endpoint is probed with the next draft")
        due = False
    ready = _llm_ready(d, sql, w, cfg, now) if drafts or due else False
    for work in drafts:  # one branch per project per sweep: a draft waits behind that project's fix
        project = work.finding.get("project_id")
        if project in busy:
            continue
        busy.add(project)
        _handle_draft(d, sql, w, cfg, work, now, counts, ready)
    return counts


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="lakebase-health-fix")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="lakebase_health")
    ap.add_argument("--pii-masking", default="true")
    ap.add_argument("--llm-endpoint", default="databricks-claude-opus-5-5")
    ap.add_argument("--llm-enabled", default="true")
    # Acceptance only, never set in the bundle: a fixed bad model reply (unsafe | non_equivalent).
    ap.add_argument("--llm-test-response", default="", choices=("", *dllm.TEST_RESPONSES))
    a = ap.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config
    from pyspark.sql import SparkSession

    from ..sql import SparkSql
    cfg = EngineConfig(catalog=a.catalog, schema=a.schema, pii_masking=pii_masking_enabled(a.pii_masking),
                       llm_endpoint=a.llm_endpoint, llm_enabled=a.llm_enabled.strip().lower() != "false",
                       llm_test_response=a.llm_test_response)
    sql, w = SparkSql(SparkSession.builder.getOrCreate()), WorkspaceClient()
    # The model call gets its own client: a 120 s read timeout, and no SDK retries (the drafter retries once itself).
    llm_w = WorkspaceClient(config=Config(http_timeout_seconds=dllm.TIMEOUT_S, retry_timeout_seconds=1))
    print(json.dumps(run_fixer(sql, w, cfg, w.current_user.me().user_name, llm_w=llm_w)))
