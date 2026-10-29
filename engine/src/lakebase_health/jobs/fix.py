import argparse
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone

from ..config import EngineConfig, pii_masking_enabled
from ..fixer import apply as applymod
from ..fixer import branch as branchmod
from ..fixer import queue, samples, verify
from ..fixer.allowlist import NotAllowed, parse
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


def default_deps(sql, w, user):
    pg = FixerPg(w, user)
    return {"pending": queue.pending, "stuck": _stuck, "claim": queue.claim, "set_state": queue.set_state,
            "record_run": queue.record_run, "events": queue.events, "branch": branchmod.verification_branch,
            "session": pg.session, "target": _target, "samples": samples.affected_queries,
            "no_samples": samples.no_samples_reason, "measure": verify.measure,
            "judge": verify.judge, "execute": applymod.execute, "recover": applymod.recover}


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


def _verify(d, sql, w, cfg, work, st, tgt):
    """Returns (passed, report, error). The report holds query ids, timings, rules and the branch name only."""
    a = work.action
    table = samples.sample_table(st, a)
    queries = d["samples"](sql, cfg, a["project_id"], table)
    report = {"branch": None, "queries": [], "rules": []}
    if not queries and st.kind in ("create_index", "drop_index") and d.get("no_samples"):
        try:
            report["samples"] = d["no_samples"](sql, cfg, a["project_id"], table)
        except Exception as e:  # the explanation is best effort; the verification still runs and fails its rule
            report["samples"] = f"sample counts unavailable: {type(e).__name__}"
    entered = False
    try:
        with d["branch"](w, tgt["project"], tgt["branch"], a["action_id"]) as b:
            entered = True
            report["branch"] = b["branch"]
            with d["session"](b["endpoint"], b["host"], tgt["database"], st.kind) as conn:
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


def run_fixer(sql, w, cfg, user, deps=None, now=None) -> dict:
    now = now or datetime.now(timezone.utc)
    d = {**(default_deps(sql, w, user) if deps is None else {}), **(deps or {})}
    counts: dict = {}
    for row in d["stuck"](sql, cfg, now):
        try:
            _bump(counts, f"recovered:{_recover(d, sql, w, cfg, row, now)}")
        except Exception:
            log.exception("recovery of action %s failed; it stays %s for the next sweep", row["action_id"], row["state"])
            _bump(counts, "recover_error")
    busy = set()
    for work in d["pending"](sql, cfg):
        project = work.action.get("project_id")
        if project in busy:
            continue
        busy.add(project)
        _handle(d, sql, w, cfg, work, now, counts)
    return counts


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="lakebase-health-fix")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="lakebase_health")
    ap.add_argument("--pii-masking", default="true")
    a = ap.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    from ..sql import SparkSql
    cfg = EngineConfig(catalog=a.catalog, schema=a.schema, pii_masking=pii_masking_enabled(a.pii_masking))
    sql, w = SparkSql(SparkSession.builder.getOrCreate()), WorkspaceClient()
    print(json.dumps(run_fixer(sql, w, cfg, w.current_user.me().user_name)))
