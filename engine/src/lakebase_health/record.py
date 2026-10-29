import json
from dataclasses import replace
from datetime import timezone

from .actions import plan_actions, shadow_rows
from .fingerprint import fingerprint
from .ledger import chain, last_hashes
from .lifecycle import apply_run
from .models import FLEET, Action, CheckOutcome, CheckResult, Finding
from .precedence import apply_precedence
from .runner import META_SPECS
from .sql import Guard

BROKEN_AFTER = 3
METRIC_CHECKS = {"sequence_exhaustion": "sequence_pct"}  # check id -> metrics_daily metric prefix

METRICS_SQL = """
MERGE INTO {md} md USING (
  SELECT current_date() AS day, project_id, endpoint_id, 'storage_bytes' AS metric, CAST(max(total_size) AS DOUBLE) AS value
  FROM {cg} WHERE ts >= current_date() AND total_size IS NOT NULL GROUP BY project_id, endpoint_id
  UNION ALL
  SELECT current_date(), project_id, endpoint_id, 'xid_age', CAST(max(oldest_frozen_xid_age) AS DOUBLE)
  FROM {dg} WHERE ts >= current_date() GROUP BY project_id, endpoint_id
  UNION ALL
  SELECT current_date(), project_id, endpoint_id, 'peak_backends', CAST(max(nb) AS DOUBLE)
  FROM (SELECT project_id, endpoint_id, ts, sum(numbackends) AS nb FROM {dg} WHERE ts >= current_date()
        GROUP BY project_id, endpoint_id, ts) GROUP BY project_id, endpoint_id
) s ON md.day = s.day AND md.project_id = s.project_id AND md.endpoint_id = s.endpoint_id AND md.metric = s.metric
WHEN MATCHED THEN UPDATE SET value = greatest(md.value, s.value)
WHEN NOT MATCHED THEN INSERT *
"""


# The engine changes an action only in these ways: the same state (TTL, rollback refresh), proposed -> expired and
# expired -> proposed. Anything else means the fixer (or a person) moved the row since the engine read it: keep theirs.
_ENGINE_MOVES = {("proposed", "expired"), ("expired", "proposed")}
ACTION_CAS = Guard(
    "t.state = s.state OR (t.state = 'proposed' AND s.state = 'expired') "
    "OR (t.state = 'expired' AND s.state = 'proposed')",
    lambda t, s: t.get("state") == s.get("state") or (t.get("state"), s.get("state")) in _ENGINE_MOVES)


def merge_actions(sql, cfg, actions) -> None:
    """Compare-and-set write of the engine's action changes (state read vs state now)."""
    sql.merge_rows(cfg.table("proposed_actions"), [a.to_row() for a in actions], ["action_id"], only_if=ACTION_CAS)


def _utc(dt):
    return dt.replace(tzinfo=timezone.utc) if dt is not None and dt.tzinfo is None else dt


def load_last_ok(sql, cfg) -> dict:
    rows = sql.query(f"SELECT check_id, max(ts) AS last_ok FROM {cfg.table('check_results')} "
                     "WHERE status IN ('ok', 'ok_stale_source') GROUP BY check_id")
    return {r["check_id"]: _utc(r["last_ok"]) for r in rows}


def _result_rows(run_id, now, outcomes) -> list:
    rows = []
    for o in outcomes:
        base = {"run_id": run_id, "ts": now, "check_id": o.check_id, "status": o.status, "error": o.error}
        rows.append({**base, "fingerprint": None, "project_id": None if o.scope == FLEET else o.scope[0],
                     "branch_id": None, "endpoint_id": None if o.scope == FLEET else o.scope[1], "object": None,
                     "measure": None, "severity": None, "confidence": None, "evidence": None, "query_text": None})
        for r in o.results:
            rows.append({**base, "fingerprint": r.fingerprint, "project_id": r.project_id, "branch_id": r.branch_id,
                         "endpoint_id": r.endpoint_id, "object": r.object, "measure": r.measure,
                         "severity": r.severity, "confidence": r.confidence,
                         "evidence": json.dumps(r.evidence, default=str, sort_keys=True), "query_text": r.query_text})
    return rows


def broken_outcome(sql, cfg, now) -> CheckOutcome | None:
    rows = sql.query(
        f"SELECT check_id, status, run_ts FROM (SELECT check_id, run_id, max(ts) AS run_ts, "
        f"max(CASE WHEN status = 'error' THEN 'error' ELSE 'ok' END) AS status FROM {cfg.table('check_results')} "
        f"WHERE fingerprint IS NULL AND ts >= current_timestamp() - INTERVAL 7 DAYS GROUP BY check_id, run_id)")
    by_check = {}
    for r in rows:
        by_check.setdefault(r["check_id"], []).append(r)
    results = []
    for check_id, runs in by_check.items():
        last = sorted(runs, key=lambda r: _utc(r["run_ts"]), reverse=True)[:BROKEN_AFTER]
        if len(last) == BROKEN_AFTER and all(r["status"] == "error" for r in last):
            results.append(CheckResult("check_broken", fingerprint("check_broken", None, None, check_id), None, None,
                                       None, check_id, float(BROKEN_AFTER), "warning", "high",
                                       {"consecutive_error_runs": BROKEN_AFTER}))
    return CheckOutcome("check_broken", FLEET, "ok", results=results)


def _gone(branches, project_id, branch_id) -> bool:
    """The branch no longer exists in a project the inventory listed (or is a verification branch, never
    inventoried). Unknown when the project was not listed, its listing failed (None) or no branch list was given."""
    if branches is None or project_id is None:
        return False
    known = branches.get(project_id)  # a project not listed may live in another workspace: unknown, not gone
    return known is not None and branch_id is not None and branch_id not in known


def persist(sql, cfg, run_id, now, outcomes, specs, inventory, branches=None) -> dict:
    """`branches`: {project uid: existing branch uids | None} from the inventory. Results on other branches are
    dropped, and open findings there resolve (their proposed actions expire, reason branch_gone)."""
    if branches is not None:
        outcomes = [replace(o, results=[r for r in o.results if not _gone(branches, r.project_id, r.branch_id)])
                    for o in outcomes]
    sql.insert_rows(cfg.table("inventory"), [e.to_row(run_id, now) for e in inventory])
    sql.insert_rows(cfg.table("check_results"), _result_rows(run_id, now, outcomes))

    all_specs = {**META_SPECS, **specs}
    meta = broken_outcome(sql, cfg, now)
    outcomes = list(outcomes) + ([meta] if meta else [])

    existing = {r["fingerprint"]: Finding.from_row(r) for r in sql.query(f"SELECT * FROM {cfg.table('findings')}")}
    before = {fp: f.status for fp, f in existing.items()}
    superseded = apply_precedence(outcomes, existing, all_specs)
    changed = apply_run(existing, outcomes, all_specs, now, cfg.resolve_after_clean_runs, superseded)
    gone = set()
    for fp, f in existing.items():
        if f.status in ("new", "open", "suppressed", "superseded") and _gone(branches, f.project_id, f.branch_id):
            evidence = json.loads(f.evidence) if f.evidence else {}
            evidence["resolved_reason"] = "branch_gone"
            f.evidence = json.dumps(evidence, default=str, sort_keys=True)
            f.status, f.resolved_at, f.last_checked_at = "resolved", now, now
            gone.add(fp)
    changed = changed + [f for fp, f in existing.items() if fp in gone and all(c is not f for c in changed)]
    sql.merge_rows(cfg.table("findings"), [f.to_row() for f in changed], ["fingerprint"])

    # LLM rewrites belong to the fixer (states, adoption); the engine plans only check actions
    all_actions = {r["action_id"]: Action.from_row(r) for r in sql.query(f"SELECT * FROM {cfg.table('proposed_actions')}")
                   if (r.get("origin") or "check") == "check"}
    actions, events = plan_actions(changed, all_actions, all_specs, now, gone)
    merge_actions(sql, cfg, actions)
    active = [a for a in actions if a.state == "proposed"]
    sql.insert_rows(cfg.table("shadow_log"), shadow_rows(active, run_id, now))
    if events:
        ids = sorted({e["action_id"] for e in events})
        sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, ids), events, now))

    try:
        sql.execute(METRICS_SQL.format(md=cfg.table("metrics_daily"), cg=cfg.raw("compute_gauges"),
                                       dg=cfg.raw("database_gauges")))
    except Exception:  # raw tables may not exist yet on a failed first ingest
        pass
    metric_rows = [{"day": now.date(), "project_id": r.project_id, "endpoint_id": r.endpoint_id,
                    "metric": f"{METRIC_CHECKS[o.check_id]}:{r.object}", "value": r.measure}
                   for o in outcomes if o.check_id in METRIC_CHECKS for r in o.results]
    sql.merge_rows(cfg.table("metrics_daily"), metric_rows, ["day", "project_id", "endpoint_id", "metric"])
    sql.execute(f"DELETE FROM {cfg.table('check_results')} "
                f"WHERE ts < current_timestamp() - INTERVAL {cfg.retention_days} DAYS")

    def count(status, new_only=False):
        return sum(1 for f in changed if f.status == status and (not new_only or before.get(f.fingerprint) != status))

    return {"findings_new": count("new"), "findings_open": count("open"),
            "findings_resolved": count("resolved", new_only=True),
            "actions_proposed": sum(1 for e in events if e["event_type"] == "proposed"),
            "results": sum(len(o.results) for o in outcomes)}


def regressed_outcome(regressions) -> CheckOutcome:
    results = [CheckResult("fix_regressed", fingerprint("fix_regressed", r["project_id"], r["endpoint_id"], r["action_id"]),
                           r["project_id"], None, r["endpoint_id"], r["action_id"], 1.0, "critical", r["confidence"],
                           {"reasons": r["reasons"], "before_ms": r.get("before_ms"), "after_ms": r.get("after_ms")})
               for r in regressions]
    return CheckOutcome("fix_regressed", FLEET, "ok", results=results)


def persist_regressions(sql, cfg, run_id, now, regressions) -> int:
    """Record fix_regressed findings through the same results/findings path as other meta checks."""
    if not regressions:
        return 0
    outcome = regressed_outcome(regressions)
    sql.insert_rows(cfg.table("check_results"), _result_rows(run_id, now, [outcome]))
    existing = {r["fingerprint"]: Finding.from_row(r) for r in sql.query(f"SELECT * FROM {cfg.table('findings')}")}
    changed = apply_run(existing, [outcome], META_SPECS, now, cfg.resolve_after_clean_runs)
    sql.merge_rows(cfg.table("findings"), [f.to_row() for f in changed], ["fingerprint"])
    return len(outcome.results)


def resolve_rolled_back(sql, cfg, now) -> int:
    """A fix_regressed finding (object = action id) resolves once its action was rolled back after the regression:
    nothing is left to fix. Read from the ledger, which keeps the event even if the action row later changes."""
    rows = sql.query(f"""
        SELECT f.* FROM {cfg.table('findings')} f
        WHERE f.check_id = 'fix_regressed' AND f.status IN ('new', 'open', 'suppressed')
          AND EXISTS (SELECT 1 FROM {cfg.table('ledger')} l WHERE l.action_id = f.object
                      AND l.event_type = 'rolled_back' AND l.ts >= f.first_seen)""")
    done = []
    for r in rows:
        f = Finding.from_row(r)
        f.status, f.resolved_at, f.last_checked_at = "resolved", now, now
        done.append(f)
    if done:
        sql.merge_rows(cfg.table("findings"), [f.to_row() for f in done], ["fingerprint"])
    return len(done)
