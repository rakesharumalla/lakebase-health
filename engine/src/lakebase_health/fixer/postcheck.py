import json
import uuid
from datetime import timedelta, timezone

from ..ledger import chain, last_hashes
from .draft.sqltext import SqlTextError, text_key
from .draft.states import ADOPTION_WINDOW

MIN_CALLS = 50
POSTCHECK_RUNS = 2  # runs with check data (this one included) after the apply before the outcome is decided
NO_DATA_CONFIRM_AFTER = timedelta(days=7)  # an applied fix whose check never ran again is confirmed, low confidence
ADOPTED_MIN_CALLS = 20  # calls of the rewritten query since verification that count as the workload switching


def decide(finding_status, before_ms, after_ms, before_lock_share, after_lock_share, calls_after, improving=False):
    reasons = []
    if finding_status in ("new", "open", "superseded") and not improving:  # superseded: the problem is still there
        reasons.append("finding still open")
    if calls_after is None or calls_after < MIN_CALLS or before_ms is None or after_ms is None or before_ms <= 0:
        return ("regressed", "high", reasons) if reasons else ("confirmed", "low", ["too little traffic to compare"])
    if after_ms > before_ms * 1.10:
        reasons.append(f"affected queries {after_ms / before_ms:.2f}x slower")
    if before_lock_share is not None and after_lock_share is not None and after_lock_share > before_lock_share + 0.05:
        reasons.append("lock wait share rose")
    return ("regressed", "high", reasons) if reasons else ("confirmed", "high", [])


def _utc(dt):
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def run_postcheck(sql, cfg, now, errors=None, run_id=None) -> list:
    """Decides applied actions. Only runs in which the finding's check finished for its endpoint (or fleet-wide)
    count, and this run (`run_id`) must be one of them: a check skipped because the compute is suspended says
    nothing about the fix, so the action stays applied and is re-evaluated later."""
    # check data: per action, the runs after the apply in which its check finished for the endpoint (or fleet-wide)
    due = sql.query(f"""
        SELECT d.*, coalesce(k.checked_runs, 0) AS checked_runs, coalesce(k.checked_now, 0) = 1 AS checked_now
        FROM (
          SELECT a.action_id, a.fingerprint, a.updated_at AS applied_at, f.status AS finding_status, f.project_id, f.endpoint_id,
                 f.measure AS current_measure,
                 (SELECT c.measure FROM {cfg.table('check_results')} c WHERE c.fingerprint = a.fingerprint
                    AND c.ts < a.updated_at ORDER BY c.ts DESC LIMIT 1) AS prior_measure
          FROM {cfg.table('proposed_actions')} a JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint
          WHERE a.state = 'applied') d
        LEFT JOIN (
          SELECT a.action_id, count(DISTINCT c.run_id) AS checked_runs,
                 max(CASE WHEN c.run_id = :run_id THEN 1 ELSE 0 END) AS checked_now
          FROM {cfg.table('proposed_actions')} a JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint
          JOIN {cfg.table('check_results')} c
            ON c.fingerprint IS NULL AND c.check_id = f.check_id AND c.ts > a.updated_at
           AND c.status IN ('ok', 'ok_stale_source')
           AND (c.endpoint_id = f.endpoint_id OR (c.project_id IS NULL AND c.endpoint_id IS NULL))
          WHERE a.state = 'applied'
          GROUP BY a.action_id) k ON k.action_id = d.action_id""", {"run_id": run_id or ""})
    out = []

    def checked(d):
        return (run_id is None or bool(d.get("checked_now"))) and int(d.get("checked_runs") or 0) >= POSTCHECK_RUNS

    def no_data(d):
        return d.get("applied_at") is not None and now - _utc(d["applied_at"]) >= NO_DATA_CONFIRM_AFTER

    due = [d for d in due if checked(d) or no_data(d)]
    for d in due:
        try:
            if not checked(d):  # 7 days and the check never produced data again: nothing says it got worse
                report = {"confidence": "low", "before_ms": None, "after_ms": None,
                          "reasons": [f"no check data for {NO_DATA_CONFIRM_AFTER.days} days after the apply"]}
                if _decide(sql, cfg, now, d, "confirmed", report):
                    out.append({**d, "outcome": "confirmed", **report})
                continue
            m = sql.query(f"""
                SELECT sum(CASE WHEN ts < :t THEN total_exec_time END) / NULLIF(sum(CASE WHEN ts < :t THEN calls END), 0) AS before_ms,
                       sum(CASE WHEN ts >= :t THEN total_exec_time END) / NULLIF(sum(CASE WHEN ts >= :t THEN calls END), 0) AS after_ms,
                       sum(CASE WHEN ts >= :t THEN calls END) AS calls_after
                FROM {cfg.raw('pg_stat_statements_counters')}
                WHERE endpoint_id = :ep AND ts >= :t - INTERVAL 24 HOURS
                  AND queryid IN (SELECT queryid FROM {cfg.raw('plan_history')} WHERE endpoint_id = :ep AND ts >= :t - INTERVAL 24 HOURS)""",
                          {"t": d["applied_at"], "ep": d["endpoint_id"]})[0]
            locks = sql.query(f"""
                SELECT coalesce(sum(CASE WHEN ts < :t AND wait_class_name = 'Lock' THEN wait_event_time END), 0) / NULLIF(sum(CASE WHEN ts < :t THEN wait_event_time END), 0) AS b,
                       coalesce(sum(CASE WHEN ts >= :t AND wait_class_name = 'Lock' THEN wait_event_time END), 0) / NULLIF(sum(CASE WHEN ts >= :t THEN wait_event_time END), 0) AS a
                FROM {cfg.raw('wait_event_counters')} WHERE endpoint_id = :ep AND ts >= :t - INTERVAL 24 HOURS
                  AND NOT (wait_class_name IN ('Extension', 'Activity') OR wait_event_name LIKE '%Idle%')""",
                              {"t": d["applied_at"], "ep": d["endpoint_id"]})[0]
            f = lambda v: None if v is None else float(v)
            cur, prior = f(d.get("current_measure")), f(d.get("prior_measure"))
            improving = cur is not None and prior is not None and prior > 0 and cur <= prior * 0.75
            outcome, confidence, reasons = decide(d["finding_status"], f(m["before_ms"]), f(m["after_ms"]), f(locks["b"]), f(locks["a"]),
                                                  None if m["calls_after"] is None else int(m["calls_after"]), improving)
            report = {"confidence": confidence, "reasons": reasons, "before_ms": f(m["before_ms"]), "after_ms": f(m["after_ms"])}
            if not _decide(sql, cfg, now, d, outcome, report):
                continue  # another transition (e.g. rollback) won; leave it alone
            out.append({**d, "outcome": outcome, **report})
        except Exception as e:
            if errors is not None:
                errors.append(f"postcheck {d['action_id']}: {type(e).__name__}: {e}")
    return out


def _decide(sql, cfg, now, d, outcome, report) -> bool:
    won = sql.query(f"UPDATE {cfg.table('proposed_actions')} SET state = :s, updated_at = :now WHERE action_id = :id AND state = 'applied'",
                    {"s": outcome, "now": now, "id": d["action_id"]})
    if not (won and int(won[0].get("num_affected_rows") or 0) == 1):
        return False
    ev = [{"action_id": d["action_id"], "event_type": outcome, "payload": report}]
    sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, [d["action_id"]]), ev, now))
    sql.insert_rows(cfg.table("action_runs"), [{"run_id": uuid.uuid4().hex, "approval_id": None, "action_id": d["action_id"],
                                                "kind": "postcheck", "started_at": now, "ended_at": now, "outcome": outcome,
                                                "report": json.dumps(report), "branch": None, "error": None}])
    sql.execute(f"UPDATE {cfg.table('shadow_log')} SET outcome_score = :s WHERE action_id = :id",
                {"s": 1.0 if outcome == "confirmed" else 0.0, "id": d["action_id"]})
    return True


def _matches(row, queryid, key) -> bool:
    if queryid and str(row.get("queryid")) == str(queryid):
        return True
    q = row.get("query")
    if not key or not q or str(q).startswith("<REDACTED"):
        return False
    try:
        return text_key(q) == key
    except SqlTextError:
        return False


def run_rewrite_postcheck(sql, cfg, now, errors=None) -> list:
    """A verified rewrite is adopted once the endpoint has run it (its query id from the branch, or the same
    normalized text) ADOPTED_MIN_CALLS times since verification; not_adopted after ADOPTION_WINDOW without."""
    due = sql.query(f"""
        SELECT a.action_id, a.updated_at AS verified_at, f.endpoint_id,
               get_json_object(r.report, '$.rewrite_queryid') AS rewrite_queryid,
               get_json_object(r.report, '$.rewrite_key') AS rewrite_key
        FROM {cfg.table('proposed_actions')} a JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint
        LEFT JOIN (SELECT action_id, max_by(report, started_at) AS report FROM {cfg.table('action_runs')}
                   WHERE kind = 'verify' AND outcome = 'verified' GROUP BY action_id) r ON r.action_id = a.action_id
        WHERE a.state = 'verified' AND a.origin = 'llm'""")
    out = []
    for d in due:
        try:
            verified_at = _utc(d["verified_at"])
            seen = sql.query(f"""
                SELECT CAST(queryid AS STRING) AS queryid, max(query) AS query, sum(calls) AS calls
                FROM {cfg.raw('pg_stat_statements_counters')}
                WHERE endpoint_id = :ep AND ts >= :t GROUP BY queryid""", {"ep": d["endpoint_id"], "t": verified_at})
            hits = [r for r in seen if _matches(r, d.get("rewrite_queryid"), d.get("rewrite_key"))]
            calls = sum(int(r.get("calls") or 0) for r in hits)
            if calls >= ADOPTED_MIN_CALLS:
                outcome, report = "adopted", {"calls": calls, "queryids": sorted(str(r["queryid"]) for r in hits)}
            elif now - verified_at >= ADOPTION_WINDOW:
                outcome, report = "not_adopted", {"calls": calls, "days": ADOPTION_WINDOW.days}
            else:
                continue
            won = sql.query(f"UPDATE {cfg.table('proposed_actions')} SET state = :s, updated_at = :now "
                            "WHERE action_id = :id AND state = 'verified'", {"s": outcome, "now": now, "id": d["action_id"]})
            if not (won and int(won[0].get("num_affected_rows") or 0) == 1):
                continue
            sql.execute(f"UPDATE {cfg.table('drafts')} SET state = :s, updated_at = :now WHERE draft_id = :id",
                        {"s": outcome, "now": now, "id": d["action_id"]})
            ev = [{"action_id": d["action_id"], "event_type": outcome, "payload": report}]
            sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, [d["action_id"]]), ev, now))
            sql.insert_rows(cfg.table("action_runs"), [{"run_id": uuid.uuid4().hex, "approval_id": None,
                                                        "action_id": d["action_id"], "kind": "postcheck",
                                                        "started_at": now, "ended_at": now, "outcome": outcome,
                                                        "report": json.dumps(report), "branch": None, "error": None}])
            out.append({**d, "outcome": outcome, **report})
        except Exception as e:
            if errors is not None:
                errors.append(f"rewrite postcheck {d['action_id']}: {type(e).__name__}: {e}")
    return out
