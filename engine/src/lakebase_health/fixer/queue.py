import hashlib
from dataclasses import dataclass
from datetime import timezone

from ..ledger import chain, last_hashes
from .allowlist import runnable
from .states import block_retry, can_request



FINDING_ACTIONABLE = ("new", "open")  # "superseded" findings are not acted on

def sql_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode()).hexdigest()


@dataclass
class Work:
    approval: dict
    action: dict
    finding_status: str | None


def _utc(dt):
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def pending(sql, cfg) -> list:
    rows = sql.query(f"""
        SELECT ap.approval_id, ap.action_id, ap.request, ap.sql_hash, ap.approver, ap.requested_at, ap.note,
               a.state, a.sql, a.rollback_sql, a.verify_on_branch, a.expires_at, a.fingerprint, a.check_id, a.kind,
               f.status AS finding_status, f.project_id, f.branch_id, f.endpoint_id, f.object, f.evidence,
               b.kind AS blocked_kind, b.report AS blocked_report, b.error AS blocked_error
        FROM {cfg.table('approvals')} ap
        JOIN {cfg.table('proposed_actions')} a ON a.action_id = ap.action_id
        LEFT JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint
        LEFT JOIN (SELECT action_id, max_by(kind, started_at) AS kind, max_by(report, started_at) AS report,
                          max_by(error, started_at) AS error
                   FROM {cfg.table('action_runs')} WHERE outcome = 'blocked' GROUP BY action_id) b
          ON b.action_id = a.action_id AND a.state = 'blocked'
        WHERE ap.approval_id NOT IN (SELECT approval_id FROM {cfg.table('action_runs')} WHERE approval_id IS NOT NULL)
        ORDER BY ap.requested_at""")
    keys_ap = ("approval_id", "action_id", "request", "sql_hash", "approver", "requested_at", "note")
    return [Work({k: r[k] for k in keys_ap}, {k: v for k, v in r.items() if k not in keys_ap or k == "action_id"},
                 r.get("finding_status")) for r in rows]


def staleness(w: Work, now) -> str | None:
    a, ap = w.action, w.approval
    if a.get("kind") == "query_rewrite":
        return "advisory"  # a rewrite is changed in application code; the fixer never runs its SQL
    target = a.get("rollback_sql") if ap["request"] == "rollback" else a.get("sql")
    if ap["request"] == "rollback" and not runnable(target):
        return "no_rollback"  # nothing to undo, or a stored rollback outside the allowlist: never executed
    if ap["request"] != "reject" and ap["sql_hash"] != sql_hash(target):
        return "sql_changed"
    if ap["request"] in ("verify", "apply") and _utc(a.get("expires_at")) and _utc(a["expires_at"]) <= now:
        return "expired"
    if ap["request"] in ("verify", "apply") and w.finding_status not in FINDING_ACTIONABLE:
        return "finding_superseded" if w.finding_status == "superseded" else "finding_resolved"
    retry = block_retry(a.get("blocked_kind"), a.get("blocked_report"), a.get("blocked_error"))
    if not can_request(ap["request"], a.get("state"), bool(a.get("verify_on_branch")), retry):
        return "illegal_state"
    return None


def claim(sql, cfg, action_id, from_state, to_state, now) -> bool:
    out = sql.query(f"UPDATE {cfg.table('proposed_actions')} SET state = :to_state, updated_at = :now "
                    "WHERE action_id = :id AND state = :from_state",
                    {"to_state": to_state, "now": now, "id": action_id, "from_state": from_state})
    return bool(out) and int(out[0].get("num_affected_rows") or 0) == 1


def set_state(sql, cfg, action_id, to_state, now) -> None:
    sql.execute(f"UPDATE {cfg.table('proposed_actions')} SET state = :to_state, updated_at = :now WHERE action_id = :id",
                {"to_state": to_state, "now": now, "id": action_id})


def record_run(sql, cfg, row: dict) -> None:
    sql.insert_rows(cfg.table("action_runs"), [row])


def events(sql, cfg, rows: list, now) -> None:
    if rows:
        ids = sorted({e["action_id"] for e in rows})
        sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, ids), rows, now, actor="fixer"))
