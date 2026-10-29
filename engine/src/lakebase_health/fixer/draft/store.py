"""Draft approvals, draft and model-call rows, the rewrite action and the endpoint status, in the Delta tables."""
import uuid
from dataclasses import dataclass
from datetime import timedelta, timezone

from .states import ADOPTION_WINDOW, RUNNING, draft_token, finding_queryid

STUCK_AFTER = timedelta(hours=4, minutes=15)  # the verification branch's TTL (4 h) plus a margin
ADVISORY_NOTE = "Advisory: change the query in your application code; this tool does not deploy rewrites."
_APPROVAL = ("approval_id", "fingerprint", "sql_hash", "approver", "requested_at")
_FINDING = ("check_id", "project_id", "branch_id", "endpoint_id", "object", "evidence", "measure")


@dataclass
class DraftWork:
    approval: dict
    finding: dict          # fingerprint, status and the columns in _FINDING
    last_attempt: int      # 0 before the first attempt
    last_state: str | None


def _utc(dt):
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def pending_drafts(sql, cfg) -> list:
    """Draft approvals no run has consumed yet, oldest first, with the finding and its latest attempt."""
    rows = sql.query(f"""
        SELECT ap.approval_id, ap.fingerprint, ap.sql_hash, ap.approver, ap.requested_at,
               f.status AS finding_status, f.check_id, f.project_id, f.branch_id, f.endpoint_id, f.object,
               f.evidence, f.measure, coalesce(d.last_attempt, 0) AS last_attempt, d.last_state
        FROM {cfg.table('approvals')} ap
        LEFT JOIN {cfg.table('findings')} f ON f.fingerprint = ap.fingerprint
        LEFT JOIN (SELECT finding_fingerprint, max(attempt) AS last_attempt, max_by(state, attempt) AS last_state
                   FROM {cfg.table('drafts')} GROUP BY finding_fingerprint) d ON d.finding_fingerprint = ap.fingerprint
        WHERE ap.request = 'draft'
          AND ap.approval_id NOT IN (SELECT approval_id FROM {cfg.table('action_runs')} WHERE approval_id IS NOT NULL)
        ORDER BY ap.requested_at""")
    return [DraftWork({k: r.get(k) for k in _APPROVAL},
                      {"fingerprint": r.get("fingerprint"), "status": r.get("finding_status"),
                       **{k: r.get(k) for k in _FINDING}},
                      int(r.get("last_attempt") or 0), r.get("last_state")) for r in rows]


def staleness(work: DraftWork, ready: bool) -> str | None:
    """Why a draft approval is consumed without drafting, or None."""
    f = work.finding
    if not ready:
        return "not_set_up"
    if f.get("status") not in ("new", "open"):
        return "finding_superseded" if f.get("status") == "superseded" else "finding_resolved"
    if finding_queryid(f.get("check_id"), f.get("evidence"), f.get("object")) is None:
        return "not_rewritable"
    if work.last_state in RUNNING:
        return "in_progress"
    if work.approval.get("sql_hash") != draft_token(f["fingerprint"], work.last_attempt + 1):
        return "attempt_changed"  # e.g. a second click: the attempt it asked for already exists
    return None


def new_draft(work: DraftWork, cfg, now) -> dict:
    return {"draft_id": uuid.uuid4().hex, "finding_fingerprint": work.finding["fingerprint"],
            "attempt": work.last_attempt + 1, "action_id": None, "approval_id": work.approval["approval_id"],
            "endpoint": cfg.llm_endpoint, "prompt_version": None, "context_hash": None, "rationale": None,
            "expected_plan_change": None, "state": "drafting", "reason": None,
            "requested_by": work.approval.get("approver"), "requested_at": work.approval.get("requested_at"),
            "updated_at": now, "ended_at": None}


def rewrite_action(draft: dict, finding: dict, sql_text: str, now) -> dict:
    """The advisory action: action_id is the draft id, so draft and verification events share one ledger chain."""
    return {"action_id": draft["draft_id"], "fingerprint": finding["fingerprint"], "check_id": finding["check_id"],
            "kind": "query_rewrite", "sql": sql_text, "rollback_sql": None, "risk": "low", "reversible": True,
            "verify_on_branch": True, "state": "verifying", "created_at": now, "updated_at": now,
            "expires_at": now + ADOPTION_WINDOW, "rollback_note": ADVISORY_NOTE, "origin": "llm",
            "draft_id": draft["draft_id"]}


def save_draft(sql, cfg, row: dict) -> None:
    sql.merge_rows(cfg.table("drafts"), [row], ["draft_id"])


def save_action(sql, cfg, row: dict) -> None:
    sql.merge_rows(cfg.table("proposed_actions"), [row], ["action_id"])


def record_llm_call(sql, cfg, row: dict) -> None:
    sql.insert_rows(cfg.table("llm_calls"), [row])


STATUS_MAX_AGE = timedelta(hours=1)


def _truthy(v) -> bool:
    return v is True or str(v).strip().lower() == "true"


def status_due(sql, cfg, now) -> bool:
    """Whether the endpoint is probed this sweep although no draft waits: always while llm_status has no row or a
    not-ready row (the app refuses draft requests until it is ready, so nothing else would ever re-probe), and
    hourly while ready."""
    rows = sql.query(f"SELECT ready, checked_at FROM {cfg.table('llm_status')} WHERE endpoint = :e LIMIT 1",
                     {"e": cfg.llm_endpoint})
    if not rows or not _truthy(rows[0].get("ready")) or rows[0].get("checked_at") is None:
        return True
    return now - _utc(rows[0]["checked_at"]) >= STATUS_MAX_AGE


def write_status(sql, cfg, row: dict) -> None:
    sql.merge_rows(cfg.table("llm_status"), [row], ["endpoint"])


def stuck_drafts(sql, cfg, now) -> list:
    rows = sql.query(f"SELECT * FROM {cfg.table('drafts')} WHERE state IN ('drafting', 'verifying')")
    return [r for r in rows if r.get("updated_at") and now - _utc(r["updated_at"]) > STUCK_AFTER]
