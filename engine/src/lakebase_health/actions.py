import hashlib
import json
from datetime import timedelta

from .fixer.allowlist import runnable
from .models import Action
from .synced import drops_synced_index

ACTION_TTL = timedelta(days=7)
FIXER_STATES = {"verifying", "verified", "verify_failed", "applying", "applied", "apply_failed", "regressed",
                "rolling_back", "rollback_failed", "blocked"}
# Not yet applied and not in the fixer's hands: the stored rollback may still follow the check's current one
ROLLBACK_REFRESH_STATES = {"proposed", "verify_failed", "verified", "apply_failed"}
# A person decided (rejected, rolled back) or the fix was confirmed: the same SQL is not proposed again under that
# action id, which would overwrite the decision and its history
DECIDED_STATES = {"rolled_back", "rejected", "confirmed"}


def runnable_fix(sql, rollback_sql) -> bool:
    """The fixer can run the fix, and its rollback if there is one (an empty rollback means nothing to undo)."""
    return runnable(sql) and (not (rollback_sql or "").strip() or runnable(rollback_sql))


def policy_level(risk: str, reversible: bool, verify_on_branch: bool) -> str:
    if not reversible or risk == "high":
        return "L1"
    if risk == "safe" and not verify_on_branch:
        return "L3"
    return "L2"


def _evidence(f) -> dict:
    try:
        e = json.loads(f.evidence) if isinstance(f.evidence, str) and f.evidence else f.evidence
    except ValueError:
        return {}
    return e if isinstance(e, dict) else {}


def _action_id(fingerprint: str, sql: str) -> str:
    return hashlib.sha256(f"{fingerprint}\x1f{sql}".encode()).hexdigest()[:32]


def plan_actions(findings: list, existing: dict, specs: dict, now, gone=frozenset()) -> tuple:
    """`gone`: fingerprints of findings resolved because their branch no longer exists (expire reason branch_gone)."""
    by_fp = {}
    for a in existing.values():
        by_fp.setdefault(a.fingerprint, []).append(a)
    changed, events = [], []

    def expire(a, reason):
        a.state, a.updated_at = "expired", now
        changed.append(a)
        events.append({"action_id": a.action_id, "event_type": "expired", "payload": {"reason": reason}})

    def refresh_rollback(a, f) -> bool:
        """A not-yet-applied action whose stored rollback cannot run (e.g. a plain CREATE INDEX stored before the check
        rendered CREATE INDEX CONCURRENTLY) takes the check's current rollback or rollback note."""
        if (a.state not in ROLLBACK_REFRESH_STATES or f.status not in ("new", "open") or a.sql != f.candidate_sql
                or runnable(a.rollback_sql)  # a working rollback is never replaced
                or (a.rollback_sql, a.rollback_note) == (f.rollback_sql, f.rollback_note)
                or not runnable_fix(f.candidate_sql, f.rollback_sql)):
            return False
        events.append({"action_id": a.action_id, "event_type": "rollback_updated",
                       "payload": {"old": a.rollback_sql, "rollback_sql": f.rollback_sql,
                                   "rollback_note": f.rollback_note}})
        a.rollback_sql, a.rollback_note, a.updated_at = f.rollback_sql, f.rollback_note, now
        return True

    for f in findings:
        mine = by_fp.get(f.fingerprint, [])
        if f.status == "superseded" or any(a.state in FIXER_STATES for a in mine):
            # superseded: keep its actions as they are until the finding returns or resolves
            changed.extend(a for a in mine if a.state != "proposed" and refresh_rollback(a, f))
            continue
        current = [a for a in mine if a.state == "proposed"]
        spec = specs.get(f.check_id)
        fix = spec.fix if spec else None
        wanted, reason = None, "branch_gone" if f.fingerprint in gone else "finding_resolved"
        if f.status in ("new", "open") and fix is not None and fix.actionable and f.candidate_sql:
            if drops_synced_index(_evidence(f), f.candidate_sql):
                reason = "synced_partition"  # the sync owns these indexes; the finding keeps its SQL as advice
            elif runnable_fix(f.candidate_sql, f.rollback_sql):
                wanted = _action_id(f.fingerprint, f.candidate_sql)
            else:
                reason = "not_runnable"  # outside the fixer allowlist: the app shows the SQL to apply by hand
        for a in current:
            if a.action_id != wanted:
                expire(a, reason if wanted is None else "sql_changed")
        if wanted is None or any(a.action_id == wanted and a.state in DECIDED_STATES for a in mine):
            continue
        keep = next((a for a in current if a.action_id == wanted), None)
        if keep is not None:
            refresh_rollback(keep, f)
            keep.expires_at, keep.updated_at = now + ACTION_TTL, now
            changed.append(keep)
            continue
        a = Action(wanted, f.fingerprint, f.check_id, fix.kind, f.candidate_sql, f.rollback_sql, fix.risk,
                   fix.reversible, fix.verify_on_branch, "proposed", now, now, now + ACTION_TTL, f.rollback_note)
        changed.append(a)
        events.append({"action_id": a.action_id, "event_type": "proposed",
                       "payload": {"fingerprint": f.fingerprint, "check_id": f.check_id, "sql": a.sql,
                                   "rollback_sql": a.rollback_sql, "rollback_note": a.rollback_note, "risk": a.risk}})
    return changed, events


def shadow_rows(actions: list, run_id: str, now) -> list:
    rows = []
    for a in actions:
        if a.state != "proposed":
            continue
        level = policy_level(a.risk, a.reversible, a.verify_on_branch)
        reason = {"L3": "safe and reversible: an L3 policy would apply it unattended",
                  "L2": "needs branch verification and one-click approval",
                  "L1": "high risk or irreversible: human-run script only"}[level]
        rows.append({"action_id": a.action_id, "evaluated_at": now, "run_id": run_id, "would_allow_level": level,
                     "would_execute": level == "L3", "reason": reason, "outcome_score": None})
    return rows
