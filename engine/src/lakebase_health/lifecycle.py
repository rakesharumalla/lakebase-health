import json
from datetime import datetime, timezone

from .models import FLEET, CheckOutcome, Finding
from .severity import SEVERITY_RANK

ACTIVE = ("new", "open", "suppressed")
# Resolved only by an event, never by clean runs: a fix_regressed finding closes on the rolled_back ledger event of
# its own action (record.resolve_rolled_back); another fix's regression report says nothing about it.
EVENT_RESOLVED = frozenset({"fix_regressed"})


def _utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _covered(f: Finding, covered: set) -> bool:
    return (f.check_id, FLEET) in covered or (f.check_id, (f.project_id, f.endpoint_id)) in covered


def apply_run(existing: dict, outcomes: list, specs: dict, now: datetime, resolve_after: int = 2,
              superseded: dict | None = None) -> list:
    """`superseded` maps fingerprints dropped by precedence to the fingerprint that supersedes them."""
    superseded = superseded or {}
    reported = {}
    covered = set()
    for o in outcomes:
        if not o.ok:
            continue
        covered.add((o.check_id, tuple(o.scope)))
        for r in o.results:
            prev = reported.get(r.fingerprint)
            if prev is None or SEVERITY_RANK[r.severity] > SEVERITY_RANK[prev.severity]:
                reported[r.fingerprint] = r

    changed = []
    for fp, r in reported.items():
        spec = specs[r.check_id]
        f = existing.get(fp)
        fresh = dict(severity=r.severity, confidence=r.confidence, measure=r.measure,
                     evidence=json.dumps(r.evidence, default=str, sort_keys=True), query_text=r.query_text,
                     candidate_sql=r.candidate_sql, rollback_sql=r.rollback_sql, rollback_note=r.rollback_note,
                     last_seen=now,
                     last_checked_at=now, clean_runs=0, title=r.title or spec.title, explain=spec.explain)
        if f is None or f.status == "resolved":
            f = Finding(fingerprint=fp, check_id=r.check_id, category=spec.category, project_id=r.project_id,
                        branch_id=r.branch_id, endpoint_id=r.endpoint_id, object=r.object, status="new",
                        first_seen=now, occurrences=1, suppressed_until=None, resolved_at=None, **fresh)
        else:
            for k, v in fresh.items():
                setattr(f, k, v)
            f.first_seen = _utc(f.first_seen)
            f.occurrences = (f.occurrences or 0) + 1
            until = _utc(f.suppressed_until)
            f.status = "suppressed" if until is not None and until > now else "open"
        changed.append(f)

    for fp, higher in superseded.items():
        f = existing.get(fp)
        if f is None or fp in reported or f.status not in ACTIVE + ("superseded",):
            continue
        evidence = json.loads(f.evidence) if f.evidence else {}
        h = existing.get(higher) or next((r for r in reported.values() if r.fingerprint == higher), None)
        link = {"check_id": h.check_id if h else None, "fingerprint": higher}
        if f.status == "superseded" and evidence.get("superseded_by") == link:
            continue
        evidence["superseded_by"] = link
        f.evidence = json.dumps(evidence, default=str, sort_keys=True)
        f.status, f.last_checked_at, f.clean_runs = "superseded", now, 0
        changed.append(f)

    for fp, f in existing.items():
        if (fp in reported or fp in superseded or f.status not in ACTIVE + ("superseded",)
                or f.check_id in EVENT_RESOLVED or not _covered(f, covered)):
            continue
        f.clean_runs = (f.clean_runs or 0) + 1
        f.last_checked_at = now
        if f.clean_runs >= resolve_after:
            f.status, f.resolved_at = "resolved", now
        changed.append(f)
    return changed
