"""Declarative precedence between checks (spec `superseded_by`).

A result of a lower check is not reported while a higher check named in its `superseded_by` has an open finding
for the same key (same endpoint and object, database or endpoint) and that finding is at least as severe. The
superseding result keeps the dropped result's evidence under evidence.supersedes; an already-open lower finding
gets status "superseded" (identity, history and suppression kept) with evidence.superseded_by pointing at the
finding that supersedes it, and returns to open under the same fingerprint when the higher finding clears.
"""
import json

from .severity import SEVERITY_RANK

ACTIVE = ("new", "open", "suppressed")
MAX_SUPERSEDES = 20


def _evidence(e) -> dict:
    if isinstance(e, dict):
        return e
    try:
        return json.loads(e) if e else {}
    except (TypeError, ValueError):
        return {}


def precedence_key(key: str, spec, endpoint_id, obj, evidence):
    if endpoint_id is None:
        return None
    if key == "endpoint":
        return (endpoint_id,)
    if key == "object":
        return (endpoint_id, obj)
    db = _evidence(evidence).get("datname")
    if db is None and spec is not None and spec.source == "postgres" and spec.per_database and obj and ":" in obj:
        db = obj.split(":", 1)[0]
    return None if db is None else (endpoint_id, db)


def _order(specs: dict, check_ids) -> list:
    """Checks ordered so every superseding check is decided before the checks it supersedes."""
    out, seen = [], set()

    def visit(i):
        if i in seen or i not in specs:
            return
        seen.add(i)
        for c in specs[i].superseded_by.checks if specs[i].superseded_by else ():
            visit(c)
        out.append(i)

    for i in sorted(check_ids):
        visit(i)
    return out


def apply_precedence(outcomes: list, existing: dict, specs: dict) -> dict:
    """Drop superseded results from `outcomes` in place. Returns {superseded fingerprint: superseding fingerprint}."""
    by_check = {}
    for o in outcomes:
        if o.ok:
            by_check.setdefault(o.check_id, []).append(o)
    superseded, reported = {}, {r.fingerprint for os_ in by_check.values() for o in os_ for r in o.results}
    survivors = {}  # check id -> list of (key-able record, fingerprint, result or None, severity)

    def holders(check_id):
        if check_id not in survivors:
            rows = [(r.endpoint_id, r.object, r.evidence, r.fingerprint, r, r.severity)
                    for o in by_check.get(check_id, []) for r in o.results]
            rows += [(f.endpoint_id, f.object, f.evidence, f.fingerprint, None, f.severity)
                     for f in existing.values()
                     if f.check_id == check_id and f.status in ACTIVE and f.fingerprint not in reported
                     and f.fingerprint not in superseded]
            survivors[check_id] = rows
        return survivors[check_id]

    for check_id in _order(specs, set(by_check) | {f.check_id for f in existing.values()}):
        spec = specs[check_id]
        rule = spec.superseded_by
        if rule is None or check_id not in by_check:
            continue
        index = {}
        for rank, higher in enumerate(rule.checks):
            for ep, obj, ev, fp, r, sev in holders(higher):
                k = precedence_key(rule.key, specs.get(higher), ep, obj, ev)
                if k is not None:
                    index.setdefault(k, []).append((rank, -SEVERITY_RANK.get(sev, 0), fp, higher, r))
        for o in by_check[check_id]:
            kept = []
            for r in o.results:
                k = precedence_key(rule.key, spec, r.endpoint_id, r.object, r.evidence)
                rank_r = SEVERITY_RANK.get(r.severity, 0)
                # only a finding at least as severe may hide this one
                candidates = [e for e in index.get(k, ()) if -e[1] >= rank_r]
                match = min(candidates) if candidates else None
                if match is None:
                    kept.append(r)
                    continue
                _, _, fp, higher, hr = match
                superseded[r.fingerprint] = fp
                if hr is not None:
                    entries = hr.evidence.setdefault("supersedes", [])
                    if len(entries) < MAX_SUPERSEDES:
                        entries.append({"check_id": check_id, "fingerprint": r.fingerprint, "object": r.object,
                                        "severity": r.severity, "measure": r.measure, "evidence": r.evidence})
            o.results = kept
        survivors.pop(check_id, None)  # recompute from the kept results if a lower check asks for it
    return superseded
