from datetime import timedelta

from .checkspec import parse_check, render_sql, render_template, template_values
from .fingerprint import fingerprint
from .models import FLEET, CheckOutcome, CheckResult
from .severity import classify, guards_pass
from .synced import annotate, synced_map

DAILY_INTERVAL = timedelta(hours=23)
_LOCATION = ("project_id", "branch_id", "endpoint_id", "object", "query_text")

META_SPECS = {
    "check_broken": parse_check({
        "id": "check_broken", "title": "A health check has failed 3 runs in a row", "category": "engine",
        "source": "delta", "sql": "SELECT 1", "thresholds": {"warning": 0},
        "explain": "The check's SQL errored on its last 3 runs (often a renamed column in a Beta system table). "
                   "Its findings are frozen until it runs again."}, stem="check_broken"),
    "extension_missing": parse_check({
        "id": "extension_missing", "title": "Extension needed by a health check is not installed",
        "category": "engine", "source": "postgres", "sql": "SELECT 1", "thresholds": {"info": 0},
        "explain": "Install it with CREATE EXTENSION IF NOT EXISTS <name>; to enable the dependent checks."},
        stem="extension_missing"),
    "fix_regressed": parse_check({
        "id": "fix_regressed", "title": "An applied fix made things worse", "category": "engine", "source": "delta",
        "sql": "SELECT 1", "thresholds": {"critical": 0},
        "explain": "After the fix was applied, the affected queries got slower, lock waits rose or the finding stayed "
                   "open. Review the evidence and approve the rollback in the app."}, stem="fix_regressed"),
}


INACTIVE = "skipped:compute_inactive"
STATS_YOUNG = "skipped:stats_young"
SCALE_TO_ZERO_OFF = "skipped:scale_to_zero_off"
SUSPEND_TOKEN = "{suspend_timeout_s}"  # filled in per endpoint from the inventory (suspend_blockers)


def endpoint_sql(sql: str, ep) -> str:
    return sql.replace(SUSPEND_TOKEN, str(int(ep.suspend_timeout_s or 0)))


def _stats_young(spec, ctx) -> str | None:
    """Why a counter-based check must not run yet (spec min_stats_hours), or None when its counters are old enough."""
    if not spec.min_stats_hours:
        return None
    h = ctx.get("stats_window_hours")
    if h is None:
        return "counters too young (reset time unknown)"
    return f"counters too young (reset {h:.0f} h ago)" if h < spec.min_stats_hours else None


class _Suspended(Exception):
    pass


def _timeout(e: Exception) -> bool:
    return type(e).__name__ in ("QueryCanceled", "TimeoutError")


def _status_for(e: Exception) -> str:
    return "timeout" if _timeout(e) else "error"


class Runner:
    def __init__(self, delta, postgres, api, excl):
        self.delta, self.postgres, self.api, self.excl = delta, postgres, api, excl

    def _results(self, spec, rows, loc=None, prefix="", ctx=None) -> list:
        out = []
        confidence = "high" if guards_pass(spec.guards, ctx or {}) else "low"
        for row in rows:
            sev = classify(row.get(spec.measure), spec.thresholds, spec.direction)
            if sev is None:
                continue
            place = dict(loc) if loc else {k: row.get(k) for k in ("project_id", "branch_id", "endpoint_id")}
            obj = prefix + str(row.get("object"))
            fix = spec.fix
            values = template_values(row)
            out.append(CheckResult(
                check_id=spec.id, fingerprint=fingerprint(spec.id, place["project_id"], place["endpoint_id"], obj),
                project_id=place["project_id"], branch_id=place["branch_id"], endpoint_id=place["endpoint_id"],
                object=obj, measure=float(row.get(spec.measure)), severity=sev, confidence=confidence,
                evidence=self._evidence(spec, row),
                query_text=row.get("query_text"),
                candidate_sql=render_template(fix.template, values) if fix else None,
                rollback_sql=render_template(fix.rollback, values) if fix else None,
                rollback_note=render_template(fix.rollback_note, values) if fix else None,
                title=render_template(spec.title_template, row)))
        return out

    @staticmethod
    def _evidence(spec, row) -> dict:
        evidence = {k: v for k, v in row.items() if k not in _LOCATION}
        if spec.query_scoped and "queryid" in evidence:  # the app groups query findings on this key
            evidence["queryid"] = None if evidence["queryid"] is None else str(evidence["queryid"])
        return evidence

    def _due(self, spec, now, last_ok) -> bool:
        last = last_ok.get(spec.id)
        return spec.schedule != "daily" or last is None or now - last >= DAILY_INTERVAL

    def run(self, specs, inventory, now, last_ok, stale_sources) -> list:
        outcomes = []
        for spec in specs:
            if spec.source == "postgres":
                continue
            if not self._due(spec, now, last_ok):
                outcomes.append(CheckOutcome(spec.id, FLEET, "skipped:not_scheduled"))
                continue
            try:
                rows = self.delta.run(spec) if spec.source == "delta" else self.api.run(spec, inventory)
                status = "ok_stale_source" if spec.tables() & set(stale_sources) else "ok"
                outcomes.append(CheckOutcome(spec.id, FLEET, status, results=self._results(spec, rows)))
            except Exception as e:
                outcomes.append(CheckOutcome(spec.id, FLEET, _status_for(e), f"{type(e).__name__}: {e}"[:1000]))
        pg_specs = [s for s in specs if s.source == "postgres"]
        if pg_specs:
            replica_specs = [s for s in pg_specs if s.replica_check]
            for ep in inventory:
                if ep.read_write:
                    out = self._run_endpoint(pg_specs, ep, now, last_ok)
                    replicas = [r for r in inventory if not r.read_write and r.project_id == ep.project_id
                                and r.branch_id == ep.branch_id]
                    if replica_specs and replicas:
                        self._apply_replica_check(out, replica_specs, replicas)
                    outcomes.extend(out)
        return outcomes

    def _apply_replica_check(self, out, specs, replicas):
        # Usage counters are per compute: an index unused on the primary may serve every replica read.
        by_id = {s.id: s for s in specs}
        targets = [o for o in out if o.check_id in by_id and o.status == "ok" and o.results]
        if not targets:
            return
        checked, unverified, seen = [], [], {o.check_id: [] for o in targets}
        for r in replicas:
            if r.opted_out or not r.active or not r.enrolled:
                unverified.append(r.endpoint_id)
                continue
            try:
                objs, trusted = {o.check_id: set() for o in targets}, True
                for db in r.databases:
                    if not self.postgres.still_active(r):
                        raise _Suspended(r.endpoint_id)  # never wake a replica: it stays unverified
                    with self.postgres.session(r, db) as session:
                        ctx = {"stats_window_hours": session.stats_window_hours()}
                        for o in targets:
                            spec = by_id[o.check_id]
                            trusted = trusted and guards_pass(spec.guards, ctx)
                            objs[o.check_id] |= {f"{db}:{row.get('object')}"
                                                 for row in session.query(render_sql(spec, self.excl))}
            except Exception:
                unverified.append(r.endpoint_id)
                continue
            # A recent counter reset (e.g. the replica scaled to zero) can't vouch for "unused", but any use it did
            # record still rules the object out, so its results always take part in the intersection.
            (checked if trusted else unverified).append(r.endpoint_id)
            for cid, found in objs.items():
                seen[cid].append(found)
        for o in targets:
            o.results = [res for res in o.results if all(res.object in found for found in seen[o.check_id])]
            for res in o.results:
                res.evidence["replicas_checked"] = checked
                if unverified:
                    res.evidence["replicas_unverified"] = unverified
                    res.confidence = "low"

    def _run_endpoint(self, specs, ep, now, last_ok) -> list:
        scope = (ep.project_id, ep.endpoint_id)
        skip = ("skipped:opted_out" if ep.opted_out else INACTIVE if not ep.active
                else "skipped:not_enrolled" if not ep.enrolled else None)
        if skip:
            return [CheckOutcome(s.id, scope, skip) for s in specs]
        loc = {"project_id": ep.project_id, "branch_id": ep.branch_id, "endpoint_id": ep.endpoint_id}
        acc = {s.id: CheckOutcome(s.id, scope, "ok") for s in specs}
        missing, ran_once, suspended = [], set(), False
        for db in ep.databases:
            if not self.postgres.still_active(ep):  # re-read right before connecting: never wake a compute
                suspended = True
                break
            try:
                with self.postgres.session(ep, db) as session:
                    ctx = {"stats_window_hours": session.stats_window_hours()}
                    synced = synced_map(session)
                    for spec in specs:
                        o = acc[spec.id]
                        if o.status != "ok":
                            continue
                        if not spec.per_database and spec.id in ran_once:
                            continue
                        if not self._due(spec, now, last_ok):
                            o.status = "skipped:not_scheduled"
                            continue
                        young = _stats_young(spec, ctx)
                        if young:  # one young database skips the endpoint: partial results would resolve the rest
                            o.status, o.error, o.results = STATS_YOUNG, young, []
                            continue
                        if SUSPEND_TOKEN in (spec.sql or "") and not (ep.suspend_timeout_s or 0) > 0:
                            o.status = SCALE_TO_ZERO_OFF
                            continue
                        if spec.requires_extension and not session.has_extension(spec.requires_extension):
                            o.status = "skipped:extension_missing"
                            missing.append({"object": f"{spec.requires_extension}", "measure": 1.0,
                                            "extension": spec.requires_extension, "needed_by": spec.id, "db": db})
                            continue
                        try:
                            rows = annotate(session.query(endpoint_sql(render_sql(spec, self.excl), ep)), synced)
                            prefix = f"{db}:" if spec.per_database else ""
                            o.results.extend(self._results(spec, rows, loc, prefix, ctx))
                            ran_once.add(spec.id)
                        except Exception as e:
                            o.status, o.error = _status_for(e), f"{type(e).__name__}: {e}"[:1000]
            except Exception as e:  # connection failed (e.g. compute suspended since inventory)
                if not self.postgres.still_active(ep):
                    suspended = True
                    break
                for o in acc.values():
                    if o.status == "ok":
                        o.status, o.error, o.results = "error", f"{type(e).__name__}: {e}"[:1000], []
        if suspended:  # partial results would age the findings of the databases not read: report none
            for o in acc.values():
                if o.status == "ok":
                    o.status, o.error, o.results = INACTIVE, None, []
            return list(acc.values()) + ([CheckOutcome("extension_missing", scope, INACTIVE)] if missing else [])
        out = list(acc.values())
        if missing:
            meta_rows = {(m["db"], m["extension"]): m for m in missing}.values()
            results = []
            for m in meta_rows:
                results.extend(self._results(META_SPECS["extension_missing"], [m], loc, f"{m['db']}:"))
            out.append(CheckOutcome("extension_missing", scope, "ok", results=results))
        return out
