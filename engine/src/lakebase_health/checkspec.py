import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from .exclusions import KNOWN_TOKENS, Exclusions
from .synced import SYNCED_TABLES_SQL

SOURCES = {"delta", "postgres", "api"}
SCHEDULES = {"every_run", "daily"}
SEVERITY_ORDER = ("info", "warning", "critical")
FIX_KINDS = {"ddl", "setting", "rewrite", "hint", "manual"}
ACTIONABLE_KINDS = {"ddl", "setting"}
RISKS = {"safe", "moderate", "high"}
# The health report's 12 scorecard areas, in report order (spec 2026-10-28 health report, section 5)
AREAS = ("cache_memory", "storage_latency", "write_path", "connections", "locks", "vacuum_xid", "indexes",
         "tables_bloat", "queries", "config_security", "scale_to_zero", "resilience")
REPLICA_CHECKS = {"intersect"}
PRECEDENCE_KEYS = ("object", "database", "endpoint")
GUARD_RE = re.compile(r"^\s*([a-z_]+)\s*(>=|<=|>|<|==)\s*(-?[0-9.]+)\s*$")
TOKEN_RE = re.compile(r"\{(excluded_[a-z_]+)\}")
VAR_RE = re.compile(r"\{([a-z_]+)\}")


class CheckSpecError(ValueError):
    pass


@dataclass(frozen=True)
class FixSpec:
    kind: str
    template: str | None
    rollback: str | None
    risk: str
    reversible: bool
    verify_on_branch: bool
    rollback_note: str | None = None  # shown instead of a rollback when there is nothing to undo

    @property
    def actionable(self) -> bool:
        return self.kind in ACTIONABLE_KINDS and self.reversible and bool(self.template)


@dataclass(frozen=True)
class SupersededBy:
    key: str  # object: same endpoint and object; database: same endpoint and database; endpoint: same endpoint
    checks: tuple


@dataclass(frozen=True)
class CheckSpec:
    id: str
    title: str
    category: str
    source: str
    explain: str
    thresholds: dict
    direction: str = "above"
    measure: str = "measure"
    sql: str | None = None
    function: str | None = None
    schedule: str = "every_run"
    guards: tuple = ()
    requires_extension: str | None = None
    replica_check: str | None = None  # "intersect": keep only objects every read replica on the branch also reports
    per_database: bool = True  # False: reads instance-wide catalogs (roles, sessions, slots); run once per endpoint
    title_template: str | None = None  # per-result title, e.g. "... for {queries} queries"; falls back to title
    superseded_by: SupersededBy | None = None  # not reported while one of these checks has an open finding
    query_scoped: bool = False  # reads per-query telemetry: platform roles are filtered out, evidence.queryid is a str
    fix: FixSpec | None = None
    area: str | None = None  # health report scorecard area (AREAS); None only for the engine's meta checks
    min_stats_hours: float | None = None  # counter-based postgres check: not run until stats are this old (h)

    def tables(self) -> set:
        return set(re.findall(r"system\.lakebase\.([a-z_]+)", self.sql or ""))


def _parse_fix(d, errors) -> FixSpec | None:
    if d is None:
        return None
    kind, risk = d.get("kind"), d.get("risk")
    if kind not in FIX_KINDS:
        errors.append(f"fix.kind must be one of {sorted(FIX_KINDS)}")
    if risk not in RISKS:
        errors.append(f"fix.risk must be one of {sorted(RISKS)}")
    reversible = bool(d.get("reversible", False))
    if kind in ACTIONABLE_KINDS and not d.get("template"):
        errors.append("fix.template is required for ddl/setting fixes")
    rollback = d.get("rollback") or None
    if rollback and rollback.lstrip().startswith(("--", "/*")):
        errors.append("fix.rollback must be SQL; for a fix with nothing to undo leave it empty and set rollback_note")
    if kind in ACTIONABLE_KINDS and reversible and not rollback and not d.get("rollback_note"):
        errors.append("fix.rollback (or fix.rollback_note when there is nothing to undo) is required when "
                      "reversible is true")
    return FixSpec(kind=kind, template=d.get("template"), rollback=rollback, risk=risk,
                   reversible=reversible, verify_on_branch=bool(d.get("verify_on_branch", False)),
                   rollback_note=d.get("rollback_note") or None)


def _parse_superseded_by(d, check_id, errors) -> SupersededBy | None:
    if d is None:
        return None
    checks = d.get("checks") if isinstance(d, dict) else None
    if (not isinstance(d, dict) or d.get("key") not in PRECEDENCE_KEYS or not isinstance(checks, list) or not checks
            or not all(isinstance(c, str) and c for c in checks) or check_id in checks):
        errors.append(f"superseded_by needs key in {list(PRECEDENCE_KEYS)} and a non-empty list of other check ids")
        return None
    return SupersededBy(key=d["key"], checks=tuple(checks))


def _check_precedence(specs: list) -> None:
    by_id = {s.id: s for s in specs}
    for s in specs:
        unknown = [c for c in (s.superseded_by.checks if s.superseded_by else ()) if c not in by_id]
        if unknown:
            raise CheckSpecError(f"{s.id}: superseded_by names unknown checks {unknown}")
    state = {}

    def visit(i, path):
        if state.get(i) == "done":
            return
        if state.get(i) == "visiting":
            raise CheckSpecError(f"superseded_by cycle: {' -> '.join(path + [i])}")
        state[i] = "visiting"
        for c in by_id[i].superseded_by.checks if by_id[i].superseded_by else ():
            visit(c, path + [i])
        state[i] = "done"

    for s in specs:
        visit(s.id, [])


def parse_check(data: dict, stem: str | None = None, api_functions: frozenset = frozenset()) -> CheckSpec:
    errors = []
    for key in ("id", "title", "category", "source", "explain"):
        if not data.get(key):
            errors.append(f"missing {key}")
    if stem is not None and data.get("id") != stem:
        errors.append(f"id {data.get('id')!r} must match filename {stem!r}")
    source = data.get("source")
    if source not in SOURCES:
        errors.append(f"source must be one of {sorted(SOURCES)}")
    if source in ("delta", "postgres") and not data.get("sql"):
        errors.append("sql is required for delta/postgres checks")
    if source == "api" and data.get("function") not in api_functions:
        errors.append(f"unknown api function {data.get('function')!r}")
    schedule = data.get("schedule", "every_run")
    if schedule not in SCHEDULES:
        errors.append(f"schedule must be one of {sorted(SCHEDULES)}")
    direction = data.get("direction", "above")
    if direction not in ("above", "below"):
        errors.append("direction must be above or below")
    thresholds = {k: float(v) for k, v in (data.get("thresholds") or {}).items()}
    if not thresholds or set(thresholds) - set(SEVERITY_ORDER):
        errors.append("thresholds must be a non-empty subset of info/warning/critical")
    else:
        vals = [thresholds[s] for s in SEVERITY_ORDER if s in thresholds]
        ordered = vals == sorted(vals) if direction == "above" else vals == sorted(vals, reverse=True)
        if not ordered:
            errors.append("thresholds out of order for direction")
    for token in TOKEN_RE.findall(data.get("sql") or ""):
        if token not in KNOWN_TOKENS:
            errors.append(f"unknown token {{{token}}}")
    guards = tuple(data.get("guards") or ())
    for g in guards:
        if not GUARD_RE.match(g):
            errors.append(f"bad guard {g!r}")
    replica_check = data.get("replica_check")
    if replica_check is not None and (source != "postgres" or replica_check not in REPLICA_CHECKS):
        errors.append(f"replica_check must be one of {sorted(REPLICA_CHECKS)} on a postgres check")
    per_database = data.get("per_database", True)
    if per_database is not True and (per_database is not False or source != "postgres"):
        errors.append("per_database must be true or false, and false only on a postgres check")
    query_scoped = data.get("query_scoped", False)
    if query_scoped not in (True, False) or (query_scoped and source != "delta"):
        errors.append("query_scoped must be true or false, and true only on a delta check")
    min_stats_hours = data.get("min_stats_hours")
    if min_stats_hours is not None and (source != "postgres" or isinstance(min_stats_hours, bool)
                                        or not isinstance(min_stats_hours, (int, float)) or min_stats_hours <= 0):
        errors.append("min_stats_hours must be a positive number, on a postgres check")
    area = data.get("area")
    if area is not None and area not in AREAS:
        errors.append(f"area must be one of {list(AREAS)}")
    superseded_by = _parse_superseded_by(data.get("superseded_by"), data.get("id"), errors)
    fix = _parse_fix(data.get("fix"), errors)
    if errors:
        raise CheckSpecError(f"{data.get('id')}: " + "; ".join(errors))
    return CheckSpec(id=data["id"], title=data["title"], category=data["category"], source=source,
                     explain=data["explain"].strip(), thresholds=thresholds, direction=direction,
                     measure=data.get("measure", "measure"), sql=data.get("sql"), function=data.get("function"),
                     schedule=schedule, guards=guards, requires_extension=data.get("requires_extension"),
                     replica_check=replica_check, per_database=per_database,
                     query_scoped=query_scoped, title_template=data.get("title_template"),
                     superseded_by=superseded_by, fix=fix, area=area,
                     min_stats_hours=float(min_stats_hours) if min_stats_hours is not None else None)


def load_checks(directory: Path, api_functions: frozenset) -> list:
    specs, seen = [], set()
    for path in sorted(Path(directory).rglob("*.yaml")):
        if path.name == "exclusions.yaml":
            continue
        spec = parse_check(yaml.safe_load(path.read_text()), stem=path.stem, api_functions=api_functions)
        if spec.id in seen:
            raise CheckSpecError(f"duplicate check id {spec.id}")
        seen.add(spec.id)
        specs.append(spec)
    _check_precedence(specs)
    return specs


def render_text(text: str, excl: Exclusions) -> str:
    """Fill the {excluded_*} and {synced_tables} tokens of any engine SQL (checks and inventory queries)."""
    out = text
    for name, value in excl.tokens().items():
        out = out.replace("{" + name + "}", value)
    return out.replace("{synced_tables}", SYNCED_TABLES_SQL)


def render_sql(spec: CheckSpec, excl: Exclusions) -> str:
    return render_text(spec.sql or "", excl)


_CREATE_INDEX = re.compile(r"^(\s*CREATE\s+(?:UNIQUE\s+)?INDEX)\s+(?!CONCURRENTLY\b)", re.I)


def template_values(row: dict) -> dict:
    """The row plus values derived for fix templates: index_def_concurrently is pg_get_indexdef output with
    CONCURRENTLY after INDEX, the form the fixer's allowlist accepts as a rollback for a dropped index."""
    out = dict(row)
    d = row.get("index_def")
    if isinstance(d, str) and _CREATE_INDEX.match(d):
        out["index_def_concurrently"] = _CREATE_INDEX.sub(r"\1 CONCURRENTLY ", d, count=1)
    return out


def render_template(template: str | None, row: dict) -> str | None:
    if not template:
        return None
    names = set(VAR_RE.findall(template))
    if any(row.get(n) is None for n in names):
        return None
    return template.format_map({n: row[n] for n in names})
