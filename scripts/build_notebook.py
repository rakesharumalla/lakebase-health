"""Generate notebooks/lakebase_health_check.py (Databricks SOURCE format) from the engine's check YAMLs.

The checks are loaded with the engine's own loader (load_checks) and every SQL statement is rendered by the engine's
own code, so the notebook runs exactly what the engine runs:
- telemetry (delta) checks: DeltaExecutor.run, i.e. render_sql (exclusion tokens) + rewrite_tables (system.lakebase.*
  -> <catalog>.<schema>.raw_*, platform-role filtering for query_scoped checks). The catalog and schema are left as
  the literal placeholders {catalog}.{schema}, which the notebook binds to its widgets.
- postgres checks: render_sql, the call the engine's Runner makes before session.query.
- api checks: the notebook calls the engine's ApiExecutor when the engine wheel is installed.

Query-scoped checks also ignore the engine and fixer service principals (the jobs' --service-roles). Pass the same
values here so the rendered SQL matches the deployed engine:

  python3 scripts/build_notebook.py --service-roles <engine_sp>,<fixer_sp> [--engine-wheel /Workspace/.../x.whl]

Edit the check YAMLs (engine/src/lakebase_health/checks), not the generated notebook.
"""
import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))

from lakebase_health.checkspec import load_checks, render_sql, render_template  # noqa: E402
from lakebase_health.config import EngineConfig  # noqa: E402
from lakebase_health.exclusions import load_exclusions  # noqa: E402
from lakebase_health.executors.api import API_CHECKS  # noqa: E402
from lakebase_health.executors.delta import DeltaExecutor  # noqa: E402
from lakebase_health.executors.postgres import SESSION_SETUP, STATS_WINDOW_SQL  # noqa: E402
from lakebase_health.fixer.allowlist import NotAllowed, parse  # noqa: E402
from lakebase_health.jobs.run import CHECKS_DIR, service_roles  # noqa: E402

OUT = ROOT / "notebooks" / "lakebase_health_check.py"
SEP = "\n\n# COMMAND ----------\n\n"
PLACEHOLDER_CFG = EngineConfig(catalog="{catalog}", schema="{schema}")
SOURCE_ORDER = (("delta", "Telemetry checks (Delta)"), ("postgres", "Postgres checks"), ("api", "API checks"))
SOURCE_LABEL = {"delta": "telemetry", "postgres": "Postgres", "api": "API"}
SOURCE_INTRO = {
    "delta": "Run over the engine's `<catalog>.<schema>.raw_*` copies of `system.lakebase` (what the engine "
             "ingests). Each cell holds the engine's SQL exactly as the engine runs it; only the catalog and schema "
             "are bound to the widgets, and the result is filtered to the `project_id` / `endpoint_id` widgets. The "
             "time windows (e.g. 24 h) are part of each check. `finding_severity` shows which rows the engine would "
             "report.",
    "postgres": "Run over a Postgres connection on one endpoint (the `endpoint_id` widget, or the project's "
                "default-branch read-write endpoint), read-only, with the engine's session settings. Checks that run "
                "per database run in every database of the branch, like the engine; the others run once. You need "
                "a Postgres role on the branch (OAuth). Connecting wakes a suspended compute, which the engine "
                "never does (it skips inactive computes).",
    "api": "Read the Lakebase API. With the engine wheel installed the cell calls the engine's own implementation "
           "(build_inventory + ApiExecutor); otherwise it runs a short SDK equivalent.",
}


class _Capture:
    """Stands in for the engine's SQL client: DeltaExecutor.run hands it the final SQL."""

    def query(self, sql):
        self.sql = sql
        return []


def engine_exclusions(roles=()):
    """The exclusions the engine job builds: exclusions.yaml plus its service roles (jobs/run.py)."""
    cfg = EngineConfig(catalog="{catalog}", service_roles=tuple(roles))
    return load_exclusions(CHECKS_DIR / "exclusions.yaml").with_query_users(service_roles(cfg, ""))


def engine_checks():
    return load_checks(CHECKS_DIR, frozenset(API_CHECKS))


def engine_sql(spec, excl):
    """The SQL the engine runs for a check, rendered by the engine's own code."""
    if spec.source == "delta":
        cap = _Capture()
        DeltaExecutor(cap, PLACEHOLDER_CFG, excl).run(spec)
        return cap.sql
    if spec.source == "postgres":
        return render_sql(spec, excl)
    return None


def md_cell(text):
    lines = text.strip("\n").splitlines()
    return "# MAGIC %md\n" + "\n".join("# MAGIC " + l if l else "# MAGIC" for l in lines)


def _code(s):
    return "`" + s.replace("`", "'") + "`"


def _thresholds(spec):
    op = ">" if spec.direction == "above" else "<"
    return "; ".join(f"{sev} if `{spec.measure}` {op} {spec.thresholds[sev]:g}"
                     for sev in ("critical", "warning", "info") if sev in spec.thresholds)


# Placeholders that hold a whole statement get a representative value; any other one is an identifier
SAMPLE = {"index_def_concurrently": "CREATE INDEX CONCURRENTLY x ON x USING btree (x)"}


def _fixer(fix):
    if fix is None:
        return "no (no candidate fix)"
    if not fix.actionable:
        return f"no ({fix.kind} fix; a person applies it)"
    def sample(t):
        return render_template(t, {n: SAMPLE.get(n, "x") for n in re.findall(r"\{([a-z_]+)\}", t)})
    try:
        kind = parse(sample(fix.template)).kind
        if fix.rollback:
            parse(sample(fix.rollback))
    except NotAllowed as e:
        return f"no: outside the fixer's allowlist ({e}), so nothing is proposed; apply it by hand"
    branch = ", verified on a branch first" if fix.verify_on_branch else ""
    return f"yes, after approval in the app (allowlisted as {kind}{branch})"


def check_md(spec):
    lines = [f"#### `{spec.id}`: {spec.title}", "",
             f"**Category:** {spec.category} | **Severity:** {_thresholds(spec)} | **Schedule:** "
             + ("every run" if spec.schedule == "every_run" else "daily"), "", spec.explain, ""]
    if spec.title_template:
        lines.append(f"- **Finding title:** {_code(spec.title_template)}")
    if spec.fix:
        f = spec.fix
        lines.append(f"- **Candidate fix** ({f.kind}, risk {f.risk}" + (", reversible" if f.reversible else "")
                     + f"): {_code(f.template or '')}")
        if f.rollback:
            lines.append(f"- **Rollback:** {_code(f.rollback)}")
        elif f.rollback_note:
            note = re.sub(r"^not needed:\s*", "", f.rollback_note, flags=re.I)
            lines.append(f"- **Rollback:** not needed — {note}")
    else:
        lines.append("- **Candidate fix:** none")
    lines.append(f"- **Fixer can apply it:** {_fixer(spec.fix)}")
    if spec.superseded_by:
        lines.append(f"- **Superseded by:** {', '.join(f'`{c}`' for c in spec.superseded_by.checks)} "
                     f"(not reported while one of them has an open finding on the same {spec.superseded_by.key})")
    if spec.query_scoped:
        lines.append("- **Query scoped:** telemetry from platform, engine and fixer roles is filtered out "
                     "(the subqueries the engine adds, visible in the SQL below)")
    if spec.source == "postgres":
        lines.append("- **Runs:** " + ("in every database" if spec.per_database
                                         else "once per endpoint (instance-wide catalogs)"))
    if spec.requires_extension:
        lines.append(f"- **Needs extension:** `{spec.requires_extension}` (skipped where it is not installed)")
    if spec.guards:
        lines.append(f"- **Confidence:** low unless {', '.join(f'`{g}`' for g in spec.guards)}")
    if spec.replica_check:
        lines.append("- **Replicas:** the engine keeps only objects every read replica on the branch also reports")
    return md_cell("\n".join(lines))


def _sql_literal(sql):
    assert '"""' not in sql and not sql.endswith("\\"), "SQL cannot be embedded in a raw triple-quoted string"
    return 'r"""\n' + sql + '"""'


def check_code(spec, excl):
    args = f'"{spec.id}", measure="{spec.measure}", thresholds={spec.thresholds!r}, direction="{spec.direction}"'
    if spec.source == "api":
        return f'# check: {spec.id}\nrun_api({args}, function="{spec.function}")'
    head = f"# check: {spec.id}\nSQL = {_sql_literal(engine_sql(spec, excl))}\n"
    if spec.source == "delta":
        return head + f"run_delta(SQL, {args})"
    return head + (f"run_pg(SQL, {args},\n       per_database={spec.per_database}, "
                   f"requires_extension={spec.requires_extension!r}, guards={tuple(spec.guards)!r})")


HEADER_MD = """# Lakebase Health Check

Every check the Lakebase Health engine runs (__COUNTS__), with the SQL the engine runs. If you are not enrolled in
the engine you can run the checks here; the last section shows the engine's open findings.

**Generated** by `scripts/build_notebook.py` from the check YAMLs in `engine/src/lakebase_health/checks`. Edit the
YAML, not this notebook, then rebuild it. `docs/CHECKS.md` describes the checks.

**How to run**
1. Run the next two cells (install, then setup) and fill in the widgets:
   - `catalog` / `schema`: where the engine keeps its tables (`raw_*`, `findings`)
   - `project_id`: optional, the project's uid or name (e.g. `healthcheck`); empty means every project
   - `endpoint_id`: optional, the endpoint's uid or name (`projects/<p>/branches/<b>/endpoints/<e>`). The Postgres
     checks run on this endpoint, or on the project's default-branch read-write endpoint when it is empty
   - `lookback_hours`: how far back the findings summary looks
2. Run any section. Telemetry cells need `SELECT` on the engine's schema; Postgres cells need a Postgres role on the
   branch; API cells need access to the projects.

Query-scoped checks ignore these roles (platform agents plus the engine and fixer service principals given at
generation time): __QUERY_USERS__."""

INSTALL_PY = '''# Optional: the engine wheel the bundle deploys. When it is installed, the API cells call the engine's own code.
ENGINE_WHEEL = __WHEEL__

import os
import subprocess
import sys

pkgs = ["psycopg[binary]>=3.1", "databricks-sdk>=0.81.0", "pyyaml>=6"]
if ENGINE_WHEEL and os.path.exists(ENGINE_WHEEL):
    pkgs.append(ENGINE_WHEEL)
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *pkgs])
dbutils.library.restartPython()'''

SETUP_PY = '''dbutils.widgets.text("catalog", "main", "catalog")
dbutils.widgets.text("schema", "lakebase_health", "schema")
dbutils.widgets.text("project_id", "", "project_id (optional)")
dbutils.widgets.text("endpoint_id", "", "endpoint_id (optional)")
dbutils.widgets.text("lookback_hours", "24", "lookback hours")

import math
import re

import pandas as pd
import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

# From the engine at generation time (scripts/build_notebook.py).
SERVICE_ROLES = __SERVICE_ROLES__
EXCLUDED_ROLES = __EXCLUDED_ROLES__
SESSION_SETUP = __SESSION_SETUP__
STATS_WINDOW_SQL = __STATS_WINDOW_SQL__

w = WorkspaceClient()
USER = w.current_user.me().user_name
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
LOOKBACK_HOURS = int(dbutils.widgets.get("lookback_hours").strip() or 24)
for _v in (CATALOG, SCHEMA):
    assert _v and "`" not in _v, f"bad catalog/schema {_v!r}"
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def T(name):
    return f"`{CATALOG}`.`{SCHEMA}`.{name}"


def _resolve_project(v):
    if not v:
        return None
    if _UUID.match(v):
        p = next((p for p in w.postgres.list_projects() if p.uid == v), None)
        assert p is not None, f"no project with uid {v}"
        return p
    return w.postgres.get_project(name=v if v.startswith("projects/") else f"projects/{v}")


def _resolve_endpoint(v, project):
    if not v:
        return None
    if v.startswith("projects/"):
        return w.postgres.get_endpoint(name=v)
    for p in ([project] if project else w.postgres.list_projects()):
        for b in w.postgres.list_branches(parent=p.name):
            for e in w.postgres.list_endpoints(parent=b.name):
                if e.uid == v:
                    return e
    raise ValueError(f"no endpoint with uid {v}")


PROJECT = _resolve_project(dbutils.widgets.get("project_id").strip())
ENDPOINT = _resolve_endpoint(dbutils.widgets.get("endpoint_id").strip(), PROJECT)
if ENDPOINT is not None and PROJECT is None:
    PROJECT = w.postgres.get_project(name=ENDPOINT.name.split("/branches/")[0])
PROJECT_UID = PROJECT.uid if PROJECT else ""
ENDPOINT_UID = ENDPOINT.uid if ENDPOINT else ""
print("project:", PROJECT.name if PROJECT else "(all)", PROJECT_UID, "| endpoint:",
      ENDPOINT.name if ENDPOINT else "(all)", ENDPOINT_UID)


def _classify(m, thresholds, direction):
    """The engine's severity rule (severity.classify): the highest threshold the measure is beyond."""
    try:
        m = float(m)
    except (TypeError, ValueError):
        return None
    if math.isnan(m):
        return None
    for sev in ("critical", "warning", "info"):
        t = thresholds.get(sev)
        if t is not None and ((direction == "above" and m > t) or (direction == "below" and m < t)):
            return sev
    return None


_RANK = "CASE finding_severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 WHEN 'info' THEN 2 ELSE 3 END"


def _show(pdf):
    if len(pdf):
        pdf = pdf.assign(_r=pdf["finding_severity"].map({"critical": 0, "warning": 1, "info": 2}).fillna(3))
        display(pdf.sort_values("_r", kind="stable").drop(columns="_r").astype(str))
    else:
        print("0 rows")


def run_delta(sql, check_id, measure, thresholds, direction):
    """A telemetry check: the engine's SQL with {catalog}.{schema} bound, filtered to the project/endpoint."""
    df = spark.sql(sql.replace("{catalog}.{schema}.", f"`{CATALOG}`.`{SCHEMA}`."))
    if PROJECT_UID and "project_id" in df.columns:
        df = df.where(F.col("project_id") == PROJECT_UID)
    if ENDPOINT_UID and "endpoint_id" in df.columns:
        df = df.where(F.col("endpoint_id") == ENDPOINT_UID)
    m, sev = F.col(measure).cast("double"), None
    for s in ("critical", "warning", "info"):
        if s in thresholds:
            cond = ((m > thresholds[s]) if direction == "above" else (m < thresholds[s])) & ~F.isnan(m)
            sev = F.when(cond, s) if sev is None else sev.when(cond, s)
    display(df.withColumn("finding_severity", sev).orderBy(F.expr(_RANK)))


def _pg_endpoint():
    if ENDPOINT is not None:
        return ENDPOINT
    assert PROJECT is not None, "set the project_id or endpoint_id widget to run the Postgres checks"
    for b in w.postgres.list_branches(parent=PROJECT.name):
        if getattr(b.status, "default", False):
            for e in w.postgres.list_endpoints(parent=b.name):
                if str(getattr(e.status.endpoint_type, "value", e.status.endpoint_type)).endswith("READ_WRITE"):
                    return e
    raise ValueError(f"{PROJECT.name}: no read-write endpoint on the default branch")


def run_pg(sql, check_id, measure, thresholds, direction, per_database=True, requires_extension=None, guards=()):
    """A Postgres check, read-only, on the chosen endpoint; in every database when per_database."""
    ep = _pg_endpoint()
    host = w.postgres.get_endpoint(name=ep.name).status.hosts.host
    branch = ep.name.split("/endpoints/")[0]
    dbs = [d.status.postgres_database for d in w.postgres.list_databases(parent=branch) if d.status]
    dbs = dbs if per_database else dbs[:1]
    print(ep.name, "| databases:", ", ".join(dbs))
    frames = []
    for db in dbs:
        token = w.postgres.generate_database_credential(endpoint=ep.name).token
        # application_name as the engine's, so the query checks ignore this traffic like the engine's own
        with psycopg.connect(host=host, user=USER, password=token, dbname=db, sslmode="require", autocommit=True,
                             application_name="lakebase-health", connect_timeout=10, prepare_threshold=None) as c:
            for stmt in SESSION_SETUP:  # search_path, default_transaction_read_only = on, statement/lock timeouts
                c.execute(stmt)
            if requires_extension and not c.execute(
                    "SELECT 1 FROM pg_extension WHERE extname = %s", (requires_extension,)).fetchone():
                print(f"{db}: skipped, extension {requires_extension} is not installed")
                continue
            if guards:
                print(f"{db}: stats window {c.execute(STATS_WINDOW_SQL).fetchone()[0]} h (confidence is low unless "
                      f"{', '.join(guards)})")
            cur = c.execute(sql)
            pdf = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        if per_database and "object" in pdf.columns:
            pdf["object"] = db + ":" + pdf["object"].astype(str)  # the engine's object naming
        frames.append(pdf.assign(database=db))
    pdf = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if len(pdf):
        pdf["finding_severity"] = [_classify(v, thresholds, direction) for v in pdf[measure]]
    _show(pdf)


def _engine_api(function):
    """The engine's own API check, when the engine wheel is installed; None otherwise."""
    try:
        from pathlib import Path

        import lakebase_health
        from lakebase_health.checkspec import load_checks
        from lakebase_health.exclusions import load_exclusions
        from lakebase_health.executors.api import API_CHECKS, ApiExecutor
        from lakebase_health.inventory import build_inventory
    except ImportError:
        return None
    checks = Path(lakebase_health.__file__).parent / "checks"
    spec = next(s for s in load_checks(checks, frozenset(API_CHECKS)) if s.function == function)
    global _INVENTORY
    if "_INVENTORY" not in globals():
        _INVENTORY, errors = build_inventory(w, USER)
        for e in errors:
            print("inventory:", e)
    inventory = [e for e in _INVENTORY if not PROJECT_UID or e.project_id == PROJECT_UID]
    return ApiExecutor(load_exclusions(checks / "exclusions.yaml")).run(spec, inventory)


def _seconds(d):
    if d is None:
        return None
    if hasattr(d, "total_seconds"):
        return d.total_seconds()
    s = str(d)
    return float(s[:-1]) if s.endswith("s") else float(s)


def _projects():
    """Projects in scope, without those opted out with the tag lakebase_health=off (as the engine does)."""
    out = []
    for p in ([PROJECT] if PROJECT else w.postgres.list_projects()):
        tags = {getattr(t, "key", None): str(getattr(t, "value", ""))
                for t in (getattr(p.status, "custom_tags", None) or [])}
        if tags.get("lakebase_health", "").lower() != "off":
            out.append(p)
    return out


def _sdk_api(function):
    """Short SDK equivalent of the engine's API checks (executors/api.py), for when the engine is not installed."""
    rows = []
    for p in _projects():
        pname = getattr(p.status, "display_name", None) or p.name
        if function == "backup_restore_window":
            s = _seconds(getattr(p.status, "history_retention_duration", None))
            if s is not None:
                rows.append({"project_id": p.uid, "object": pname, "measure": s / 86400.0,
                             "restore_window_days": s / 86400.0})
            continue
        for b in w.postgres.list_branches(parent=p.name):
            bobj = f"{pname}/{b.name.rsplit('/', 1)[-1]}"
            default = bool(getattr(b.status, "default", False))
            if function == "default_branch_unprotected" and default and not getattr(b.status, "is_protected", False):
                rows.append({"project_id": p.uid, "branch_id": b.uid, "object": bobj, "measure": 1.0})
            elif function == "primary_without_ha" and default:
                for e in w.postgres.list_endpoints(parent=b.name):
                    rw = str(getattr(e.status.endpoint_type, "value", e.status.endpoint_type)).endswith("READ_WRITE")
                    group = getattr(e.status, "group", None)
                    ha_min = int(group.min) if group is not None and group.min else 1
                    if rw and ha_min < 2:
                        rows.append({"project_id": p.uid, "branch_id": b.uid, "endpoint_id": e.uid, "object": bobj,
                                     "measure": 1.0, "ha_min": ha_min})
            elif function == "password_roles":
                for r in w.postgres.list_roles(parent=b.name):
                    role = r.status.postgres_role if r.status else None
                    if role and "PASSWORD" in str(r.status.auth_method) and role not in EXCLUDED_ROLES:
                        rows.append({"project_id": p.uid, "branch_id": b.uid, "object": f"{bobj}:{role}",
                                     "measure": 1.0})
    return rows


def run_api(check_id, measure, thresholds, direction, function):
    rows = _engine_api(function)
    if rows is None:
        print("engine wheel not installed: running the SDK equivalent")
        rows = _sdk_api(function)
    pdf = pd.DataFrame(rows)
    if len(pdf):
        pdf["finding_severity"] = [_classify(v, thresholds, direction) for v in pdf[measure]]
    _show(pdf)'''

SUMMARY_MD = """## The engine's findings

What the engine currently reports for the project (and endpoint) in the widgets: open and new findings seen in the
last `lookback_hours`, by severity and by check, then the findings themselves. It reads
`<catalog>.<schema>.findings`, so it works without being enrolled. The last engine runs show how fresh it is."""

SUMMARY_PY = '''_args = {"project_id": PROJECT_UID, "endpoint_id": ENDPOINT_UID, "hours": LOOKBACK_HOURS}
_where = """status IN ('new', 'open') AND (:project_id = '' OR project_id = :project_id)
  AND (:endpoint_id = '' OR endpoint_id = :endpoint_id)
  AND last_seen >= current_timestamp() - make_dt_interval(0, :hours)"""
_rank = "CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END"
display(spark.sql(f"""SELECT severity, count(*) AS findings, count_if(status = 'new') AS new
FROM {T('findings')} WHERE {_where} GROUP BY severity ORDER BY {_rank}""", args=_args))
display(spark.sql(f"""SELECT check_id, category, severity, count(*) AS findings, count_if(status = 'new') AS new,
  max(last_seen) AS last_seen
FROM {T('findings')} WHERE {_where} GROUP BY check_id, category, severity ORDER BY {_rank}, findings DESC""",
                  args=_args))
display(spark.sql(f"""SELECT severity, status, check_id, title, object, measure, confidence, first_seen, last_seen,
  candidate_sql, project_id, endpoint_id
FROM {T('findings')} WHERE {_where} ORDER BY {_rank}, check_id, measure DESC""", args=_args))
display(spark.sql(f"SELECT run_id, started_at, ended_at, status FROM {T('runs')} ORDER BY started_at DESC LIMIT 5"))'''


def build(roles=(), engine_wheel=""):
    """The notebook text and the number of checks per source."""
    excl = engine_exclusions(roles)
    specs = engine_checks()
    counts = {src: sum(s.source == src for s in specs) for src, _ in SOURCE_ORDER}
    header = HEADER_MD.replace("__COUNTS__", ", ".join(f"{counts[src]} {SOURCE_LABEL[src]}" for src, _ in SOURCE_ORDER))
    header = header.replace("__QUERY_USERS__", ", ".join(f"`{u}`" for u in excl.query_users)
                            + (f", and roles matching `{excl.query_user_regex}`" if excl.query_user_regex else ""))
    setup = (SETUP_PY.replace("__SERVICE_ROLES__", repr(tuple(roles)))
             .replace("__EXCLUDED_ROLES__", repr(tuple(excl.roles)))
             .replace("__SESSION_SETUP__", repr(tuple(SESSION_SETUP)))
             .replace("__STATS_WINDOW_SQL__", repr(STATS_WINDOW_SQL)))
    cells = [md_cell(header), INSTALL_PY.replace("__WHEEL__", repr(engine_wheel)), setup]
    for src, title in SOURCE_ORDER:
        group = [s for s in specs if s.source == src]
        if not group:
            continue
        cells.append(md_cell(f"## {title}\n\n{SOURCE_INTRO[src]}"))
        for category in sorted({s.category for s in group}):
            cells.append(md_cell(f"### {SOURCE_LABEL[src]}: {category}"))
            for spec in sorted((s for s in group if s.category == category), key=lambda s: s.id):
                cells += [check_md(spec), check_code(spec, excl)]
    cells += [md_cell(SUMMARY_MD), SUMMARY_PY]
    return "# Databricks notebook source\n" + SEP.join(cells) + "\n", counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--service-roles", default="", help="comma-separated engine/fixer SP application IDs")
    ap.add_argument("--engine-wheel", default="", help="workspace path of the engine wheel (optional)")
    a = ap.parse_args()
    roles = tuple(r.strip() for r in a.service_roles.split(",") if r.strip())
    text, counts = build(roles, a.engine_wheel)
    a.out.parent.mkdir(exist_ok=True)
    a.out.write_text(text)
    print(f"wrote {a.out}: {sum(counts.values())} checks ({', '.join(f'{v} {k}' for k, v in counts.items())})")


if __name__ == "__main__":
    main()
