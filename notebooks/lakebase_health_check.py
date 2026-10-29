# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Health Check
# MAGIC
# MAGIC Every check the Lakebase Health engine runs (31 telemetry, 29 Postgres, 4 API), with the SQL the engine runs. If you are not enrolled in
# MAGIC the engine you can run the checks here; the last section shows the engine's open findings.
# MAGIC
# MAGIC **Generated** by `scripts/build_notebook.py` from the check YAMLs in `engine/src/lakebase_health/checks`. Edit the
# MAGIC YAML, not this notebook, then rebuild it. `docs/CHECKS.md` describes the checks.
# MAGIC
# MAGIC **How to run**
# MAGIC 1. Run the next two cells (install, then setup) and fill in the widgets:
# MAGIC    - `catalog` / `schema`: where the engine keeps its tables (`raw_*`, `findings`)
# MAGIC    - `project_id`: optional, the project's uid or name (e.g. `healthcheck`); empty means every project
# MAGIC    - `endpoint_id`: optional, the endpoint's uid or name (`projects/<p>/branches/<b>/endpoints/<e>`). The Postgres
# MAGIC      checks run on this endpoint, or on the project's default-branch read-write endpoint when it is empty
# MAGIC    - `lookback_hours`: how far back the findings summary looks
# MAGIC 2. Run any section. Telemetry cells need `SELECT` on the engine's schema; Postgres cells need a Postgres role on the
# MAGIC    branch; API cells need access to the projects.
# MAGIC
# MAGIC Query-scoped checks ignore these roles (platform agents plus the engine and fixer service principals given at
# MAGIC generation time): `cloud_admin`, `databricks_control_plane`, `databricks_monitor`, `00000000-0000-4000-8000-0000000000e1`, `00000000-0000-4000-8000-0000000000f1`, and roles matching `^databricks_writer_[0-9]+$`.

# COMMAND ----------

# Optional: the engine wheel the bundle deploys. When it is installed, the API cells call the engine's own code.
ENGINE_WHEEL = '/Workspace/Users/user@example.com/.bundle/lakebase-health/dev/artifacts/.internal/lakebase_health-0.1.0-py3-none-any.whl'

import os
import subprocess
import sys

pkgs = ["psycopg[binary]>=3.1", "databricks-sdk>=0.81.0", "pyyaml>=6"]
if ENGINE_WHEEL and os.path.exists(ENGINE_WHEEL):
    pkgs.append(ENGINE_WHEEL)
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *pkgs])
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "catalog")
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
SERVICE_ROLES = ('00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1')
EXCLUDED_ROLES = ('cloud_admin', 'databricks_control_plane', 'databricks_superuser', 'databricks_monitor', 'authenticator')
SESSION_SETUP = ('DO $$ BEGIN PERFORM set_config(\'search_path\', concat_ws(\',\', \'"$user"\', \'public\', (SELECT string_agg(DISTINCT quote_ident(n.nspname), \',\') FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE n.nspname NOT IN (\'public\', \'pg_catalog\'))), false); END $$', 'SET default_transaction_read_only = on', "SET statement_timeout = '30s'", "SET lock_timeout = '1s'")
STATS_WINDOW_SQL = 'SELECT extract(epoch FROM now() - coalesce((SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()), pg_postmaster_start_time()))::float8 / 3600.0 AS h'

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
    _show(pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Telemetry checks (Delta)
# MAGIC
# MAGIC Run over the engine's `<catalog>.<schema>.raw_*` copies of `system.lakebase` (what the engine ingests). Each cell holds the engine's SQL exactly as the engine runs it; only the catalog and schema are bound to the widgets, and the result is filtered to the `project_id` / `endpoint_id` widgets. The time windows (e.g. 24 h) are part of each check. `finding_severity` shows which rows the engine would report.

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: capacity

# COMMAND ----------

# MAGIC %md
# MAGIC #### `buffer_hit_rate`: Postgres buffer cache hit rate low
# MAGIC
# MAGIC **Category:** capacity | **Severity:** warning if `measure` < 90; info if `measure` < 95 | **Schedule:** every run
# MAGIC
# MAGIC shared_buffers hit ratio per database (24 h). On Lakebase this is only the first cache tier in front of the local SSD cache, so judge lfc_hit_rate first; a low value here is informational unless the LFC rate is also low.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Superseded by:** `lfc_hit_rate` (not reported while one of them has an open finding on the same endpoint)

# COMMAND ----------

# check: buffer_hit_rate
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
       100.0 * sum(shared_blks_hit) / NULLIF(sum(shared_blks_hit) + sum(shared_blks_read), 0) AS measure
FROM {catalog}.{schema}.raw_database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id, datname
HAVING sum(shared_blks_hit) + sum(shared_blks_read) > 10000
"""
run_delta(SQL, "buffer_hit_rate", measure="measure", thresholds={'info': 95.0, 'warning': 90.0}, direction="below")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `connection_utilization`: Connections near the compute limit
# MAGIC
# MAGIC **Category:** capacity | **Severity:** critical if `measure` > 85; warning if `measure` > 70 | **Schedule:** every run
# MAGIC
# MAGIC Peak open backends as a share of max_connections (24 h). Near the limit, new connections fail (53300). Use a client-side pool; with autoscaling the limit follows min(max CU, 8 x min CU), so raise min CU if needed.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: connection_utilization
SQL = r"""
WITH conn AS (
  SELECT project_id, branch_id, endpoint_id, compute_id, ts, sum(numbackends) AS backends
  FROM {catalog}.{schema}.raw_database_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, compute_id, ts),
lim AS (
  SELECT compute_id, date_trunc('MINUTE', ts) AS m, max(max_connections) AS max_conn
  FROM {catalog}.{schema}.raw_compute_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY compute_id, date_trunc('MINUTE', ts))
SELECT conn.project_id, conn.branch_id, conn.endpoint_id, conn.endpoint_id AS object,
       100.0 * max(conn.backends / NULLIF(lim.max_conn, 0)) AS measure,
       max(conn.backends) AS peak_backends, max(lim.max_conn) AS max_connections
FROM conn JOIN lim ON conn.compute_id = lim.compute_id AND date_trunc('MINUTE', conn.ts) = lim.m
GROUP BY conn.project_id, conn.branch_id, conn.endpoint_id
"""
run_delta(SQL, "connection_utilization", measure="measure", thresholds={'warning': 70.0, 'critical': 85.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `cpu_saturation`: CPU saturated (p95 over 24 h)
# MAGIC
# MAGIC **Category:** capacity | **Severity:** critical if `measure` > 85; warning if `measure` > 70 | **Schedule:** every run
# MAGIC
# MAGIC p95 of CPU used versus the compute's logical CPUs over the last 24 hours. Sustained high CPU slows every query. Fix the top CPU queries first (see top_db_time_query); if CPU is pinned at the autoscaling maximum, raise max CU.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `Optimize the top CPU queries, or raise autoscaling_limit_max_cu on {object}.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)

# COMMAND ----------

# check: cpu_saturation
SQL = r"""
WITH c AS (
  SELECT project_id, branch_id, endpoint_id, compute_id, ts,
         host_cpu_seconds / NULLIF(timestampdiff(MILLISECOND, previous_ts, ts) / 1000.0, 0) AS cores_used
  FROM {catalog}.{schema}.raw_compute_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS),
g AS (
  SELECT compute_id, date_trunc('MINUTE', ts) AS m, max(host_logical_cpus) AS cpus
  FROM {catalog}.{schema}.raw_compute_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY compute_id, date_trunc('MINUTE', ts))
SELECT c.project_id, c.branch_id, c.endpoint_id, c.endpoint_id AS object,
       100 * percentile_approx(c.cores_used / NULLIF(g.cpus, 0), 0.95) AS measure,
       100 * max(c.cores_used / NULLIF(g.cpus, 0)) AS max_cpu_pct
FROM c JOIN g ON c.compute_id = g.compute_id AND date_trunc('MINUTE', c.ts) = g.m
GROUP BY c.project_id, c.branch_id, c.endpoint_id
"""
run_delta(SQL, "cpu_saturation", measure="measure", thresholds={'warning': 70.0, 'critical': 85.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `cu_sizing`: Compute size does not match the workload
# MAGIC
# MAGIC **Category:** capacity | **Severity:** warning if `measure` > 1; info if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Compute size against 7 days of telemetry. measure = 2 (undersized) when, for at least an hour at the largest size the compute reached, p95 CPU is above 85%, the working set exceeds the cache, or the cache hit rate is below 95% over at least 1M reads. measure = 1 (oversized) when an always-on compute of 2 CU or more sat at its minimum size with p95 CPU under 20%, a working set under 40% of the cache and a 99% hit rate. Short bursts, cold caches after scale-to-zero and pressure while still scaled down are ignored.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `measure 2: raise max CU (min CU if cache-bound). measure 1: lower min CU or enable scale-to-zero on non-prod.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Superseded by:** `cpu_saturation`, `lfc_hit_rate`, `working_set_vs_cache` (not reported while one of them has an open finding on the same endpoint)

# COMMAND ----------

# check: cu_sizing
SQL = r"""
WITH s AS (
  SELECT c.project_id, c.branch_id, c.endpoint_id, timestampdiff(SECOND, c.previous_ts, c.ts) AS secs,
         c.host_cpu_seconds / NULLIF(timestampdiff(MILLISECOND, c.previous_ts, c.ts) / 1000.0, 0)
           / NULLIF(g.host_logical_cpus, 0) AS cpu,
         c.lfc_hits, c.lfc_misses, g.host_memory_total_bytes AS mem,
         g.lfc_working_set_size_60m / NULLIF(g.lfc_size_limit, 0) AS ws
  FROM {catalog}.{schema}.raw_compute_counters c
  JOIN {catalog}.{schema}.raw_compute_gauges g
    ON c.compute_id = g.compute_id AND date_trunc('MINUTE', c.ts) = date_trunc('MINUTE', g.ts)
  WHERE c.ts >= current_timestamp() - INTERVAL 7 DAYS
    -- a sample spanning a suspend or restart gives meaningless CPU rates
    AND timestampdiff(SECOND, c.previous_ts, c.ts) BETWEEN 1 AND 300),
r AS (SELECT endpoint_id, max(mem) AS mem_max, min(mem) AS mem_min FROM s GROUP BY endpoint_id),
a AS (
  SELECT s.project_id, s.branch_id, s.endpoint_id, max(r.mem_max) AS mem_max, max(r.mem_min) AS mem_min,
         sum(s.secs) / 3600.0 AS hours,
         sum(CASE WHEN s.mem = r.mem_max THEN s.secs ELSE 0 END) / 3600.0 AS hours_at_max,
         sum(CASE WHEN s.mem = r.mem_min THEN s.secs ELSE 0 END) / 3600.0 AS hours_at_min,
         percentile_approx(CASE WHEN s.mem = r.mem_max THEN s.cpu END, 0.95) AS cpu_max,
         percentile_approx(CASE WHEN s.mem = r.mem_min THEN s.cpu END, 0.95) AS cpu_min,
         percentile_approx(CASE WHEN s.mem = r.mem_max THEN s.ws END, 0.95) AS ws_max,
         percentile_approx(CASE WHEN s.mem = r.mem_min THEN s.ws END, 0.95) AS ws_min,
         sum(CASE WHEN s.mem = r.mem_max THEN s.lfc_hits + s.lfc_misses END) AS reads_max,
         100.0 * try_divide(sum(CASE WHEN s.mem = r.mem_max THEN s.lfc_hits END),
                            sum(CASE WHEN s.mem = r.mem_max THEN s.lfc_hits + s.lfc_misses END)) AS hit_max,
         sum(CASE WHEN s.mem = r.mem_min THEN s.lfc_hits + s.lfc_misses END) AS reads_min,
         100.0 * try_divide(sum(CASE WHEN s.mem = r.mem_min THEN s.lfc_hits END),
                            sum(CASE WHEN s.mem = r.mem_min THEN s.lfc_hits + s.lfc_misses END)) AS hit_min
  FROM s JOIN r ON r.endpoint_id = s.endpoint_id
  GROUP BY s.project_id, s.branch_id, s.endpoint_id)
-- classify (plain SQL only below this line: a unit test runs it on sqlite)
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       mem_max / 1073741824.0 AS max_size_gib, mem_min / 1073741824.0 AS min_size_gib, hours, hours_at_max,
       hours_at_min, 100 * cpu_max AS p95_cpu_pct_at_max, 100 * cpu_min AS p95_cpu_pct_at_min,
       100 * ws_max AS p95_ws_pct_at_max, 100 * ws_min AS p95_ws_pct_at_min, hit_max AS lfc_hit_pct_at_max,
       reads_max AS lfc_reads_at_max,
       CASE WHEN hours_at_max >= 1
                 AND (cpu_max > 0.85 OR ws_max > 1.0 OR (reads_max >= 1000000 AND hit_max < 95)) THEN 2
            WHEN mem_min >= 3.5 * 1073741824 AND hours >= 151 AND hours_at_min >= 0.9 * hours
                 AND cpu_min < 0.20 AND ws_min < 0.40
                 AND (reads_min IS NULL OR reads_min < 1000000 OR hit_min >= 99) THEN 1
            ELSE 0 END AS measure
FROM a
"""
run_delta(SQL, "cu_sizing", measure="measure", thresholds={'info': 0.0, 'warning': 1.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `lfc_hit_rate`: Local SSD cache hit rate below 99%
# MAGIC
# MAGIC **Category:** capacity | **Severity:** critical if `measure` < 95; warning if `measure` < 99 | **Schedule:** every run
# MAGIC
# MAGIC Share of page reads served from the compute's local file cache instead of remote storage (24 h). OLTP needs 99%+. If the working set exceeds the cache, raise min CU (the cache follows the current size) or reduce pages read.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `Raise min CU on {object} or index the queries reading the most pages.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)

# COMMAND ----------

# check: lfc_hit_rate
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       100.0 * sum(lfc_hits) / NULLIF(sum(lfc_hits) + sum(lfc_misses), 0) AS measure,
       sum(lfc_hits) AS hits, sum(lfc_misses) AS misses
FROM {catalog}.{schema}.raw_compute_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id
HAVING sum(lfc_hits) + sum(lfc_misses) > 10000
"""
run_delta(SQL, "lfc_hit_rate", measure="measure", thresholds={'warning': 99.0, 'critical': 95.0}, direction="below")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `storage_utilization`: Storage near its limit
# MAGIC
# MAGIC **Category:** capacity | **Severity:** critical if `measure` > 85; warning if `measure` > 70 | **Schedule:** every run
# MAGIC
# MAGIC Used storage as a share of the limit (latest reading, 24 h). Write performance drops at the quota. Drop unused or duplicate indexes, reclaim bloat, archive cold data, or shorten the restore window.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: storage_utilization
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       100.0 * max_by(total_size, ts) / NULLIF(max_by(total_size_limit, ts), 0) AS measure,
       max_by(total_size, ts) AS used_bytes, max_by(total_size_limit, ts) AS limit_bytes
FROM {catalog}.{schema}.raw_compute_gauges
WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND total_size IS NOT NULL
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "storage_utilization", measure="measure", thresholds={'warning': 70.0, 'critical': 85.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `working_set_vs_cache`: Working set larger than the compute cache
# MAGIC
# MAGIC **Category:** capacity | **Severity:** critical if `measure` > 100; warning if `measure` > 80 | **Schedule:** every run
# MAGIC
# MAGIC Peak 60-minute working set as a share of the local cache size (both in bytes, 24 h). Above 100% the data a workload touches no longer fits in cache. Raise min/max CU or shrink the working set (unused indexes, bloat).
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Superseded by:** `lfc_hit_rate` (not reported while one of them has an open finding on the same endpoint)

# COMMAND ----------

# check: working_set_vs_cache
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       100.0 * max(lfc_working_set_size_60m) / NULLIF(max(lfc_size_limit), 0) AS measure,
       max(lfc_working_set_size_60m) AS working_set_bytes, max(lfc_size_limit) AS cache_bytes
FROM {catalog}.{schema}.raw_compute_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "working_set_vs_cache", measure="measure", thresholds={'warning': 80.0, 'critical': 100.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `write_backpressure`: Storage throttled writes
# MAGIC
# MAGIC **Category:** capacity | **Severity:** critical if `measure` > 600; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Seconds in the last 24 h during which storage throttled writes because WAL volume exceeded what it could absorb. Batch bulk loads, prefer COPY, drop unused indexes, and split huge UPDATE/DELETE statements.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: write_backpressure
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       sum(backpressure_throttling_seconds) AS measure, sum(host_disk_written_bytes) AS disk_written_bytes
FROM {catalog}.{schema}.raw_compute_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "write_backpressure", measure="measure", thresholds={'warning': 0.0, 'critical': 600.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: connectivity

# COMMAND ----------

# MAGIC %md
# MAGIC #### `connection_drops`: Client connections dropped (SSL EOF, resets)
# MAGIC
# MAGIC **Category:** connectivity | **Severity:** critical if `measure` > 1000; warning if `measure` > 100; info if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC Connections that ended abnormally in the last 24 hours, per compute (compute_mode tells primary from read replica). ssl_eof and client_lost usually mean the client or a proxy/load balancer closed an idle connection; set the pool's max idle time below 24 hours, enable TCP keepalives and validate connections before use. If compute_restarts is non-zero the compute restarted (scale to zero, replica restart or failover) and dropped its sessions; retry on connection errors and check replica_lag for the same window. TLS client-certificate failures are excluded (a client configuration issue, not a drop).
# MAGIC
# MAGIC - **Candidate fix** (manual, risk safe): ``
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)

# COMMAND ----------

# check: connection_drops
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object, any_value(compute_mode) AS compute_mode,
       sum(CASE WHEN message RLIKE '(?i)^(SSL error: unexpected eof|could not accept SSL connection: (EOF detected|Connection reset))' THEN 1 ELSE 0 END)
         + sum(CASE WHEN message RLIKE '^(could not receive data from client|could not send data to client)' THEN 1 ELSE 0 END)
         + sum(CASE WHEN message RLIKE '^(connection to client lost|unexpected EOF on client connection)' THEN 1 ELSE 0 END) AS measure,
       sum(CASE WHEN message RLIKE '(?i)^(SSL error: unexpected eof|could not accept SSL connection: (EOF detected|Connection reset))' THEN 1 ELSE 0 END) AS ssl_eof,
       sum(CASE WHEN message RLIKE '^(could not receive data from client|could not send data to client)' THEN 1 ELSE 0 END) AS connection_reset,
       sum(CASE WHEN message RLIKE '^(connection to client lost|unexpected EOF on client connection)' THEN 1 ELSE 0 END) AS client_lost,
       sum(CASE WHEN message RLIKE '^(the database system is starting up|terminating walreceiver|could not receive data from WAL stream)' THEN 1 ELSE 0 END) AS compute_restarts,
       max(ts) AS last_seen
FROM {catalog}.{schema}.raw_postgres_logs
WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND sqlstate IN ('08006', '08P01', '57P01', '57P03')
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "connection_drops", measure="measure", thresholds={'info': 10.0, 'warning': 100.0, 'critical': 1000.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: forecast

# COMMAND ----------

# MAGIC %md
# MAGIC #### `storage_runway`: Storage projected to fill soon
# MAGIC
# MAGIC **Category:** forecast | **Severity:** critical if `measure` < 30; warning if `measure` < 90 | **Schedule:** every run
# MAGIC
# MAGIC Days until storage reaches its limit at the 7-day linear growth rate. Act before the runway is gone.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: storage_runway
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       (max_by(total_size_limit, ts) - max_by(total_size, ts))
         / NULLIF(regr_slope(total_size, unix_timestamp(ts)) * 86400, 0) AS measure,
       regr_slope(total_size, unix_timestamp(ts)) * 86400 AS growth_bytes_per_day
FROM {catalog}.{schema}.raw_compute_gauges
WHERE ts >= current_timestamp() - INTERVAL 7 DAYS AND total_size IS NOT NULL
GROUP BY project_id, branch_id, endpoint_id
HAVING regr_slope(total_size, unix_timestamp(ts)) > 0
"""
run_delta(SQL, "storage_runway", measure="measure", thresholds={'warning': 90.0, 'critical': 30.0}, direction="below")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: incidents

# COMMAND ----------

# MAGIC %md
# MAGIC #### `commit_quorum_wait`: Commits wait for the WAL storage quorum
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 5; warning if `measure` > 2 | **Schedule:** every run
# MAGIC
# MAGIC Longest stretch (seconds) one statement spent in the SyncRep wait in the last 24 hours. The commit is written but the storage quorum (safekeepers) has not acknowledged it yet; normal is a few milliseconds (fleet p99 22 ms). While it waits the session keeps its row locks, so other writers queue behind it and can hit statement timeouts. This is a platform-side delay, not application contention: open a support case with the endpoint ID and the first_seen/last_seen window. hot_row_lock shows which waiters were blocked by these commits.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk safe): ``
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)

# COMMAND ----------

# check: commit_quorum_wait
SQL = r"""
WITH stalls AS (
  SELECT project_id, branch_id, endpoint_id, pid, unix_millis(ts) - query_age_ms AS query_start_ms,
         (unix_millis(max(ts)) - unix_millis(min(ts))) / 1000.0 + 1 AS stall_s, min(ts) AS first_ts, max(ts) AS last_ts
  FROM {catalog}.{schema}.raw_active_session_history
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND wait_event = 'SyncRep' AND query_age_ms IS NOT NULL
  GROUP BY project_id, branch_id, endpoint_id, pid, unix_millis(ts) - query_age_ms
)
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object, max(stall_s) AS measure,
       count(*) AS stalled_statements, sum(CASE WHEN stall_s >= 2 THEN 1 ELSE 0 END) AS stalls_over_2s,
       min(first_ts) AS first_seen, max(last_ts) AS last_seen
FROM stalls
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "commit_quorum_wait", measure="measure", thresholds={'warning': 2.0, 'critical': 5.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `deadlocks`: Deadlocks detected
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 2; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Deadlocks in the last 2 hours per database. Lock rows in a consistent order, keep transactions short, retry on 40P01, and index foreign keys (missing_fk_index) to narrow lock scope. Details are in postgres_logs (40P01).
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: deadlocks
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object, sum(deadlocks) AS measure
FROM {catalog}.{schema}.raw_database_counters WHERE ts >= current_timestamp() - INTERVAL 2 HOURS
GROUP BY project_id, branch_id, endpoint_id, datname
"""
run_delta(SQL, "deadlocks", measure="measure", thresholds={'warning': 0.0, 'critical': 2.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `gin_cleanup_lock_wait`: Writes wait for a GIN pending-list cleanup
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 600; warning if `measure` > 60 | **Schedule:** every run
# MAGIC
# MAGIC Total seconds sessions waited on page locks in the last 24 hours (avg_wait_s per wait). On Lakebase this is the GIN pending-list cleanup lock: with fastupdate on, a write that finds the pending list over gin_pending_list_limit merges it into the index itself, and other writers queue behind it. See gin_pending_list for the index. Options, cheapest first: drop the index if it is unused on every compute; index only the text you search (and scope it, e.g. by tenant prefix with btree_gin); run gin_clean_pending_list on a schedule from one worker with a short lock_timeout; or set fastupdate = off so each write pays only for its own entries.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk safe): ``
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)

# COMMAND ----------

# check: gin_cleanup_lock_wait
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object, sum(wait_event_time) AS measure,
       sum(wait_event_count) AS waits, sum(wait_event_time) / NULLIF(sum(wait_event_count), 0) AS avg_wait_s,
       max(ts) AS last_seen
FROM {catalog}.{schema}.raw_wait_event_counters
WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND wait_class_name = 'Lock' AND wait_event_name = 'page'
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "gin_cleanup_lock_wait", measure="measure", thresholds={'warning': 60.0, 'critical': 600.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `hot_row_lock`: Writes wait on a row lock
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 10; warning if `measure` > 3 | **Schedule:** every run
# MAGIC
# MAGIC Longest time (seconds) a sampled statement waited for a row lock (tuple or transactionid) in the last 24 hours, per query. Each wait is joined to the blocking session's sample at the same instant to name the cause in likely_cause: commit_wait means the lock holder was itself waiting for the WAL storage quorum (platform side, see commit_quorum_wait); idle_in_transaction means the application holds a transaction open between statements (set idle_in_transaction_session_timeout, commit sooner); active means real hot-key contention (shorten the transaction, avoid many writers updating the same row, batch per key, or use SKIP LOCKED for queues). not_sampled means the holder was not captured at that instant.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk safe): ``
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: hot_row_lock
SQL = r"""
WITH waits AS (
  SELECT w.project_id, w.branch_id, w.endpoint_id, w.queryid, w.pid, w.query_age_ms,
         CASE WHEN b.pid IS NULL THEN 'not_sampled'
              WHEN b.wait_event = 'SyncRep' THEN 'commit_wait'
              WHEN b.backend_state IN (3, 4) THEN 'idle_in_transaction'
              ELSE 'active' END AS blocker_class
  FROM (SELECT t.* FROM {catalog}.{schema}.raw_active_session_history t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid FROM {catalog}.{schema}.raw_plan_history WHERE ts >= current_timestamp() - INTERVAL 8 DAYS AND (coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$')) x ON t.branch_id = x.branch_id AND t.userid = x.userid) w
  LEFT JOIN (SELECT t.* FROM {catalog}.{schema}.raw_active_session_history t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid FROM {catalog}.{schema}.raw_plan_history WHERE ts >= current_timestamp() - INTERVAL 8 DAYS AND (coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$')) x ON t.branch_id = x.branch_id AND t.userid = x.userid) b
    ON b.endpoint_id = w.endpoint_id AND b.ts = w.ts AND b.pid = w.blocking_pid
   AND b.ts >= current_timestamp() - INTERVAL 24 HOURS
  WHERE w.ts >= current_timestamp() - INTERVAL 24 HOURS AND w.wait_event_type = 'Lock'
    AND w.wait_event IN ('tuple', 'transactionid')
), agg AS (
  SELECT project_id, branch_id, endpoint_id, queryid, max(query_age_ms) / 1000.0 AS measure,
         count(*) AS samples, count(DISTINCT pid) AS waiting_sessions,
         sum(CASE WHEN blocker_class = 'commit_wait' THEN 1 ELSE 0 END) AS blocked_by_commit_wait,
         sum(CASE WHEN blocker_class = 'idle_in_transaction' THEN 1 ELSE 0 END) AS blocked_by_idle_in_transaction,
         sum(CASE WHEN blocker_class = 'active' THEN 1 ELSE 0 END) AS blocked_by_active,
         sum(CASE WHEN blocker_class = 'not_sampled' THEN 1 ELSE 0 END) AS blocker_not_sampled
  FROM waits
  GROUP BY project_id, branch_id, endpoint_id, queryid
)
SELECT project_id, branch_id, endpoint_id,
       endpoint_id || '/' || coalesce(cast(queryid AS STRING), 'unknown') AS object, cast(queryid AS STRING) AS queryid,
       measure, samples, waiting_sessions, blocked_by_commit_wait, blocked_by_idle_in_transaction,
       blocked_by_active, blocker_not_sampled,
       CASE WHEN greatest(blocked_by_commit_wait, blocked_by_idle_in_transaction, blocked_by_active) = 0 THEN 'not_sampled'
            WHEN blocked_by_commit_wait >= greatest(blocked_by_idle_in_transaction, blocked_by_active) THEN 'commit_wait'
            WHEN blocked_by_idle_in_transaction >= blocked_by_active THEN 'idle_in_transaction'
            ELSE 'active' END AS likely_cause
FROM agg
"""
run_delta(SQL, "hot_row_lock", measure="measure", thresholds={'warning': 3.0, 'critical': 10.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `idle_in_tx_time`: Sessions spend a lot of time idle in transaction
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 20; warning if `measure` > 5 | **Schedule:** every run
# MAGIC
# MAGIC Share of session time spent idle inside an open transaction (24 h). Such sessions hold locks and block vacuum. Set idle_in_transaction_session_timeout (30-60 s) and fix application transaction scoping.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: idle_in_tx_time
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
       100.0 * sum(idle_in_transaction_time_ms) / NULLIF(sum(session_time_ms), 0) AS measure,
       sum(sessions_killed) AS sessions_killed, sum(sessions_fatal) AS sessions_fatal
FROM {catalog}.{schema}.raw_database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id, datname
HAVING sum(session_time_ms) > 600000
"""
run_delta(SQL, "idle_in_tx_time", measure="measure", thresholds={'warning': 5.0, 'critical': 20.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `lock_blocking_history`: Sessions were blocked by other sessions
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 300; warning if `measure` > 60 | **Schedule:** every run
# MAGIC
# MAGIC Longest time (seconds) a sampled session spent blocked by another session in the last 2 hours. Usually an idle-in-transaction session holding a row lock. Set idle_in_transaction_session_timeout and lock_timeout.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Superseded by:** `blocked_session` (not reported while one of them has an open finding on the same endpoint)

# COMMAND ----------

# check: lock_blocking_history
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       max(query_age_ms) / 1000.0 AS measure, count(DISTINCT pid) AS blocked_pids, count(*) AS samples
FROM {catalog}.{schema}.raw_active_session_history
WHERE ts >= current_timestamp() - INTERVAL 2 HOURS AND blocking_pid IS NOT NULL AND blocking_pid <> 0
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "lock_blocking_history", measure="measure", thresholds={'warning': 60.0, 'critical': 300.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `lock_wait_share`: Lock waits are a large share of wait time
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 25; warning if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC Share of wait time in the Lock class (24 h), excluding client think-time and platform background waits (Activity, Extension:Neon*). See blocked_session and missing_fk_index.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: lock_wait_share
SQL = r"""
WITH w AS (
  SELECT project_id, branch_id, endpoint_id, wait_class_name, sum(wait_event_time) AS t
  FROM {catalog}.{schema}.raw_wait_event_counters
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND wait_class_name NOT IN ('Activity', 'Client')
    AND NOT (wait_class_name = 'Extension' AND wait_event_name RLIKE '^Neon')
  GROUP BY project_id, branch_id, endpoint_id, wait_class_name)
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       100.0 * sum(CASE WHEN wait_class_name = 'Lock' THEN t ELSE 0 END) / NULLIF(sum(t), 0) AS measure,
       sum(t) AS total_wait_s
FROM w GROUP BY project_id, branch_id, endpoint_id HAVING sum(t) > 60
"""
run_delta(SQL, "lock_wait_share", measure="measure", thresholds={'warning': 10.0, 'critical': 25.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `postgres_errors`: Infrastructure-level Postgres errors
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 10; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Count of resource, operator, system or internal errors (SQLSTATE 53*, 57P*, 58*, XX*), idle-in-transaction terminations (25P03) and lock-not-available (55P03) in the last 2 hours. Deadlocks are covered separately.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: postgres_errors
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || sqlstate AS object, sqlstate,
       count(*) AS measure, max(regexp_replace(message, '[0-9]+', 'N')) AS message_pattern, max(ts) AS last_seen
FROM {catalog}.{schema}.raw_postgres_logs
WHERE ts >= current_timestamp() - INTERVAL 2 HOURS AND severity IN ('ERROR', 'FATAL', 'PANIC')
  AND (sqlstate RLIKE '^(53|57P|58|XX)' OR sqlstate IN ('25P03', '55P03'))
GROUP BY project_id, branch_id, endpoint_id, sqlstate
"""
run_delta(SQL, "postgres_errors", measure="measure", thresholds={'warning': 0.0, 'critical': 10.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `statement_timeouts`: Statements cancelled by statement_timeout
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 100; warning if `measure` > 10; info if `measure` > 1 | **Schedule:** every run
# MAGIC
# MAGIC Statements cancelled by statement_timeout (SQLSTATE 57014) in the last 24 hours. A cancelled write is lost unless the client retries. while_locking_row counts cancellations whose error context says the statement was waiting for a row lock; check hot_row_lock for who held it (application transaction vs. a commit stalled on the storage quorum). The rest timed out doing work: see top_db_time_query, storage_wait and gin_cleanup_lock_wait.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk safe): ``
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)

# COMMAND ----------

# check: statement_timeouts
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object, count(*) AS measure,
       sum(CASE WHEN context RLIKE '(?i)while (locking|updating|deleting) tuple' THEN 1 ELSE 0 END) AS while_locking_row,
       any_value(compute_mode) AS compute_mode, max(ts) AS last_seen
FROM {catalog}.{schema}.raw_postgres_logs
WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND sqlstate = '57014'
  AND message LIKE 'canceling statement due to statement timeout%'
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "statement_timeouts", measure="measure", thresholds={'info': 1.0, 'warning': 10.0, 'critical': 100.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: indexing

# COMMAND ----------

# MAGIC %md
# MAGIC #### `seq_scan_read_ratio`: Database reads far more rows than it uses
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 100; warning if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC rows_returned (rows read, dominated by sequential scans) divided by rows_fetched (rows from index scans) per database over 24 h. A high ratio means missing indexes; see seq_scan_heavy and top_db_time_query.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Superseded by:** `seq_scan_heavy` (not reported while one of them has an open finding on the same database)

# COMMAND ----------

# check: seq_scan_read_ratio
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object, datname,
       sum(rows_returned) / NULLIF(sum(rows_fetched), 0) AS measure,
       sum(rows_returned) AS rows_returned, sum(rows_fetched) AS rows_fetched
FROM {catalog}.{schema}.raw_database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id, datname
HAVING sum(rows_returned) > 1000000
"""
run_delta(SQL, "seq_scan_read_ratio", measure="measure", thresholds={'warning': 10.0, 'critical': 100.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC #### `xid_runway`: Transaction ID age is high
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 1e+09; warning if `measure` > 2e+08 | **Schedule:** every run
# MAGIC
# MAGIC Current age of the oldest unfrozen transaction ID per database, with days until 1.5 billion at the 7-day rate. Remove freeze blockers (xmin_horizon_holder, replication_slot_retention) and VACUUM (FREEZE) the oldest tables.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Superseded by:** `xid_age_table` (not reported while one of them has an open finding on the same database)

# COMMAND ----------

# check: xid_runway
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object, datname,
       max_by(oldest_frozen_xid_age, ts) AS measure,
       (1500000000 - max_by(oldest_frozen_xid_age, ts))
         / NULLIF(regr_slope(oldest_frozen_xid_age, unix_timestamp(ts)) * 86400, 0) AS days_to_1_5b,
       max_by(oldest_mxid_age, ts) AS mxid_age
FROM {catalog}.{schema}.raw_database_gauges WHERE ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY project_id, branch_id, endpoint_id, datname
"""
run_delta(SQL, "xid_runway", measure="measure", thresholds={'warning': 200000000.0, 'critical': 1000000000.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: performance

# COMMAND ----------

# MAGIC %md
# MAGIC #### `disk_spill`: Query spills sorts or hashes to disk
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 100; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Executions in the last 24 h whose plan sorted on disk or used more than one hash batch, per query and role. Raise work_mem for that role (not globally), or add an index that delivers the sort order.
# MAGIC
# MAGIC - **Candidate fix** (setting, risk moderate, reversible): `ALTER ROLE {role_ident} SET work_mem = '64MB';`
# MAGIC - **Rollback:** `ALTER ROLE {role_ident} RESET work_mem;`
# MAGIC - **Fixer can apply it:** no: outside the fixer's allowlist (comments, string literals and dollar quoting are not allowed), so nothing is proposed; apply it by hand
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: disk_spill
SQL = r"""
SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) || '@' || usename AS object, CAST(queryid AS STRING) AS queryid, usename,
       concat('"', replace(usename, '"', '""'), '"') AS role_ident, max(query_text) AS query_text,
       count(*) AS measure, max(temp_blks_written) AS max_temp_blks,
       max(CASE WHEN plan_json RLIKE '"Sort Space Type":\\s*"Disk"' THEN 1 ELSE 0 END) AS disk_sort,
       max(CASE WHEN plan_json RLIKE '"Hash Batches":\\s*([2-9]|[1-9][0-9]+)' THEN 1 ELSE 0 END) AS hash_spill
FROM (SELECT * FROM {catalog}.{schema}.raw_plan_history WHERE NOT ((coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$') OR (coalesce(application_name, '') IN ('lakebase-health', 'lakebase-health-fixer'))))
WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND analyze_enabled AND usename IS NOT NULL
  AND (plan_json RLIKE '"Sort Space Type":\\s*"Disk"' OR plan_json RLIKE '"Hash Batches":\\s*([2-9]|[1-9][0-9]+)')
GROUP BY project_id, branch_id, endpoint_id, queryid, usename
"""
run_delta(SQL, "disk_spill", measure="measure", thresholds={'warning': 0.0, 'critical': 100.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `plan_flip`: Query switches between fast and slow plans
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 10; warning if `measure` > 2 | **Schedule:** every run
# MAGIC
# MAGIC Ratio between the slowest and fastest median latency of a query's plan shapes on the same endpoint (7 days, >= 5 executions per shape). Fix statistics first; pin the good plan with pg_hint_plan only as a last resort.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `ANALYZE the tables of query {object}; if the bad plan persists, pin the good one with a pg_hint_plan hint.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: plan_flip
SQL = r"""
WITH p AS (
  SELECT project_id, branch_id, endpoint_id, queryid, plan_fingerprint, max(query_text) AS query_text,
         count(*) AS execs, percentile_approx(duration_ms, 0.5) AS p50_ms
  FROM (SELECT * FROM {catalog}.{schema}.raw_plan_history WHERE NOT ((coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$') OR (coalesce(application_name, '') IN ('lakebase-health', 'lakebase-health-fixer')))) WHERE ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY project_id, branch_id, endpoint_id, queryid, plan_fingerprint HAVING count(*) >= 5)
SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, CAST(queryid AS STRING) AS queryid, max(query_text) AS query_text,
       count(*) AS plan_shapes, max(p50_ms) / NULLIF(min(p50_ms), 0) AS measure
FROM p GROUP BY project_id, branch_id, endpoint_id, queryid HAVING count(*) > 1
"""
run_delta(SQL, "plan_flip", measure="measure", thresholds={'warning': 2.0, 'critical': 10.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `query_regression`: Query got slower than its baseline
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 2; warning if `measure` > 1.5 | **Schedule:** every run
# MAGIC
# MAGIC Mean latency in the last 24 h divided by the previous 6 days, per endpoint and query (>= 50 calls in each window). Correlate with recent DDL and statistics; fix with ANALYZE / CREATE STATISTICS before pinning plans.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: query_regression
SQL = r"""
WITH w AS (
  SELECT project_id, branch_id, endpoint_id, queryid, max(query) AS query_text,
         CASE WHEN ts >= current_timestamp() - INTERVAL 24 HOURS THEN 'recent' ELSE 'baseline' END AS win,
         sum(calls) AS calls, sum(total_exec_time) AS t
  FROM (SELECT t.* FROM {catalog}.{schema}.raw_pg_stat_statements_counters t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid FROM {catalog}.{schema}.raw_plan_history WHERE ts >= current_timestamp() - INTERVAL 8 DAYS AND (coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$')) x ON t.branch_id = x.branch_id AND t.userid = x.userid)
  WHERE ts >= current_timestamp() - INTERVAL 7 DAYS AND toplevel
  GROUP BY project_id, branch_id, endpoint_id, queryid,
           CASE WHEN ts >= current_timestamp() - INTERVAL 24 HOURS THEN 'recent' ELSE 'baseline' END)
SELECT r.project_id, r.branch_id, r.endpoint_id, CAST(r.queryid AS STRING) AS object, CAST(r.queryid AS STRING) AS queryid, r.query_text,
       b.t / NULLIF(b.calls, 0) AS baseline_mean_ms, r.t / NULLIF(r.calls, 0) AS recent_mean_ms,
       r.calls AS recent_calls,
       (r.t / NULLIF(r.calls, 0)) / NULLIF(b.t / NULLIF(b.calls, 0), 0) AS measure
FROM w r JOIN w b ON r.endpoint_id = b.endpoint_id AND r.queryid = b.queryid
                 AND r.win = 'recent' AND b.win = 'baseline'
WHERE r.calls >= 50 AND b.calls >= 50
"""
run_delta(SQL, "query_regression", measure="measure", thresholds={'warning': 1.5, 'critical': 2.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `row_misestimate`: Planner row estimates are far off
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 50; warning if `measure` > 10; info if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Share of the endpoint's database time (24 h) spent in read queries whose planner row estimate is off by 10x or more in the median execution (at least 50 calls, 1 s total and 3 sampled executions over 2 hours). The worst 10 by time are listed with estimated and actual rows. Run ANALYZE on the tables they read; add CREATE STATISTICS (dependencies, ndistinct) for correlated columns.
# MAGIC
# MAGIC - **Finding title:** `Row estimates far off for {queries} queries`
# MAGIC - **Candidate fix** (manual, risk safe): `ANALYZE the tables read by the queries in evidence.top_queries; consider CREATE STATISTICS (dependencies, ndistinct) on correlated columns.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: row_misestimate
SQL = r"""
WITH e AS (
  SELECT project_id, branch_id, endpoint_id, queryid, ts, plan_rows, actual_rows,
         greatest(greatest(actual_rows, 1) / greatest(plan_rows, 1),
                  greatest(plan_rows, 1) / greatest(actual_rows, 1)) AS factor
  FROM (SELECT * FROM {catalog}.{schema}.raw_plan_history WHERE NOT ((coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$') OR (coalesce(application_name, '') IN ('lakebase-health', 'lakebase-health-fixer'))))
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND analyze_enabled
    -- writes return 0 rows from ModifyTable; a redacted plan (PII mask) cannot show the node type, so it counts
    -- only when rows came back
    AND ((plan_json RLIKE '"Node Type"' AND NOT plan_json RLIKE '"Node Type":\\s*"ModifyTable"')
         OR (NOT plan_json RLIKE '"Node Type"' AND actual_rows > 0))),
m AS (
  SELECT project_id, branch_id, endpoint_id, queryid, count(*) AS sampled_execs,
         count(DISTINCT date_trunc('HOUR', ts)) AS sampled_hours, percentile_approx(factor, 0.5) AS factor,
         percentile_approx(plan_rows, 0.5) AS est_rows, percentile_approx(actual_rows, 0.5) AS actual_rows
  FROM e GROUP BY project_id, branch_id, endpoint_id, queryid),
s AS (
  SELECT endpoint_id, queryid, sum(calls) AS calls, sum(total_exec_time) AS total_ms
  FROM (SELECT t.* FROM {catalog}.{schema}.raw_pg_stat_statements_counters t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid FROM {catalog}.{schema}.raw_plan_history WHERE ts >= current_timestamp() - INTERVAL 8 DAYS AND (coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$')) x ON t.branch_id = x.branch_id AND t.userid = x.userid)
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND toplevel
  GROUP BY endpoint_id, queryid),
t AS (SELECT endpoint_id, sum(total_ms) AS endpoint_ms FROM s GROUP BY endpoint_id),
q AS (
  SELECT m.*, s.calls, s.total_ms
  FROM m JOIN s ON s.endpoint_id = m.endpoint_id AND s.queryid = m.queryid
  WHERE s.calls >= 50 AND s.total_ms >= 1000 AND m.sampled_execs >= 3 AND m.sampled_hours >= 2
    AND m.factor >= 10)
SELECT q.project_id, q.branch_id, q.endpoint_id, q.endpoint_id AS object, count(*) AS queries,
       round(sum(q.total_ms), 1) AS misestimated_ms, round(max(t.endpoint_ms), 1) AS endpoint_ms,
       100.0 * sum(q.total_ms) / NULLIF(max(t.endpoint_ms), 0) AS measure,
       slice(sort_array(collect_list(named_struct(
         'total_ms', round(q.total_ms, 1), 'queryid', CAST(q.queryid AS STRING), 'calls', q.calls,
         'est_rows', q.est_rows, 'actual_rows', q.actual_rows, 'factor', round(q.factor, 1))), false), 1, 10)
         AS top_queries
FROM q JOIN t ON t.endpoint_id = q.endpoint_id
GROUP BY q.project_id, q.branch_id, q.endpoint_id
"""
run_delta(SQL, "row_misestimate", measure="measure", thresholds={'info': 0.0, 'warning': 10.0, 'critical': 50.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `sort_limit_waste`: Sort reads far more rows than the LIMIT returns
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 1000; warning if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC For plans topped by Limit with a Sort beneath, the largest row count read by a plan node divided by the rows the LIMIT returned (24 h). An index matching ORDER BY lets Postgres stop early instead of sorting everything.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `Create an index matching the ORDER BY of query {object}, verified on a branch.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: sort_limit_waste
SQL = r"""
WITH s AS (
  SELECT project_id, branch_id, endpoint_id, queryid, query_text,
         regexp_extract_all(plan_json, '"Node Type":\\s*"([^"]+)"', 1) AS types,
         transform(regexp_extract_all(plan_json, '"Actual Rows":\\s*([0-9.]+)', 1), x -> try_cast(x AS DOUBLE)) AS rws
  FROM (SELECT * FROM {catalog}.{schema}.raw_plan_history WHERE NOT ((coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$') OR (coalesce(application_name, '') IN ('lakebase-health', 'lakebase-health-fixer'))))
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND analyze_enabled)
SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, CAST(queryid AS STRING) AS queryid, max(query_text) AS query_text,
       count(*) AS execs, max(array_max(rws)) AS max_rows_read, max(try_element_at(rws, 1)) AS limit_rows,
       max(array_max(rws) / greatest(try_element_at(rws, 1), 1)) AS measure
FROM s
WHERE try_element_at(types, 1) = 'Limit'
  AND (array_contains(slice(types, 2, 3), 'Sort') OR array_contains(slice(types, 2, 3), 'Incremental Sort'))
GROUP BY project_id, branch_id, endpoint_id, queryid
"""
run_delta(SQL, "sort_limit_waste", measure="measure", thresholds={'warning': 10.0, 'critical': 1000.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `storage_wait`: Query waits on remote storage
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 30; warning if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC Average share of a query's duration spent waiting on pageserver reads after local cache misses (24 h, >= 10 executions). Index it so it reads fewer pages, enlarge the cache (min CU), or pg_prewarm hot tables.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: storage_wait
SQL = r"""
SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, CAST(queryid AS STRING) AS queryid, max(query_text) AS query_text,
       count(*) AS execs, avg(duration_ms) AS avg_ms,
       100 * avg(neon_getpage_wait_us / 1000.0 / NULLIF(duration_ms, 0)) AS measure
FROM (SELECT * FROM {catalog}.{schema}.raw_plan_history WHERE NOT ((coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$') OR (coalesce(application_name, '') IN ('lakebase-health', 'lakebase-health-fixer')))) WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id, queryid HAVING count(*) >= 10
"""
run_delta(SQL, "storage_wait", measure="measure", thresholds={'warning': 10.0, 'critical': 30.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `top_db_time_query`: One query dominates database time
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 50; warning if `measure` > 25 | **Schedule:** every run
# MAGIC
# MAGIC Share of the endpoint's total execution time spent in one normalized query (24 h, at least 60 s total). Optimize it first: check its plan (disk_spill, row_misestimate) and indexes (seq_scan_heavy, missing_fk_index).
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Query scoped:** telemetry from platform, engine and fixer roles is filtered out (the subqueries the engine adds, visible in the SQL below)

# COMMAND ----------

# check: top_db_time_query
SQL = r"""
WITH q AS (
  SELECT project_id, branch_id, endpoint_id, queryid, max(query) AS query_text, sum(calls) AS calls,
         sum(total_exec_time) AS total_ms, sum(temp_blks_written) AS temp_blks_written
  FROM (SELECT t.* FROM {catalog}.{schema}.raw_pg_stat_statements_counters t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid FROM {catalog}.{schema}.raw_plan_history WHERE ts >= current_timestamp() - INTERVAL 8 DAYS AND (coalesce(usename, '') IN ('cloud_admin', 'databricks_control_plane', 'databricks_monitor', '00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000f1') OR coalesce(usename, '') RLIKE '^databricks_writer_[0-9]+$')) x ON t.branch_id = x.branch_id AND t.userid = x.userid)
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND toplevel
  GROUP BY project_id, branch_id, endpoint_id, queryid)
SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, CAST(queryid AS STRING) AS queryid, query_text, calls,
       total_ms / NULLIF(calls, 0) AS mean_ms, temp_blks_written,
       100.0 * total_ms / NULLIF(sum(total_ms) OVER (PARTITION BY endpoint_id), 0) AS measure
FROM q
QUALIFY total_ms > 60000
"""
run_delta(SQL, "top_db_time_query", measure="measure", thresholds={'warning': 25.0, 'critical': 50.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: replication

# COMMAND ----------

# MAGIC %md
# MAGIC #### `replica_lag`: Read replica lags behind the primary
# MAGIC
# MAGIC **Category:** replication | **Severity:** critical if `measure` > 30; warning if `measure` > 1 | **Schedule:** every run
# MAGIC
# MAGIC p95 replication lag in seconds for read replicas over 24 h. Size the replica like the primary, shorten long replica queries, and smooth bulk writes on the primary.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: replica_lag
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
       percentile_approx(replication_lag_time_ms, 0.95) / 1000.0 AS measure,
       max(replication_lag_bytes) AS max_lag_bytes
FROM {catalog}.{schema}.raw_compute_gauges
WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND compute_mode <> 'primary'
  AND replication_lag_time_ms IS NOT NULL
GROUP BY project_id, branch_id, endpoint_id
"""
run_delta(SQL, "replica_lag", measure="measure", thresholds={'warning': 1.0, 'critical': 30.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: security

# COMMAND ----------

# MAGIC %md
# MAGIC #### `auth_failures`: Repeated authentication failures
# MAGIC
# MAGIC **Category:** security | **Severity:** critical if `measure` > 50; warning if `measure` > 5 | **Schedule:** every run
# MAGIC
# MAGIC Failed logins (28P01, 28000) per user in the last 24 h. Check for expired OAuth tokens in clients, or for password guessing against roles with native passwords.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: auth_failures
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || coalesce(usename, '?') AS object,
       count(*) AS measure, max(ts) AS last_seen
FROM {catalog}.{schema}.raw_postgres_logs
WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND sqlstate IN ('28P01', '28000')
GROUP BY project_id, branch_id, endpoint_id, usename
"""
run_delta(SQL, "auth_failures", measure="measure", thresholds={'warning': 5.0, 'critical': 50.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### telemetry: throughput

# COMMAND ----------

# MAGIC %md
# MAGIC #### `rollback_ratio`: High transaction rollback ratio
# MAGIC
# MAGIC **Category:** throughput | **Severity:** critical if `measure` > 5; warning if `measure` > 1 | **Schedule:** every run
# MAGIC
# MAGIC Share of transactions rolled back per database (24 h). Look up the cause in postgres_errors: serialization failures, deadlocks, constraint violations, or ORMs rolling back read-only transactions.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: rollback_ratio
SQL = r"""
SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
       100.0 * sum(xact_rollback) / NULLIF(sum(xact_commit) + sum(xact_rollback), 0) AS measure,
       (sum(xact_commit) + sum(xact_rollback)) / NULLIF(sum(timestampdiff(SECOND, previous_ts, ts)), 0) AS avg_tps
FROM {catalog}.{schema}.raw_database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY project_id, branch_id, endpoint_id, datname
HAVING sum(xact_commit) + sum(xact_rollback) > 1000
"""
run_delta(SQL, "rollback_ratio", measure="measure", thresholds={'warning': 1.0, 'critical': 5.0}, direction="above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Postgres checks
# MAGIC
# MAGIC Run over a Postgres connection on one endpoint (the `endpoint_id` widget, or the project's default-branch read-write endpoint), read-only, with the engine's session settings. Checks that run per database run in every database of the branch, like the engine; the others run once. You need a Postgres role on the branch (OAuth). Connecting wakes a suspended compute, which the engine never does (it skips inactive computes).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: config

# COMMAND ----------

# MAGIC %md
# MAGIC #### `config_drift`: Role or database setting override
# MAGIC
# MAGIC **Category:** config | **Severity:** info if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC A setting overridden with ALTER ROLE / ALTER DATABASE (platform roles and settings excluded). The setting value is part of the finding identity, so a changed value appears as a new finding and the old one resolves.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: config_drift
SQL = r"""
SELECT coalesce(r.rolname, '*') || '@' || coalesce(d.datname, '*') || ':' || cfg AS object,
       coalesce(r.rolname, '*') AS role_name, coalesce(d.datname, '*') AS database_name, cfg AS setting,
       1.0::float8 AS measure
FROM pg_db_role_setting s
LEFT JOIN pg_roles r ON r.oid = s.setrole LEFT JOIN pg_database d ON d.oid = s.setdatabase
CROSS JOIN LATERAL unnest(s.setconfig) AS cfg
WHERE coalesce(r.rolname, '') !~ '^(pg_|databricks_)' AND coalesce(r.rolname, '') NOT IN ('cloud_admin', 'databricks_control_plane', 'databricks_superuser', 'databricks_monitor', 'authenticator')
  AND split_part(cfg, '=', 1) !~ '^(databricks|neon|neon_monitor|compute_ctl|lakebase)\.|^neon_'
"""
run_pg(SQL, "config_drift", measure="measure", thresholds={'info': 0.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `guardrail_settings`: Guardrail setting missing or too loose
# MAGIC
# MAGIC **Category:** config | **Severity:** warning if `measure` > 1; info if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC One result per setting. idle_in_transaction_session_timeout above 60 s (Lakebase default is 5 min) is a warning with a database-level fix; statement_timeout = 0 and track_io_timing = off are informational.
# MAGIC
# MAGIC - **Candidate fix** (setting, risk moderate, reversible): `{fix_sql}`
# MAGIC - **Rollback:** `{fix_rollback}`
# MAGIC - **Fixer can apply it:** no: outside the fixer's allowlist (statement is not one of the allowed fix kinds), so nothing is proposed; apply it by hand
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: guardrail_settings
SQL = r"""
SELECT current_database() || ':' || name AS object, name, setting,
       quote_ident(current_database()) AS db_ident,
       CASE name  -- nested CASE: Postgres does not guarantee AND evaluation order, and setting::bigint fails on 'off'
         WHEN 'idle_in_transaction_session_timeout' THEN CASE WHEN setting = '0' THEN 2.0
                                                            WHEN setting::bigint > 60000 THEN 2.0 ELSE 0.0 END
         WHEN 'statement_timeout' THEN CASE WHEN setting = '0' THEN 1.0 ELSE 0.0 END
         WHEN 'track_io_timing' THEN CASE WHEN setting = 'off' THEN 1.0 ELSE 0.0 END
         ELSE 0.0 END AS measure,
       CASE WHEN name = 'idle_in_transaction_session_timeout'
            THEN 'ALTER DATABASE ' || quote_ident(current_database()) || ' SET idle_in_transaction_session_timeout = ''60s'';' END AS fix_sql,
       CASE WHEN name = 'idle_in_transaction_session_timeout'
            THEN 'ALTER DATABASE ' || quote_ident(current_database()) || ' RESET idle_in_transaction_session_timeout;' END AS fix_rollback
FROM pg_settings
WHERE name IN ('idle_in_transaction_session_timeout', 'statement_timeout', 'track_io_timing')
"""
run_pg(SQL, "guardrail_settings", measure="measure", thresholds={'info': 0.0, 'warning': 1.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: forecast

# COMMAND ----------

# MAGIC %md
# MAGIC #### `sequence_exhaustion`: Sequence or identity column nearing its limit
# MAGIC
# MAGIC **Category:** forecast | **Severity:** critical if `measure` > 75; warning if `measure` > 50 | **Schedule:** every run
# MAGIC
# MAGIC Share of the usable range consumed, measured against the column type (an int4 column fails at 2^31-1 even with a bigint sequence). est_days_left uses the insert rate since statistics reset. Migrate to bigint, rehearsed on a branch.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `Migrate {object} to bigint: add a bigint column, backfill in batches, swap in a window; rehearse on a branch.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: sequence_exhaustion
SQL = r"""
WITH seq_cols AS (
  SELECT n.nspname AS seq_schema, sc.relname AS seq_name,
         quote_ident(tn.nspname) || '.' || quote_ident(t.relname) AS table_ident, a.attname AS column_name,
         format_type(a.atttypid, a.atttypmod) AS column_type, t.oid AS table_oid,
         (CASE a.atttypid WHEN 'int2'::regtype THEN 32767 WHEN 'int4'::regtype THEN 2147483647
                          ELSE 9223372036854775807 END)::numeric AS column_max
  FROM pg_depend d
  JOIN pg_class sc ON sc.oid = d.objid AND sc.relkind = 'S'
  JOIN pg_namespace n ON n.oid = sc.relnamespace
  JOIN pg_class t ON t.oid = d.refobjid JOIN pg_namespace tn ON tn.oid = t.relnamespace
  JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
  WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
    AND tn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage'))
SELECT sc.table_ident || '.' || quote_ident(sc.column_name) AS object, sc.column_type, ps.last_value,
       least(sc.column_max, ps.max_value::numeric) AS effective_max,
       (100.0 * ps.last_value / least(sc.column_max, ps.max_value::numeric))::float8 AS measure,
       ((least(sc.column_max, ps.max_value::numeric) - ps.last_value)
         / NULLIF(t.n_tup_ins / NULLIF(extract(epoch FROM now() - coalesce(
             (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()),
             pg_postmaster_start_time())), 0), 0) / 86400)::float8 AS est_days_left
FROM seq_cols sc
JOIN pg_sequences ps ON ps.schemaname = sc.seq_schema AND ps.sequencename = sc.seq_name
LEFT JOIN pg_stat_user_tables t ON t.relid = sc.table_oid
WHERE ps.last_value IS NOT NULL
"""
run_pg(SQL, "sequence_exhaustion", measure="measure", thresholds={'warning': 50.0, 'critical': 75.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: incidents

# COMMAND ----------

# MAGIC %md
# MAGIC #### `blocked_session`: Session blocked by another session
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 300; warning if `measure` > 60 | **Schedule:** every run
# MAGIC
# MAGIC A session waiting on a lock held by another session (seconds blocked). Resolve the blocker first; durable fixes are idle_in_transaction_session_timeout, lock_timeout for DDL and short transactions.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `Blocking pid {blocking_pid} ({blocking_state}); end it if safe: SELECT pg_terminate_backend({blocking_pid});`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: blocked_session
SQL = r"""
SELECT 'pid ' || a.pid AS object, a.pid AS blocked_pid, b.pid AS blocking_pid, b.state AS blocking_state,
       a.usename, extract(epoch FROM now() - a.query_start)::float8 AS measure, left(a.query, 200) AS query_text
FROM pg_stat_activity a
CROSS JOIN LATERAL unnest(pg_blocking_pids(a.pid)) AS bp(pid)
JOIN pg_stat_activity b ON b.pid = bp.pid
WHERE a.pid <> pg_backend_pid() AND a.usename NOT IN ('cloud_admin')
"""
run_pg(SQL, "blocked_session", measure="measure", thresholds={'warning': 60.0, 'critical': 300.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `idle_in_transaction`: Session idle inside an open transaction
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 300; warning if `measure` > 60 | **Schedule:** every run
# MAGIC
# MAGIC A session that opened a transaction and went idle (seconds in that state). It holds its locks and snapshot, blocking writers and vacuum. Lower idle_in_transaction_session_timeout and fix the application.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `After confirming with the session owner: SELECT pg_terminate_backend({pid});`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: idle_in_transaction
SQL = r"""
SELECT 'pid ' || pid AS object, pid, usename, application_name,
       extract(epoch FROM now() - state_change)::float8 AS measure,
       extract(epoch FROM now() - xact_start)::float8 AS xact_age_s, left(query, 200) AS query_text
FROM pg_stat_activity
WHERE state LIKE 'idle in transaction%' AND pid <> pg_backend_pid() AND usename NOT IN ('cloud_admin')
"""
run_pg(SQL, "idle_in_transaction", measure="measure", thresholds={'warning': 60.0, 'critical': 300.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `long_transaction`: Long-running transaction
# MAGIC
# MAGIC **Category:** incidents | **Severity:** critical if `measure` > 1800; warning if `measure` > 300 | **Schedule:** every run
# MAGIC
# MAGIC An active transaction open for more than 5 minutes (seconds). Long transactions hold locks and block vacuum. Set statement_timeout for application roles and split long batch jobs.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `Investigate pid {pid}; cancel with SELECT pg_cancel_backend({pid}); if safe.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: long_transaction
SQL = r"""
SELECT 'pid ' || pid AS object, pid, usename, application_name, wait_event_type, wait_event,
       extract(epoch FROM now() - xact_start)::float8 AS measure, left(query, 200) AS query_text
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND xact_start IS NOT NULL AND state = 'active'
  AND pid <> pg_backend_pid() AND usename NOT IN ('cloud_admin')
"""
run_pg(SQL, "long_transaction", measure="measure", thresholds={'warning': 300.0, 'critical': 1800.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: indexing

# COMMAND ----------

# MAGIC %md
# MAGIC #### `duplicate_index`: Duplicate index
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 1.07374e+09; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Two or more indexes with identical table, columns, operator classes, expressions and predicate. Keep one (unique and primary-key indexes are always kept) and drop the rest.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk high, reversible): `DROP INDEX CONCURRENTLY {index_ident};`
# MAGIC - **Rollback:** `{index_def_concurrently};`
# MAGIC - **Fixer can apply it:** yes, after approval in the app (allowlisted as drop_index, verified on a branch first)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: duplicate_index
SQL = r"""
WITH d AS (
  SELECT i.indexrelid, i.indisunique, i.indisprimary,
         i.indrelid::text || '|' || i.indclass::text || '|' || i.indkey::text || '|' ||
         coalesce(i.indexprs::text, '') || '|' || coalesce(i.indpred::text, '') AS sig,
         quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, n.nspname
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')),
r AS (SELECT d.*, row_number() OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS rn,
             first_value(ident) OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS kept
      FROM d)
SELECT ident AS object, ident AS index_ident, kept AS kept_index,
       pg_relation_size(indexrelid)::float8 AS measure, pg_get_indexdef(indexrelid) AS index_def
FROM r WHERE rn > 1 AND NOT indisunique AND NOT indisprimary
"""
run_pg(SQL, "duplicate_index", measure="measure", thresholds={'warning': 0.0, 'critical': 1073741824.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `gin_pending_list`: GIN index pending list near its limit
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 90; warning if `measure` > 50; info if `measure` > 25 | **Schedule:** every run
# MAGIC
# MAGIC How full the GIN fast-update pending list is, as a percentage of gin_pending_list_limit, at the time of the check. When a write pushes the list past the limit, that write merges the whole list into the index itself, which can take seconds and blocks other writers (see gin_cleanup_lock_wait). Check idx_scan on every compute before keeping a large GIN index; then prefer a smaller index (only the searched text, scoped by tenant), a scheduled gin_clean_pending_list, or fastupdate = off if per-write cost is acceptable.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk safe): ``
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database
# MAGIC - **Needs extension:** `pgstattuple` (skipped where it is not installed)

# COMMAND ----------

# check: gin_pending_list
SQL = r"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
       quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS index_ident, t.relname AS table_name,
       g.pending_pages, g.pending_tuples, lim.limit_kb, s.idx_scan, pg_relation_size(c.oid)::float8 AS index_bytes,
       coalesce((SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'fastupdate'), 'on') AS fastupdate,
       (100.0 * g.pending_pages * current_setting('block_size')::int / (1024.0 * lim.limit_kb))::float8 AS measure
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_am a ON a.oid = c.relam AND a.amname = 'gin'
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class t ON t.oid = i.indrelid
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = c.oid
CROSS JOIN LATERAL (SELECT coalesce((SELECT option_value::int FROM pg_options_to_table(c.reloptions)
                                     WHERE option_name = 'gin_pending_list_limit'),
                                    (SELECT setting::int FROM pg_settings WHERE name = 'gin_pending_list_limit')) AS limit_kb) lim
CROSS JOIN LATERAL pgstatginindex(c.oid::regclass) g
WHERE i.indisvalid AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "gin_pending_list", measure="measure", thresholds={'info': 25.0, 'warning': 50.0, 'critical': 90.0}, direction="above",
       per_database=True, requires_extension='pgstattuple', guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `invalid_index`: Invalid index left by a failed build
# MAGIC
# MAGIC **Category:** indexing | **Severity:** warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC An index marked invalid (usually a failed CREATE INDEX CONCURRENTLY). It is maintained on every write but never used for reads. Drop it, or rebuild it with REINDEX INDEX CONCURRENTLY after fixing the cause.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `DROP INDEX CONCURRENTLY {index_ident};  -- or: REINDEX INDEX CONCURRENTLY {index_ident};`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: invalid_index
SQL = r"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
       quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS index_ident, 1.0::float8 AS measure
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT i.indisvalid AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "invalid_index", measure="measure", thresholds={'warning': 0.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `missing_fk_index`: Foreign key without a supporting index
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 1.07374e+09; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC The referencing columns of a foreign key have no index (measure = child table size). Every DELETE or key UPDATE on the parent then scans the child while holding locks, a common cause of lock waits and deadlocks.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk moderate, reversible): `CREATE INDEX CONCURRENTLY {index_name} ON {child_table} ({fk_columns});`
# MAGIC - **Rollback:** `DROP INDEX CONCURRENTLY IF EXISTS {index_schema}.{index_name};`
# MAGIC - **Fixer can apply it:** yes, after approval in the app (allowlisted as create_index, verified on a branch first)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: missing_fk_index
SQL = r"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(t.relname) || ' (' || cols.list || ')' AS object,
       quote_ident(n.nspname) || '.' || quote_ident(t.relname) AS child_table, cols.list AS fk_columns,
       quote_ident(n.nspname) AS index_schema,
       quote_ident(left(t.relname || '_' || cols.names || '_fk_idx', 63)) AS index_name,
       c.conname AS constraint_name, pg_relation_size(t.oid)::float8 AS measure
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace
CROSS JOIN LATERAL (
  SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY array_position(c.conkey, a.attnum)) AS list,
         string_agg(a.attname, '_' ORDER BY array_position(c.conkey, a.attnum)) AS names
  FROM pg_attribute a WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)) cols
WHERE c.contype = 'f' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
  AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.conrelid
                  AND (i.indkey::text || ' ') LIKE array_to_string(c.conkey, ' ') || ' %')
"""
run_pg(SQL, "missing_fk_index", measure="measure", thresholds={'warning': 0.0, 'critical': 1073741824.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `oversized_search_index`: Large GIN/GiST search index
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 1.07374e+10; warning if `measure` > 1.07374e+09 | **Schedule:** every run
# MAGIC
# MAGIC GIN or GiST index of 1 GB or more (measure = bytes). Every insert and update re-indexes the indexed text, so a search index over a whole document (for example a trigram index on value::text) costs write latency, storage, vacuum time and cache. If searches are always scoped (one user or tenant), a B-tree on the scope key plus filtering those rows is often enough: check idx_scan on every compute and EXPLAIN (ANALYZE, BUFFERS) for the largest scopes on a branch. If a search index is needed, index only the searched text, put the scope key in it (btree_gin), and prefer full-text search (tsvector) over trigrams when word matching is enough.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk safe): ``
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: oversized_search_index
SQL = r"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
       quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS index_ident, t.relname AS table_name,
       a.amname AS access_method, (i.indexprs IS NOT NULL) AS is_expression, s.idx_scan,
       pg_relation_size(c.oid)::float8 AS measure, pg_total_relation_size(t.oid)::float8 AS table_total_bytes,
       pg_get_indexdef(c.oid) AS index_def
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_am a ON a.oid = c.relam AND a.amname IN ('gin', 'gist')
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class t ON t.oid = i.indrelid
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = c.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage') AND pg_relation_size(c.oid) >= 268435456
"""
run_pg(SQL, "oversized_search_index", measure="measure", thresholds={'warning': 1073741824.0, 'critical': 10737418240.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `redundant_index`: Index covered by a wider index
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 1.07374e+09; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC A non-unique index whose columns are a leading prefix of another valid index on the same table with the same access method, e.g. (a) next to (a, b). The wider index serves the same lookups.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk high, reversible): `DROP INDEX CONCURRENTLY {index_ident};`
# MAGIC - **Rollback:** `{index_def_concurrently};`
# MAGIC - **Fixer can apply it:** yes, after approval in the app (allowlisted as drop_index, verified on a branch first)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: redundant_index
SQL = r"""
SELECT quote_ident(na.nspname) || '.' || quote_ident(ca.relname) AS object,
       quote_ident(na.nspname) || '.' || quote_ident(ca.relname) AS index_ident,
       quote_ident(nb.nspname) || '.' || quote_ident(cb.relname) AS covered_by,
       pg_relation_size(a.indexrelid)::float8 AS measure, pg_get_indexdef(a.indexrelid) AS index_def
FROM pg_index a
JOIN pg_index b ON a.indrelid = b.indrelid AND a.indexrelid <> b.indexrelid
JOIN pg_class ca ON ca.oid = a.indexrelid JOIN pg_namespace na ON na.oid = ca.relnamespace
JOIN pg_class cb ON cb.oid = b.indexrelid JOIN pg_namespace nb ON nb.oid = cb.relnamespace
WHERE ca.relam = cb.relam AND NOT a.indisunique AND NOT a.indisprimary AND b.indisvalid
  AND a.indpred IS NULL AND b.indpred IS NULL AND a.indexprs IS NULL AND b.indexprs IS NULL
  AND b.indkey::text LIKE a.indkey::text || ' %'
  AND na.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "redundant_index", measure="measure", thresholds={'warning': 0.0, 'critical': 1073741824.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `seq_scan_heavy`: Table read mostly by large sequential scans
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 1e+06; warning if `measure` > 10000 | **Schedule:** every run
# MAGIC
# MAGIC Average rows read per sequential scan on a table with more than 10k live rows. Find the queries scanning it (top_db_time_query) and index their filter and join columns; verify the index on a branch.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `Index the filter columns of the queries scanning {object}.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database
# MAGIC - **Confidence:** low unless `stats_window_hours >= 24`

# COMMAND ----------

# check: seq_scan_heavy
SQL = r"""
SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object, seq_scan, idx_scan, n_live_tup,
       seq_tup_read, (seq_tup_read::float8 / NULLIF(seq_scan, 0)) AS measure,
       pg_relation_size(relid)::float8 AS table_bytes
FROM pg_stat_user_tables
WHERE n_live_tup > 10000 AND seq_scan > 0 AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "seq_scan_heavy", measure="measure", thresholds={'warning': 10000.0, 'critical': 1000000.0}, direction="above",
       per_database=True, requires_extension=None, guards=('stats_window_hours >= 24',))

# COMMAND ----------

# MAGIC %md
# MAGIC #### `unused_index`: Index never used
# MAGIC
# MAGIC **Category:** indexing | **Severity:** critical if `measure` > 1.07374e+09; warning if `measure` > 1.04858e+08; info if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Non-unique, non-constraint index with zero scans since statistics reset (measure = size in bytes) on the primary and on every read replica of the branch (usage counters are per compute, so an index can serve only replica reads). It costs writes and storage. Confidence is low when a replica could not be read or its counters were reset recently; only trust this when the statistics window covers a full business cycle.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk high, reversible): `DROP INDEX CONCURRENTLY {index_ident};`
# MAGIC - **Rollback:** `{index_def_concurrently};`
# MAGIC - **Fixer can apply it:** yes, after approval in the app (allowlisted as drop_index, verified on a branch first)
# MAGIC - **Superseded by:** `duplicate_index`, `redundant_index` (not reported while one of them has an open finding on the same object)
# MAGIC - **Runs:** in every database
# MAGIC - **Confidence:** low unless `stats_window_hours >= 24`
# MAGIC - **Replicas:** the engine keeps only objects every read replica on the branch also reports

# COMMAND ----------

# check: unused_index
SQL = r"""
SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS object,
       quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS index_ident,
       s.relname AS table_name, s.idx_scan, pg_relation_size(s.indexrelid)::float8 AS measure,
       pg_get_indexdef(s.indexrelid) AS index_def
FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0 AND NOT i.indisunique AND NOT i.indisprimary AND i.indisvalid
  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid)
  AND s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "unused_index", measure="measure", thresholds={'info': 0.0, 'warning': 104857600.0, 'critical': 1073741824.0}, direction="above",
       per_database=True, requires_extension=None, guards=('stats_window_hours >= 24',))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC #### `autovacuum_blocker`: Dead tuples pile up because something blocks vacuum
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 30; warning if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC Links a table's dead tuples (measure = dead %) to the blocker preventing cleanup: autovacuum disabled on the table, an old snapshot from a long transaction, a replication slot, or a prepared transaction.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `Remove the blocker ({blocker}) first; see the matching finding for {object}.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: autovacuum_blocker
SQL = r"""
WITH dead AS (
  SELECT relid, quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
         (100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0))::float8 AS dead_pct
  FROM pg_stat_user_tables WHERE n_live_tup + n_dead_tup > 10000 AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')),
blockers AS (
  SELECT 'long_transaction' AS kind, max(age(backend_xmin))::float8 AS xmin_age FROM pg_stat_activity
    WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid() AND usename NOT IN ('cloud_admin')
  UNION ALL
  SELECT 'replication_slot', max(greatest(age(xmin), age(catalog_xmin)))::float8 FROM pg_replication_slots
    WHERE slot_name NOT IN ('wal_retention_slot', 'wal_proposer_slot')
  UNION ALL
  SELECT 'prepared_transaction', max(age(transaction))::float8 FROM pg_prepared_xacts),
x AS (
  SELECT d.object, d.dead_pct AS measure,
         CASE WHEN c.reloptions @> ARRAY['autovacuum_enabled=false'] THEN 'autovacuum_disabled'
              ELSE (SELECT kind FROM blockers WHERE xmin_age > 1000000 ORDER BY xmin_age DESC LIMIT 1) END AS blocker
  FROM dead d JOIN pg_class c ON c.oid = d.relid WHERE d.dead_pct > 10)
SELECT object, measure, blocker FROM x WHERE blocker IS NOT NULL
"""
run_pg(SQL, "autovacuum_blocker", measure="measure", thresholds={'warning': 10.0, 'critical': 30.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `autovacuum_disabled`: Autovacuum disabled on a table
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC The table has autovacuum_enabled = false, so dead tuples accumulate and transaction IDs are never frozen by autovacuum. Re-enable it unless a documented process vacuums this table.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk safe, reversible): `ALTER TABLE {table_ident} RESET (autovacuum_enabled);`
# MAGIC - **Rollback:** `ALTER TABLE {table_ident} SET (autovacuum_enabled = false);`
# MAGIC - **Fixer can apply it:** yes, after approval in the app (allowlisted as reloptions)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: autovacuum_disabled
SQL = r"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
       quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_ident,
       array_to_string(c.reloptions, ',') AS reloptions, 1.0::float8 AS measure
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm') AND c.reloptions @> ARRAY['autovacuum_enabled=false']
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "autovacuum_disabled", measure="measure", thresholds={'warning': 0.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `autovacuum_scale_factor`: Large table needs per-table autovacuum tuning
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 30; warning if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC A table with more than 1M live rows, default autovacuum scale factor and more than 10% dead tuples. The default 0.2 waits for 20% of the table to be dead; lower it per table.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk safe, reversible): `ALTER TABLE {table_ident} SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.01);`
# MAGIC - **Rollback:** `ALTER TABLE {table_ident} RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_scale_factor);`
# MAGIC - **Fixer can apply it:** yes, after approval in the app (allowlisted as reloptions)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: autovacuum_scale_factor
SQL = r"""
SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.relname) AS object,
       quote_ident(s.schemaname) || '.' || quote_ident(s.relname) AS table_ident, s.n_live_tup, s.n_dead_tup,
       (100.0 * s.n_dead_tup / NULLIF(s.n_live_tup + s.n_dead_tup, 0))::float8 AS measure
FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
WHERE s.n_live_tup > 1000000 AND s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
  AND coalesce(array_to_string(c.reloptions, ','), '') NOT LIKE '%autovacuum_vacuum_scale_factor%'
  AND NOT coalesce(c.reloptions @> ARRAY['autovacuum_enabled=false'], false)
"""
run_pg(SQL, "autovacuum_scale_factor", measure="measure", thresholds={'warning': 10.0, 'critical': 30.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `dead_tuples`: High share of dead tuples
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 20; warning if `measure` > 10 | **Schedule:** every run
# MAGIC
# MAGIC Dead tuples as a share of all tuples on tables with more than 10k rows. Vacuum is falling behind or blocked; check autovacuum_blocker first, then tune autovacuum per table.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk safe, reversible): `VACUUM (ANALYZE) {table_ident};`
# MAGIC - **Rollback:** not needed — VACUUM changes no data
# MAGIC - **Fixer can apply it:** no: outside the fixer's allowlist (statement is not one of the allowed fix kinds), so nothing is proposed; apply it by hand
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: dead_tuples
SQL = r"""
SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
       quote_ident(schemaname) || '.' || quote_ident(relname) AS table_ident,
       n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count,
       (100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0))::float8 AS measure
FROM pg_stat_user_tables
WHERE n_live_tup + n_dead_tup > 10000 AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "dead_tuples", measure="measure", thresholds={'warning': 10.0, 'critical': 20.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `index_bloat`: B-tree index bloat
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` < 50; warning if `measure` < 70 | **Schedule:** daily
# MAGIC
# MAGIC Average leaf density of B-tree indexes over 100 MB (pgstatindex; daily). A fresh B-tree is about 90% dense. Rebuild online with REINDEX INDEX CONCURRENTLY.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk moderate, reversible): `REINDEX INDEX CONCURRENTLY {index_ident};`
# MAGIC - **Rollback:** not needed — REINDEX rebuilds the same index and changes no data
# MAGIC - **Fixer can apply it:** no: outside the fixer's allowlist (statement is not one of the allowed fix kinds), so nothing is proposed; apply it by hand
# MAGIC - **Runs:** in every database
# MAGIC - **Needs extension:** `pgstattuple` (skipped where it is not installed)

# COMMAND ----------

# check: index_bloat
SQL = r"""
SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS object,
       quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS index_ident,
       pg_relation_size(s.indexrelid)::float8 AS index_bytes, x.leaf_fragmentation::float8 AS leaf_fragmentation,
       x.avg_leaf_density::float8 AS measure
FROM pg_stat_user_indexes s
JOIN pg_class c ON c.oid = s.indexrelid JOIN pg_am am ON am.oid = c.relam AND am.amname = 'btree'
CROSS JOIN LATERAL pgstatindex(s.indexrelid) x
WHERE pg_relation_size(s.indexrelid) > 104857600 AND s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "index_bloat", measure="measure", thresholds={'warning': 70.0, 'critical': 50.0}, direction="below",
       per_database=True, requires_extension='pgstattuple', guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `prepared_transaction`: Forgotten prepared transaction
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 86400; warning if `measure` > 3600 | **Schedule:** every run
# MAGIC
# MAGIC A two-phase (prepared) transaction older than an hour (seconds). It holds locks and blocks vacuum until it is committed or rolled back by its transaction manager.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `If no transaction manager owns it: ROLLBACK PREPARED '{object}';`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: prepared_transaction
SQL = r"""
SELECT gid AS object, owner, database, extract(epoch FROM now() - prepared)::float8 AS measure,
       age(transaction)::float8 AS xid_age
FROM pg_prepared_xacts
"""
run_pg(SQL, "prepared_transaction", measure="measure", thresholds={'warning': 3600.0, 'critical': 86400.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `table_bloat`: Table bloat
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 40; warning if `measure` > 20 | **Schedule:** daily
# MAGIC
# MAGIC Approximate free space share in the 15 largest tables over 10 MB (pgstattuple_approx; runs daily because it reads pages). VACUUM makes space reusable but does not shrink; shrinking needs VACUUM FULL in a window.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `Plan VACUUM FULL {object} in a maintenance window (ACCESS EXCLUSIVE lock), then tune autovacuum.`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database
# MAGIC - **Needs extension:** `pgstattuple` (skipped where it is not installed)

# COMMAND ----------

# check: table_bloat
SQL = r"""
SELECT quote_ident(t.schemaname) || '.' || quote_ident(t.relname) AS object,
       pg_table_size(t.relid)::float8 AS table_bytes, a.dead_tuple_percent::float8 AS dead_pct,
       a.approx_free_percent::float8 AS measure
FROM (SELECT relid, schemaname, relname FROM pg_stat_user_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage') AND pg_table_size(relid) > 10485760
      ORDER BY pg_table_size(relid) DESC LIMIT 15) t
CROSS JOIN LATERAL pgstattuple_approx(t.relid) a
"""
run_pg(SQL, "table_bloat", measure="measure", thresholds={'warning': 20.0, 'critical': 40.0}, direction="above",
       per_database=True, requires_extension='pgstattuple', guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `xid_age_table`: Table with old unfrozen transaction IDs
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 1e+09; warning if `measure` > 2e+08 | **Schedule:** every run
# MAGIC
# MAGIC age(relfrozenxid) of a table above 100M. Freeze the oldest tables first; remove freeze blockers if age keeps rising.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk safe, reversible): `VACUUM (FREEZE) {table_ident};`
# MAGIC - **Rollback:** not needed — VACUUM FREEZE changes no data
# MAGIC - **Fixer can apply it:** no: outside the fixer's allowlist (statement is not one of the allowed fix kinds), so nothing is proposed; apply it by hand
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: xid_age_table
SQL = r"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
       quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_ident,
       age(c.relfrozenxid)::float8 AS measure, mxid_age(c.relminmxid)::float8 AS mxid_age
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage') AND age(c.relfrozenxid) > 100000000
"""
run_pg(SQL, "xid_age_table", measure="measure", thresholds={'warning': 200000000.0, 'critical': 1000000000.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `xmin_horizon_holder`: Session holds back vacuum and freezing
# MAGIC
# MAGIC **Category:** maintenance | **Severity:** critical if `measure` > 2e+08; warning if `measure` > 5e+07 | **Schedule:** every run
# MAGIC
# MAGIC A session whose snapshot (backend_xmin) is very old in transaction IDs. Vacuum cannot remove rows newer than it and freezing stalls. Find the owner and end the transaction.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `After confirming with the session owner: SELECT pg_terminate_backend({pid});`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: xmin_horizon_holder
SQL = r"""
SELECT 'pid ' || pid AS object, pid, usename, application_name, state, age(backend_xmin)::float8 AS measure,
       extract(epoch FROM now() - xact_start)::float8 AS xact_age_s, left(query, 200) AS query_text
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid() AND usename NOT IN ('cloud_admin')
"""
run_pg(SQL, "xmin_horizon_holder", measure="measure", thresholds={'warning': 50000000.0, 'critical': 200000000.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: performance

# COMMAND ----------

# MAGIC %md
# MAGIC #### `stale_statistics`: Table statistics are missing or stale
# MAGIC
# MAGIC **Category:** performance | **Severity:** critical if `measure` > 100; warning if `measure` > 20 | **Schedule:** every run
# MAGIC
# MAGIC Rows modified since the last ANALYZE as a share of live rows (100 when never analyzed). Stale statistics give the planner bad row estimates. Run ANALYZE; it is cheap and safe.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk safe, reversible): `ANALYZE {table_ident};`
# MAGIC - **Rollback:** not needed — ANALYZE only refreshes planner statistics
# MAGIC - **Fixer can apply it:** yes, after approval in the app (allowlisted as analyze)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: stale_statistics
SQL = r"""
SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
       quote_ident(schemaname) || '.' || quote_ident(relname) AS table_ident,
       n_live_tup, n_mod_since_analyze, last_analyze, last_autoanalyze,
       (CASE WHEN last_analyze IS NULL AND last_autoanalyze IS NULL THEN 100.0
             ELSE 100.0 * n_mod_since_analyze / NULLIF(n_live_tup, 0) END)::float8 AS measure
FROM pg_stat_user_tables
WHERE n_live_tup > 1000 AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
"""
run_pg(SQL, "stale_statistics", measure="measure", thresholds={'warning': 20.0, 'critical': 100.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: replication

# COMMAND ----------

# MAGIC %md
# MAGIC #### `replication_slot_retention`: Inactive replication slot retains WAL
# MAGIC
# MAGIC **Category:** replication | **Severity:** critical if `measure` > 1.07374e+09; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC An inactive replication slot (measure = WAL bytes retained) also holds back xmin/catalog_xmin. Lakebase platform slots are excluded. Confirm no consumer (Lakehouse Sync, CDC) owns it before dropping.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk high): `Confirm no consumer owns slot {object}, then: SELECT pg_drop_replication_slot('{object}');`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: replication_slot_retention
SQL = r"""
SELECT slot_name AS object, slot_type, plugin, age(xmin)::float8 AS xmin_age,
       age(catalog_xmin)::float8 AS catalog_xmin_age,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::float8 AS measure
FROM pg_replication_slots
WHERE NOT active AND slot_name NOT IN ('wal_retention_slot', 'wal_proposer_slot')
"""
run_pg(SQL, "replication_slot_retention", measure="measure", thresholds={'warning': 0.0, 'critical': 1073741824.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### `table_without_pk`: Table without a primary key
# MAGIC
# MAGIC **Category:** schema | **Severity:** warning if `measure` > 1.04858e+08; info if `measure` > -1 | **Schedule:** every run
# MAGIC
# MAGIC Tables without a primary key cannot be safely replicated or synced (logical replication, CDC) and are prone to duplicates. measure = table size; warning above 100 MB.
# MAGIC
# MAGIC - **Candidate fix** (manual, risk moderate): `Add a primary key to {object} (or a unique NOT NULL key).`
# MAGIC - **Fixer can apply it:** no (manual fix; a person applies it)
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: table_without_pk
SQL = r"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object, pg_relation_size(c.oid)::float8 AS measure
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage')
  AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = c.oid AND contype = 'p')
"""
run_pg(SQL, "table_without_pk", measure="measure", thresholds={'info': -1.0, 'warning': 104857600.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Postgres: security

# COMMAND ----------

# MAGIC %md
# MAGIC #### `non_ssl_sessions`: Client sessions without SSL
# MAGIC
# MAGIC **Category:** security | **Severity:** critical if `measure` > 10; warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC Remote client sessions not using SSL (platform agents on localhost are excluded). Always connect with sslmode=require.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: non_ssl_sessions
SQL = r"""
SELECT coalesce(a.usename, '?') || '@' || coalesce(host(a.client_addr), 'local') AS object,
       count(*)::float8 AS measure
FROM pg_stat_ssl s JOIN pg_stat_activity a USING (pid)
WHERE a.backend_type = 'client backend' AND NOT s.ssl AND a.usename NOT IN ('cloud_admin')
  AND a.client_addr IS DISTINCT FROM '127.0.0.1' AND a.client_addr IS DISTINCT FROM '::1'
GROUP BY 1
"""
run_pg(SQL, "non_ssl_sessions", measure="measure", thresholds={'warning': 0.0, 'critical': 10.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `privileged_roles`: Login role with powerful attributes
# MAGIC
# MAGIC **Category:** security | **Severity:** warning if `measure` > 1; info if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC A login role (not platform-managed) with BYPASSRLS, REPLICATION or CREATEROLE (measure = number of these). Keep such roles few and audited; use group roles for least privilege.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)
# MAGIC - **Runs:** once per endpoint (instance-wide catalogs)

# COMMAND ----------

# check: privileged_roles
SQL = r"""
SELECT quote_ident(rolname) AS object, rolcreaterole, rolcreatedb, rolbypassrls, rolreplication,
       (rolbypassrls::int + rolreplication::int + rolcreaterole::int)::float8 AS measure
FROM pg_roles
WHERE rolcanlogin AND rolname !~ '^(pg_|databricks_)' AND rolname NOT IN ('cloud_admin', 'databricks_control_plane', 'databricks_superuser', 'databricks_monitor', 'authenticator')
  AND (rolbypassrls OR rolreplication OR rolcreaterole)
"""
run_pg(SQL, "privileged_roles", measure="measure", thresholds={'info': 0.0, 'warning': 1.0}, direction="above",
       per_database=False, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC #### `public_schema_create`: Any role can create objects in a schema
# MAGIC
# MAGIC **Category:** security | **Severity:** warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC PUBLIC holds CREATE on the schema, so every role can create objects there (an object-squatting and privilege-escalation risk). Revoke it and grant CREATE only to owning roles.
# MAGIC
# MAGIC - **Candidate fix** (ddl, risk moderate, reversible): `REVOKE CREATE ON SCHEMA {schema_ident} FROM PUBLIC;`
# MAGIC - **Rollback:** `GRANT CREATE ON SCHEMA {schema_ident} TO PUBLIC;`
# MAGIC - **Fixer can apply it:** no: outside the fixer's allowlist (statement is not one of the allowed fix kinds), so nothing is proposed; apply it by hand
# MAGIC - **Runs:** in every database

# COMMAND ----------

# check: public_schema_create
SQL = r"""
SELECT quote_ident(nspname) AS object, quote_ident(nspname) AS schema_ident, 1.0::float8 AS measure
FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'hint_plan', 'sage') AND nspname NOT LIKE 'pg\_%'
  AND has_schema_privilege('public', oid, 'CREATE')
"""
run_pg(SQL, "public_schema_create", measure="measure", thresholds={'warning': 0.0}, direction="above",
       per_database=True, requires_extension=None, guards=())

# COMMAND ----------

# MAGIC %md
# MAGIC ## API checks
# MAGIC
# MAGIC Read the Lakebase API. With the engine wheel installed the cell calls the engine's own implementation (build_inventory + ApiExecutor); otherwise it runs a short SDK equivalent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: backup

# COMMAND ----------

# MAGIC %md
# MAGIC #### `backup_restore_window`: Point-in-time restore window is short
# MAGIC
# MAGIC **Category:** backup | **Severity:** critical if `measure` < 3; warning if `measure` < 7 | **Schedule:** every run
# MAGIC
# MAGIC The project's restore window in days (2-30, default 7). Production should keep at least 7 days; regulated workloads 14-30. Change it in project Settings > History window.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: backup_restore_window
run_api("backup_restore_window", measure="measure", thresholds={'warning': 7.0, 'critical': 3.0}, direction="below", function="backup_restore_window")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `default_branch_unprotected`: Default (production) branch is not protected
# MAGIC
# MAGIC **Category:** backup | **Severity:** warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC The project's default branch can be reset or deleted. Protect it (update-branch spec.is_protected = true).
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: default_branch_unprotected
run_api("default_branch_unprotected", measure="measure", thresholds={'warning': 0.0}, direction="above", function="default_branch_unprotected")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `primary_without_ha`: Primary compute has no high-availability secondary
# MAGIC
# MAGIC **Category:** backup | **Severity:** info if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC The default branch's read-write compute runs without HA secondaries. For production, configure an HA group (min >= 2) so a failure fails over automatically.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: primary_without_ha
run_api("primary_without_ha", measure="measure", thresholds={'info': 0.0}, direction="above", function="primary_without_ha")

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: security

# COMMAND ----------

# MAGIC %md
# MAGIC #### `password_roles`: Role uses a native Postgres password
# MAGIC
# MAGIC **Category:** security | **Severity:** warning if `measure` > 0 | **Schedule:** every run
# MAGIC
# MAGIC The role authenticates with a Postgres password instead of Databricks OAuth. Prefer OAuth roles, which leave no password to leak or rotate.
# MAGIC
# MAGIC - **Candidate fix:** none
# MAGIC - **Fixer can apply it:** no (no candidate fix)

# COMMAND ----------

# check: password_roles
run_api("password_roles", measure="measure", thresholds={'warning': 0.0}, direction="above", function="password_roles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The engine's findings
# MAGIC
# MAGIC What the engine currently reports for the project (and endpoint) in the widgets: open and new findings seen in the
# MAGIC last `lookback_hours`, by severity and by check, then the findings themselves. It reads
# MAGIC `<catalog>.<schema>.findings`, so it works without being enrolled. The last engine runs show how fresh it is.

# COMMAND ----------

_args = {"project_id": PROJECT_UID, "endpoint_id": ENDPOINT_UID, "hours": LOOKBACK_HOURS}
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
display(spark.sql(f"SELECT run_id, started_at, ended_at, status FROM {T('runs')} ORDER BY started_at DESC LIMIT 5"))
