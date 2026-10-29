import argparse
import json
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..checkspec import load_checks
from ..config import RAW_TABLES, EngineConfig, pii_masking_enabled
from ..ddl import ensure_masks, ensure_schema
from ..exclusions import load_exclusions
from ..executors.api import API_CHECKS, ApiExecutor
from ..executors.delta import DeltaExecutor
from ..executors.postgres import PostgresExecutor
from ..ingest import ingest_all
from ..inventory import build_inventory
from ..fixer.postcheck import run_postcheck
from ..record import load_last_ok, persist, persist_regressions, resolve_rolled_back
from ..runner import Runner

# Engine and analysis runs share one lock on `runs`; a run older than this was killed by the job timeout (both jobs
# set timeout_seconds to it), so its leftover 'running' row no longer holds the lock.
RUN_TIMEOUT = timedelta(hours=3)
CHECKS_DIR = Path(__file__).resolve().parent.parent / "checks"
APPLICATION_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def service_roles(cfg, me: str) -> tuple:
    """Roles whose queries are engine traffic: the configured SPs, plus the job identity when it is an SP."""
    return tuple(cfg.service_roles) + ((me,) if me and APPLICATION_ID_RE.match(me) else ())


def stale_from_watermarks(sql, cfg, now) -> set:
    """Sources whose last ingested telemetry is older than twice the ingest schedule (or never ingested)."""
    rows = sql.query(f"SELECT source_table, max_ts FROM {cfg.table('ingest_watermarks')}")
    # Spark returns TIMESTAMP as naive datetimes in the session time zone (UTC on the job)
    latest = {r["source_table"]: r["max_ts"] if r["max_ts"].tzinfo else r["max_ts"].replace(tzinfo=timezone.utc)
              for r in rows if r.get("max_ts") is not None}
    cutoff = now - timedelta(minutes=2 * cfg.ingest_interval_minutes)
    return {s for s in RAW_TABLES if s not in latest or latest[s] < cutoff}


def running_elsewhere(sql, cfg, run_id, now) -> list:
    """Other engine/analysis runs still 'running' and younger than the run timeout (they hold the run lock)."""
    rows = sql.query(f"SELECT run_id FROM {cfg.table('runs')} "
                     "WHERE status = 'running' AND run_id <> :me AND started_at >= :since",
                     {"me": run_id, "since": now - RUN_TIMEOUT})
    return [r["run_id"] for r in rows]


def run_engine(sql, w, cfg, connect=None, now=None, checks_dir=None, skip_ingest=False) -> dict:
    """Full run (ingest + analysis), or with skip_ingest=True an analysis-only run that never reads system.lakebase."""
    injected_now = now is not None  # tests inject `now`; real runs stamp ended_at with the wall clock
    now = now or datetime.now(timezone.utc)
    checks_dir = Path(checks_dir or CHECKS_DIR)
    run = {"run_id": uuid.uuid4().hex, "started_at": now, "ended_at": None, "status": "running",
           "step_seconds": None, "counts": None, "errors": None}
    ensure_schema(sql, cfg)
    sql.insert_rows(cfg.table("runs"), [run])
    if not cfg.engine_enabled:
        run.update(status="disabled", ended_at=now)
        sql.merge_rows(cfg.table("runs"), [run], ["run_id"])
        return run
    holders = running_elsewhere(sql, cfg, run["run_id"], now)
    if holders:  # our row is written first, so two runs starting together can both skip but never both proceed
        run.update(status="skipped", ended_at=now,
                   errors=json.dumps([f"another run is in progress: {', '.join(sorted(holders))}"]))
        sql.merge_rows(cfg.table("runs"), [run], ["run_id"])
        return run

    errors, steps = [], {}
    if skip_ingest:
        try:
            stale = stale_from_watermarks(sql, cfg, now)
        except Exception as e:
            errors.append(f"watermarks: {type(e).__name__}: {e}")
            stale = set(RAW_TABLES)
    else:
        t = time.monotonic()
        ingested = ingest_all(sql, cfg, run["run_id"], now)
        errors += [f"ingest {r.source}: {r.error}" for r in ingested if r.status != "ok"]
        stale = {r.source for r in ingested if r.status != "ok"}
        steps["ingest"] = time.monotonic() - t
    try:  # idempotent and cheap: the analysis job applies a pii_masking change too
        ensure_masks(sql, cfg)
    except Exception as e:
        errors.append(f"masks: {type(e).__name__}: {e}")

    t = time.monotonic()
    me = w.current_user.me().user_name
    branches = {}
    inventory, inv_errors = build_inventory(w, me, branches_out=branches)
    errors += [f"inventory {m}" for m in inv_errors]
    steps["inventory"] = time.monotonic() - t

    t = time.monotonic()
    excl = load_exclusions(checks_dir / "exclusions.yaml").with_query_users(service_roles(cfg, me))
    specs = load_checks(checks_dir, frozenset(API_CHECKS))
    runner = Runner(DeltaExecutor(sql, cfg, excl), PostgresExecutor(w, me, connect), ApiExecutor(excl), excl)
    outcomes = runner.run(specs, inventory, now, load_last_ok(sql, cfg), stale)
    errors += [f"check {o.check_id} {o.scope}: {o.error}" for o in outcomes if o.status in ("error", "timeout")]
    steps["checks"] = time.monotonic() - t

    t = time.monotonic()
    counts = persist(sql, cfg, run["run_id"], now, outcomes, {s.id: s for s in specs}, inventory, branches)
    steps["record"] = time.monotonic() - t

    t = time.monotonic()
    try:
        regressions = [r for r in run_postcheck(sql, cfg, now, errors, run_id=run["run_id"]) if r["outcome"] == "regressed"]
        counts["fixes_regressed"] = 0
        for r in regressions:
            try:
                counts["fixes_regressed"] += persist_regressions(sql, cfg, run["run_id"], now, [r])
            except Exception as e:
                errors.append(f"postcheck persist {r['action_id']}: {type(e).__name__}: {e}")
    except Exception as e:
        errors.append(f"postcheck: {type(e).__name__}: {e}")
    try:
        counts["fixes_rolled_back"] = resolve_rolled_back(sql, cfg, now)
    except Exception as e:
        errors.append(f"rolled back fixes: {type(e).__name__}: {e}")
    steps["postcheck"] = time.monotonic() - t

    run.update(ended_at=now if injected_now else datetime.now(timezone.utc),
               status="ok_with_errors" if errors else "ok",
               step_seconds=json.dumps({k: round(v, 1) for k, v in steps.items()}),
               counts=json.dumps(counts), errors=json.dumps(errors)[:20000] if errors else None)
    sql.merge_rows(cfg.table("runs"), [run], ["run_id"])
    return run


def main(argv=None) -> None:
    """Full engine run: ingest from system.lakebase, then analysis."""
    _main(argv, skip_ingest=False)


def analyze(argv=None) -> None:
    """Analysis only (inventory, checks, record, postcheck) over the already-ingested telemetry."""
    _main(argv, skip_ingest=True)


def _main(argv, skip_ingest) -> None:
    p = argparse.ArgumentParser(prog="lakebase-health")
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", default="lakebase_health")
    p.add_argument("--engine-enabled", default="true")
    p.add_argument("--ingest-interval-minutes", type=int, default=60)
    p.add_argument("--pii-masking", default="true")
    p.add_argument("--service-roles", default="", help="comma-separated engine/fixer SP application IDs")
    a = p.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    from ..sql import SparkSql

    cfg = EngineConfig(catalog=a.catalog, schema=a.schema, engine_enabled=a.engine_enabled.lower() == "true",
                       ingest_interval_minutes=a.ingest_interval_minutes, pii_masking=pii_masking_enabled(a.pii_masking),
                       service_roles=tuple(r.strip() for r in a.service_roles.split(",") if r.strip()))
    run = run_engine(SparkSql(SparkSession.builder.getOrCreate()), WorkspaceClient(), cfg, skip_ingest=skip_ingest)
    print(json.dumps(run, default=str))
