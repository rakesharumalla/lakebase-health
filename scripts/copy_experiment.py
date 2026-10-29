"""Measure: system.lakebase.compute_gauges queried live vs. a clustered Delta copy.

Steps (all on a SQL warehouse, timings from query history):
  1. backfill:     CREATE TABLE <target> CLUSTER BY (project_id, ts) AS SELECT * FROM system table (7d, all projects)
  2. optimize:     OPTIMIZE <target>
  3. checks:       checks 4, 6, 27 from HEALTH_CHECKS.md, run against the system table and the copy, in parallel
  4. incremental:  INSERT INTO <target> SELECT * FROM system table WHERE ts > (max ts already copied)

Usage: python3 scripts/copy_experiment.py --profile my-profile --warehouse <id> \
         --project-id <uid> --target my_catalog.lakebase_health.raw_compute_gauges
"""
import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (ExecuteStatementRequestOnWaitTimeout, QueryFilter,
                                        StatementParameterListItem, StatementState)

DOC = Path(__file__).resolve().parent.parent / "HEALTH_CHECKS.md"
SOURCE = "system.lakebase.compute_gauges"


def doc_check_sql(heading_prefix):
    """First SQL statement of the check whose heading starts with heading_prefix."""
    text = DOC.read_text()
    sec = text.split("### " + heading_prefix, 1)[1]
    block = sec.split("```sql", 1)[1].split("```", 1)[0]
    return block.split(";")[0].strip()


def run(w, wh, sql, params=None):
    t = time.time()
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=wh, wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()] or None)
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        try:
            r = w.statement_execution.get_statement(r.statement_id)
        except Exception as e:  # network drop while polling: the statement keeps running server-side
            print(f"poll error ({type(e).__name__}), retrying", flush=True)
    wall = time.time() - t
    if r.status.state != StatementState.SUCCEEDED:
        return {"error": r.status.error.message if r.status.error else str(r.status.state), "wall_s": round(wall)}
    m = None
    for _ in range(12):  # query history lags a little behind statement completion
        try:
            res = w.query_history.list(filter_by=QueryFilter(statement_ids=[r.statement_id]), include_metrics=True).res
        except Exception:
            res = None
        if res and res[0].metrics and res[0].metrics.compilation_time_ms is not None:
            m = res[0].metrics
            break
        time.sleep(5)
    rows = r.result.data_array if r.result and r.result.data_array else []
    return {"wall_s": round(wall, 1),
            "compile_s": round((m.compilation_time_ms or 0) / 1000, 1) if m else None,
            "exec_s": round((m.execution_time_ms or 0) / 1000, 1) if m else None,
            "files_read": m.read_files_count if m else None,
            "files_pruned": m.pruned_files_count if m else None,
            "rows": len(rows), "sample": rows[:3]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--project-id", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--skip-backfill", action="store_true")
    a = ap.parse_args()
    w = WorkspaceClient(profile=a.profile)
    out = {}

    def log(k, v):
        out[k] = v
        print(json.dumps({k: v}, default=str), flush=True)

    schema = a.target.rsplit(".", 1)[0]
    run(w, a.warehouse, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    if not a.skip_backfill:
        log("1_backfill", run(w, a.warehouse,
            f"CREATE OR REPLACE TABLE {a.target} CLUSTER BY (project_id, ts) AS "
            f"SELECT * FROM {SOURCE} WHERE ts >= current_timestamp() - INTERVAL 7 DAYS"))
    log("2_optimize", run(w, a.warehouse, f"OPTIMIZE {a.target}"))
    if True:
        log("2b_layout", run(w, a.warehouse, f"SELECT numFiles, round(sizeInBytes/1024/1024) AS size_mb FROM (DESCRIBE DETAIL {a.target})"))

    checks = {name: doc_check_sql(h) for name, h in
              [("check4_working_set", "4. Working set"), ("check6_storage", "6. Storage"), ("check27_replica_lag", "27. Replication")]}
    jobs = []
    for name, sql in checks.items():
        assert SOURCE in sql, name
        jobs.append((f"3_{name}_system_table", sql))
        jobs.append((f"3_{name}_delta_copy", sql.replace(SOURCE, a.target)))
    with ThreadPoolExecutor(len(jobs)) as pool:
        for key, res in zip([k for k, _ in jobs],
                            pool.map(lambda j: run(w, a.warehouse, j[1], {"project_id": a.project_id}), jobs)):
            log(key, res)

    log("4_incremental_append", run(w, a.warehouse,
        f"INSERT INTO {a.target} SELECT * FROM {SOURCE} "
        f"WHERE ts > (SELECT max(ts) FROM {a.target}) AND ts >= current_timestamp() - INTERVAL 1 DAY"))
    Path("/tmp/lbnb/copy_experiment.json").write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
