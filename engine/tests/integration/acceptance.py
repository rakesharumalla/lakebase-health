"""Acceptance checks for the engine against the seeded healthcheck project (spec section 7).

Usage: engine/.venv/bin/python engine/tests/integration/acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --engine-sp 00000000-0000-4000-8000-0000000000e1
Prerequisite: bundle deployed (`databricks bundle deploy --var catalog=... --var engine_sp=...`), seed + workload run.
"""
import argparse
import json
import subprocess
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

EXPECTED = {  # planted problem -> check id(s); at least one must have an active finding on the project
    "duplicate index": ["duplicate_index"],
    "redundant prefix index": ["redundant_index"],
    "unused indexes": ["unused_index"],
    "invalid index": ["invalid_index"],
    "FK without index": ["missing_fk_index"],
    "seq scans dominate": ["seq_scan_heavy", "seq_scan_read_ratio", "top_db_time_query"],
    "int4 sequence at 75%": ["sequence_exhaustion"],
    "autovacuum disabled": ["autovacuum_disabled"],
    "dead tuples": ["dead_tuples"],
    "table without PK": ["table_without_pk"],
    "PUBLIC CREATE": ["public_schema_create"],
    "deadlocks": ["deadlocks"],
    "idle-in-tx blocking": ["lock_blocking_history", "blocked_session", "idle_in_transaction"],
    "disk spills": ["disk_spill"],
    "write back-pressure": ["write_backpressure"],
    "never analyzed (borrowed)": ["stale_statistics"],
    "sort vs LIMIT (borrowed)": ["sort_limit_waste"],
    "autovacuum blocker (borrowed)": ["autovacuum_blocker"],
    "statement timeouts while locking a row": ["statement_timeouts"],
    "row-lock waits with blocker classified": ["hot_row_lock"],
    "GIN pending list filling": ["gin_pending_list"],
    "writers waiting on GIN cleanup": ["gin_cleanup_lock_wait"],
    "dropped client connections": ["connection_drops"],
}
# Not plantable on a test project: commit_quorum_wait (platform WAL quorum), oversized_search_index (needs a
# >= 1 GB GIN index). The replica side of unused_index is covered by unit tests (no read replica on healthcheck).
MUST_NOT_FIND = ["wal_retention_slot", "wal_proposer_slot", "cloud_admin", "fsync", "full_page_writes", "sage."]


def q(w, wh, sql):
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wh, wait_timeout="50s",
                                                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE)
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        r = w.statement_execution.get_statement(r.statement_id)
    assert r.status.state == StatementState.SUCCEEDED, r.status.error
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []


def run_job(profile, catalog, engine_sp):
    t = time.time()
    subprocess.check_call(["databricks", "bundle", "run", "lakebase_health_engine", "--var", f"catalog={catalog}",
                           "--var", f"engine_sp={engine_sp}", "--profile", profile])
    return time.time() - t


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--engine-sp"):
        ap.add_argument(a, required=True)
    a = ap.parse_args()
    w = WorkspaceClient(profile=a.profile)
    t = f"{a.catalog}.lakebase_health"
    failures = []

    secs = run_job(a.profile, a.catalog, a.engine_sp)
    print(f"run 1: {secs / 60:.1f} min")
    if secs > 30 * 60:
        failures.append(f"criterion 2: run took {secs / 60:.1f} min (> 30)")

    active = q(w, a.warehouse, f"SELECT check_id, object FROM {t}.findings WHERE project_id = '{a.project_uid}' "
                               "AND status IN ('new', 'open')")
    ids = {r["check_id"] for r in active}
    for problem, checks in EXPECTED.items():
        ok = any(c in ids for c in checks)
        print(("PASS " if ok else "FAIL ") + f"{problem}: {checks}")
        if not ok:
            failures.append(f"criterion 1: missed {problem}")
    for bad in MUST_NOT_FIND:
        hits = [r for r in active if bad in (r["object"] or "")]
        if hits:
            failures.append(f"criterion 1: must-not-find {bad!r} in {hits[:3]}")

    errs = q(w, a.warehouse, f"SELECT check_id, endpoint_id, error FROM {t}.check_results WHERE run_id = "
                             f"(SELECT run_id FROM {t}.runs ORDER BY started_at DESC LIMIT 1) "
                             "AND status IN ('error', 'timeout')")
    for e in errs:
        failures.append(f"check error: {e['check_id']} on {e['endpoint_id']}: {(e['error'] or '')[:200]}")
    print(f"check errors in latest run: {len(errs)}")

    before = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.findings")[0]["n"]
    actions_before = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.proposed_actions WHERE state = 'proposed'")[0]["n"]
    run_job(a.profile, a.catalog, a.engine_sp)
    after = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.findings")[0]["n"]
    actions_after = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.proposed_actions WHERE state = 'proposed'")[0]["n"]
    dupes = q(w, a.warehouse, f"SELECT fingerprint FROM {t}.findings GROUP BY fingerprint HAVING count(*) > 1")
    if dupes or int(after) < int(before) or int(actions_after) > int(actions_before) + 5:
        failures.append(f"criterion 3: findings {before}->{after}, proposed {actions_before}->{actions_after}, dupes {len(dupes)}")
    print(f"idempotency: findings {before}->{after}, proposed actions {actions_before}->{actions_after}")

    print("\n".join(["", "FAILURES:"] + failures) if failures else "\nALL AUTOMATED ACCEPTANCE CHECKS PASSED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
