"""Execute every ```sql block in HEALTH_CHECKS.md and report errors / row counts.

Blocks that reference system.lakebase run on a SQL warehouse (Statement Execution API, :project_id bound);
all others run on the Lakebase endpoint via psycopg.

Usage: python3 scripts/test_checks.py --profile my-profile \
         --endpoint projects/healthcheck/branches/production/endpoints/primary \
         --warehouse <id> --project-id <system-table project_id> [--only pg|sys] [--show N]
"""
import argparse
import json
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementParameterListItem, StatementState

DOC = Path(__file__).resolve().parent.parent / "HEALTH_CHECKS.md"


def cli(*args):
    return json.loads(subprocess.check_output(["databricks", *args, "-o", "json"]))


def blocks():
    """Yield (check_heading, block_index, sql_statement) for each statement in each ```sql block."""
    heading, i = "preamble", 0
    lines = DOC.read_text().splitlines()
    n = 0
    while n < len(lines):
        line = lines[n]
        if line.startswith("### "):
            heading, i = line[4:].strip(), 0
        if line.strip() == "```sql":
            body = []
            n += 1
            while lines[n].strip() != "```":
                body.append(lines[n])
                n += 1
            i += 1
            for j, stmt in enumerate(split_statements("\n".join(body))):
                yield heading, f"{i}.{j + 1}", stmt
        n += 1


def split_statements(sql):
    # Strip line comments, then split on ';' at line end (none of the checks use ';' inside literals at EOL).
    no_comments = "\n".join(re.sub(r"--.*$", "", l) for l in sql.splitlines())
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", no_comments) if s.strip()]


def run_pg(dsn, stmt):
    with psycopg.connect(dsn, autocommit=True) as c:
        # Same as the notebook: put extension schemas (which may not be "public") on the search path.
        c.execute("""DO $$ BEGIN
          PERFORM set_config('search_path', concat_ws(',', '"$user"', 'public', (
            SELECT string_agg(DISTINCT quote_ident(n.nspname), ',') FROM pg_extension e
            JOIN pg_namespace n ON n.oid = e.extnamespace WHERE n.nspname NOT IN ('public', 'pg_catalog'))), false);
        END $$""")
        cur = c.execute(stmt)
        rows = cur.fetchall() if cur.description else []
        cols = [d.name for d in cur.description] if cur.description else []
        return cols, rows


def run_sys(w, warehouse, project_id, stmt):
    params = [StatementParameterListItem(name="project_id", value=project_id)] if ":project_id" in stmt else None
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=warehouse, parameters=params,
                                                wait_timeout="50s", on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE)
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(r.status.error.message if r.status.error else r.status.state)
    cols = [c.name for c in r.manifest.schema.columns]
    rows = r.result.data_array or [] if r.result else []
    return cols, rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--project-id", required=True)
    ap.add_argument("--only", choices=["pg", "sys"])
    ap.add_argument("--match", help="only run checks whose heading contains any of these |-separated texts")
    ap.add_argument("--show", type=int, default=3, help="rows to print per statement")
    ap.add_argument("--parallel", type=int, default=12, help="concurrent warehouse statements (system tables compile slowly)")
    a = ap.parse_args()

    host = cli("postgres", "get-endpoint", a.endpoint, "--profile", a.profile)["status"]["hosts"]["host"]
    token = cli("postgres", "generate-database-credential", a.endpoint, "--profile", a.profile)["token"]
    user = cli("current-user", "me", "--profile", a.profile)["userName"]
    dsn = f"host={host} user={user} password={token} dbname=databricks_postgres sslmode=require"
    w = WorkspaceClient(profile=a.profile)

    todo = []
    for heading, idx, stmt in blocks():
        kind = "sys" if "system.lakebase" in stmt else "pg"
        if (a.only and kind != a.only) or (a.match and not any(m.strip().lower() in heading.lower() for m in a.match.split("|"))):
            continue
        todo.append((heading, idx, kind, stmt))

    def run(item):
        heading, idx, kind, stmt = item
        t = time.time()
        try:
            cols, rows = run_pg(dsn, stmt) if kind == "pg" else run_sys(w, a.warehouse, a.project_id, stmt)
            return item, cols, rows, None, time.time() - t
        except Exception as e:  # report and continue
            return item, None, None, str(e).splitlines()[0][:300], time.time() - t

    ok = fail = 0
    with ThreadPoolExecutor(max_workers=a.parallel) as pool:
        for (heading, idx, kind, _), cols, rows, err, secs in pool.map(run, todo):
            if err:
                fail += 1
                print(f"FAIL [{kind}] {heading} #{idx} ({secs:.0f}s): {err}")
                continue
            ok += 1
            print(f"OK   [{kind}] {heading} #{idx} ({secs:.0f}s): {len(rows)} rows")
            if rows and a.show:
                print("       " + " | ".join(cols))
                for r in rows[: a.show]:
                    print("       " + " | ".join(str(v)[:40] for v in r))
    print(f"\n{ok} ok, {fail} failed")


if __name__ == "__main__":
    main()
