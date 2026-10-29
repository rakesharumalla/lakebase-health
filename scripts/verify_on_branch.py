"""Prototype of the branch-verified fix loop (DESIGN.md, Option D).

Runs the affected queries on a throwaway branch, applies the candidate DDL there, re-runs them, and
measures read speedup, plan change, write penalty and index size. Prints a JSON report with a confidence score.

Usage: python3 scripts/verify_on_branch.py --profile my-profile \
         --endpoint projects/healthcheck/branches/hc-verify/endpoints/primary
"""
import argparse
import json
import statistics
import subprocess
import time

import psycopg

CANDIDATE_DDL = "CREATE INDEX CONCURRENTLY orders_customer_id_idx ON hc_test.orders (customer_id)"
ROLLBACK_DDL = "DROP INDEX CONCURRENTLY IF EXISTS hc_test.orders_customer_id_idx"
QUERIES = [  # from the seq-scan / FK findings; params are representative values
    ("SELECT count(*) FROM hc_test.orders WHERE customer_id = %s", [17, 4242, 199999]),
    ("DELETE FROM hc_test.customers WHERE id = %s", [0]),  # FK check path on parent delete (id 0 does not exist -> no-op)
]
WRITE_PROBE = "INSERT INTO hc_test.orders (customer_id, status, amount) SELECT 1 + (g % 200000), 'new', 1 FROM generate_series(1, 20000) g"
REPEATS = 3


def cli(*args):
    return json.loads(subprocess.check_output(["databricks", *args, "-o", "json"]))


def explain(c, sql, param):
    plan = c.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, (param,)).fetchone()[0][0]
    top = plan["Plan"]

    def nodes(n):
        yield n["Node Type"]
        for ch in n.get("Plans", []):
            yield from nodes(ch)
    return {"ms": plan["Execution Time"], "nodes": sorted(set(nodes(top))),
            "shared_read": top.get("Shared Read Blocks", 0), "shared_hit": top.get("Shared Hit Blocks", 0)}


def measure(c):
    out = {}
    for sql, params in QUERIES:
        runs = []
        for p in params:
            explain(c, sql, p)  # warm-up: branch caches start cold
            runs += [explain(c, sql, p) for _ in range(REPEATS)]
        out[sql] = {"p50_ms": statistics.median(r["ms"] for r in runs),
                    "nodes": sorted({n for r in runs for n in r["nodes"]}),
                    "avg_blocks": statistics.mean(r["shared_read"] + r["shared_hit"] for r in runs)}
    c.execute("BEGIN")
    t = time.perf_counter()
    c.execute(WRITE_PROBE)
    out["write_probe_ms"] = (time.perf_counter() - t) * 1000
    c.execute("ROLLBACK")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--endpoint", required=True)
    a = ap.parse_args()
    host = cli("postgres", "get-endpoint", a.endpoint, "--profile", a.profile)["status"]["hosts"]["host"]
    token = cli("postgres", "generate-database-credential", a.endpoint, "--profile", a.profile)["token"]
    user = cli("current-user", "me", "--profile", a.profile)["userName"]
    dsn = f"host={host} user={user} password={token} dbname=databricks_postgres sslmode=require"

    with psycopg.connect(dsn, autocommit=True, application_name="hc-verify") as c:
        c.execute(ROLLBACK_DDL)
        c.execute("ANALYZE hc_test.orders")
        before = measure(c)
        t = time.perf_counter()
        c.execute(CANDIDATE_DDL)
        ddl_s = time.perf_counter() - t
        size = c.execute("SELECT pg_relation_size('hc_test.orders_customer_id_idx')").fetchone()[0]
        after = measure(c)

    report = {"candidate_ddl": CANDIDATE_DDL, "rollback_ddl": ROLLBACK_DDL,
              "ddl_seconds": round(ddl_s, 1), "index_size_mb": round(size / 2**20, 1), "queries": []}
    speedups = []
    for sql, _ in QUERIES:
        b, x = before[sql], after[sql]
        sp = b["p50_ms"] / max(x["p50_ms"], 1e-3)
        speedups.append(sp)
        report["queries"].append({"sql": sql, "before_p50_ms": round(b["p50_ms"], 3), "after_p50_ms": round(x["p50_ms"], 3),
                                  "speedup_x": round(sp, 1), "plan_before": b["nodes"], "plan_after": x["nodes"],
                                  "blocks_before": round(b["avg_blocks"]), "blocks_after": round(x["avg_blocks"])})
    write_penalty = after["write_probe_ms"] / before["write_probe_ms"] - 1
    report["write_penalty_pct"] = round(100 * write_penalty, 1)
    # Confidence: measured speedup on the primary query, plan actually uses the index, bounded write penalty.
    uses_index = any("Index" in n for q in report["queries"] for n in q["plan_after"])
    score = min(1.0, max(0.0, (min(speedups[0], 100) / 100) * 0.6 + (0.25 if uses_index else 0)
                         + (0.15 if write_penalty < 0.25 else 0.05 if write_penalty < 0.5 else 0)))
    report["confidence"] = round(score, 2)
    report["verdict"] = "RECOMMEND" if score >= 0.7 else "REVIEW" if score >= 0.4 else "REJECT"
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
