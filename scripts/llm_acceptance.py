"""End-to-end acceptance for LLM query rewrites (spec 2026-10-29-lakebase-health-llm-rewrites-design.md, section 8).

Usage: TEST_SP_SECRET=... engine/.venv/bin/python scripts/llm_acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --test-sp-id <application id> --test-sp-secret-env TEST_SP_SECRET [--steps 0,1,2,3,4,5,6]
Needs the deployed bundle (fixer SP enrolled on healthcheck, grants script run) and the hc_test schema. Step 0 makes
the planted correlated subquery (scripts/workload.py PLANTED_QUERY) the top query: it runs the workload in
`--mode rewrite` and then the engine job, until a top_db_time_query finding exists. The planted query is found by
its queryid in pg_stat_statements on healthcheck (spec §10: system.lakebase masks query text for the SPs), and its
finding by that queryid. Steps find their finding and attempts from the app and the tables, so a re-run resumes. Step 2 inserts draft approvals directly and starts the
fixer job itself with the acceptance-only --llm-test-response flag (never part of the bundle).
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fix_acceptance import WarehouseSql, call, sp_token  # noqa: E402
from lakebase_health import ledger  # noqa: E402
from lakebase_health.config import EngineConfig  # noqa: E402
from lakebase_health.executors.postgres import SEARCH_PATH_SQL  # noqa: E402
from lakebase_health.fixer import params as fparams  # noqa: E402
from lakebase_health.fixer import pgss  # noqa: E402
from lakebase_health.fixer.draft import context as dcontext  # noqa: E402
from lakebase_health.fixer.draft import prompt as dprompt  # noqa: E402
from lakebase_health.fixer.draft.sqltext import normalize  # noqa: E402
from lakebase_health.fixer.draft.states import FINAL, draft_token  # noqa: E402
from lakebase_health.fixer.draft.static import shape  # noqa: E402
from lakebase_health.fixer.pg import FIXER_SETUP  # noqa: E402

from databricks.sdk import WorkspaceClient  # noqa: E402

PLANTED_MARK = "o2.status = o.status"
ORDER_ID = re.compile(r"(?<!\d)16\d{8}(?!\d)")  # hc_test.orders ids (1600000001..1601000000): sampled $1/$2 values
LLM_CALL_COLUMNS = {"call_id", "draft_id", "endpoint", "prompt_hash", "response_hash", "input_tokens", "output_tokens",
                    "latency_ms", "status", "request_id", "called_at"}


class Acceptance:
    def __init__(self, a):
        self.a = a
        self.w = WorkspaceClient(profile=a.profile)
        self.sql = WarehouseSql(self.w, a.warehouse)
        self.cfg = EngineConfig(catalog=a.catalog)
        self.url = self.w.apps.get("lakebase-health").url.rstrip("/")
        self.failures, self.drafts = [], set()
        self.project = next(p for p in self.w.postgres.list_projects() if p.uid == a.project_uid).name
        branch = next(b for b in self.w.postgres.list_branches(parent=self.project)
                      if b.name.endswith("/branches/production"))
        self.ep = f"{branch.name}/endpoints/primary"
        self.databases = [d.status.postgres_database for d in self.w.postgres.list_databases(parent=branch.name)
                          if d.status]
        self._planted = None

    @contextmanager
    def pg(self, database="databricks_postgres", kind="draft_context"):
        """The owner's session on healthcheck production, set up like the fixer's read-only session."""
        import psycopg
        from workload import conninfo
        dsn = conninfo(self.a.profile, self.ep).replace("dbname=databricks_postgres", f"dbname={database}")
        with psycopg.connect(dsn, autocommit=True, prepare_threshold=None) as conn:
            for stmt in FIXER_SETUP(kind):
                conn.execute(stmt)
            yield conn

    def planted_queryid(self):
        """The planted query's queryid, from pg_stat_statements on healthcheck (never from the masked telemetry)."""
        if self._planted is None:
            with self.pg() as conn:
                conn.execute(SEARCH_PATH_SQL)
                row = conn.execute("SELECT s.queryid::text FROM pg_stat_statements s WHERE s.toplevel "
                                   "AND s.query LIKE %s AND s.query ILIKE 'select%%' "
                                   "ORDER BY s.total_exec_time DESC LIMIT 1", (f"%{PLANTED_MARK}%",)).fetchone()
            if not row:
                raise RuntimeError("the planted query is not in pg_stat_statements on healthcheck; run step 0")
            self._planted = row[0]
            print(f"planted query: queryid {self._planted} (pg_stat_statements on healthcheck)")
        return self._planted

    def owner(self):
        return self.w.config.authenticate()["Authorization"].split()[1]

    def check(self, crit, ok, msg):
        print(f"[{'PASS' if ok else 'FAIL'}] {crit}: {msg}", flush=True)
        if not ok:
            self.failures.append(f"{crit}: {msg}")
        return ok

    def job(self, suffix):
        return next(j.job_id for j in self.w.jobs.list() if j.settings.name.endswith(suffix))

    def finding(self):
        qid = self.planted_queryid()
        rows = self.sql.query(f"""
            SELECT fingerprint, status FROM {self.cfg.table('findings')}
            WHERE project_id = :p AND check_id = 'top_db_time_query' AND status IN ('new', 'open')
              AND (object = :q OR get_json_object(evidence, '$.queryid') = :q)
            ORDER BY last_seen DESC""", {"p": self.a.project_uid, "q": qid})
        if not rows:
            raise RuntimeError("no open top_db_time_query finding for the planted query; run step 0")
        return rows[0]["fingerprint"]

    def view(self, fp, token=None):
        status, body = call(f"{self.url}/api/findings/{fp}/drafts", token or self.owner())
        if status != 200:
            raise RuntimeError(f"GET drafts -> {status} {body}")
        return body

    def wait_attempt(self, fp, attempt, timeout=45 * 60):
        """Waits until attempt `attempt` exists and is final; returns it (with its parsed report)."""
        t = time.time()
        while time.time() - t < timeout:
            v = self.view(fp)
            mine = [x for x in v["attempts"] if int(x["attempt"]) == attempt]
            if mine and mine[0]["state"] in FINAL and not v["pending"]:
                self.drafts.add(mine[0]["draft_id"])
                return {**mine[0], "report": json.loads(mine[0]["report"] or "{}")}
            if v.get("refused") and not v["pending"] and not mine:
                raise RuntimeError(f"request refused: {v['refused']}")
            time.sleep(30)
        raise RuntimeError(f"attempt {attempt} not final after {timeout} s")

    def request(self, fp, token=None):
        return call(f"{self.url}/api/findings/{fp}/drafts", token or self.owner(), {})

    def _state(self, fp, draft_id):
        return next(x["state"] for x in self.view(fp)["attempts"] if x["draft_id"] == draft_id)

    @staticmethod
    def rule(report, name):
        return next((r for r in report.get("rules", []) if r.get("rule") == name), {})

    def step0(self):
        """Makes the planted query the top query: the rewrite workload, then a full engine run."""
        if self._finding_or_none():
            print("planted finding already open; skipping the workload")
            return
        subprocess.run([sys.executable, str(ROOT / "scripts" / "workload.py"), "--profile", self.a.profile,
                        "--endpoint", self.ep, "--minutes", str(self.a.workload_minutes), "--mode", "rewrite"],
                       check=True)
        run = self.w.jobs.run_now(job_id=self.job("lakebase-health-engine")).result(timeout=timedelta(minutes=90))
        self.check("0", run.state.result_state.value == "SUCCESS", f"engine run {run.run_id}")
        self.check("0", self._finding_or_none() is not None, "planted query has a top_db_time_query finding")

    def _finding_or_none(self):
        try:
            return self.finding()
        except RuntimeError:
            return None

    def step1(self):
        fp = self.finding()
        v = self.view(fp)
        self.check("1", v["setUp"]["ready"], f"drafting set up: {v['setUp']}")
        s, body = self.request(fp)
        self.check("1", s == 202, f"draft request -> {s} {body}")
        d = self.wait_attempt(fp, body["attempt"])
        r = d["report"]
        self.check("1", d["state"] == "verified", f"attempt {d['attempt']} is {d['state']}: {d.get('reason')}")
        self.check("1", self.rule(r, "equivalence").get("passed"), f"equivalence: {self.rule(r, 'equivalence')}")
        self.check("1", self.rule(r, "speed").get("passed"), f"speed (>= 1.2x): {self.rule(r, 'speed')}")
        plan = self.rule(r, "plan")
        self.check("1", plan.get("passed") and not plan.get("warning"), f"plan changed as described: {plan}")
        self.check("1", bool(r.get("rewrite_queryid")), f"rewrite query id recorded: {r.get('rewrite_queryid')}")

    def _switch_run(self, fp, response):
        """A draft approval for the next attempt, then one fixer run with the test switch."""
        job = self.job("lakebase-health-fixer")
        while list(self.w.jobs.list_runs(job_id=job, active_only=True)):
            time.sleep(20)
        while time.gmtime().tm_min % 15 in (14, 0):  # keep clear of the 15-minute schedule
            time.sleep(20)
        nxt = self.view(fp)["nextAttempt"]
        me = self.w.current_user.me().user_name
        self.sql.execute(f"INSERT INTO {self.cfg.table('approvals')} (approval_id, action_id, fingerprint, request, "
                         "sql_hash, approver, requested_at, note) VALUES (:id, NULL, :fp, 'draft', :h, :me, "
                         "current_timestamp(), :note)",
                         {"id": uuid.uuid4().hex, "fp": fp, "h": draft_token(fp, nxt), "me": me,
                          "note": f"acceptance test switch {response}"})
        params = list(self.w.jobs.get(job).settings.tasks[0].python_wheel_task.parameters)
        run = self.w.jobs.run_now(job_id=job, python_params=params + ["--llm-test-response", response])
        run = run.result(timeout=timedelta(minutes=60))
        self.check("2", run.state.result_state.value == "SUCCESS", f"fixer run {run.run_id} with {response}")
        return self.wait_attempt(fp, nxt)

    def step2(self):
        fp = self.finding()
        d = self._switch_run(fp, "unsafe")
        self.check("2", d["state"] == "draft_failed" and str(d["reason"]).startswith("data_modifying_cte"),
                   f"unsafe reply -> {d['state']}: {d['reason']}")
        d = self._switch_run(fp, "non_equivalent")
        self.check("2", d["state"] == "verify_failed" and str(d["reason"]).startswith("equivalence"),
                   f"non-equivalent reply -> {d['state']}: {d['reason']}")

    def step3(self):
        fp = self.finding()
        before = self.view(fp)
        self.check("3", before["canRequest"], "Retry is offered after a final attempt")
        s, body = self.request(fp)
        self.check("3", s == 202 and body["attempt"] == before["nextAttempt"], f"retry -> {s} {body}")
        self.wait_attempt(fp, body["attempt"])
        attempts = [int(x["attempt"]) for x in self.view(fp)["attempts"]]
        self.check("3", len(attempts) >= 2 and attempts == sorted(set(attempts), reverse=True),
                   f"card lists attempts {attempts}")

    def step4(self):
        cols = {r["col_name"] for r in self.sql.query(f"DESCRIBE TABLE {self.cfg.table('llm_calls')}")
                if r["col_name"] and not r["col_name"].startswith("#")}
        self.check("4", cols == LLM_CALL_COLUMNS, f"llm_calls columns {sorted(cols)}")
        calls = self.sql.query(f"SELECT prompt_hash, response_hash, input_tokens, output_tokens, status "
                               f"FROM {self.cfg.table('llm_calls')} ORDER BY called_at DESC LIMIT 50")
        ok = [c for c in calls if c["status"] == "ok"]
        self.check("4", bool(ok) and all(len(c["prompt_hash"]) == 64 and len(c["response_hash"] or "") == 64
                                         and c["input_tokens"] and c["output_tokens"] for c in ok),
                   f"{len(ok)} model calls hold hashes and token counts")
        # The payload the fixer sends, rebuilt with the fixer's code from pg_stat_statements on healthcheck, and value
        # sets drawn by the fixer's sampler (read-only, statement timeout): none of the values are in the payload.
        fp = self.finding()
        f = self.sql.query(f"SELECT * FROM {self.cfg.table('findings')} WHERE fingerprint = :fp", {"fp": fp})[0]

        @contextmanager
        def session(endpoint, host, database, kind):
            with self.pg(database, kind) as conn:
                yield conn
        src = dcontext.load_source(self.sql, self.cfg, session, {"endpoint": self.ep, "host": None,
                                                                 "databases": self.databases}, f)
        self.check("4", src["origin"] == "pg_stat_statements" and src["queryid"] == self.planted_queryid(),
                   f"query text from {src['origin']} for queryid {src['queryid']} in {src['database']}")
        norm, _ = normalize(src["query"])
        original = shape(norm)
        with self.pg(src["database"]) as conn:
            plan = dcontext.plan_context(pgss.generic_plan(conn, norm))
            tables = dcontext.load_catalog(conn, original.tables)
            sets = fparams.sample(conn, src["query"], src["mean_rows"])
        payload = json.dumps(dprompt.messages(dcontext.build_context(f, norm, original.ordered, plan, tables)))
        values = {v for s in sets for v in s if v}
        self.check("4", plan is not None, "the model gets the generic plan (EXPLAIN GENERIC_PLAN)")
        self.check("4", values and not any(v in payload for v in values) and not ORDER_ID.search(payload),
                   f"payload ({len(payload)} chars) holds none of {len(values)} sampled values and no order id")
        # What the fixer wrote for the drafts: reports, drafts, ledger payloads hold no order id (a sampled value).
        ids = ",".join(f"'{i}'" for i in sorted(self.drafts | {x["draft_id"] for x in self.view(fp)["attempts"]}))
        written = json.dumps(self.sql.query(
            f"SELECT r.report, r.error FROM {self.cfg.table('action_runs')} r WHERE r.action_id IN ({ids})") +
            self.sql.query(f"SELECT reason, rationale, expected_plan_change FROM {self.cfg.table('drafts')} "
                           f"WHERE draft_id IN ({ids})") +
            self.sql.query(f"SELECT payload FROM {self.cfg.table('ledger')} WHERE action_id IN ({ids})"), default=str)
        self.check("4", not ORDER_ID.search(written), f"reports, drafts and ledger ({len(written)} chars) hold no order id")

    def step5(self):
        left = [b.name for b in self.w.postgres.list_branches(parent=self.project) if "/branches/lbh-verify-" in b.name]
        self.check("5", not left, f"lbh-verify-* branches left: {left}")
        fp = self.finding()
        ids = {x["draft_id"] for x in self.view(fp)["attempts"]} | self.drafts
        id_list = ",".join(f"'{i}'" for i in sorted(ids))
        rows = self.sql.query(f"SELECT event_id, action_id, event_type, actor, payload, "
                              f"date_format(ts, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\") AS ts, prev_hash, hash "
                              f"FROM {self.cfg.table('ledger')} WHERE action_id IN ({id_list}) ORDER BY action_id, ts")
        from datetime import datetime, timezone
        for r in rows:
            r["ts"] = datetime.fromisoformat(r["ts"]).replace(tzinfo=timezone.utc)
        self.check("5", bool(rows) and ledger.verify(rows), f"ledger chain over {len(rows)} events, {len(ids)} drafts")
        sp = sp_token(self.w.config.host, self.a.test_sp_id, os.environ[self.a.test_sp_secret_env])
        s, body = self.request(fp, sp)
        self.check("5", s == 403, f"non-manager draft request -> {s} {body}")

    def step6(self):
        fp = self.finding()
        verified = [x for x in self.view(fp)["attempts"] if x["state"] in ("verified", "adopted")]
        if not verified:
            raise RuntimeError("no verified rewrite; run step 1")
        d = verified[0]
        if d["state"] != "adopted":
            with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
                f.write(d["sql"])
            subprocess.run([sys.executable, str(ROOT / "scripts" / "workload.py"), "--profile", self.a.profile,
                            "--endpoint", self.ep, "--minutes", str(self.a.workload_minutes), "--mode", "rewrite",
                            "--rewrite-file", f.name], check=True)
            for _ in range(2):  # system.lakebase can lag one engine run behind
                self.w.jobs.run_now(job_id=self.job("lakebase-health-engine")).result(timeout=timedelta(minutes=90))
                if self._state(fp, d["draft_id"]) == "adopted":
                    break
        state = self._state(fp, d["draft_id"])
        self.check("6", state == "adopted", f"rewrite {d['draft_id'][:8]} is {state}")


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--test-sp-id", "--test-sp-secret-env"):
        ap.add_argument(a, required=True)
    ap.add_argument("--steps", default="0,1,2,3,4,5,6")
    ap.add_argument("--workload-minutes", type=float, default=15)
    a = ap.parse_args()
    acc = Acceptance(a)
    for step in a.steps.split(","):
        try:
            getattr(acc, f"step{step.strip()}")()
        except Exception as e:
            acc.check(step, False, f"{type(e).__name__}: {e}")
    print(f"drafts touched: {sorted(acc.drafts)}")
    print("\n".join(["FAILURES:"] + acc.failures) if acc.failures else "ALL LLM-REWRITE ACCEPTANCE CHECKS PASSED")
    sys.exit(1 if acc.failures else 0)


if __name__ == "__main__":
    main()
