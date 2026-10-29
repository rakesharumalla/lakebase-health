"""End-to-end acceptance for the fix loop (spec 2026-10-28-lakebase-health-fix-loop-design.md) on a test project.

Usage: TEST_SP_SECRET=... engine/.venv/bin/python scripts/fix_acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --test-sp-id <application id> --test-sp-secret-env TEST_SP_SECRET [--steps 1,2,3,4,5,6]
Needs the planted hc_test schema (scripts/seed_test_workload.sql), the deployed bundle with a fixer SP, and
scripts/fixer_grants.sh. Steps can run separately; each finds its action from the tables, so a re-run resumes.
Step 4 writes the documented test hook (state 'applied', updated_at 3 h back) and runs the post-apply check
locally with decide() forced to 'regressed'. Step 5 overwrites one action's SQL and restores it afterwards.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))
from lakebase_health import ledger  # noqa: E402
from lakebase_health.config import EngineConfig  # noqa: E402
from lakebase_health.fixer import postcheck  # noqa: E402
from lakebase_health.record import persist_regressions  # noqa: E402

IN_FLIGHT = {"verifying", "applying", "rolling_back"}
TAMPER_SQL = "DROP TABLE hc_test.audit_events"
PLANTED = {"customers_email_idx2": "CREATE INDEX CONCURRENTLY customers_email_idx2 ON hc_test.customers (email)",
           "customers_region_idx": "CREATE INDEX CONCURRENTLY customers_region_idx ON hc_test.customers (region)"}


class WarehouseSql:
    """The engine's Sql protocol over a SQL warehouse, enough for run_postcheck and persist_regressions."""

    def __init__(self, w, warehouse):
        self.w, self.wh, self._types = w, warehouse, {}

    def _run(self, stmt, args=None):
        params = [StatementParameterListItem(name=k, value=None if v is None else
                                             (v.isoformat() if isinstance(v, datetime) else str(v)),
                                             type="TIMESTAMP" if isinstance(v, datetime) else "STRING")
                  for k, v in (args or {}).items()]
        r = self.w.statement_execution.execute_statement(warehouse_id=self.wh, statement=stmt, parameters=params,
                                                         wait_timeout="50s")
        while r.status.state.value in ("PENDING", "RUNNING"):
            time.sleep(3)
            r = self.w.statement_execution.get_statement(r.statement_id)
        if r.status.state.value != "SUCCEEDED":
            raise RuntimeError(f"{r.status.error.message if r.status.error else r.status.state}: {stmt[:200]}")
        cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
        rows = (r.result.data_array or []) if r.result else []
        return [dict(zip(cols, row)) for row in rows]

    def query(self, stmt, args=None):
        return self._run(stmt, args)

    def execute(self, stmt, args=None):
        self._run(stmt, args)

    def table_exists(self, name):
        return bool(self._run(f"SHOW TABLES IN {name.rsplit('.', 1)[0]} LIKE '{name.rsplit('.', 1)[1]}'"))

    def _schema(self, table):
        if table not in self._types:
            rows = self._run(f"DESCRIBE TABLE {table}")
            cols = []
            for r in rows:
                if not r["col_name"] or r["col_name"].startswith("#"):
                    break
                cols.append((r["col_name"], r["data_type"]))
            self._types[table] = cols
        return self._types[table]

    def insert_rows(self, table, rows):
        cols = self._schema(table)
        for row in rows:
            args = {f"p{i}": row.get(c) for i, (c, _) in enumerate(cols)}
            vals = ", ".join(f"CAST(:p{i} AS {t})" for i, (_, t) in enumerate(cols))
            self._run(f"INSERT INTO {table} ({', '.join(c for c, _ in cols)}) VALUES ({vals})", args)

    def merge_rows(self, table, rows, keys):
        cols = self._schema(table)
        for row in rows:
            args = {f"p{i}": row.get(c) for i, (c, _) in enumerate(cols)}
            src = ", ".join(f"CAST(:p{i} AS {t}) AS {c}" for i, (c, t) in enumerate(cols))
            on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
            self._run(f"MERGE INTO {table} t USING (SELECT {src}) s ON {on} "
                      "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *", args)


def sp_token(host, client_id, secret):
    data = f"grant_type=client_credentials&scope=all-apis&client_id={client_id}&client_secret={secret}".encode()
    req = urllib.request.Request(f"{host.rstrip('/')}/oidc/v1/token", data=data, method="POST")
    with urllib.request.urlopen(req) as r:
        return json.load(r)["access_token"]


def call(url, token, body=None):
    data = None if body is None else json.dumps(body).encode()
    headers = {"Authorization": f"Bearer {token}", **({"Content-Type": "application/json"} if data else {})}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.load(e)
        except Exception:
            return e.code, None


class Acceptance:
    def __init__(self, a):
        self.a = a
        self.w = WorkspaceClient(profile=a.profile)
        self.sql = WarehouseSql(self.w, a.warehouse)
        self.cfg = EngineConfig(catalog=a.catalog)
        self.url = self.w.apps.get("lakebase-health").url.rstrip("/")
        self.failures, self.touched = [], set()
        self.project = next(p for p in self.w.postgres.list_projects() if p.uid == a.project_uid).name
        branch = next(b for b in self.w.postgres.list_branches(parent=self.project)
                      if b.name.endswith("/branches/production"))
        self.ep = f"{branch.name}/endpoints/primary"

    def owner(self):
        return self.w.config.authenticate()["Authorization"].split()[1]

    def check(self, crit, ok, msg):
        print(f"[{'PASS' if ok else 'FAIL'}] {crit}: {msg}", flush=True)
        if not ok:
            self.failures.append(f"{crit}: {msg}")
        return ok

    def pg(self, stmt):
        env = {**os.environ, "PROFILE": self.a.profile, "EP": self.ep}
        out = subprocess.run(["bash", str(ROOT / "scripts" / "pg.sh"), "-At", "-v", "ON_ERROR_STOP=1", "-c", stmt],
                             env=env, capture_output=True, text=True, check=True)
        return [line.split("|") for line in out.stdout.splitlines() if line]

    def action(self, check_id, obj_like, states=None):
        rows = self.sql.query(f"""
            SELECT a.action_id, a.state, a.sql, a.rollback_sql, f.status AS finding_status, f.fingerprint
            FROM {self.cfg.table('proposed_actions')} a JOIN {self.cfg.table('findings')} f ON f.fingerprint = a.fingerprint
            WHERE f.project_id = :p AND a.check_id = :c AND f.object LIKE :o ORDER BY a.updated_at DESC""",
                              {"p": self.a.project_uid, "c": check_id, "o": obj_like})
        rows = [r for r in rows if states is None or r["state"] in states]
        if not rows:
            raise RuntimeError(f"no {check_id} action on {obj_like} in states {states}")
        self.touched.add(rows[0]["action_id"])
        return rows[0]

    def state(self, aid):
        return self.sql.query(f"SELECT state FROM {self.cfg.table('proposed_actions')} WHERE action_id = :id",
                              {"id": aid})[0]["state"]

    def request(self, aid, kind, token=None):
        # Server clock, so the comparison with updated_at / ledger ts is not skewed by the local clock.
        self.requested_at = self.sql.query("SELECT date_format(current_timestamp(), "
                                           "\"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\") AS t")[0]["t"]
        return call(f"{self.url}/api/actions/{aid}/requests", token or self.owner(), {"request": kind})

    def _moved_since(self, aid, since):
        """True once the fixer has touched the action after `since`: a state change or a new ledger event."""
        rows = self.sql.query(f"""
            SELECT (SELECT count(*) FROM {self.cfg.table('proposed_actions')}
                    WHERE action_id = :id AND updated_at > to_timestamp(:t)) +
                   (SELECT count(*) FROM {self.cfg.table('ledger')}
                    WHERE action_id = :id AND ts > to_timestamp(:t)) AS n""", {"id": aid, "t": since})
        return int(rows[0]["n"]) > 0

    def wait(self, aid, from_state, timeout=45 * 60):
        """Waits for the fixer (triggered by the app, or its 15-minute schedule) to finish with the action: it is
        not in flight and has changed state or been touched since the last request (so verify_failed ->
        verify_failed is detected)."""
        since = getattr(self, "requested_at", None)
        t = time.time()
        while time.time() - t < timeout:
            s = self.state(aid)
            if s not in IN_FLIGHT and (s != from_state or (since and self._moved_since(aid, since))):
                return s
            time.sleep(30)
        return self.state(aid)

    def outcome_run(self, aid, kind):
        """The newest run row of this kind that is an outcome, not the 'claimed' marker."""
        status, body = call(f"{self.url}/api/actions/{aid}", self.owner())
        runs = [r for r in (body or {}).get("runs", []) if r.get("kind") == kind and r.get("outcome") != "claimed"]
        runs.sort(key=lambda r: str(r.get("started_at")), reverse=True)
        run = runs[0] if runs else {}
        try:
            report = json.loads(run.get("report") or "{}")
        except ValueError:
            report = {}
        return run, report

    def rule(self, report, name):
        return next((r for r in report.get("rules", []) if r.get("rule") == name), None)

    def no_verify_branches(self):
        return [b.name for b in self.w.postgres.list_branches(parent=self.project) if "/branches/lbh-verify-" in b.name]

    def index(self, name=None, table=None, column=None):
        if name:
            rows = self.pg(f"SELECT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass('hc_test.{name}')")
        else:
            rows = self.pg("SELECT i.indisvalid FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                           f"AND a.attnum = i.indkey[0] WHERE i.indrelid = 'hc_test.{table}'::regclass "
                           f"AND a.attname = '{column}' AND i.indnatts = 1")
        if not rows:
            return "absent"
        return "valid" if any(r[0] == "t" for r in rows) else "invalid"

    def ensure_planted(self, name):
        if self.index(name=name) == "absent":
            self.pg(PLANTED[name])
            print(f"re-created planted index {name}")

    def verify_then_apply(self, crit, check_id, obj_like, rule_name):
        act = self.action(check_id, obj_like, {"proposed", "verify_failed", "blocked", "verified", "apply_failed",
                                               "applied", "confirmed", "regressed"})
        aid = act["action_id"]
        if act["state"] in ("proposed", "verify_failed", "blocked"):  # blocked: a privilege block retried after enrolment
            s, body = self.request(aid, "verify")
            self.check(crit, s == 202, f"verify request {aid[:8]} -> {s} {body}")
            st = self.wait(aid, act["state"])
            run, report = self.outcome_run(aid, "verify")
            r = self.rule(report, rule_name)
            self.check(crit, st == "verified", f"{check_id} state after verify = {st}; error={run.get('error')}; "
                                               f"rules={report.get('rules')}")
            self.check(crit, bool(r and r["passed"]), f"rule {rule_name}: {r}")
            left = self.no_verify_branches()
            self.check(crit, not left, f"lbh-verify-* branches left: {left}")
            act["state"] = st
        if act["state"] in ("verified", "apply_failed"):
            s, body = self.request(aid, "apply")
            self.check(crit, s == 202, f"apply request {aid[:8]} -> {s} {body}")
            st = self.wait(aid, act["state"])
            run, _ = self.outcome_run(aid, "apply")
            self.check(crit, st == "applied", f"{check_id} state after apply = {st}; error={run.get('error')}")
        return aid

    def step1(self):
        aid = self.verify_then_apply("1", "missing_fk_index", "%hc_test.orders (customer_id)%", "index_used_with_speedup")
        if self.state(aid) in ("applied", "confirmed"):
            ix = self.index(table="orders", column="customer_id")
            self.check("1", ix == "valid", f"index on hc_test.orders (customer_id) is {ix}")

    def step2(self):
        act = self.action("duplicate_index", "%hc_test.customers_email_idx2", None)
        if act["state"] in ("proposed", "verify_failed", "blocked"):
            self.ensure_planted("customers_email_idx2")
        aid = self.verify_then_apply("2", "duplicate_index", "%hc_test.customers_email_idx2", "index_unused_before")
        if self.state(aid) in ("applied", "confirmed"):
            ix = self.index(name="customers_email_idx2")
            self.check("2", ix == "absent", f"customers_email_idx2 is {ix}")

    def engine_run(self):
        """One run of the analysis-only job (no ingest): post-apply checks need fresh findings, not fresh telemetry."""
        job = next(j.job_id for j in self.w.jobs.list(name=None) if j.settings.name.endswith("lakebase-health-analysis"))
        t = time.time()
        run = self.w.jobs.run_now(job_id=job).result(timeout=timedelta(minutes=60))
        print(f"  analysis run {run.run_id}: {time.time() - t:.0f} s")
        return run.state.result_state.value if run.state.result_state else str(run.state.life_cycle_state)

    def step3(self):
        for i in (1, 2):
            self.check("3", self.engine_run() == "SUCCESS", f"analysis run {i}")
        for check_id, obj in (("missing_fk_index", "%hc_test.orders (customer_id)%"),
                              ("duplicate_index", "%hc_test.customers_email_idx2")):
            act = self.action(check_id, obj)
            self.check("3", act["state"] == "confirmed", f"{check_id} action {act['action_id'][:8]} is {act['state']}")
            self.check("3", act["finding_status"] == "resolved", f"{check_id} finding is {act['finding_status']}")

    def step4(self):
        act = self.action("redundant_index", "%hc_test.customers_region_idx", None)
        if act["state"] in ("proposed", "verify_failed"):
            self.ensure_planted("customers_region_idx")
        aid = self.verify_then_apply("4", "redundant_index", "%hc_test.customers_region_idx", "index_unused_before")
        if self.state(aid) in ("applied", "confirmed"):
            # Documented test hook: make the action look applied 3 h ago, then force the post-check verdict.
            now = datetime.now(timezone.utc)
            self.sql.execute(f"UPDATE {self.cfg.table('proposed_actions')} SET state = 'applied', updated_at = :t "
                             "WHERE action_id = :id", {"t": now - timedelta(hours=3), "id": aid})
            original = postcheck.decide
            postcheck.decide = lambda *a, **k: ("regressed", "high", ["forced by acceptance test"])
            try:
                errors = []
                regs = [r for r in postcheck.run_postcheck(self.sql, self.cfg, now, errors)
                        if r["outcome"] == "regressed" and r["action_id"] == aid]
                for r in regs:
                    persist_regressions(self.sql, self.cfg, uuid.uuid4().hex, now, [r])
            finally:
                postcheck.decide = original
            self.check("4", bool(regs) and not errors, f"forced post-check regressed {len(regs)}; errors {errors}")
            fx = self.sql.query(f"SELECT severity, status FROM {self.cfg.table('findings')} "
                                "WHERE check_id = 'fix_regressed' AND object = :id", {"id": aid})
            self.check("4", any(f["severity"] == "critical" and f["status"] in ("new", "open") for f in fx),
                       f"fix_regressed findings {fx}")
        s, body = call(f"{self.url}/api/actions/{aid}", self.owner())
        self.check("4", s == 200 and "rollback" in (body or {}).get("requests", []),
                   f"GET action -> {s}, requests {(body or {}).get('requests')}")
        st = self.state(aid)
        if st == "regressed":
            s, body = self.request(aid, "rollback")
            self.check("4", s == 202, f"rollback request -> {s} {body}")
            st = self.wait(aid, "regressed")
            run, _ = self.outcome_run(aid, "rollback")
            self.check("4", st == "rolled_back", f"state after rollback = {st}; error={run.get('error')}")
        ix = self.index(name="customers_region_idx")
        self.check("4", ix == "valid", f"customers_region_idx is {ix}")

    def step5(self):
        sp = sp_token(self.w.config.host, self.a.test_sp_id, os.environ[self.a.test_sp_secret_env])
        act = self.action("redundant_index", "%hc_test.customers_region_idx", None)
        s, body = call(f"{self.url}/api/actions/{act['action_id']}", sp)
        self.check("5", s == 200 and body.get("canManage") is False, f"test SP GET action -> {s}, "
                                                                      f"canManage {(body or {}).get('canManage')}")
        s, _ = self.request(act["action_id"], "verify", sp)
        self.check("5", s == 403, f"test SP request -> {s}")
        # Tamper: the approval covers the tampered text, so only the allowlist stands between it and the database.
        # public_schema_create is no longer proposed (outside the allowlist, B2): tamper a runnable proposed action
        t = self.action("stale_statistics", "%hc_test.orders", {"proposed", "verify_failed"})
        tbl = self.cfg.table("proposed_actions")
        orig = self.sql.query(f"SELECT sql, state, updated_at FROM {tbl} WHERE action_id = :id", {"id": t["action_id"]})[0]
        self.sql.execute(f"UPDATE {tbl} SET sql = :s WHERE action_id = :id", {"s": TAMPER_SQL, "id": t["action_id"]})
        try:
            s, body = self.request(t["action_id"], "verify")
            self.check("5", s == 202, f"owner verify of tampered action -> {s} {body}")
            st = self.wait(t["action_id"], orig["state"])
            self.check("5", st == "blocked", f"tampered action state = {st}")
        finally:
            self.sql.execute(f"UPDATE {tbl} SET sql = :s, state = :st, updated_at = CAST(:u AS TIMESTAMP) "
                             "WHERE action_id = :id",
                             {"s": orig["sql"], "st": orig["state"], "u": orig["updated_at"], "id": t["action_id"]})
            print(f"restored action {t['action_id'][:8]} sql and state ({orig['state']})")
        exists = self.pg("SELECT to_regclass('hc_test.audit_events') IS NOT NULL")
        self.check("5", exists == [["t"]], f"hc_test.audit_events exists: {exists}")

    def step6(self):
        for check_id, obj in (("missing_fk_index", "%hc_test.orders (customer_id)%"),
                              ("duplicate_index", "%hc_test.customers_email_idx2"),
                              ("redundant_index", "%hc_test.customers_region_idx"),
                              ("public_schema_create", "%hc_test")):
            self.action(check_id, obj)
        ids = ",".join(f"'{i}'" for i in sorted(self.touched))
        # JSON results carry milliseconds only; the hash covers microseconds (warehouse session time zone is UTC).
        rows = self.sql.query(f"SELECT event_id, action_id, event_type, actor, payload, "
                              f"date_format(ts, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\") AS ts, prev_hash, hash "
                              f"FROM {self.cfg.table('ledger')} WHERE action_id IN ({ids}) AND ts >= :since "
                              "ORDER BY action_id, ts", {"since": self.a.ledger_since})
        for r in rows:
            r["ts"] = datetime.fromisoformat(r["ts"]).replace(tzinfo=timezone.utc)
        self.check("6", bool(rows) and ledger.verify(rows), f"ledger chain over {len(rows)} events for "
                                                             f"{len(self.touched)} actions")
        app_sp = self.w.apps.get("lakebase-health").service_principal_client_id
        engine_sp = self.a.engine_sp
        writes = self.pg(f"""
            SELECT r.rolname, count(*) FILTER (WHERE has_table_privilege(r.oid, c.oid, 'INSERT,UPDATE,DELETE,TRUNCATE')),
                   count(*) FILTER (WHERE pg_has_role(r.oid, c.relowner, 'USAGE'))
            FROM pg_roles r CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE r.rolname IN ('{app_sp}', '{engine_sp}') AND c.relkind IN ('r', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_%'
            GROUP BY r.rolname""")
        self.check("6", bool(writes) and all(w[1] == "0" and w[2] == "0" for w in writes),
                   f"engine/app SP write privileges (role, writable tables, owned tables): {writes}")


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--test-sp-id", "--test-sp-secret-env"):
        ap.add_argument(a, required=True)
    ap.add_argument("--engine-sp", default="00000000-0000-4000-8000-0000000000e1")
    ap.add_argument("--steps", default="1,2,3,4,5,6")
    # Events written before the per-action chain (engine versions before the fix loop) chained globally.
    ap.add_argument("--ledger-since", default="1970-01-01T00:00:00Z", help="verify ledger events from this UTC time on")
    a = ap.parse_args()
    acc = Acceptance(a)
    for step in a.steps.split(","):
        try:
            getattr(acc, f"step{step.strip()}")()
        except Exception as e:
            acc.check(step, False, f"{type(e).__name__}: {e}")
    print(f"touched actions: {sorted(acc.touched)}")
    print("\n".join(["FAILURES:"] + acc.failures) if acc.failures else "ALL FIX-LOOP ACCEPTANCE CHECKS PASSED")
    sys.exit(1 if acc.failures else 0)


if __name__ == "__main__":
    main()
