"""Acceptance for the health report (spec 2026-10-28-lakebase-health-report-design.md, section 8) on healthcheck.

Usage: TEST_SP_SECRET=... engine/.venv/bin/python scripts/report_acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --test-sp-id <application id> --test-sp-secret-env TEST_SP_SECRET [--steps 1,2,3,4,5,6,7,8] \
         [--synced-pg-table lbh_acceptance.synced_orders --synced-pipeline <pipeline id>]
Needs the deployed bundle, the grants script run, healthcheck's production branch enrolled and the hc_test schema.
Step 4 holds one idle client connection; it is only testable when scale to zero is on and Postgres has been up for
24 h. Step 5 never creates anything: the operator creates the synced table first (plan Task 18, after asking the
user) and passes its Postgres name and pipeline id; the step then triggers one full refresh of that pipeline.
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fix_acceptance import WarehouseSql, call, sp_token  # noqa: E402
from lakebase_health.config import EngineConfig  # noqa: E402

from databricks.sdk import WorkspaceClient  # noqa: E402

RANK = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
IDLE_APP = "lbh-acceptance-idle"


def raw_get(url, token):
    """Status, body and the response headers (an email.message.Message: names match in any case)."""
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, r.read().decode(), r.headers
    except urllib.error.HTTPError as e:
        return e.code, "", e.headers


def inventory_size(row):
    """The inventory size to compare with pg_relation_size: an index row's index_bytes, a table row's data_bytes
    (a table row's index_bytes is the total of its indexes)."""
    return int(row["index_bytes"] if row.get("index_name") else row.get("data_bytes") or 0)


class Acceptance:
    def __init__(self, a):
        self.a = a
        self.w = WorkspaceClient(profile=a.profile)
        self.sql = WarehouseSql(self.w, a.warehouse)
        self.cfg = EngineConfig(catalog=a.catalog)
        self.url = self.w.apps.get("lakebase-health").url.rstrip("/")
        self.failures = []
        self.project = next(p for p in self.w.postgres.list_projects() if p.uid == a.project_uid).name
        branch = next(b for b in self.w.postgres.list_branches(parent=self.project)
                      if b.name.endswith("/branches/production"))
        self.branch_uid = branch.uid
        self.ep = f"{branch.name}/endpoints/primary"

    def owner(self):
        return self.w.config.authenticate()["Authorization"].split()[1]

    def check(self, crit, ok, msg):
        print(f"[{'PASS' if ok else 'FAIL'}] {crit}: {msg}", flush=True)
        if not ok:
            self.failures.append(f"{crit}: {msg}")
        return ok

    def report(self, token=None, branch="default"):
        t = time.time()
        status, body = call(f"{self.url}/api/report/{self.a.project_uid}/{branch}", token or self.owner())
        return status, body, time.time() - t

    def connect(self, app="lakebase-health-notebook"):
        """A Postgres session on the primary as the CLI user (the notebook's application name: never counted)."""
        import psycopg
        host = self.w.postgres.get_endpoint(name=self.ep).status.hosts.host
        token = self.w.postgres.generate_database_credential(endpoint=self.ep).token
        me = self.w.current_user.me().user_name
        return psycopg.connect(host=host, user=me, password=token, dbname="databricks_postgres", sslmode="require",
                               autocommit=True, application_name=app, prepare_threshold=None)

    def rows(self, stmt):
        with self.connect() as conn:
            return conn.execute("/* lakebase-health */ " + stmt).fetchall()

    def analysis_run(self):
        job = next(j.job_id for j in self.w.jobs.list() if j.settings.name.endswith("lakebase-health-analysis"))
        t = time.time()
        run = self.w.jobs.run_now(job_id=job).result(timeout=timedelta(minutes=60))
        print(f"  analysis run {run.run_id}: {time.time() - t:.0f} s")

    def open_findings(self, check_id=None):
        q = (f"SELECT fingerprint, check_id, object, severity, evidence FROM {self.cfg.table('findings')} "
             "WHERE project_id = :p AND (branch_id = :b OR branch_id IS NULL) AND status IN ('new', 'open')")
        args = {"p": self.a.project_uid, "b": self.branch_uid}
        if check_id:
            q += " AND check_id = :c"
            args["c"] = check_id
        return self.sql.query(q, args)

    def step1(self):
        """Score and grade; each area's state matches its open findings."""
        status, rep, _ = self.report()
        if not self.check("1", status == 200, f"report status {status}"):
            return
        s = rep["score"]
        self.check("1", s["assessed"] > 0 and s["grade"] in "ABCDF" and s["pct"] is not None,
                   f"score {s['pct']} %, grade {s['grade']}, assessed {s['assessed']} of {s['total']}")
        by_area = {}
        for f in rep["findings"]:
            if f["area"]:
                by_area.setdefault(f["area"], []).append(f["severity"])
        expect = {"critical": "fail", "warning": "warn", "info": "warn"}
        for a in rep["areas"]:
            if a["state"] == "not_assessed":
                continue
            sev = by_area.get(a["id"], [])
            want = "pass" if not sev else expect["critical" if "critical" in sev else "warning" if "warning" in sev
                                                else "info"]
            self.check("1", a["state"] == want, f"{a['id']}: {a['state']} with open {sorted(set(sev))}")
        sql_open = len(self.open_findings())
        self.check("1", len(rep["findings"]) == min(sql_open, 2000), f"{len(rep['findings'])} findings vs {sql_open} open")

    def step2(self):
        """Planted problems are in the runbook, ordered by severity, each with SQL or a note."""
        _, rep, _ = self.report()
        items = rep["runbook"]
        ranks = [RANK[i["priority"]] for i in items]
        self.check("2", ranks == sorted(ranks), f"{len(items)} items ordered by priority")
        planted = [i for i in items if "hc_test" in i["object"]]
        self.check("2", len(planted) >= 5, f"{len(planted)} runbook items on hc_test objects")
        self.check("2", all(i["sql"] or i["note"] for i in items), "every item has SQL or a note")
        with_sql = [i for i in planted if i["sql"]]
        self.check("2", len(with_sql) >= 3, f"{len(with_sql)} planted items with SQL")

    def step3(self):
        """Inventory lists the hc_test tables and indexes with sizes matching pg_relation_size."""
        _, rep, _ = self.report()
        tables = [t for t in rep["sections"]["tables"]["data"] if t["schema_name"] == "hc_test"]
        indexes = [i for i in rep["sections"]["indexes"]["data"] if i["schema_name"] == "hc_test"]
        self.check("3", len(tables) >= 4 and len(indexes) >= 4, f"{len(tables)} hc_test tables, {len(indexes)} indexes")
        live = dict(self.rows("SELECT c.relname, pg_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n "
                              "ON n.oid = c.relnamespace WHERE n.nspname = 'hc_test' AND c.relkind IN ('r', 'i')"))
        for t in tables + indexes:
            name = t.get("index_name") or t["table_name"]
            got = inventory_size(t)
            want = live.get(name)
            self.check("3", want is not None and abs(got - want) <= max(0.05 * want, 8192),
                       f"{name}: inventory {got} B, pg_relation_size {want} B")

    def step4(self):
        """An idle client held past the suspend timeout raises suspend_blockers."""
        _, rep, _ = self.report()
        primary = next(e for e in rep["endpoints"] if e["type"] == "primary")
        uptime = rep["sections"]["endpoint"]["data"]["latest"].get("uptime_s") or 0
        if not primary["suspendTimeoutS"]:
            self.check("4", False, "NOT TESTABLE: scale to zero is off on the primary; ask the user before changing it")
            return
        if uptime < 24 * 3600:
            self.check("4", False, f"NOT TESTABLE YET: Postgres up {uptime / 3600:.1f} h; re-run step 4 after 24 h")
            return
        with self.connect(IDLE_APP) as conn:
            conn.execute("SELECT 1")
            wait = primary["suspendTimeoutS"] + 60
            print(f"  holding an idle {IDLE_APP} session {wait} s, then one analysis run")
            time.sleep(wait)
            self.analysis_run()
            found = [f for f in self.open_findings("suspend_blockers") if f["object"].startswith(f"{IDLE_APP} as ")]
        self.check("4", bool(found), f"suspend_blockers for {IDLE_APP}: {[f['object'] for f in found]}")
        if found:
            ev = json.loads(found[0]["evidence"])
            self.check("4", "pid" not in ev and ev.get("idle_sessions", 0) >= 1, f"evidence {ev}")

    def step5(self):
        """A synced table: named on its storage, its unused index reviewed without a fix, a full refresh found."""
        if not (self.a.synced_pg_table and self.a.synced_pipeline):
            self.check("5", False, "NOT RUN: create the synced table first (plan Task 18), then pass "
                                   "--synced-pg-table and --synced-pipeline")
            return
        name = self.a.synced_pg_table
        _, rep, _ = self.report()
        storage = [t for t in rep["sections"]["tables"]["data"] if t.get("synced_table")]
        self.check("5", any(t["synced_table"].replace('"', "") == name for t in storage),
                   f"storage named after the synced table: {[(t['table_name'], t['synced_table']) for t in storage]}")
        review = [f for f in self.open_findings("synced_index_review") if name in f["evidence"]]
        self.check("5", bool(review), f"synced_index_review findings: {[f['object'] for f in review]}")
        if review:
            fps = ", ".join(f"'{f['fingerprint']}'" for f in review)
            acts = self.sql.query(f"SELECT action_id FROM {self.cfg.table('proposed_actions')} WHERE fingerprint IN ({fps})")
            self.check("5", acts == [], f"no fix action for the reviewed index ({len(acts)} found)")
        drops = self.sql.query(f"""
            SELECT a.action_id FROM {self.cfg.table('proposed_actions')} a JOIN {self.cfg.table('findings')} f
            ON f.fingerprint = a.fingerprint WHERE f.project_id = :p AND a.sql LIKE 'DROP INDEX%'
              AND get_json_object(f.evidence, '$.synced_table') IS NOT NULL""", {"p": self.a.project_uid})
        self.check("5", drops == [], f"no DROP INDEX action on synced storage ({len(drops)} found)")
        print("  full refresh of the synced table's pipeline, then two analysis runs")
        upd = self.w.pipelines.start_update(pipeline_id=self.a.synced_pipeline, full_refresh=True)
        for _ in range(120):
            u = self.w.pipelines.get_update(pipeline_id=self.a.synced_pipeline, update_id=upd.update_id).update
            if str(getattr(u.state, "value", u.state)) in ("COMPLETED", "FAILED", "CANCELED"):
                break
            time.sleep(15)
        self.check("5", str(getattr(u.state, "value", u.state)) == "COMPLETED", f"refresh {u.state}")
        self.analysis_run()
        self.analysis_run()
        refresh = [f for f in self.open_findings("synced_full_refresh") if name in f["object"].replace('"', "")]
        self.check("5", bool(refresh), f"synced_full_refresh: {[(f['object'], f['severity']) for f in refresh]}")

    def step6(self):
        """The download opens offline: no scripts or external requests, no query text for a PII member; audited."""
        token = self.owner()
        t0 = self.sql.query("SELECT current_timestamp() AS now")[0]["now"]
        status, html, headers = raw_get(f"{self.url}/api/report/{self.a.project_uid}/default.html", token)
        if not self.check("6", status == 200, f"download status {status}"):
            return
        cd = headers.get("Content-Disposition", "")
        self.check("6", re.search(r'filename="lakebase-health-healthcheck-production-\d{8}T\d{4}Z\.html"', cd) is not None, cd)
        self.check("6", not re.search(r"<script|https?://|\bsrc=|url\(|<link", html, re.I), "no scripts or external URLs")
        texts = [r["query_text"] for r in self.sql.query(
            f"SELECT DISTINCT query_text FROM {self.cfg.table('findings')} WHERE project_id = :p "
            "AND query_text IS NOT NULL AND query_text NOT LIKE '<REDACTED%' LIMIT 50", {"p": self.a.project_uid})]
        leaked = [q for q in texts if len(q) > 12 and q in html]
        self.check("6", texts != [] and leaked == [], f"{len(texts)} known query texts, {len(leaked)} in the file")
        audit = self.sql.query(f"SELECT actor, detail FROM {self.cfg.table('audit_events')} WHERE action = 'report_export' "
                               "AND project_id = :p AND ts >= :t", {"p": self.a.project_uid, "t": t0})
        self.check("6", len(audit) >= 1, f"audit rows: {audit}")
        out = ROOT / "report-acceptance.html"
        out.write_text(html)
        print(f"  saved {out}: open it with the network off to confirm it renders")

    def step7(self):
        """The test SP sees only healthcheck's report; any other project or branch is 404."""
        sp = sp_token(self.w.config.host, self.a.test_sp_id, os.environ[self.a.test_sp_secret_env])
        status, _, _ = self.report(sp)
        self.check("7", status == 200, f"test SP healthcheck report: {status}")
        _, fleet = call(f"{self.url}/api/fleet", self.owner())
        others = [p["project_id"] for p in fleet["projects"] if p["project_id"] != self.a.project_uid][:3]
        for uid in others:
            s, _ = call(f"{self.url}/api/report/{uid}/default", sp)
            self.check("7", s == 404, f"test SP, project {uid}: {s}")
            s, _ = call(f"{self.url}/api/report/{uid}/default.html", sp)
            self.check("7", s == 404, f"test SP download, project {uid}: {s}")
        s, _, _ = self.report(sp, branch="br-not-a-branch-0000")
        self.check("7", s == 404, f"test SP, unknown branch: {s}")

    def step8(self):
        """Report data loads in under 5 s with a warm warehouse."""
        self.report()
        _, _, secs = self.report()
        self.check("8", secs < 5, f"report JSON in {secs:.1f} s")


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--test-sp-id", "--test-sp-secret-env"):
        ap.add_argument(a, required=True)
    ap.add_argument("--steps", default="1,2,3,4,5,6,7,8")
    ap.add_argument("--synced-pg-table", default="", help="schema.table of the synced table in Postgres")
    ap.add_argument("--synced-pipeline", default="", help="pipeline id of the synced table")
    a = ap.parse_args()
    acc = Acceptance(a)
    for step in a.steps.split(","):
        try:
            getattr(acc, f"step{step.strip()}")()
        except Exception as e:
            acc.check(step, False, f"{type(e).__name__}: {e}")
    print("\n".join(["FAILURES:"] + acc.failures) if acc.failures else "ALL REPORT ACCEPTANCE CHECKS PASSED")
    sys.exit(1 if acc.failures else 0)


if __name__ == "__main__":
    main()
