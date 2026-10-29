"""Acceptance for the read-only app (spec 2026-10-28-lakebase-health-app-design.md, section 5).

Usage: TEST_SP_SECRET=... engine/.venv/bin/python scripts/app_acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --test-sp-id <application id> --test-sp-secret-env TEST_SP_SECRET
The test SP must have CAN_USE on the app and a Lakebase Postgres role on the healthcheck project only.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

from databricks.sdk import WorkspaceClient


def sp_token(host, client_id, secret):
    data = f"grant_type=client_credentials&scope=all-apis&client_id={client_id}&client_secret={secret}".encode()
    req = urllib.request.Request(f"{host.rstrip('/')}/oidc/v1/token", data=data, method="POST")
    with urllib.request.urlopen(req) as r:
        return json.load(r)["access_token"]


def get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, json.load(r), time.time() - t
    except urllib.error.HTTPError as e:
        return e.code, None, time.time() - t


def sql(w, wh, stmt):
    r = w.statement_execution.execute_statement(warehouse_id=wh, statement=stmt, wait_timeout="50s")
    return r.result.data_array or [] if r.result else []


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--test-sp-id", "--test-sp-secret-env"):
        ap.add_argument(a, required=True)
    a = ap.parse_args()
    w = WorkspaceClient(profile=a.profile)
    app = w.apps.get("lakebase-health")
    url = app.url.rstrip("/")
    owner = w.config.authenticate()["Authorization"].split()[1]
    sp = sp_token(w.config.host, a.test_sp_id, os.environ[a.test_sp_secret_env])
    failures = []

    get(f"{url}/api/meta", owner)  # warm the warehouse and the owner's access cache
    s, fleet, fleet_s = get(f"{url}/api/fleet", owner)
    owner_ids = {p["project_id"] for p in (fleet or {}).get("projects", [])}
    if s != 200 or len(owner_ids) < 2:
        failures.append(f"1: owner fleet status {s}, {len(owner_ids)} projects")
    s, _, sp_cold_s = get(f"{url}/api/meta", sp)  # cold access lookup (branches + roles per project)
    print(f"role-based access lookup (cold): {sp_cold_s:.1f}s")
    s, fleet_sp, sp_fleet_s = get(f"{url}/api/fleet", sp)
    sp_ids = {p["project_id"] for p in (fleet_sp or {}).get("projects", [])}
    if s != 200 or sp_ids != {a.project_uid}:
        failures.append(f"1: test SP sees {sp_ids} (status {s})")
    other = next((i for i in owner_ids if i != a.project_uid), None)
    if other:
        s, _, _ = get(f"{url}/api/projects/{other}", sp)
        if s != 404:
            failures.append(f"1: test SP got {s} for another project")

    s, proj, proj_s = get(f"{url}/api/projects/{a.project_uid}", owner)
    [[expected]] = sql(w, a.warehouse,
                       f"SELECT count(*) FROM {a.catalog}.lakebase_health.findings WHERE project_id = '{a.project_uid}' "
                       "AND (status IN ('new','open') OR (status = 'resolved' AND resolved_at >= current_timestamp() - INTERVAL 7 DAYS))")
    got = len(proj["findings"]) if proj else None
    if s != 200 or got != int(expected):
        failures.append(f"2: project page {got} findings, table {expected}")

    ep = next(c["endpoint_id"] for c in proj["computes"] if c["endpoint_type"].endswith("READ_WRITE"))
    frm = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 24 * 3600))
    s, tl, _ = get(f"{url}/api/endpoints/{ep}/timeline?from={frm}", owner)
    if s != 200 or not tl["locks"] or not any(e["kind"] == "statement_timeout" for e in tl["events"]):
        failures.append(f"3: timeline status {s}, locks {len(tl['locks']) if tl else 0}")

    with_text = [f for f in proj["findings"] if f["check_id"] in ("top_db_time_query", "plan_flip", "query_regression", "disk_spill")]
    if with_text:
        s, fd, finding_s = get(f"{url}/api/findings/{with_text[0]['fingerprint']}", sp)
        if s != 200 or fd["finding"].get("query_text") not in (None, "[masked]"):
            failures.append(f"4: query text not masked for test SP (status {s})")
    else:
        finding_s = 0.0
        failures.append("4: no finding with query text on the project to check masking")

    grants = sql(w, a.warehouse, f"SHOW GRANTS `{app.service_principal_client_id}` ON SCHEMA {a.catalog}.lakebase_health")
    privs = {row[1] for row in grants if row[0] == app.service_principal_client_id}
    if privs - {"USE SCHEMA", "SELECT", "USE CATALOG"}:
        failures.append(f"5: app SP has {privs}")

    for name, secs in (("fleet", fleet_s), ("project", proj_s), ("finding", finding_s), ("fleet as role-based user", sp_fleet_s)):
        if secs > 3:
            failures.append(f"6: {name} took {secs:.1f}s")

    print("\n".join(["FAILURES:"] + failures) if failures else "ALL APP ACCEPTANCE CHECKS PASSED")
    print("note: criterion 4 (unmasked for PII members) needs a databricks_pii_access member; unit tests cover it if the group is absent")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
