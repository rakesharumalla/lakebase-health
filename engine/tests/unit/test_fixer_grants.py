"""scripts/fixer_grants.sh resolves the fixer job by its bundle ID for the chosen target (B5)."""
import json
import os
import stat
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "fixer_grants.sh"
UUID = "00000000-0000-4000-8000-0000000000f1"

# A stand-in `databricks` CLI: it logs every call, and `jobs list` offers a decoy job of another target whose name
# also ends in lakebase-health-fixer.
FAKE_CLI = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ["CLI_LOG"], "a") as f:
    f.write(json.dumps(args) + "\n")
target = args[args.index("-t") + 1] if "-t" in args else os.environ.get("DATABRICKS_BUNDLE_TARGET", "dev")
if args[:2] == ["api", "post"]:
    print(json.dumps({"status": {"state": "SUCCEEDED"}, "statement_id": "s1"}))
elif args[:2] == ["jobs", "list"]:
    print(json.dumps([{"job_id": 111, "settings": {"name": "[dev someone] lakebase-health-fixer"}}]))
elif args[:2] == ["bundle", "summary"]:
    print(json.dumps({"resources": {"jobs": {"lakebase_health_fixer": {"id": {"prod": "333", "dev": "222"}[target]}}}}))
elif args[:2] == ["bundle", "validate"]:
    print(json.dumps({"workspace": {"root_path": f"/Workspace/bundle/{target}"}}))
elif args[:2] == ["workspace", "get-status"]:
    print(json.dumps({"object_id": 42}))
elif args[:2] == ["serving-endpoints", "get"]:
    ep_id = os.environ.get("FAKE_EP_ID", "")
    print(json.dumps({"name": args[2], **({"id": ep_id} if ep_id else {})}))
'''


def _run(tmp_path, target, **extra):
    cli = tmp_path / "databricks"
    cli.write_text(FAKE_CLI)
    cli.chmod(cli.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "log.jsonl"
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "CLI_LOG": str(log), "PROFILE": "p",
           "CATALOG": "cat", "ENGINE_SP": UUID, "FIXER_SP": UUID, "APP_SP": UUID, "WAREHOUSE": "abc123"}
    env.update(extra)
    if target:
        env["DATABRICKS_BUNDLE_TARGET"] = target
    out = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, cwd=REPO)
    assert out.returncode == 0, out.stderr
    return out.stdout, [json.loads(line) for line in log.read_text().splitlines()]


def test_fixer_job_comes_from_the_bundle_summary_of_the_target(tmp_path):
    for target, job in (("prod", "333"), (None, "222")):
        stdout, calls = _run(tmp_path, target)
        [perm] = [c for c in calls if c[:3] == ["permissions", "update", "jobs"]]
        assert perm[3] == job and f"job {job}" in stdout, (target, perm)
        [summary] = [c for c in calls if c[:2] == ["bundle", "summary"]]
        assert summary[summary.index("-t") + 1] == (target or "dev")
        assert not any(c[:2] == ["jobs", "list"] for c in calls)
        (tmp_path / "log.jsonl").unlink()


def _statements(calls):
    return [json.loads(c[c.index("--json") + 1])["statement"] for c in calls if c[:2] == ["api", "post"]]


def test_the_fixer_may_write_the_draft_tables(tmp_path):
    _, calls = _run(tmp_path, None)
    stmts = _statements(calls)
    for t in ("drafts", "llm_calls", "llm_status"):
        assert f"GRANT MODIFY ON TABLE cat.lakebase_health.{t} TO `{UUID}`" in stmts, t


def test_the_fixer_gets_can_query_on_a_custom_endpoint(tmp_path):
    stdout, calls = _run(tmp_path, None, FAKE_EP_ID="ep-123", LLM_ENDPOINT="my-model")
    [get] = [c for c in calls if c[:2] == ["serving-endpoints", "get"]]
    assert get[2] == "my-model"
    [upd] = [c for c in calls if c[:2] == ["serving-endpoints", "update-permissions"]]
    assert upd[2] == "ep-123"
    acl = json.loads(upd[upd.index("--json") + 1])["access_control_list"]
    assert acl == [{"service_principal_name": UUID, "permission_level": "CAN_QUERY"}]
    assert "CAN_QUERY on my-model" in stdout


def test_a_foundation_model_endpoint_needs_no_grant_and_llm_off_skips_it(tmp_path):
    stdout, calls = _run(tmp_path, None)
    assert [c[2] for c in calls if c[:2] == ["serving-endpoints", "get"]] == ["databricks-claude-opus-5-5"]
    assert not any(c[:2] == ["serving-endpoints", "update-permissions"] for c in calls)
    assert "foundation model endpoint" in stdout
    (tmp_path / "log.jsonl").unlink()
    stdout, calls = _run(tmp_path, None, LLM_ENABLED="false")
    assert not any(c[0] == "serving-endpoints" for c in calls) and "no model endpoint grant" in stdout


def test_the_app_may_write_approvals_and_audit_events(tmp_path):
    _, calls = _run(tmp_path, None)
    app = [s for s in _statements(calls) if s.startswith("GRANT MODIFY") and s.endswith(f"TO `{UUID}`")]
    assert "GRANT MODIFY ON TABLE cat.lakebase_health.audit_events TO `" + UUID + "`" in app
    assert "GRANT MODIFY ON TABLE cat.lakebase_health.approvals TO `" + UUID + "`" in app
