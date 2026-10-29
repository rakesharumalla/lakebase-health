# Final review C1: the fixer connects to the finding's own database (evidence datname, or the object's
# "<db>:" prefix) for verify, apply, rollback and recovery; when it cannot tell, it refuses as blocked.
import json
from datetime import timedelta
from types import SimpleNamespace as NS

from lakebase_health.jobs import fix
from tests.fakes import FakeSql
from tests.unit.test_fix_job import CFG, NOW, SQL, Recorder, deps, stuck_row, work

DBS = ["databricks_postgres", "app", "app_staging"]


def fake_w(dbs=DBS):
    ep = NS(name="projects/x/branches/production/endpoints/primary",
            status=NS(endpoint_type="EndpointType.ENDPOINT_TYPE_READ_WRITE", hosts=NS(host="h")))
    return NS(postgres=NS(
        list_projects=lambda: [NS(uid="p1", name="projects/x")],
        list_branches=lambda parent: [NS(uid="br1", name="projects/x/branches/production")],
        list_endpoints=lambda parent: [ep],
        list_databases=lambda parent: [NS(status=NS(postgres_database=d)) for d in dbs]))


def action(**kw):
    return {"project_id": "p1", "branch_id": "br1", **kw}


def test_target_uses_the_database_prefix_of_the_finding_object_not_the_first_database():
    tgt = fix._target(fake_w(), action(object="app_staging:public.orders"))
    assert tgt["database"] == "app_staging"
    assert fix._target(fake_w(), action(object="app:public.orders"))["database"] == "app"


def test_target_prefers_the_evidence_datname():
    tgt = fix._target(fake_w(), action(object="public.orders", evidence=json.dumps({"datname": "app"})))
    assert tgt["database"] == "app"


def test_target_refuses_when_the_database_cannot_be_determined():
    for a in (action(object="public.orders"), action(object="other:public.orders"),
              action(object="public.orders", evidence=json.dumps({"datname": "gone"}))):
        try:
            fix._target(fake_w(), a)
        except fix.NoDatabase:
            continue
        raise AssertionError(f"no NoDatabase for {a}")


def _with_real_target(rec, sessions):
    d = deps(rec)
    d["target"] = fix._target
    inner = d["session"]

    def session(endpoint, host, db, kind):
        sessions.append(db)
        return inner(endpoint, host, db, kind)

    d["session"] = session
    return d


def test_apply_runs_in_the_finding_database():
    w = work(request="apply", state="verified")
    w.action["object"] = "app:public.orders"
    rec, dbs = Recorder([w]), []
    fix.run_fixer(None, fake_w(), CFG, "fixer", deps=_with_real_target(rec, dbs), now=NOW)
    assert dbs == ["app"] and ("a1", None, "applied") in rec.states


def test_verify_runs_in_the_finding_database_on_the_branch():
    w = work()
    w.action["object"] = "app_staging:public.orders"
    rec, dbs = Recorder([w]), []
    fix.run_fixer(None, fake_w(), CFG, "fixer", deps=_with_real_target(rec, dbs), now=NOW)
    assert dbs == ["app_staging"] and rec.sessions == ["e"]


def test_unknown_database_blocks_without_executing():
    w = work(request="apply", state="verified")
    w.action["object"] = "public.orders"
    rec, dbs = Recorder([w]), []
    out = fix.run_fixer(None, fake_w(), CFG, "fixer", deps=_with_real_target(rec, dbs), now=NOW)
    assert rec.executed == [] and dbs == [] and ("a1", None, "blocked") in rec.states
    assert json.loads(rec.runs[-1]["report"])["blocked_reason"] == "no_database"
    assert "database" in rec.runs[-1]["error"] and out == {"blocked": 1}


def test_recovery_runs_in_the_finding_database():
    row = {**stuck_row("applying", timedelta(hours=3), "a1"), "project_id": "p1", "branch_id": "br1",
           "object": "app:public.orders"}
    rec, dbs = Recorder([], stuck=[row]), []
    fix.run_fixer(None, fake_w(), CFG, "fixer", deps=_with_real_target(rec, dbs), now=NOW)
    assert dbs == ["app"] and rec.recovered == [(SQL, "a1")]


def test_recovery_with_an_unknown_database_blocks():
    row = {**stuck_row("applying", timedelta(hours=3), "a1"), "project_id": "p1", "branch_id": "br1",
           "object": "public.orders"}
    rec, dbs = Recorder([], stuck=[row]), []
    out = fix.run_fixer(None, fake_w(), CFG, "fixer", deps=_with_real_target(rec, dbs), now=NOW)
    assert dbs == [] and rec.recovered == [] and ("a1", None, "blocked") in rec.states
    assert out == {"recovered:blocked": 1}


def test_stuck_query_reads_the_finding_object_and_evidence():
    sql = FakeSql()
    fix._stuck(sql, CFG, NOW)
    assert "f.object" in sql.statements[0][0] and "f.evidence" in sql.statements[0][0]
