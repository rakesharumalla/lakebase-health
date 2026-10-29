"""Duplicate-warning guards: stable fingerprints, instance-wide checks, platform-query exclusion, precedence."""
from datetime import timedelta
from pathlib import Path

import pytest
import yaml

from lakebase_health.checkspec import CheckSpecError, load_checks
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.lifecycle import apply_run
from lakebase_health.models import CheckOutcome
from lakebase_health.synced import SYNCED_MAP_SQL
from tests.unit.test_executors import ep
from tests.unit.test_runner import NOW, FakePg, FakeSession, runner, spec

CHECKS_DIR = Path(__file__).resolve().parents[2] / "src" / "lakebase_health" / "checks"
INSTANCE_WIDE = {"privileged_roles", "config_drift", "long_transaction", "idle_in_transaction", "blocked_session",
                 "prepared_transaction", "replication_slot_retention", "xmin_horizon_holder", "non_ssl_sessions",
                 "suspend_blockers"}


def packaged():
    return {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}


class CountingSession(FakeSession):
    def __init__(self, rows):
        super().__init__(rows)
        self.calls = 0

    def query(self, sql):
        if sql != SYNCED_MAP_SQL:  # the runner's synced-table lookup, once per database session
            self.calls += 1
        return super().query(sql)


def test_instance_wide_check_runs_once_per_endpoint_without_database_prefix():
    s = spec("roles", per_database=False)
    sess = CountingSession([{"object": '"app"', "measure": 1.0}])
    [o] = runner(pg=FakePg(sess)).run([s], [ep(databases=["db1", "db2", "db3"])], NOW, {}, set())
    assert sess.calls == 1
    assert [r.object for r in o.results] == ['"app"']


def test_per_database_check_still_prefixes_each_database():
    s = spec("tables")
    sess = CountingSession([{"object": "public.t", "measure": 1.0}])
    [o] = runner(pg=FakePg(sess)).run([s], [ep(databases=["db1", "db2"])], NOW, {}, set())
    assert sorted(r.object for r in o.results) == ["db1:public.t", "db2:public.t"]


def test_per_database_flag_is_postgres_only():
    with pytest.raises(CheckSpecError, match="per_database"):
        spec("d", "delta", per_database=False)


def test_cluster_catalog_checks_are_declared_instance_wide():
    specs = packaged()
    assert {i for i, s in specs.items() if not s.per_database} == INSTANCE_WIDE


def test_database_prefixed_duplicates_resolve_once_instance_wide_results_arrive():
    s = spec("roles", per_database=False)
    old_runner_spec = spec("roles")
    sess = FakeSession([{"object": '"app"', "measure": 1.0}])
    inv = [ep(databases=["db1", "db2"])]
    findings = {}
    for when, sp in ((NOW, old_runner_spec), (NOW + timedelta(hours=1), s), (NOW + timedelta(hours=2), s)):
        outcomes = runner(pg=FakePg(sess)).run([sp], inv, when, {}, set())
        findings.update({f.fingerprint: f for f in apply_run(findings, outcomes, {"roles": sp}, when)})
    status = {f.object: f.status for f in findings.values()}
    assert status == {"db1:\"app\"": "resolved", "db2:\"app\"": "resolved", '"app"': "open"}


# --- platform queries -------------------------------------------------------------------------------------------

import re
import sqlite3

from lakebase_health.config import EngineConfig
from lakebase_health.exclusions import load_exclusions
from lakebase_health.executors.delta import DeltaExecutor
from tests.fakes import FakeSql

PER_QUERY = {"plan_flip", "storage_wait", "query_regression", "top_db_time_query", "disk_spill", "sort_limit_waste",
             "hot_row_lock"}
QUERY_SCOPED = PER_QUERY | {"row_misestimate"}
PKG_EXCL = load_exclusions(CHECKS_DIR / "exclusions.yaml")


def _sqlite_true(predicate: str, **cols) -> bool:
    db = sqlite3.connect(":memory:")
    db.create_function("REGEXP", 2, lambda rx, v: v is not None and re.search(rx, v) is not None)
    db.execute(f"CREATE TABLE r AS SELECT {', '.join(f'? AS {c}' for c in cols)}", list(cols.values()))
    sql = predicate.replace(" RLIKE ", " REGEXP ")
    return bool(db.execute(f"SELECT CASE WHEN {sql} THEN 1 ELSE 0 END FROM r").fetchone()[0])


@pytest.mark.parametrize("role", ["databricks_writer_16407", "databricks_writer_1", "cloud_admin",
                                  "databricks_control_plane", "databricks_monitor"])
def test_platform_roles_are_excluded(role):
    assert _sqlite_true(PKG_EXCL.platform_role_sql("usename"), usename=role)


@pytest.mark.parametrize("role", ["alice@example.com", "app_writer_1", "databricks_writer", "databricks_writer_app",
                                  "my_databricks_writer_1", "databricks_reader_16407", "authenticator", "twenty",
                                  "6f0c2a1e-1111-4222-8333-944455556666", None])
def test_customer_roles_are_not_excluded(role):
    assert not _sqlite_true(PKG_EXCL.platform_role_sql("usename"), usename=role)


def test_engine_and_fixer_service_principals_are_excluded_once_named():
    sp = "6f0c2a1e-1111-4222-8333-944455556666"
    excl = PKG_EXCL.with_query_users([sp, "", None])
    assert _sqlite_true(excl.platform_role_sql("usename"), usename=sp)
    assert not _sqlite_true(excl.platform_role_sql("usename"), usename="alice@example.com")


def test_engine_application_names_are_excluded_but_customer_apps_are_not():
    pred = PKG_EXCL.platform_app_sql("application_name")
    assert _sqlite_true(pred, application_name="lakebase-health")
    assert _sqlite_true(pred, application_name="lakebase-health-fixer")
    for app in ("psql", "lakebase-health-notebook", "whats-new-studio", None):  # the notebook is a tool: tool_app_sql
        assert not _sqlite_true(pred, application_name=app)


def _rewritten(sql_text, query_scoped=True):
    s = spec("q", "delta", sql=sql_text, query_scoped=query_scoped)
    fake = FakeSql()
    DeltaExecutor(fake, EngineConfig(catalog="cat"), PKG_EXCL).run(s)
    return fake.statements[-1][0]


def test_query_scoped_check_reads_query_telemetry_without_platform_roles():
    out = _rewritten("SELECT 1 FROM system.lakebase.plan_history p JOIN system.lakebase.pg_stat_statements_counters c "
                     "JOIN system.lakebase.active_session_history a")
    assert "system.lakebase" not in out
    assert out.count("databricks_writer_") == 3 and "cloud_admin" in out and "lakebase-health" in out
    assert "LEFT ANTI JOIN" in out and "cat.lakebase_health.raw_pg_stat_statements_counters" in out


def test_other_delta_checks_are_not_filtered():
    out = _rewritten("SELECT 1 FROM system.lakebase.plan_history p", query_scoped=False)
    assert out.strip() == "SELECT 1 FROM cat.lakebase_health.raw_plan_history p"


def test_query_scoped_flag_is_declared_on_every_query_check():
    specs = packaged()
    assert {i for i, s in specs.items() if s.query_scoped} == QUERY_SCOPED


@pytest.mark.parametrize("check_id", sorted(PER_QUERY))
def test_per_query_checks_select_a_queryid_column(check_id):
    assert re.search(r"AS queryid\b", packaged()[check_id].sql)


def test_query_scoped_results_carry_queryid_as_string_evidence():
    s = spec("q", "delta", query_scoped=True)
    rows = [{"project_id": "p", "branch_id": "b", "endpoint_id": "e", "object": "-42", "queryid": -42, "measure": 1.0},
            {"project_id": "p", "branch_id": "b", "endpoint_id": "e", "object": "e/unknown", "queryid": None,
             "measure": 1.0}]
    from tests.unit.test_runner import FakeDelta
    [o] = runner(FakeDelta(rows)).run([s], [], NOW, {}, set())
    assert [r.evidence["queryid"] for r in o.results] == ["-42", None]


def test_run_engine_excludes_its_own_service_principal(tmp_path):
    from tests.unit.test_run import fake_w
    from lakebase_health.jobs.run import run_engine
    from types import SimpleNamespace as NS
    (tmp_path / "exclusions.yaml").write_text((CHECKS_DIR / "exclusions.yaml").read_text())
    (tmp_path / "q.yaml").write_text(
        "id: q\ntitle: Q\ncategory: c\nsource: delta\nexplain: e\nthresholds: {warning: 0}\nquery_scoped: true\n"
        "sql: SELECT 1 AS measure FROM system.lakebase.plan_history\n")
    sp, fixer = "6f0c2a1e-1111-4222-8333-944455556666", "7a0c2a1e-1111-4222-8333-944455556666"
    w = fake_w()
    w.current_user = NS(me=lambda: NS(user_name=sp))
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run_engine(sql, w, EngineConfig(catalog="cat", service_roles=(fixer,)), connect=lambda **k: None, now=NOW,
               checks_dir=tmp_path, skip_ingest=True)
    [stmt] = sql.ran(r"1 AS measure FROM")
    assert sp in stmt and fixer in stmt


def test_run_engine_does_not_exclude_a_human_running_the_engine(tmp_path):
    from tests.unit.test_run import fake_w
    from lakebase_health.jobs.run import run_engine
    from types import SimpleNamespace as NS
    (tmp_path / "exclusions.yaml").write_text((CHECKS_DIR / "exclusions.yaml").read_text())
    (tmp_path / "q.yaml").write_text(
        "id: q\ntitle: Q\ncategory: c\nsource: delta\nexplain: e\nthresholds: {warning: 0}\nquery_scoped: true\n"
        "sql: SELECT 1 AS measure FROM system.lakebase.plan_history\n")
    w = fake_w()
    w.current_user = NS(me=lambda: NS(user_name="alice@example.com"))
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run_engine(sql, w, EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path,
               skip_ingest=True)
    [stmt] = sql.ran(r"1 AS measure FROM")
    assert "alice@example.com" not in stmt


# --- row_misestimate: one finding per endpoint -------------------------------------------------------------------

from lakebase_health.fingerprint import fingerprint
from lakebase_health.models import FLEET, CheckResult


def _top(n):
    return [{"queryid": str(-i), "calls": 100, "total_ms": 1000.0 * (n - i), "est_rows": 1, "actual_rows": 500,
             "factor": 500.0} for i in range(n)]


def _endpoint_row(ep_id="e", queries=12, share=42.0):
    return {"project_id": "p", "branch_id": "b", "endpoint_id": ep_id, "object": ep_id, "queries": queries,
            "measure": share, "top_queries": _top(min(queries, 10))}


def test_title_template_renders_per_result_and_lifecycle_keeps_it():
    from tests.unit.test_runner import FakeDelta
    s = spec("rm", "delta", title="Row estimates far off", title_template="Row estimates far off for {queries} queries")
    [o] = runner(FakeDelta([_endpoint_row(queries=12)])).run([s], [], NOW, {}, set())
    [r] = o.results
    assert r.title == "Row estimates far off for 12 queries"
    [f] = apply_run({}, [o], {"rm": s}, NOW)
    assert f.title == "Row estimates far off for 12 queries"


def test_row_misestimate_is_one_finding_per_endpoint_without_query_text():
    from tests.unit.test_runner import FakeDelta
    s = packaged()["row_misestimate"]
    rows = [_endpoint_row("e1", 12, 42.0), _endpoint_row("e2", 1, 3.0), _endpoint_row("e3", 2, 75.0)]
    [o] = runner(FakeDelta(rows)).run([s], [], NOW, {}, set())
    assert [(r.object, r.severity) for r in o.results] == [("e1", "warning"), ("e2", "info"), ("e3", "critical")]
    r = o.results[0]
    assert r.title == "Row estimates far off for 12 queries" and r.query_text is None
    assert len(r.evidence["top_queries"]) == 10 and r.evidence["queries"] == 12
    assert r.fingerprint == fingerprint("row_misestimate", "p", "e1", "e1")


def test_row_misestimate_sql_applies_floors_and_names_no_query_text():
    sql = packaged()["row_misestimate"].sql
    for floor in ("calls >= 50", "total_ms >= 1000", "sampled_execs >= 3", "sampled_hours >= 2", "factor >= 10"):
        assert floor in sql
    assert "query_text" not in sql and "query)" not in sql
    # a redacted plan_json (PII masking) cannot show ModifyTable: it counts only when rows came back (not a write)
    assert "NOT plan_json RLIKE '\"Node Type\"' AND actual_rows > 0" in sql
    assert "endpoint_id AS object" in sql and "slice(" in sql and ", 1, 10)" in sql
    assert packaged()["row_misestimate"].thresholds == {"info": 0.0, "warning": 10.0, "critical": 50.0}


def test_old_per_query_row_misestimate_findings_resolve_under_the_normal_lifecycle():
    from tests.unit.test_runner import FakeDelta
    s = packaged()["row_misestimate"]
    specs = {"row_misestimate": s}
    old = [CheckResult("row_misestimate", fingerprint("row_misestimate", "p", "e1", q), "p", "b", "e1", q, 50.0,
                       "warning", "high", {"execs": 3}, query_text="SELECT 1") for q in ("-1", "-2", "7")]
    findings = {}
    findings.update({f.fingerprint: f for f in apply_run(findings, [CheckOutcome("row_misestimate", FLEET, "ok",
                                                                                 results=old)], specs, NOW)})
    findings[old[2].fingerprint].status = "suppressed"
    findings[old[2].fingerprint].suppressed_until = NOW + timedelta(days=30)
    for h in (1, 2):
        outcomes = runner(FakeDelta([_endpoint_row("e1", 3, 20.0)])).run([s], [], NOW + timedelta(hours=h), {}, set())
        findings.update({f.fingerprint: f for f in apply_run(findings, outcomes, specs, NOW + timedelta(hours=h))})
    by_obj = {f.object: f.status for f in findings.values()}
    assert by_obj == {"-1": "resolved", "-2": "resolved", "7": "resolved", "e1": "open"}


# --- precedence: superseded_by -----------------------------------------------------------------------------------

import json

from lakebase_health.checkspec import parse_check
from lakebase_health.config import EngineConfig as _Cfg
from lakebase_health.ledger import verify
from lakebase_health.precedence import apply_precedence
from lakebase_health.record import persist

DDL = {"kind": "ddl", "template": "DROP INDEX {object};", "rollback": "CREATE INDEX CONCURRENTLY x ON public.t (a);", "risk": "moderate",
       "reversible": True}
SPECS = {
    "unused": parse_check({"id": "unused", "title": "U", "category": "c", "source": "postgres", "explain": "e",
                           "sql": "SELECT 1", "thresholds": {"info": 0}, "fix": DDL,
                           "superseded_by": {"key": "object", "checks": ["dup", "redundant"]}}, stem="unused"),
    "dup": parse_check({"id": "dup", "title": "D", "category": "c", "source": "postgres", "explain": "e",
                        "sql": "SELECT 1", "thresholds": {"warning": 0}}, stem="dup"),
    "redundant": parse_check({"id": "redundant", "title": "R", "category": "c", "source": "postgres",
                              "explain": "e", "sql": "SELECT 1", "thresholds": {"warning": 0}}, stem="redundant"),
    "ratio": parse_check({"id": "ratio", "title": "Q", "category": "c", "source": "delta", "explain": "e",
                          "sql": "SELECT 1", "thresholds": {"warning": 0},
                          "superseded_by": {"key": "database", "checks": ["heavy"]}}, stem="ratio"),
    "heavy": parse_check({"id": "heavy", "title": "H", "category": "c", "source": "postgres", "explain": "e",
                          "sql": "SELECT 1", "thresholds": {"warning": 0}}, stem="heavy"),
    "cache": parse_check({"id": "cache", "title": "C", "category": "c", "source": "delta", "explain": "e",
                          "sql": "SELECT 1", "thresholds": {"warning": 0},
                          "superseded_by": {"key": "endpoint", "checks": ["lfc"]}}, stem="cache"),
    "lfc": parse_check({"id": "lfc", "title": "L", "category": "c", "source": "delta", "explain": "e",
                        "sql": "SELECT 1", "thresholds": {"warning": 0},
                        "superseded_by": {"key": "endpoint", "checks": ["top"]}}, stem="lfc"),
    "top": parse_check({"id": "top", "title": "T", "category": "c", "source": "delta", "explain": "e",
                        "sql": "SELECT 1", "thresholds": {"warning": 0}}, stem="top"),
}


def res(check, obj, ep_id="e", sev="warning", evidence=None, candidate=None):
    return CheckResult(check, fingerprint(check, "p", ep_id, obj), "p", "b", ep_id, obj, 1.0, sev, "high",
                       dict(evidence or {"k": check}), candidate_sql=candidate)


def out(check, *results, scope=("p", "e")):
    return CheckOutcome(check, scope, "ok", results=list(results))


def test_superseded_by_spec_is_validated():
    base = {"id": "x", "title": "X", "category": "c", "source": "delta", "explain": "e", "sql": "SELECT 1",
            "thresholds": {"warning": 0}}
    for bad in ({"key": "table", "checks": ["y"]}, {"key": "object", "checks": []}, ["y"],
                {"key": "object", "checks": ["x"]}):
        with pytest.raises(CheckSpecError, match="superseded_by"):
            parse_check({**base, "superseded_by": bad}, stem="x")


def test_load_checks_rejects_unknown_and_cyclic_precedence(tmp_path):
    def write(i, sup):
        (tmp_path / f"{i}.yaml").write_text(yaml.safe_dump(
            {"id": i, "title": i, "category": "c", "source": "delta", "explain": "e", "sql": "SELECT 1",
             "thresholds": {"warning": 0}, "superseded_by": {"key": "endpoint", "checks": [sup]}}))
    write("a", "nope")
    with pytest.raises(CheckSpecError, match="unknown"):
        load_checks(tmp_path, frozenset())
    write("a", "b")
    write("b", "a")
    with pytest.raises(CheckSpecError, match="cycle"):
        load_checks(tmp_path, frozenset())


def test_lower_result_is_dropped_and_its_evidence_kept_on_the_superseding_result():
    lower, higher = res("unused", "db:public.i_dup", evidence={"idx_scan": 0}), res("dup", "db:public.i_dup")
    outcomes = [out("unused", lower, res("unused", "db:public.other")), out("dup", higher)]
    superseded = apply_precedence(outcomes, {}, SPECS)
    assert superseded == {lower.fingerprint: higher.fingerprint}
    assert [r.object for r in outcomes[0].results] == ["db:public.other"]
    [entry] = higher.evidence["supersedes"]
    assert entry["check_id"] == "unused" and entry["fingerprint"] == lower.fingerprint
    assert entry["evidence"] == {"idx_scan": 0}


def test_precedence_keys_object_database_and_endpoint():
    outcomes = [out("unused", res("unused", "db:public.i", ep_id="e2")), out("dup", res("dup", "db:public.i")),
                out("ratio", res("ratio", "e/db", evidence={"datname": "db"}),
                    res("ratio", "e/other", evidence={"datname": "other"})),
                out("heavy", res("heavy", "db:public.big_table")),
                out("cache", res("cache", "e"), res("cache", "e9", ep_id="e9")), out("lfc", res("lfc", "e"))]
    apply_precedence(outcomes, {}, SPECS)
    left = {(o.check_id, r.object) for o in outcomes for r in o.results}
    assert ("unused", "db:public.i") in left            # same object, other endpoint: kept
    assert ("ratio", "e/db") not in left and ("ratio", "e/other") in left
    assert ("cache", "e") not in left and ("cache", "e9") in left


def test_an_open_superseding_finding_from_an_earlier_run_counts():
    higher = res("dup", "db:public.i")
    [f] = apply_run({}, [out("dup", higher)], SPECS, NOW)
    lower = res("unused", "db:public.i")
    outcomes = [out("unused", lower)]
    assert apply_precedence(outcomes, {f.fingerprint: f}, SPECS) == {lower.fingerprint: higher.fingerprint}
    f.status = "resolved"
    outcomes = [out("unused", res("unused", "db:public.i"))]
    assert apply_precedence(outcomes, {f.fingerprint: f}, SPECS) == {} and len(outcomes[0].results) == 1


def test_superseded_superseder_does_not_hide_lower_results_and_chains_point_at_the_survivor():
    outcomes = [out("cache", res("cache", "e")), out("lfc", res("lfc", "e")), out("top", res("top", "e"))]
    superseded = apply_precedence(outcomes, {}, SPECS)
    top_fp = fingerprint("top", "p", "e", "e")
    assert superseded == {fingerprint("lfc", "p", "e", "e"): top_fp}
    assert [r.object for r in outcomes[0].results] == ["e"]


def _ledger_sql(cfg):
    t = cfg.table
    sql = FakeSql(responses=[(r"max_by\(hash", lambda s, a: [{"action_id": r["action_id"], "hash": r["hash"]}
                                                              for r in sql.tables.get(t("ledger"), [])])])
    return sql


def test_existing_lower_finding_is_superseded_not_resolved_and_keeps_identity_and_action():
    cfg = _Cfg(catalog="cat")
    t = cfg.table
    sql = _ledger_sql(cfg)
    lower = lambda: res("unused", "db:public.i", sev="info", candidate="DROP INDEX CONCURRENTLY public.i;")
    persist(sql, cfg, "r1", NOW, [out("unused", lower()), out("dup")], SPECS, [])
    [f] = sql.tables[t("findings")]
    assert f["status"] == "new" and sql.tables[t("proposed_actions")][0]["state"] == "proposed"
    c = persist(sql, cfg, "r2", NOW + timedelta(hours=1), [out("unused", lower()), out("dup", res("dup", "db:public.i"))],
                SPECS, [])
    by_check = {r["check_id"]: r for r in sql.tables[t("findings")]}
    assert by_check["unused"]["status"] == "superseded" and by_check["unused"]["resolved_at"] is None
    assert by_check["unused"]["first_seen"] == f["first_seen"]
    assert json.loads(by_check["unused"]["evidence"])["superseded_by"] == {
        "check_id": "dup", "fingerprint": by_check["dup"]["fingerprint"]}
    assert json.loads(by_check["dup"]["evidence"])["supersedes"][0]["fingerprint"] == f["fingerprint"]
    assert c["findings_resolved"] == 0
    assert sql.tables[t("proposed_actions")][0]["state"] == "proposed"   # not expired while superseded
    ledger = sql.tables[t("ledger")]
    assert [e["event_type"] for e in ledger] == ["proposed"] and verify(ledger)
    # idempotent while the higher finding stays open
    persist(sql, cfg, "r3", NOW + timedelta(hours=2), [out("unused", lower()), out("dup", res("dup", "db:public.i"))],
            SPECS, [])
    rows = [r for r in sql.tables[t("findings")] if r["check_id"] == "unused"]
    assert len(rows) == 1 and rows[0]["status"] == "superseded" and len(sql.tables[t("ledger")]) == 1
    # the higher finding clears (resolves after its clean runs): the same finding returns to open, same fingerprint
    for h in (3, 4, 5):
        persist(sql, cfg, f"r{h + 1}", NOW + timedelta(hours=h), [out("unused", lower()), out("dup")], SPECS, [])
    assert {r["check_id"]: r["status"] for r in sql.tables[t("findings")]}["dup"] == "resolved"
    rows = [r for r in sql.tables[t("findings")] if r["check_id"] == "unused"]
    assert len(rows) == 1 and rows[0]["status"] == "open" and rows[0]["fingerprint"] == f["fingerprint"]
    assert rows[0]["first_seen"] == f["first_seen"]
    assert sql.tables[t("proposed_actions")][0]["state"] == "proposed"


def test_superseded_finding_keeps_its_suppression():
    lower = res("unused", "db:public.i", sev="info")
    [f] = apply_run({}, [out("unused", lower)], SPECS, NOW)
    f.status, f.suppressed_until = "suppressed", NOW + timedelta(days=3)
    higher = res("dup", "db:public.i")
    [s] = [x for x in apply_run({f.fingerprint: f}, [out("dup", higher)], SPECS, NOW + timedelta(hours=1),
                                superseded={f.fingerprint: higher.fingerprint}) if x.fingerprint == f.fingerprint]
    assert s.status == "superseded" and s.suppressed_until == NOW + timedelta(days=3)
    [back] = apply_run({f.fingerprint: s}, [out("unused", res("unused", "db:public.i", sev="info")), out("dup")],
                       SPECS, NOW + timedelta(hours=2))
    assert back.status == "suppressed"


def test_superseded_finding_resolves_when_its_own_condition_clears():
    lower = res("unused", "db:public.i", sev="info")
    [f] = apply_run({}, [out("unused", lower)], SPECS, NOW)
    f.status = "superseded"
    for h in (1, 2):
        changed = apply_run({f.fingerprint: f}, [out("unused"), out("dup")], SPECS, NOW + timedelta(hours=h))
    assert [c.status for c in changed] == ["resolved"]


def test_a_less_severe_finding_never_hides_a_more_severe_one():
    lower = res("unused", "db:public.i", sev="critical")
    outcomes = [out("unused", lower), out("dup", res("dup", "db:public.i", sev="warning"))]
    assert apply_precedence(outcomes, {}, SPECS) == {}
    assert len(outcomes[0].results) == 1
    same = res("unused", "db:public.j", sev="warning")
    outcomes = [out("unused", same), out("dup", res("dup", "db:public.j", sev="warning"))]
    assert apply_precedence(outcomes, {}, SPECS) == {same.fingerprint: fingerprint("dup", "p", "e", "db:public.j")}


def test_fixer_and_postcheck_treat_superseded_as_not_actionable_and_still_present():
    from lakebase_health.fixer import postcheck, queue
    assert queue.FINDING_ACTIONABLE == ("new", "open")
    status, _, reasons = postcheck.decide("superseded", 100, 50, 0, 0, 500)
    assert status == "regressed" and "finding still open" in reasons


PRECEDENCE = {
    "buffer_hit_rate": ("endpoint", {"lfc_hit_rate"}),
    "working_set_vs_cache": ("endpoint", {"lfc_hit_rate"}),
    "cu_sizing": ("endpoint", {"cpu_saturation", "lfc_hit_rate", "working_set_vs_cache"}),
    "unused_index": ("object", {"duplicate_index", "redundant_index"}),
    "seq_scan_read_ratio": ("database", {"seq_scan_heavy"}),
    "xid_runway": ("database", {"xid_age_table"}),
    "lock_blocking_history": ("endpoint", {"blocked_session"}),
}


def test_packaged_precedence_pairs():
    specs = packaged()
    got = {i: (s.superseded_by.key, set(s.superseded_by.checks)) for i, s in specs.items() if s.superseded_by}
    assert got == PRECEDENCE
    for i in ("seq_scan_read_ratio", "xid_runway"):
        assert re.search(r"\bdatname,", specs[i].sql)


# --- cu_sizing calibration --------------------------------------------------------------------------------------

GIB = 1024 ** 3


def _cu_row(**kw):
    """One endpoint's aggregates as cu_sizing's `a` CTE produces them: an idle, right-sized 1 CU compute."""
    row = dict(project_id="p", branch_id="b", endpoint_id="e", mem_max=2 * GIB, mem_min=2 * GIB, hours=160.0,
               hours_at_max=160.0, hours_at_min=160.0, cpu_max=0.3, cpu_min=0.3, ws_max=0.5, ws_min=0.5,
               reads_max=5e6, hit_max=99.5, reads_min=5e6, hit_min=99.5)
    row.update(kw)
    return row


def _classify(**kw):
    sql = packaged()["cu_sizing"].sql
    final = sql[sql.index("-- classify"):]
    row = _cu_row(**kw)
    db = sqlite3.connect(":memory:")
    db.execute(f"CREATE TABLE a ({', '.join(row)})")
    db.execute(f"INSERT INTO a VALUES ({', '.join('?' for _ in row)})", list(row.values()))
    cur = db.execute(final)
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, cur.fetchone()))["measure"]


def test_cu_sizing_right_sized_compute_is_quiet():
    assert _classify() == 0


@pytest.mark.parametrize("kw", [dict(cpu_max=0.9), dict(ws_max=1.2), dict(reads_max=5e6, hit_max=80.0)])
def test_cu_sizing_undersized_needs_pressure_at_its_largest_size(kw):
    assert _classify(**kw) == 2


@pytest.mark.parametrize("kw", [
    dict(cpu_max=0.9, hours_at_max=0.5),          # a short burst at the top size is not sustained
    dict(reads_max=5000.0, hit_max=0.0),          # cold cache after scale-to-zero: too few reads to judge
    dict(reads_max=None, hit_max=None),           # no cache reads at all (idle compute)
    dict(cpu_min=0.95, cpu_max=0.3),              # busy only while scaled down: autoscaling did its job
])
def test_cu_sizing_ignores_bursts_cold_caches_and_pressure_below_the_max(kw):
    assert _classify(**kw) == 0


def test_cu_sizing_oversized_is_an_always_on_idle_compute_above_two_cu():
    idle = dict(mem_min=8 * GIB, mem_max=8 * GIB, cpu_min=0.03, cpu_max=0.03, ws_min=0.1, ws_max=0.1)
    assert _classify(**idle) == 1
    assert _classify(**{**idle, "reads_min": None, "hit_min": None}) == 1     # no reads: cache need is nil
    assert _classify(**{**idle, "mem_min": 2 * GIB, "mem_max": 2 * GIB}) == 0  # 1 CU: little to save
    assert _classify(**{**idle, "hours": 40.0, "hours_at_min": 40.0}) == 0    # scales to zero most of the week
    assert _classify(**{**idle, "hours_at_min": 100.0}) == 0                  # it does scale up at times
    assert _classify(**{**idle, "cpu_min": 0.25}) == 0
    assert _classify(**{**idle, "hit_min": 90.0}) == 0                        # cache-bound at min: not oversized


def test_cu_sizing_drops_counter_samples_that_span_a_suspend():
    sql = packaged()["cu_sizing"].sql
    assert "timestampdiff(SECOND, c.previous_ts, c.ts) BETWEEN 1 AND 300" in sql
