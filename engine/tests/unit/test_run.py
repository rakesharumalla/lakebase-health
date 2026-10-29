from datetime import datetime, timezone
from types import SimpleNamespace as NS

from lakebase_health.config import EngineConfig
from lakebase_health.jobs.run import run_engine
from tests.fakes import FakeSql

NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)


def fake_w():
    pg = NS(list_projects=lambda: [])
    return NS(postgres=pg, current_user=NS(me=lambda: NS(user_name="svc")))


def write_check(tmp_path):
    (tmp_path / "exclusions.yaml").write_text(
        "slots: [s]\nusers: [u]\nroles: [r]\nrole_regex: '^(pg_)'\nschemas: [pg_catalog]\nsettings_regex: '^neon_'\n")
    (tmp_path / "x.yaml").write_text(
        "id: x\ntitle: X\ncategory: c\nsource: delta\nexplain: e\nthresholds: {warning: 0}\n"
        "sql: SELECT 'p' AS project_id, 'b' AS branch_id, 'e' AS endpoint_id, 'o' AS object, 1.0 AS measure "
        "FROM system.lakebase.compute_gauges\n")


def test_run_engine_end_to_end_with_fakes(tmp_path):
    write_check(tmp_path)
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}]),
                             (r"raw_compute_gauges$", [{"project_id": "p", "branch_id": "b", "endpoint_id": "e",
                                                        "object": "o", "measure": 1.0}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert run["status"] == "ok"
    assert sql.tables["cat.lakebase_health.findings"][0]["check_id"] == "x"
    [final] = sql.tables["cat.lakebase_health.runs"]
    assert final["status"] == "ok" and final["ended_at"] == NOW


def test_disabled_engine_does_nothing(tmp_path):
    write_check(tmp_path)
    sql = FakeSql()
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat", engine_enabled=False), now=NOW, checks_dir=tmp_path)
    assert run["status"] == "disabled"
    assert not sql.ran("system.lakebase") and "cat.lakebase_health.findings" not in sql.tables


def test_ingest_errors_are_reported_not_fatal(tmp_path):
    write_check(tmp_path)
    sql = FakeSql(responses=[(r"system\.lakebase\.", RuntimeError("no access")),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert run["status"] == "ok_with_errors" and "no access" in run["errors"]


def test_regressed_postcheck_becomes_critical_fix_regressed_finding(tmp_path, monkeypatch):
    write_check(tmp_path)
    reg = {"action_id": "a1", "fingerprint": "fp", "project_id": "p", "endpoint_id": "e", "outcome": "regressed",
           "confidence": "high", "reasons": ["finding still open"]}
    monkeypatch.setattr("lakebase_health.jobs.run.run_postcheck", lambda sql, cfg, now, errors=None, run_id=None: [reg])
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert run["status"] == "ok"
    [f] = [f for f in sql.tables["cat.lakebase_health.findings"] if f["check_id"] == "fix_regressed"]
    assert f["severity"] == "critical" and f["object"] == "a1" and "finding still open" in f["evidence"]


def test_postcheck_failure_does_not_fail_run(tmp_path, monkeypatch):
    write_check(tmp_path)

    def boom(sql, cfg, now, errors=None, run_id=None):
        raise RuntimeError("delta down")

    monkeypatch.setattr("lakebase_health.jobs.run.run_postcheck", boom)
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert run["status"] == "ok_with_errors" and "postcheck" in run["errors"] and "delta down" in run["errors"]


def test_skip_ingest_never_touches_system_lakebase_and_marks_stale_from_watermarks(tmp_path):
    write_check(tmp_path)
    from datetime import timedelta
    wms = [{"source_table": "compute_gauges", "max_ts": NOW - timedelta(minutes=121)},
           {"source_table": "plan_history", "max_ts": NOW - timedelta(minutes=30)}]
    sql = FakeSql(responses=[(r"FROM cat\.lakebase_health\.ingest_watermarks", wms),
                             (r"raw_compute_gauges$", [{"project_id": "p", "branch_id": "b", "endpoint_id": "e",
                                                        "object": "o", "measure": 1.0}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path, skip_ingest=True)
    assert not sql.ran("system.lakebase")
    assert run["status"] == "ok" and "ingest" not in run["step_seconds"]
    [f] = sql.tables["cat.lakebase_health.findings"]
    assert f["check_id"] == "x"
    assert {r["status"] for r in sql.tables["cat.lakebase_health.check_results"] if r["check_id"] == "x"} \
        == {"ok_stale_source"}


def test_skip_ingest_fresh_watermark_is_not_stale(tmp_path):
    write_check(tmp_path)
    from datetime import timedelta
    wms = [{"source_table": "compute_gauges", "max_ts": NOW - timedelta(minutes=119)}]
    sql = FakeSql(responses=[(r"FROM cat\.lakebase_health\.ingest_watermarks", wms),
                             (r"raw_compute_gauges$", [{"project_id": "p", "branch_id": "b", "endpoint_id": "e",
                                                        "object": "o", "measure": 1.0}])])
    run_engine(sql, fake_w(), EngineConfig(catalog="cat", ingest_interval_minutes=60), connect=lambda **k: None,
               now=NOW, checks_dir=tmp_path, skip_ingest=True)
    assert {r["status"] for r in sql.tables["cat.lakebase_health.check_results"] if r["check_id"] == "x"} == {"ok"}


def test_skip_ingest_missing_watermark_is_stale():
    from lakebase_health.jobs.run import stale_from_watermarks
    sql = FakeSql(responses=[(r"ingest_watermarks", [])])
    assert "compute_gauges" in stale_from_watermarks(sql, EngineConfig(catalog="cat"), NOW)


def test_analyze_entry_point_runs_without_ingest(monkeypatch):
    import lakebase_health.jobs.run as run_mod
    seen = {}
    monkeypatch.setattr(run_mod, "_main", lambda argv, skip_ingest: seen.update(argv=argv, skip=skip_ingest))
    run_mod.analyze(["--catalog", "c"])
    assert seen == {"argv": ["--catalog", "c"], "skip": True}
    run_mod.main(["--catalog", "c"])
    assert seen["skip"] is False


def test_skip_ingest_accepts_naive_utc_watermarks_from_spark():
    from datetime import timedelta
    from lakebase_health.jobs.run import stale_from_watermarks
    naive = NOW.replace(tzinfo=None)
    sql = FakeSql(responses=[(r"ingest_watermarks", [{"source_table": "compute_gauges", "max_ts": naive},
                                                     {"source_table": "plan_history",
                                                      "max_ts": naive - timedelta(hours=3)}])])
    stale = stale_from_watermarks(sql, EngineConfig(catalog="cat"), NOW)
    assert "compute_gauges" not in stale and "plan_history" in stale


def test_analysis_run_applies_the_mask_setting_too(tmp_path):
    """B3: a pii_masking change takes effect within one analysis run, not only on the next full engine run."""
    write_check(tmp_path)
    for masking, verb in ((False, "DROP MASK"), (True, "SET MASK")):
        sql = FakeSql(responses=[(r"FROM cat\.lakebase_health\.ingest_watermarks", [])],
                      existing_tables={"cat.lakebase_health.raw_plan_history"})
        run = run_engine(sql, fake_w(), EngineConfig(catalog="cat", pii_masking=masking), connect=lambda **k: None,
                         now=NOW, checks_dir=tmp_path, skip_ingest=True)
        assert sql.ran(rf"raw_plan_history ALTER COLUMN query_text {verb}"), verb
        assert not sql.ran("system.lakebase") and "masks" not in (run["errors"] or "")


def test_fix_regressed_finding_resolves_once_its_action_is_rolled_back(tmp_path, monkeypatch):
    """B4: the rollback undid the regression, so the critical finding closes."""
    write_check(tmp_path)
    monkeypatch.setattr("lakebase_health.jobs.run.run_postcheck", lambda sql, cfg, now, errors=None, run_id=None: [])
    T = "cat.lakebase_health."
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    base = {"check_id": "fix_regressed", "title": "t", "category": "engine", "project_id": "p", "branch_id": None,
            "endpoint_id": "e", "status": "open", "severity": "critical", "confidence": "high", "measure": 1.0,
            "first_seen": NOW, "last_seen": NOW, "last_checked_at": NOW, "occurrences": 1, "clean_runs": 0,
            "evidence": "{}", "query_text": None, "explain": "x", "candidate_sql": None, "rollback_sql": None}
    sql.tables[T + "findings"] = [{**base, "fingerprint": "fr1", "object": "a1"},
                                  {**base, "fingerprint": "fr2", "object": "a2"}]
    sql.tables[T + "proposed_actions"] = [{"action_id": "a1", "state": "rolled_back"},
                                          {"action_id": "a2", "state": "regressed"}]
    sql.responses.insert(0, (r"check_id = 'fix_regressed'.*'rolled_back'", lambda s, a: [
        dict(f) for f in sql.tables[T + "findings"] if f["object"] == "a1" and f["status"] == "open"]))
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    by_fp = {f["fingerprint"]: f for f in sql.tables[T + "findings"]}
    assert by_fp["fr1"]["status"] == "resolved" and by_fp["fr1"]["resolved_at"] == NOW
    assert by_fp["fr2"]["status"] == "open"
    assert run["status"] == "ok" and '"fixes_rolled_back": 1' in run["counts"]


def test_fix_regressed_resolves_from_the_rolled_back_ledger_event_even_if_the_action_row_moved_on():
    from lakebase_health.record import resolve_rolled_back
    sql = FakeSql(responses=[(r"event_type = 'rolled_back'", [
        {"fingerprint": "fr1", "check_id": "fix_regressed", "object": "a1", "status": "new"}])])
    assert resolve_rolled_back(sql, EngineConfig(catalog="cat"), NOW) == 1
    [stmt] = sql.ran(r"FROM cat\.lakebase_health\.findings f")
    assert "ledger" in stmt and "l.ts >= f.first_seen" in stmt and "proposed_actions" not in stmt
    [row] = sql.tables["cat.lakebase_health.findings"]
    assert row["status"] == "resolved" and row["resolved_at"] == NOW


def test_a_fix_regressed_finding_never_resolves_from_other_regressions():
    """Final review I2: each regression is its own FLEET outcome; it must not count as a clean run for the other
    open fix_regressed findings. Only the rolled_back event of the finding's own action resolves it."""
    from datetime import timedelta

    from lakebase_health.record import persist_regressions
    cfg = EngineConfig(catalog="cat")
    sql = FakeSql()
    for i, aid in enumerate(("a", "b", "c")):
        reg = {"action_id": aid, "project_id": "p", "endpoint_id": "e", "confidence": "high", "reasons": ["r"]}
        persist_regressions(sql, cfg, f"run{i}", NOW + timedelta(minutes=15 * i), [reg])
    status = {f["object"]: f["status"] for f in sql.tables["cat.lakebase_health.findings"]}
    assert status == {"a": "new", "b": "new", "c": "new"}
def test_rewrite_postcheck_runs_in_every_engine_run_and_its_failure_is_not_fatal(tmp_path, monkeypatch):
    write_check(tmp_path)
    monkeypatch.setattr("lakebase_health.jobs.run.run_rewrite_postcheck",
                        lambda sql, cfg, now, errors=None: [{"outcome": "adopted"}, {"outcome": "not_adopted"}])
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
    assert '"rewrites_adopted": 1' in run["counts"]

    def boom(sql, cfg, now, errors=None):
        raise RuntimeError("no drafts table")
    monkeypatch.setattr("lakebase_health.jobs.run.run_rewrite_postcheck", boom)
    run = run_engine(FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])]), fake_w(),
                     EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
    assert run["status"] == "ok_with_errors" and "rewrite postcheck: RuntimeError" in run["errors"]


def test_every_run_stores_query_texts_and_their_failure_is_not_fatal(tmp_path, monkeypatch):
    import json
    from lakebase_health import querytexts
    write_check(tmp_path)
    got = []
    monkeypatch.setattr(querytexts, "collect", lambda pg, inv, now: got.append(inv) or (
        [{"endpoint_id": "ep-a", "queryid": 1, "query": "SELECT $1"}], ["ep-a"], ["query_texts ep-b: RuntimeError"]))
    sql = FakeSql()
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path, skip_ingest=True)
    assert got == [[]] and sql.tables["cat.lakebase_health.query_texts"][0]["queryid"] == 1
    assert json.loads(run["counts"])["query_texts"] == 1 and "query_texts ep-b: RuntimeError" in run["errors"]
    monkeypatch.setattr(querytexts, "collect", lambda *a: (_ for _ in ()).throw(RuntimeError("boom")))
    run = run_engine(FakeSql(), fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path, skip_ingest=True)
    assert run["status"] == "ok_with_errors" and "query_texts: RuntimeError: boom" in run["errors"]
