# Final review I1: the engine and analysis jobs share a run lock (compare-and-set on runs): a run that finds
# another run still 'running' and younger than the run timeout exits as skipped. The engine's writes to
# proposed_actions are compare-and-set on the state it read, so a fixer claim in between always wins.
from datetime import timedelta
from pathlib import Path

import yaml

from lakebase_health.config import EngineConfig
from lakebase_health.jobs import run as runmod
from lakebase_health.models import Action
from lakebase_health.record import merge_actions
from lakebase_health.sql import SparkSql
from tests.fakes import FakeSql
from tests.unit.test_run import NOW, fake_w, write_check

CFG = EngineConfig(catalog="cat")
REPO = Path(__file__).resolve().parents[3]


def _sql(running):
    return FakeSql(responses=[(r"FROM cat\.lakebase_health\.runs\s+WHERE status = 'running'", running),
                              (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])


def test_a_run_while_another_is_running_exits_skipped(tmp_path):
    write_check(tmp_path)
    sql = _sql([{"run_id": "other", "started_at": NOW - timedelta(minutes=10)}])
    run = runmod.run_engine(sql, fake_w(), CFG, connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
    assert run["status"] == "skipped"
    assert not sql.ran("system.lakebase") and "cat.lakebase_health.findings" not in sql.tables
    [row] = sql.tables["cat.lakebase_health.runs"]
    assert row["status"] == "skipped" and row["ended_at"] == NOW and "other" in row["errors"]


def test_the_lock_query_ignores_this_run_and_runs_older_than_the_timeout(tmp_path):
    write_check(tmp_path)
    sql = _sql([])
    run = runmod.run_engine(sql, fake_w(), CFG, connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
    assert run["status"] == "ok"
    [(stmt, args)] = [(s, a) for s, a in sql.statements if "status = 'running'" in s]
    assert args["me"] == run["run_id"] and args["since"] == NOW - runmod.RUN_TIMEOUT
    assert "run_id <> :me" in stmt and "started_at >= :since" in stmt


def test_analysis_run_takes_the_same_lock(tmp_path):
    write_check(tmp_path)
    sql = _sql([{"run_id": "engine", "started_at": NOW - timedelta(minutes=20)}])
    run = runmod.run_engine(sql, fake_w(), CFG, connect=lambda **k: None, now=NOW, checks_dir=tmp_path,
                            skip_ingest=True)
    assert run["status"] == "skipped" and "cat.lakebase_health.findings" not in sql.tables


def test_both_jobs_time_out_at_the_lock_timeout():
    for f, key in (("resources/engine_job.yml", "lakebase_health_engine"),
                   ("resources/analysis_job.yml", "lakebase_health_analysis")):
        job = yaml.safe_load((REPO / f).read_text())["resources"]["jobs"][key]
        assert job["timeout_seconds"] == int(runmod.RUN_TIMEOUT.total_seconds()), f


def _action(state):
    return Action("a1", "fp", "c", "ddl", "CREATE INDEX CONCURRENTLY ON s.t (c);", None, "low", True, True,
                  state, NOW, NOW, NOW + timedelta(days=7))


def test_engine_action_write_never_overwrites_a_fixer_state_change():
    sql = FakeSql()
    sql.tables["cat.lakebase_health.proposed_actions"] = [_action("verifying").to_row()]  # the fixer claimed it
    merge_actions(sql, CFG, [_action("expired")])  # planned from the 'proposed' row it read earlier
    assert sql.tables["cat.lakebase_health.proposed_actions"][0]["state"] == "verifying"


def test_engine_action_write_applies_when_the_state_is_unchanged_and_inserts_new_actions():
    sql = FakeSql()
    sql.tables["cat.lakebase_health.proposed_actions"] = [_action("proposed").to_row()]
    new = Action("a2", "fp2", "c", "ddl", "x", None, "low", True, True, "proposed", NOW, NOW, NOW)
    merge_actions(sql, CFG, [_action("expired"), new])
    states = {r["action_id"]: r["state"] for r in sql.tables["cat.lakebase_health.proposed_actions"]}
    assert states == {"a1": "expired", "a2": "proposed"}


class FakeSpark:
    def __init__(self):
        self.stmts = []

    def sql(self, stmt, args=None):
        self.stmts.append(stmt)
        return type("R", (), {"collect": lambda self: []})()


def test_an_expired_action_can_be_proposed_again_but_a_decided_one_is_left_alone():
    sql = FakeSql()
    sql.tables["cat.lakebase_health.proposed_actions"] = [_action("expired").to_row()]
    merge_actions(sql, CFG, [_action("proposed")])
    assert sql.tables["cat.lakebase_health.proposed_actions"][0]["state"] == "proposed"
    sql.tables["cat.lakebase_health.proposed_actions"] = [_action("applying").to_row()]
    merge_actions(sql, CFG, [_action("proposed")])
    assert sql.tables["cat.lakebase_health.proposed_actions"][0]["state"] == "applying"


def test_spark_merge_puts_the_guard_on_the_matched_clause():
    spark = FakeSpark()
    s = SparkSql(spark)
    s._df = lambda table, rows: type("D", (), {"createOrReplaceTempView": lambda self, v: None})()
    guard = merge_actions.__globals__["ACTION_CAS"]
    s.merge_rows("t", [{"action_id": "a"}], ["action_id"], only_if=guard)
    assert f"WHEN MATCHED AND ({guard.sql}) THEN UPDATE SET *" in spark.stmts[0]
