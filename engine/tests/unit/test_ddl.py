import pytest

from lakebase_health.config import EngineConfig
from lakebase_health.ddl import MASKED_COLUMNS, ensure_masks, ensure_schema
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
TABLES = ("runs", "ingest_watermarks", "inventory", "check_results", "findings", "proposed_actions",
          "shadow_log", "ledger", "approvals", "action_runs", "metrics_daily", "drafts", "llm_calls", "llm_status", "query_texts")


def test_ensure_schema_creates_everything_idempotently():
    sql = FakeSql()
    ensure_schema(sql, CFG)
    assert sql.ran(r"CREATE SCHEMA IF NOT EXISTS cat\.lakebase_health")
    for t in TABLES:
        assert sql.ran(rf"CREATE TABLE IF NOT EXISTS cat\.lakebase_health\.{t} \("), t
    assert sql.ran(r"CREATE FUNCTION IF NOT EXISTS cat\.lakebase_health\.mask_text")
    assert all("IF NOT EXISTS" in s or s.startswith("SHOW COLUMNS") for s, _ in sql.statements)


def test_columns_added_later_are_added_to_existing_tables_once():
    old = [{"col_name": c} for c in ("fingerprint", "rollback_sql")]
    sql = FakeSql(responses=[(r"SHOW COLUMNS IN cat\.lakebase_health\.(findings|proposed_actions)$", old)])
    ensure_schema(sql, CFG)
    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.findings ADD COLUMNS \(rollback_note STRING\)$")
    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ADD COLUMNS \(rollback_note STRING\)$")
    current = [{"col_name": c} for c in ("fingerprint", "rollback_sql", "rollback_note", "origin", "draft_id")]
    sql = FakeSql(responses=[(r"SHOW COLUMNS IN", current)])
    ensure_schema(sql, CFG)
    assert not sql.ran(r"ADD COLUMNS")
    [ddl] = sql.ran(r"TABLE IF NOT EXISTS cat\.lakebase_health\.proposed_actions ")
    assert "rollback_note STRING" in ddl


def test_findings_has_clean_runs_and_query_text():
    sql = FakeSql()
    ensure_schema(sql, CFG)
    [ddl] = sql.ran(r"TABLE IF NOT EXISTS cat\.lakebase_health\.findings ")
    assert "clean_runs INT" in ddl and "query_text STRING" in ddl


def test_masks_only_on_existing_tables():
    sql = FakeSql(existing_tables={"cat.lakebase_health.raw_plan_history", "cat.lakebase_health.findings"})
    ensure_masks(sql, CFG)
    stmts = sql.ran("SET MASK")
    assert any("raw_plan_history ALTER COLUMN plan_json SET MASK cat.lakebase_health.mask_text" in s for s in stmts)
    assert any("findings ALTER COLUMN query_text" in s for s in stmts)
    assert not any("raw_postgres_logs" in s for s in stmts)
    assert set(MASKED_COLUMNS["raw_plan_history"]) == {"query_text", "plan_json", "parameters"}


def test_masks_dropped_when_pii_masking_is_off():
    from lakebase_health.config import EngineConfig
    cfg = EngineConfig(catalog="cat", pii_masking=False)
    sql = FakeSql(existing_tables={"cat.lakebase_health.raw_plan_history", "cat.lakebase_health.findings"})
    ensure_masks(sql, cfg)
    assert not sql.ran("SET MASK")
    drops = sql.ran("DROP MASK")
    assert "ALTER TABLE cat.lakebase_health.raw_plan_history ALTER COLUMN query_text DROP MASK" in drops
    assert any("findings ALTER COLUMN query_text DROP MASK" in s for s in drops)
    assert len(drops) == len(MASKED_COLUMNS["raw_plan_history"]) + len(MASKED_COLUMNS["findings"])


def test_dropping_a_mask_that_is_not_there_is_tolerated():
    from lakebase_health.config import EngineConfig
    cfg = EngineConfig(catalog="cat", pii_masking=False)
    sql = FakeSql(responses=[(r"raw_plan_history ALTER COLUMN query_text DROP MASK", RuntimeError("no mask on column"))],
                  existing_tables={"cat.lakebase_health.raw_plan_history"})
    ensure_masks(sql, cfg)  # does not raise
    assert len(sql.ran("DROP MASK")) == 3


def test_masking_is_on_by_default():
    from lakebase_health.config import EngineConfig
    assert EngineConfig(catalog="cat").pii_masking is True


@pytest.mark.parametrize("err", ["[PERMISSION_DENIED] User does not have MANAGE on table",
                                 "[INSUFFICIENT_PRIVILEGES] cannot alter table"])
def test_dropping_masks_fails_loudly_on_permission_errors(err):
    sql = FakeSql(responses=[(r"DROP MASK", RuntimeError(err))], existing_tables={"cat.lakebase_health.raw_plan_history"})
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED|INSUFFICIENT_PRIVILEGES"):
        ensure_masks(sql, EngineConfig(catalog="cat", pii_masking=False))


def test_rewrite_columns_and_tables_are_added_to_existing_installs():
    sql = FakeSql(responses=[(r"SHOW COLUMNS IN cat\.lakebase_health\.proposed_actions$", [{"col_name": "rollback_note"}]),
                             (r"SHOW COLUMNS IN cat\.lakebase_health\.approvals$", [{"col_name": "note"}])])
    ensure_schema(sql, CFG)
    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ADD COLUMNS \(origin STRING\)$")
    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ADD COLUMNS \(draft_id STRING\)$")
    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.approvals ADD COLUMNS \(fingerprint STRING\)$")
    [calls] = sql.ran(r"TABLE IF NOT EXISTS cat\.lakebase_health\.llm_calls ")
    assert "prompt_hash STRING" in calls and "response_hash STRING" in calls
    assert "prompt STRING" not in calls and "response STRING" not in calls and "content" not in calls


def test_rewrite_sql_is_masked_only_on_llm_rows():
    sql = FakeSql(existing_tables={"cat.lakebase_health.proposed_actions"})
    ensure_schema(sql, CFG)
    [fn] = sql.ran(r"FUNCTION IF NOT EXISTS cat\.lakebase_health\.mask_llm_text")
    assert "coalesce(origin, 'check') <> 'llm'" in fn
    ensure_masks(sql, CFG)
    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ALTER COLUMN sql SET MASK "
                   r"cat\.lakebase_health\.mask_llm_text USING COLUMNS \(origin\)$")
    off = FakeSql(existing_tables={"cat.lakebase_health.proposed_actions"})
    ensure_masks(off, EngineConfig(catalog="cat", pii_masking=False))
    assert off.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ALTER COLUMN sql DROP MASK$")
