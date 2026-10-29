import re
from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.ingest import ingest_all
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
WM = datetime(2026, 10, 26, 23, tzinfo=timezone.utc)


def test_first_run_backfills_with_clustering():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 5}])])
    [r] = ingest_all(sql, CFG, "r1", NOW, sources=("compute_gauges",))
    [ctas] = sql.ran("CREATE TABLE")
    assert "cat.lakebase_health.raw_compute_gauges CLUSTER BY (project_id, ts)" in ctas
    assert "FROM system.lakebase.compute_gauges WHERE ts >= current_timestamp() - INTERVAL 7 DAYS" in ctas
    assert r.status == "ok"
    assert sql.tables["cat.lakebase_health.ingest_watermarks"][0]["max_ts"] == NOW


DESCRIBE = (r"^DESCRIBE TABLE cat\.lakebase_health\.raw_compute_gauges",
            [{"col_name": "project_id", "data_type": "string"}, {"col_name": "ts", "data_type": "timestamp"},
             {"col_name": "rescue", "data_type": "variant"}, {"col_name": "", "data_type": ""},
             {"col_name": "# Clustering Information", "data_type": ""}, {"col_name": "project_id", "data_type": "string"}])


def test_incremental_uses_overlap_and_anti_join_skipping_variant_columns():
    sql = FakeSql(existing_tables={"cat.lakebase_health.raw_compute_gauges"},
                  responses=[(r"FROM cat\.lakebase_health\.ingest_watermarks", [{"max_ts": WM}]), DESCRIBE,
                             (r"^INSERT INTO", [{"num_inserted_rows": 42}]),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 9}])])
    [r] = ingest_all(sql, CFG, "r1", NOW, sources=("compute_gauges",))
    [(stmt, args)] = [(s, a) for s, a in sql.statements if s.startswith("INSERT INTO")]
    assert "FROM (SELECT * FROM system.lakebase.compute_gauges WHERE ts > :lo) s LEFT ANTI JOIN" in stmt
    assert "(SELECT project_id, ts FROM cat.lakebase_health.raw_compute_gauges WHERE ts > :lo) t" in stmt
    assert "ON s.project_id <=> t.project_id AND s.ts <=> t.ts" in stmt
    assert "EXCEPT" not in stmt and "rescue" not in stmt and "Clustering" not in stmt
    assert args["lo"] == WM - timedelta(minutes=10)
    assert r.rows_added == 42
    assert not sql.ran("CREATE TABLE")


def test_retention_delete_runs():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    ingest_all(sql, CFG, "r1", NOW, sources=("compute_gauges",))
    assert sql.ran(r"DELETE FROM cat\.lakebase_health\.raw_compute_gauges WHERE ts < current_timestamp\(\) - INTERVAL 90 DAYS")


def test_one_table_failing_does_not_stop_others_or_advance_watermark():
    sql = FakeSql(responses=[(r"system\.lakebase\.plan_history", RuntimeError("boom")),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    results = ingest_all(sql, CFG, "r1", NOW, sources=("plan_history", "compute_gauges"))
    assert [r.status for r in results] == ["error", "ok"]
    assert "boom" in results[0].error
    wms = sql.tables["cat.lakebase_health.ingest_watermarks"]
    assert [w["source_table"] for w in wms] == ["compute_gauges"]


def test_tables_are_processed_sequentially_in_given_order():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    ingest_all(sql, CFG, "r1", NOW, sources=("b_table", "a_table"))
    ctas = sql.ran("CREATE TABLE")
    assert "raw_b_table" in ctas[0] and "raw_a_table" in ctas[1]


# Final review I6: with masking on, a raw table holding query text is created empty, masked, then filled; masks are
# set again before every insert, and a SET MASK failure fails that table's ingest (nothing inserted).
PH = "cat.lakebase_health.raw_plan_history"


def _order(sql):
    return [s.split()[0] + (" MASK" if "SET MASK" in s else "") for s, _ in sql.statements
            if re.match(r"^(CREATE TABLE|ALTER TABLE|INSERT INTO)", s)]


def test_a_new_masked_raw_table_is_created_empty_and_masked_before_the_first_insert():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 5}])])
    [r] = ingest_all(sql, EngineConfig(catalog="cat", pii_masking=True), "r1", NOW, sources=("plan_history",))
    assert r.status == "ok"
    assert _order(sql) == ["CREATE", "ALTER MASK", "ALTER MASK", "ALTER MASK", "INSERT"]
    [ctas] = sql.ran("CREATE TABLE")
    assert "WHERE false" in ctas and "INTERVAL" not in ctas
    [ins] = sql.ran("^INSERT INTO")
    assert f"INSERT INTO {PH} SELECT * FROM system.lakebase.plan_history WHERE ts >= current_timestamp() - INTERVAL 7 DAYS" in ins
    assert {c for c in ("query_text", "plan_json", "parameters") if sql.ran(rf"ALTER COLUMN {c} SET MASK")} == \
        {"query_text", "plan_json", "parameters"}


def test_masks_are_set_before_every_incremental_insert():
    sql = FakeSql(existing_tables={PH},
                  responses=[(r"FROM cat\.lakebase_health\.ingest_watermarks", [{"max_ts": WM}]),
                             (r"^DESCRIBE TABLE", [{"col_name": "project_id", "data_type": "string"}]),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 9}])])
    ingest_all(sql, EngineConfig(catalog="cat", pii_masking=True), "r1", NOW, sources=("plan_history",))
    assert _order(sql) == ["ALTER MASK", "ALTER MASK", "ALTER MASK", "INSERT"]


def test_a_set_mask_failure_fails_the_table_closed():
    sql = FakeSql(responses=[(r"SET MASK", RuntimeError("PERMISSION_DENIED: no MANAGE")),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 5}])])
    results = ingest_all(sql, EngineConfig(catalog="cat", pii_masking=True), "r1", NOW,
                         sources=("plan_history", "compute_gauges"))
    assert [r.status for r in results] == ["error", "ok"] and "masks" in results[0].error
    assert not sql.ran(r"^INSERT INTO cat\.lakebase_health\.raw_plan_history")
    assert [w["source_table"] for w in sql.tables["cat.lakebase_health.ingest_watermarks"]] == ["compute_gauges"]


def test_with_masking_off_raw_tables_are_created_with_their_data_as_before():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 5}])])
    ingest_all(sql, EngineConfig(catalog="cat", pii_masking=False), "r1", NOW, sources=("plan_history",))
    assert _order(sql) == ["CREATE"] and "INTERVAL 7 DAYS" in sql.ran("CREATE TABLE")[0]


def test_an_empty_masked_table_left_by_a_failed_run_is_backfilled():
    sql = FakeSql(existing_tables={PH}, responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": None, "n": 0}])])
    [r] = ingest_all(sql, EngineConfig(catalog="cat", pii_masking=True), "r1", NOW, sources=("plan_history",))
    assert r.status == "ok" and _order(sql) == ["ALTER MASK", "ALTER MASK", "ALTER MASK", "INSERT"]
    assert "INTERVAL 7 DAYS" in sql.ran("^INSERT INTO")[0]
