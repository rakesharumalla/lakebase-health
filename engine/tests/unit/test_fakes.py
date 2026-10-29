from lakebase_health.config import EngineConfig, RAW_TABLES
from tests.fakes import FakeSql


def test_config_table_names():
    cfg = EngineConfig(catalog="c")
    assert cfg.table("findings") == "c.lakebase_health.findings"
    assert cfg.raw("compute_gauges") == "c.lakebase_health.raw_compute_gauges"
    assert len(RAW_TABLES) == 10


def test_fake_sql_records_and_answers():
    sql = FakeSql(responses=[(r"SELECT max\(ts\)", [{"m": 1}])])
    assert sql.query("SELECT max(ts) AS m FROM t") == [{"m": 1}]
    sql.execute("DELETE FROM t", {"a": 1})
    assert sql.statements[-1] == ("DELETE FROM t", {"a": 1})


def test_fake_sql_merge_and_select_star():
    sql = FakeSql()
    sql.merge_rows("c.s.t", [{"k": 1, "v": "a"}], ["k"])
    sql.merge_rows("c.s.t", [{"k": 1, "v": "b"}, {"k": 2, "v": "c"}], ["k"])
    assert sorted(r["v"] for r in sql.query("SELECT * FROM c.s.t")) == ["b", "c"]
    assert sql.table_exists("c.s.t")
