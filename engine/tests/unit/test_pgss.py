"""Statement text from pg_stat_statements on the branch itself (spec §10), not the masked system tables."""
from types import SimpleNamespace

from lakebase_health.fixer import pgss

Q = "SELECT o.id FROM hc_test.orders o WHERE o.status IN ($1,$2) ORDER BY o.id;"


class Conn:
    def __init__(self, answers=None, error=None):
        self.answers, self.error, self.ran = answers or {}, error, []

    def execute(self, stmt, args=None):
        self.ran.append((stmt, args))
        if self.error and stmt.startswith("EXPLAIN"):
            raise self.error
        rows = next((v for k, v in self.answers.items() if k in stmt), [])
        return SimpleNamespace(fetchone=lambda: rows[0] if rows else None, fetchall=lambda: rows)


def test_entry_by_queryid_and_dbid_gives_the_spaced_text_its_database_and_mean_rows():
    conn = Conn({"FROM pg_stat_statements s": [(Q, "databricks_postgres", 16407, 1047, 942989)]})
    e = pgss.entry(conn, "-9132220112240109409", 16407)
    assert e == {"query": "SELECT o.id FROM hc_test.orders o WHERE o.status IN ($1 ,$2 ) ORDER BY o.id",
                 "database": "databricks_postgres", "dbid": 16407, "calls": 1047, "mean_rows": 942989 / 1047}
    stmt, args = conn.ran[0]
    assert "s.queryid = %s" in stmt and "s.dbid = %s" in stmt and "s.toplevel" in stmt
    assert args == (-9132220112240109409, 16407)


def test_entry_without_a_dbid_takes_the_busiest_database_and_a_missing_entry_is_none():
    conn = Conn({"FROM pg_stat_statements s": [(Q, "app", 5, 0, 0)]})
    e = pgss.entry(conn, "42", None)
    assert e["database"] == "app" and e["mean_rows"] is None
    assert "s.dbid = %s" not in conn.ran[0][0] and conn.ran[0][1] == (42,) and "ORDER BY s.total_exec_time DESC" in conn.ran[0][0]
    assert pgss.entry(Conn(), "42", None) is None


def test_statements_of_a_database_skip_the_fixers_own_and_rank_by_total_time():
    conn = Conn({"FROM pg_stat_statements s": [("1", Q, 10, 30, 900.0), ("2", "SELECT 1", 0, 0, 1.0)]})
    out = pgss.statements(conn, "databricks_postgres")
    assert out == [{"queryid": "1", "query": "SELECT o.id FROM hc_test.orders o WHERE o.status IN ($1 ,$2 ) ORDER BY o.id",
                    "calls": 10, "mean_rows": 3.0, "total_ms": 900.0},
                   {"queryid": "2", "query": "SELECT 1", "calls": 0, "mean_rows": None, "total_ms": 1.0}]
    stmt, args = conn.ran[0]
    assert "datname = %s" in stmt and "current_user" in stmt and "ORDER BY s.total_exec_time DESC" in stmt
    assert args == ("databricks_postgres",)


def test_available_reads_the_view_through_the_search_path():
    assert pgss.available(Conn({"to_regclass('pg_stat_statements')": [(True,)]}))
    assert not pgss.available(Conn({"to_regclass('pg_stat_statements')": [(False,)]}))


def test_generic_plan_explains_the_normalized_text_without_running_it():
    doc = [{"Plan": {"Node Type": "Index Scan", "Index Cond": "((id >= $1) AND (id <= $2))"}}]
    conn = Conn({"EXPLAIN": [(doc,)]})
    assert pgss.generic_plan(conn, "SELECT 1 FROM t WHERE id BETWEEN $1 AND $2") == doc[0]
    assert conn.ran[0][0] == "EXPLAIN (FORMAT JSON, GENERIC_PLAN) SELECT 1 FROM t WHERE id BETWEEN $1 AND $2"
    assert pgss.generic_plan(Conn(error=RuntimeError("could not determine data type of parameter $1")), "SELECT $1") \
        is None
