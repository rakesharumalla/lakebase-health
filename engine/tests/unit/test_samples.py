"""The fix loop's affected queries (spec §10): text from pg_stat_statements on the source branch, in the finding's
database, busiest first; parameter values come from the data on the verification branch (fixer/params.py)."""
import json
from types import SimpleNamespace

from lakebase_health.fixer.samples import affected_queries


class _Conn:
    """A read-only stats session on the source branch: pg_stat_statements rows (queryid, query, calls, rows, ms)."""

    def __init__(self, rows, installed=True):
        self.rows, self.installed, self.ran = rows, installed, []

    def execute(self, stmt, args=None):
        self.ran.append((stmt, args))
        rows = [(self.installed,)] if stmt.startswith("SELECT to_regclass") else self.rows
        return SimpleNamespace(fetchone=lambda: rows[0] if rows else None, fetchall=lambda: rows)


def test_affected_queries_are_the_databases_statements_on_the_table_busiest_first():
    rows = [("1", "SELECT * FROM t WHERE id = $1;", 10, 10, 900.0), ("2", " update t set a = $1 ", 4, 4, 50.0),
            ("3", "SELECT * FROM other WHERE id = $1", 9, 9, 40.0)]
    conn = _Conn(rows)
    out = affected_queries(conn, "public.t", "app")
    assert out == [{"queryid": "1", "query": "SELECT * FROM t WHERE id = $1", "mean_rows": 1.0, "is_write": False},
                   {"queryid": "2", "query": "update t set a = $1", "mean_rows": 1.0, "is_write": True}]
    stmt, args = conn.ran[-1]
    assert "FROM pg_stat_statements s" in stmt and args == ("app",)  # the finding's database, not the whole project


def test_affected_queries_keep_the_limit_and_need_pg_stat_statements():
    rows = [(str(i), f"SELECT {i} FROM t", 1, 1, float(100 - i)) for i in range(20)]
    assert [q["queryid"] for q in affected_queries(_Conn(rows), "t", "app", limit=3)] == ["0", "1", "2"]
    assert affected_queries(_Conn(rows, installed=False), "t", "app") == []


# --- live criterion 4 (redundant_index on hc_test.customers): the fixer searched query text for the index name ---
import re

from lakebase_health.fixer.allowlist import parse
from lakebase_health.fixer.samples import _table_regex, no_samples_reason, sample_table


def _rx(table):
    return re.compile(_table_regex(table).replace("(?i)", ""), re.I)


def test_table_regex_respects_the_schema():
    rx = _rx("hc_test.customers")
    for text in ("SELECT * FROM customers WHERE id = $1", "select 1 from hc_test.customers c",
                 'SELECT 1 FROM "hc_test"."customers"', "UPDATE Customers SET a = 1", "join customers\non x"):
        assert rx.search(text), text
    for text in ("SELECT 1 FROM other.customers", "DROP INDEX hc_test.customers_region_idx", "FROM my_customers",
                 'FROM "other"."customers"', "FROM customers$1"):
        assert not rx.search(text), text


def test_sample_table_for_a_drop_is_the_indexed_table():
    drop = parse("DROP INDEX CONCURRENTLY hc_test.customers_region_idx;")
    rb = "CREATE INDEX CONCURRENTLY customers_region_idx ON hc_test.customers USING btree (region);"
    assert sample_table(drop, {"rollback_sql": rb}) == "hc_test.customers"
    legacy = "CREATE INDEX customers_region_idx ON hc_test.customers USING btree (region);"  # stored before the fix
    assert sample_table(drop, {"rollback_sql": legacy}) == "hc_test.customers"
    ev = json.dumps({"index_def": 'CREATE INDEX i ON "hc_test"."Cust" USING btree (a)'})
    assert sample_table(drop, {"rollback_sql": None, "evidence": ev}) == '"hc_test"."Cust"'
    assert sample_table(drop, {"rollback_sql": None, "evidence": None}) is None
    create = parse("CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id)")
    assert sample_table(create, {}) == "hc_test.orders"


def test_no_samples_reason_gives_counts_and_parameter_reasons_never_values():
    why = no_samples_reason(0, [], "hc_test.customers", "app")
    assert why == "0 statements in pg_stat_statements on database app mention hc_test.customers"
    why = no_samples_reason(2, ["query 7: parameter $2 is not bound to a column", "query 9: no rows with values in t"],
                            "hc_test.customers", "app")
    assert why.startswith("2 statements") and "2 could not be sampled: query 7: parameter $2" in why
    assert "could not tell" in no_samples_reason(0, [], None, "app")


def test_only_single_select_or_dml_statements_are_sampled():
    """Live criterion 4: a session's 'SET ...; SET ...; EXPLAIN ... customers' text failed PREPARE with SyntaxError."""
    rows = [(qid, text, 1, 1, 1.0) for qid, text in (
        ("1", "SET enable_seqscan = off; SET enable_bitmapscan = off; EXPLAIN (COSTS OFF) "
              "SELECT count(*) FROM hc_test.customers WHERE region = $1"),
        ("2", "EXPLAIN SELECT 1 FROM customers"),
        ("3", "CREATE INDEX i ON hc_test.customers (a)"),
        ("4", "  with c AS (SELECT 1 FROM customers) SELECT * FROM c;"),
        ("5", "DELETE FROM customers WHERE id = $1"))]
    out = affected_queries(_Conn(rows), "hc_test.customers", "app")
    assert [q["queryid"] for q in out] == ["4", "5"]
