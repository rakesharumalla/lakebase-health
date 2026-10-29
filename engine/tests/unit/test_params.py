"""Parameter values drawn from the data on the verification branch (spec §10): each $n is mapped to a column."""
import re
from types import SimpleNamespace

import pytest

from lakebase_health.fixer import params as P

PLANTED = ("SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
           "AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 WHERE o2.id BETWEEN $1 AND $2 "
           "AND o2.status = o.status) ORDER BY o.id")
COLUMNS = {"hc_test.orders": {"id", "status", "amount", "customer_id", "note"},
           "hc_test.customers": {"id", "email", "region"}}


def cols(ref):
    return COLUMNS.get(ref)


def roles(query):
    return [(b.param, b.role, b.table, b.column) for b in P.bindings(query, cols)]


def test_the_planted_range_maps_both_parameters_to_the_id_column():
    assert roles(PLANTED) == [(1, "low", "hc_test.orders", "id"), (2, "high", "hc_test.orders", "id")]


def test_comparisons_in_lists_like_and_limits_are_mapped():
    q = ("SELECT c.email FROM hc_test.orders o JOIN hc_test.customers c ON c.id = o.customer_id "
         "WHERE o.status IN ($1, $2) AND $3 = c.region AND email LIKE $4 AND o.amount >= $5::numeric "
         "AND $6 > o.id ORDER BY o.id LIMIT $7 OFFSET $8")
    assert roles(q) == [(1, "eq", "hc_test.orders", "status"), (2, "eq", "hc_test.orders", "status"),
                        (3, "eq", "hc_test.customers", "region"), (4, "like", "hc_test.customers", "email"),
                        (5, "low", "hc_test.orders", "amount"), (6, "high", "hc_test.orders", "id"),
                        (7, "limit", None, None), (8, "offset", None, None)]


def test_adjacent_placeholders_from_pg_stat_statements_are_read():
    assert [b.param for b in P.bindings("SELECT id FROM hc_test.orders WHERE status IN ($1,$2)", cols)] == [1, 2]


@pytest.mark.parametrize("query, k", [
    ("SELECT $1::int + amount FROM hc_test.orders", 1),
    ("SELECT id FROM hc_test.orders WHERE amount > $1 * 2", 1),
    ("SELECT id FROM hc_test.orders WHERE id = $1 AND date_trunc($2, now()) IS NOT NULL", 2),
    ("SELECT id FROM hc_test.orders WHERE missing_col = $1", 1),
    ("WITH c AS (SELECT id FROM hc_test.orders) SELECT id FROM c WHERE c.id = $1", 1),
    ("SELECT o.id FROM hc_test.orders o JOIN hc_test.customers c ON c.id = o.customer_id WHERE id = $1", 1),
])
def test_a_parameter_not_bound_to_a_column_is_unsampled_with_its_number(query, k):
    with pytest.raises(P.Unsampled, match=re.escape(f"parameter ${k} is not bound to a column")):
        P.bindings(query, cols)


class Conn:
    """Answers the sampler's catalog and draw queries; records every statement."""

    def __init__(self, rows=None, sample_rows=None, reltuples=900000.0):
        self.rows = rows if rows is not None else {}
        self.sample_rows = sample_rows
        self.reltuples, self.ran = reltuples, []

    def execute(self, stmt, args=None):
        self.ran.append((stmt, args))
        out = []
        if stmt.startswith("SELECT a.attname"):
            out = [(c,) for c in sorted(COLUMNS.get(args[0].replace('"', ""), ()))]
        elif stmt.startswith("SELECT c.reltuples"):
            out = [(self.reltuples,)]
        elif "TABLESAMPLE" in stmt:
            table = re.search(r'FROM ("[^"]+"(?:\."[^"]+")?) TABLESAMPLE', stmt).group(1)
            out = self.sample_rows.get(table, []) if self.sample_rows is not None else self.rows.get(table, [])
        elif " FROM (SELECT " in stmt:
            out = self.rows.get(re.search(r'FROM ("[^"]+"(?:\."[^"]+")?) WHERE', stmt).group(1), [])
        return SimpleNamespace(fetchall=lambda: out, fetchone=lambda: out[0] if out else None)


def test_the_range_is_drawn_from_existing_values_and_spans_the_statements_mean_rows():
    conn = Conn({'"hc_test"."orders"': [("1600004111", "1600005011"), ("1600200000", "1600200900"),
                                         ("1600700000", "1600700900")]})
    sets = P.sample(conn, PLANTED, mean_rows=900.6)
    assert sets == [["1600004111", "1600005011"], ["1600200000", "1600200900"], ["1600700000", "1600700900"]]
    [draw] = [s for s, _ in conn.ran if "TABLESAMPLE" in s]
    assert 'ORDER BY x."id" LIMIT 901)' in draw and "IS NOT NULL" in draw and "random()" in draw
    assert "SYSTEM (0.1111" in draw  # about 1,000 of 900,000 rows
    stmts = [s for s, _ in conn.ran]
    assert stmts[0] == "BEGIN READ ONLY" and stmts[1].startswith("SET LOCAL statement_timeout") and stmts[-1] == "ROLLBACK"


def test_equalities_share_one_row_per_table_and_like_takes_an_escaped_prefix():
    q = ("SELECT 1 FROM hc_test.customers c WHERE c.region = $1 AND c.email LIKE $2 LIMIT $3")
    conn = Conn({'"hc_test"."customers"': [("emea", "5%_off@x.com"), ("apac", "bo@y.com")]})
    assert P.sample(conn, q) == [["emea", "5\\%\\_%", "10"], ["apac", "bo@%", "25"]]


def test_an_empty_sample_falls_back_to_the_whole_table_and_no_rows_is_unsampled():
    q = "SELECT 1 FROM hc_test.customers WHERE region = $1"
    conn = Conn({'"hc_test"."customers"': [("emea",)]}, sample_rows={})
    assert P.sample(conn, q) == [["emea"]]
    with pytest.raises(P.Unsampled, match="no rows with values in hc_test.customers"):
        P.sample(Conn({}), q)


def test_a_query_without_parameters_needs_no_data():
    conn = Conn()
    assert P.sample(conn, "SELECT count(*) FROM hc_test.orders") == [[]] and conn.ran == []
    assert P.sample(conn, "SELECT id FROM hc_test.orders LIMIT $1") == [["10"], ["25"], ["50"]]


def test_duplicate_value_sets_are_dropped():
    conn = Conn({'"hc_test"."customers"': [("emea",), ("emea",), ("apac",)]})
    assert P.sample(conn, "SELECT 1 FROM hc_test.customers WHERE region = $1") == [["emea"], ["apac"]]


def test_check_maps_with_the_live_catalog_without_reading_rows():
    conn = Conn()
    assert [b.column for b in P.check(conn, PLANTED)] == ["id", "id"]
    assert not any("TABLESAMPLE" in s for s, _ in conn.ran)


def test_aliases_are_resolved_per_scope():
    """Final review I-3: the alias map was global, first alias wins, so `o` reused in a subquery for customers bound
    the subquery's column to orders (or the outer one to customers)."""
    q = ("SELECT o.id FROM hc_test.orders o WHERE o.status = $1 AND EXISTS "
         "(SELECT 1 FROM hc_test.customers o WHERE o.region = $2)")
    assert roles(q) == [(1, "eq", "hc_test.orders", "status"), (2, "eq", "hc_test.customers", "region")]
    q = ("SELECT x.id FROM (SELECT o.id FROM hc_test.customers o WHERE o.region = $1) x "
         "JOIN hc_test.orders o ON o.customer_id = x.id WHERE o.status = $2")
    assert roles(q) == [(1, "eq", "hc_test.customers", "region"), (2, "eq", "hc_test.orders", "status")]
    # a correlated subquery sees the outer alias
    q = ("SELECT c.id FROM hc_test.customers c WHERE EXISTS "
         "(SELECT 1 FROM hc_test.orders o WHERE o.customer_id = c.id AND c.region = $1)")
    assert roles(q) == [(1, "eq", "hc_test.customers", "region")]


def test_a_non_recursive_cte_body_reads_the_real_table_of_its_name():
    q = "WITH orders AS (SELECT * FROM hc_test.orders WHERE status = $1) SELECT id FROM orders"
    assert roles(q) == [(1, "eq", "hc_test.orders", "status")]
    q = "WITH customers AS (SELECT * FROM customers WHERE region = $1) SELECT id FROM customers"
    assert [(b.param, b.table) for b in P.bindings(q, lambda ref: {"region", "id"})] == [(1, "customers")]
