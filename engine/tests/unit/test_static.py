import pytest

from lakebase_health.fixer.draft.static import StaticError, check, shape

ORIGINAL = ("SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
            "AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
            "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) ORDER BY o.id")
GOOD = ("WITH a AS (SELECT status, avg(amount) AS avg_amount FROM hc_test.orders WHERE id BETWEEN $1 AND $2 "
        "GROUP BY status) SELECT o.id, o.status, o.amount FROM hc_test.orders o JOIN a ON a.status = o.status "
        "WHERE o.id BETWEEN $1 AND $2 AND o.amount > a.avg_amount ORDER BY o.id")
WINDOW = ("SELECT id, status, amount FROM (SELECT o.id, o.status, o.amount, avg(o.amount) OVER "
          "(PARTITION BY o.status) AS a FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2) s "
          "WHERE amount > a ORDER BY id")


def test_shape_of_the_original():
    s = shape(ORIGINAL)
    assert s.tables == {("hc_test", "orders")} and s.functions == {"avg"} and s.columns == 3 and s.ordered
    assert shape("SELECT * FROM hc_test.orders WHERE id = $1").columns is None  # a star: count unknown


@pytest.mark.parametrize("rewrite", [GOOD, WINDOW,
                                     "SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 "
                                     "AND $2 AND EXISTS (SELECT 1 FROM hc_test.orders o2 WHERE o2.id = o.id) ORDER BY o.id"])
def test_equivalent_shapes_pass(rewrite):
    assert check(ORIGINAL, rewrite).ordered


@pytest.mark.parametrize("name,rewrite,failed", [
    ("extra statement", GOOD + "; SELECT 1", "one_statement"),
    ("new literal", GOOD.replace("ORDER BY o.id", "AND o.amount > 5 ORDER BY o.id"), "new_literal"),
    ("other table", GOOD.replace("JOIN a ON", "JOIN hc_test.customers c ON c.id = o.customer_id JOIN a ON"),
     "other_table"),
    ("data-modifying CTE", "WITH d AS (DELETE FROM hc_test.orders WHERE id = $1 RETURNING 1) "
                           "SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
                           "ORDER BY o.id", "data_modifying_cte"),
    ("hint comment", "/*+ SeqScan(o) */ " + GOOD, "comment"),
    ("line comment", GOOD + " -- fast", "comment"),
    ("pg_sleep", GOOD.replace("o.amount FROM hc_test.orders o JOIN", "o.amount + pg_sleep(1) FROM hc_test.orders o JOIN"),
     "unsafe_function"),
    ("volatile function", GOOD.replace("ORDER BY o.id", "ORDER BY random()"), "unsafe_function"),
    ("SET", "SET work_mem = $1 || $2", "select_only"),
    ("UPDATE", "UPDATE hc_test.orders SET amount = $1 WHERE id = $2", "select_only"),
    ("a parameter dropped", GOOD.replace("$2", "$1"), "parameters"),
    ("a parameter added", GOOD.replace("ORDER BY o.id", "AND o.id <> $3 ORDER BY o.id"), "parameters"),
    ("fewer columns", GOOD.replace("o.id, o.status, o.amount FROM hc_test.orders o JOIN",
                                   "o.id, o.status FROM hc_test.orders o JOIN"), "column_count"),
    ("FOR UPDATE", GOOD + " FOR UPDATE", "locking"),
    ("SELECT INTO", "SELECT o.id, o.status, o.amount INTO hc_test.copy FROM hc_test.orders o "
                    "WHERE o.id BETWEEN $1 AND $2 ORDER BY o.id", "select_into"),
    ("garbage", "SELEC FROM (", "parse"),
])
def test_unsafe_rewrites_fail_with_the_check_name(name, rewrite, failed):
    with pytest.raises(StaticError) as e:
        check(ORIGINAL, rewrite)
    assert e.value.check == failed, (name, e.value)


def test_functions_the_original_calls_are_allowed():
    original = "SELECT md5(note) FROM hc_test.orders WHERE id = $1"
    check(original, "SELECT md5(o.note) AS h FROM hc_test.orders o WHERE o.id = $1")
    with pytest.raises(StaticError, match="md5"):
        check("SELECT note FROM hc_test.orders WHERE id = $1", "SELECT md5(note) FROM hc_test.orders WHERE id = $1")
    with pytest.raises(StaticError, match="pg_sleep"):
        check("SELECT pg_sleep($1)", "SELECT pg_sleep($1)")  # denied even when the original calls it


@pytest.mark.parametrize("rewrite", [
    "SELECT o.id FROM hc_test.orders o WHERE o.ssn = '123-45-6789' AND o.x = $$unterminated",
    "SELECT o.id FROM hc_test.orders o WHERE o.ssn = '123-45-6789' AND (((",
])
def test_parse_and_tokenizer_errors_are_recorded_by_type_only(rewrite):
    """Final review I-2: the parser's and tokenizer's messages quote the SQL, literal values included."""
    with pytest.raises(StaticError) as e:
        check(ORIGINAL, rewrite)
    assert e.value.check == "parse" and "123-45-6789" not in str(e.value) and e.value.detail in (
        "TokenError", "ParseError", "SqlTextError")


SIMPLE = "SELECT o.id, o.total FROM hc_test.orders o WHERE o.customer_id = $1"


@pytest.mark.parametrize("rewrite", [
    # Final review I-5: inside a non-recursive CTE's own body, its name is the real table
    "WITH secrets AS (SELECT 1 AS id, 1 AS total FROM secrets) SELECT o.id, o.total FROM hc_test.orders o, secrets "
    "WHERE o.customer_id = $1",
    "SELECT o.id, o.total FROM hc_test.orders o WHERE o.customer_id = $1 AND NOT EXISTS "
    "(WITH secrets AS (SELECT * FROM secrets) SELECT 1 FROM secrets)",
    # a CTE is not visible to the CTEs before it
    "WITH a AS (SELECT * FROM secrets), secrets AS (SELECT 1 AS x) SELECT o.id, o.total FROM hc_test.orders o "
    "WHERE o.customer_id = $1 AND NOT EXISTS (SELECT 1 FROM a)",
])
def test_a_cte_cannot_hide_a_table_the_original_does_not_read(rewrite):
    with pytest.raises(StaticError) as e:
        check(SIMPLE, rewrite)
    assert e.value.check in ("other_table", "cte_shadows_table")


@pytest.mark.parametrize("name", ["orders", "Orders", "customers"])
def test_a_cte_may_not_be_named_after_a_table_of_the_original_or_the_catalog_context(name):
    rewrite = (f"WITH {name} AS (SELECT o.id, o.total FROM hc_test.orders o WHERE o.customer_id = $1) "
               f"SELECT id, total FROM {name}")
    with pytest.raises(StaticError) as e:
        check(SIMPLE, rewrite, visible=["hc_test.orders", "hc_test.customers"])
    assert e.value.check == "cte_shadows_table"


def test_a_recursive_cte_may_refer_to_itself():
    rewrite = ("WITH RECURSIVE r AS (SELECT o.id, o.total FROM hc_test.orders o WHERE o.customer_id = $1 "
               "UNION ALL SELECT r.id, r.total FROM r WHERE 1 = 0) SELECT id, total FROM r")
    assert check(SIMPLE, rewrite).tables == frozenset({("hc_test", "orders")})
