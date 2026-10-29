import json

from lakebase_health.fixer.samples import affected_queries, parse_params


def test_parse_params():
    assert parse_params("$1 = '110448'") == ["110448"]
    assert parse_params("$1 = 'user36', $2 = 'it''s', $3 = 'a, b'") == ["user36", "it's", "a, b"]
    assert parse_params("") == [] and parse_params(None) == []
    assert parse_params("$1 = NULL") == [None]
    assert parse_params("garbage") is None


class _Cfg:
    def raw(self, name):
        return f"main.hc.raw_{name}"


class _Sql:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def query(self, stmt, args=None):
        self.calls.append((stmt, args))
        return self.rows


def test_affected_queries_shapes_rows_and_binds_params():
    rows = [
        {"queryid": "1", "query": "SELECT * FROM t WHERE id = $1;", "params": ["$1 = '5'", "bad", "$1 = 'it''s'"]},
        {"queryid": "2", "query": " update t set a = 1 ", "params": None},
    ]
    sql = _Sql(rows)
    out = affected_queries(sql, _Cfg(), "proj", "public.t")
    assert out[0] == {"queryid": "1", "query": "SELECT * FROM t WHERE id = $1", "params": [["5"], ["it's"]], "is_write": False}
    assert out[1]["params"] == [[]] and out[1]["is_write"] is True
    stmt, args = sql.calls[0]
    assert "main.hc.raw_plan_history" in stmt and args["p"] == "proj" and args["rx"] == _table_regex("public.t")


def test_affected_queries_drops_unmeasurable_placeholder_queries_and_unparseable_samples():
    rows = [
        {"queryid": "1", "query": "SELECT * FROM t WHERE id = $1", "params": ["garbage"]},
        {"queryid": "2", "query": "SELECT * FROM t WHERE id = $1", "params": None},
        {"queryid": "3", "query": "SELECT * FROM t WHERE id = $1", "params": ["garbage", "$1 = '7'"]},
        {"queryid": "4", "query": "SELECT count(*) FROM t", "params": []},
    ]
    out = affected_queries(_Sql(rows), _Cfg(), "proj", "t")
    assert [(q["queryid"], q["params"]) for q in out] == [("3", [["7"]]), ("4", [[]])]


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


class _Seq:
    """Answers each query with the next row list."""
    def __init__(self, *answers):
        self.answers, self.calls = list(answers), []

    def query(self, stmt, args=None):
        self.calls.append((stmt, args))
        return self.answers.pop(0) if self.answers else []


def test_affected_queries_widens_the_window_when_the_last_day_has_nothing():
    sql = _Seq([], [{"queryid": "9", "query": "SELECT count(*) FROM customers", "params": []}])
    out = affected_queries(sql, _Cfg(), "proj", "hc_test.customers")
    assert [q["queryid"] for q in out] == ["9"]
    assert "INTERVAL 24 HOURS" in sql.calls[0][0] and "INTERVAL 168 HOURS" in sql.calls[1][0]


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


def test_no_samples_reason_names_redacted_rows():
    sql = _Seq([{"matched": 0, "redacted": 812, "placeholders": 0}])
    why = no_samples_reason(sql, _Cfg(), "proj", "hc_test.customers")
    assert "hc_test.customers" in why and "812" in why and "redacted" in why
    assert ":p" not in why and sql.calls[0][1]["p"] == "proj"
    sql = _Seq([{"matched": 40, "redacted": 0, "placeholders": 40}])
    assert "bind parameters" in no_samples_reason(sql, _Cfg(), "proj", "hc_test.customers")
    assert "could not tell" in no_samples_reason(sql, _Cfg(), "proj", None)


def test_only_single_select_or_dml_statements_are_sampled():
    """Live criterion 4: a session's 'SET ...; SET ...; EXPLAIN ... customers' text failed PREPARE with SyntaxError."""
    rows = [
        {"queryid": "1", "query": "SET enable_seqscan = off; SET enable_bitmapscan = off; EXPLAIN (COSTS OFF) "
                                  "SELECT count(*) FROM hc_test.customers WHERE region = 'x'", "params": []},
        {"queryid": "2", "query": "EXPLAIN SELECT 1 FROM customers", "params": []},
        {"queryid": "3", "query": "CREATE INDEX i ON hc_test.customers (a)", "params": []},
        {"queryid": "4", "query": "  with c AS (SELECT 1 FROM customers) SELECT * FROM c;", "params": []},
        {"queryid": "5", "query": "DELETE FROM customers WHERE id = $1", "params": ["$1 = '3'"]},
    ]
    out = affected_queries(_Sql(rows), _Cfg(), "proj", "hc_test.customers")
    assert [q["queryid"] for q in out] == ["4", "5"]
