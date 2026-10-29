from types import SimpleNamespace

import pytest
from lakebase_health.fixer.allowlist import parse
from lakebase_health.fixer.pg import FIXER_SETUP, FixerPg, fixer_index_name, index_state, with_index_name


class Conn:
    def __init__(self, rows=None):
        self.ran, self.rows, self.closed = [], rows or [], False

    def execute(self, sql, params=None):
        self.ran.append(sql)
        return SimpleNamespace(fetchall=lambda: self.rows, fetchone=lambda: self.rows[0] if self.rows else None)

    def close(self):
        self.closed = True


def test_the_rewrite_verification_session_is_read_only():
    """Final review I-4: model SQL ran in a writable session with a 10 min statement timeout."""
    setup = FIXER_SETUP("query_rewrite")
    assert "SET default_transaction_read_only = on" in setup and "SET statement_timeout = '30s'" in setup


def test_guards_per_kind_and_writable_session():
    assert "SET statement_timeout = '2h'" in FIXER_SETUP("create_index")
    assert "SET statement_timeout = '10min'" in FIXER_SETUP("analyze")
    for k in ("create_index", "analyze"):
        assert "SET lock_timeout = '5s'" in FIXER_SETUP(k)
        assert not any("read_only" in s for s in FIXER_SETUP(k))
    conn = Conn()
    w = SimpleNamespace(postgres=SimpleNamespace(generate_database_credential=lambda endpoint: SimpleNamespace(token="t")))
    seen = {}
    pg = FixerPg(w, "fixer-sp", connect=lambda **kw: (seen.update(kw), conn)[1])
    with pg.session("projects/p/branches/b/endpoints/primary", "h", "db", "drop_index"):
        pass
    assert seen["application_name"] == "lakebase-health-fixer" and seen["autocommit"] is True and conn.closed
    # Lakebase endpoint connections do not keep psycopg's auto-prepared statements (and measure() runs
    # DEALLOCATE ALL); "prepared statement _pg3_0 does not exist" otherwise
    assert "prepare_threshold" in seen and seen["prepare_threshold"] is None


def test_index_state():
    assert index_state(Conn([(True,)]), "s.i") == "valid"
    assert index_state(Conn([(False,)]), "s.i") == "invalid"
    assert index_state(Conn([]), "s.i") == "absent"


ACTION = "ABCDEF1234567890"
NAME = "lbh_abcdef123456"


def test_fixer_index_name_is_deterministic_and_lowercase():
    assert fixer_index_name(ACTION) == NAME
    assert fixer_index_name(ACTION) == fixer_index_name(ACTION)


def test_unnamed_index_gets_name_after_concurrently():
    sql, qualified = with_index_name("CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id)", "hc_test.orders", ACTION)
    assert sql == f"CREATE INDEX CONCURRENTLY {NAME} ON hc_test.orders (customer_id)"
    assert qualified == f"hc_test.{NAME}"


def test_unnamed_index_with_if_not_exists_and_quoted_schema():
    src = 'CREATE INDEX CONCURRENTLY IF NOT EXISTS ON "hc_test"."orders" USING btree ("customer_id")'
    sql, qualified = with_index_name(src, '"hc_test"."orders"', ACTION)
    assert sql == f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {NAME} ON "hc_test"."orders" USING btree ("customer_id")'
    assert qualified == f'"hc_test".{NAME}'


def test_unqualified_table_returns_bare_name():
    sql, qualified = with_index_name("CREATE INDEX CONCURRENTLY ON orders (c)", "orders", ACTION)
    assert sql == f"CREATE INDEX CONCURRENTLY {NAME} ON orders (c)"
    assert qualified == NAME


def test_already_named_index_passes_through_unchanged():
    src = 'CREATE INDEX CONCURRENTLY "ix" ON "hc_test"."orders" USING btree ("customer_id")'
    sql, qualified = with_index_name(src, '"hc_test"."orders"', ACTION)
    assert sql == src
    assert qualified == '"hc_test"."ix"'


@pytest.mark.parametrize("src", ["DROP INDEX CONCURRENTLY hc_test.i", "ANALYZE hc_test.orders"])
def test_non_create_statement_raises(src):
    with pytest.raises(ValueError):
        with_index_name(src, "hc_test.orders", ACTION)


@pytest.mark.parametrize("src,table", [
    ("CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id)", "hc_test.orders"),
    ('CREATE INDEX CONCURRENTLY IF NOT EXISTS ON "hc_test"."orders" USING btree ("customer_id")', '"hc_test"."orders"'),
])
def test_produced_sql_still_passes_allowlist(src, table):
    sql, _ = with_index_name(src, table, ACTION)
    stmt = parse(sql)
    assert stmt.kind == "create_index" and stmt.table == table


def test_the_stats_read_on_the_source_branch_is_read_only_and_short():
    from lakebase_health.fixer.pg import FIXER_SETUP
    setup = FIXER_SETUP("stats")
    assert "SET default_transaction_read_only = on" in setup and "SET statement_timeout = '30s'" in setup


def test_tagged_conn_renders_literals_against_a_real_psycopg_connection():
    # verify.measure and draft verify call Literal(v).as_string(conn) on the TaggedConn; psycopg's
    # Transformer reads conn.adapters and conn.connection, which the wrapper must delegate (C1)
    import psycopg
    from psycopg import pq, sql as pgsql
    from lakebase_health.fixer.draft.verify import _args
    from lakebase_health.fixer.pg import TaggedConn
    raw = psycopg.Connection(pq.PGconn.connect_start(b"host=127.0.0.1 port=1 dbname=x connect_timeout=1"))
    try:
        tc = TaggedConn(raw)
        assert pgsql.Literal(5).as_string(tc) == "5"
        assert pgsql.Literal("a'b").as_string(tc) == "'a''b'"
        assert _args(tc, [1, "x"]) == "1, 'x'"
        assert tc.adapters is raw.adapters
    finally:
        raw.pgconn.finish()
