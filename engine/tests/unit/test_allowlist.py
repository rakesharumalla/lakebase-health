import pytest
from lakebase_health.fixer.allowlist import NotAllowed, parse

OK = [
    ("CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);", "create_index", "hc_test.orders"),
    ('CREATE INDEX CONCURRENTLY "ix" ON "hc_test"."orders" USING btree ("customer_id")', "create_index", '"hc_test"."orders"'),
    ("CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id, created DESC)", "create_index", "hc_test.orders"),
    ('ANALYZE "tablé";', "analyze", '"tablé"'),
    ("DROP INDEX CONCURRENTLY IF EXISTS hc_test.customers_email_idx2;", "drop_index", "hc_test.customers_email_idx2"),
    ("ANALYZE hc_test.orders;", "analyze", "hc_test.orders"),
    ("ALTER TABLE hc_test.orders SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.01);", "reloptions", "hc_test.orders"),
    ("ALTER TABLE hc_test.orders RESET (autovacuum_enabled);", "reloptions", "hc_test.orders"),
]
BAD = [
    "CREATE UNIQUE INDEX CONCURRENTLY ON t (c)",
    "CREATE INDEX ON t (c)",                                  # not concurrent
    "DROP INDEX hc_test.i",                                   # not concurrent
    "DROP TABLE hc_test.orders",
    "ANALYZE",                                                # whole database
    "ALTER TABLE t SET (fillfactor = 50)",                    # not autovacuum
    "ALTER TABLE t SET (autovacuum_enabled = false); DROP TABLE t",
    "CREATE INDEX CONCURRENTLY ON t (c) /* ; */; DELETE FROM t",
    "ANALYZE t -- \n; DROP TABLE t",
    'DROP INDEX CONCURRENTLY "a;b"',                          # quoted ; is fine in name but only one statement allowed
    "VACUUM FULL t",
    "ANALYZE VERBOSE",
    "ANALYZE VERBOSE t",
    "ANALYZE (VERBOSE) t",
    "ANALYZE\xa0t",
    "CREATE INDEX CONCURRENTLY ON t (c)\x1c",
    "ANALYZE \u017fomething",
    "ANALYZE t\x1c; DROP TABLE t",
    "DROP INDEX CONCURRENTLY \u0131x",
    "CREATE INDEX CONCURRENTLY ON t ((lower(c)))",
    "CREATE INDEX CONCURRENTLY ON t (f(c))",
    "CREATE INDEX CONCURRENTLY ON t (c) WHERE c > 0",
    "CREATE INDEX CONCURRENTLY ON t (c text_pattern_ops)",
]


@pytest.mark.parametrize("sql,kind,table", OK)
def test_allowed(sql, kind, table):
    s = parse(sql)
    assert s.kind == kind and (s.table or s.target) == table


@pytest.mark.parametrize("sql", BAD)
def test_rejected(sql):
    if sql == 'DROP INDEX CONCURRENTLY "a;b"':
        assert parse(sql).kind == "drop_index"
        return
    with pytest.raises(NotAllowed):
        parse(sql)
