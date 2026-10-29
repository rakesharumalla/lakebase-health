"""Monitoring tools' own queries (pg_sage, the engine, fixer and notebook) never count in the query checks, and
customer queries are never dropped by accident."""
import re
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace as NS

import pytest

from lakebase_health.checkspec import parse_check
from lakebase_health.config import EngineConfig
from lakebase_health.exclusions import load_exclusions, tag_regex
from lakebase_health.executors.delta import DeltaExecutor
from lakebase_health.executors.postgres import QUERY_TAG, PostgresExecutor, tagged
from lakebase_health.fixer.pg import FixerPg
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.pg_inventory import STATEMENTS_SQL, statements_rows
from tests.fakes import FakeSql
from tests.unit.test_executors import FakeConn, ep

EXCL = load_exclusions(CHECKS_DIR / "exclusions.yaml")
RX = tag_regex(EXCL.tool_tags)


@pytest.mark.parametrize("q", ["/* pg_sage */ SELECT * FROM pg_stat_activity", "  /*lakebase-health*/ SELECT 1",
                               "/* lakebase-health check=unused_index */ SELECT 1", "\n/* pg_sage v2 */\nSELECT $1"])
def test_tool_tagged_queries_match(q):
    assert re.search(RX, q)


@pytest.mark.parametrize("q", ["SELECT * FROM orders", "SELECT 1 /* pg_sage */", "SELECT '/* pg_sage */' AS x",
                               "/* pg_sage_fork */ SELECT 1", "/* lakebase-healthy */ SELECT 1",
                               "-- Who blocks whom\nSELECT 1", "/* app=checkout */ SELECT 1", "<REDACTED>", ""])
def test_customer_queries_do_not_match(q):
    assert not re.search(RX, q)


def _sqlite(pred, **cols):
    db = sqlite3.connect(":memory:")
    db.create_function("REGEXP", 2, lambda rx, v: v is not None and re.search(rx, v) is not None)
    db.execute(f"CREATE TABLE r AS SELECT {', '.join(f'? AS {c}' for c in cols)}", list(cols.values()))
    # Spark unescapes a doubled backslash in a string literal; sqlite does not
    sql = pred.replace(" RLIKE ", " REGEXP ").replace("\\\\", "\\")
    return bool(db.execute(f"SELECT CASE WHEN {sql} THEN 1 ELSE 0 END FROM r").fetchone()[0])


def test_tool_applications_and_tags_as_sql_predicates():
    apps = EXCL.tool_app_sql("application_name")
    for app in ("pg_sage", "lakebase-health-notebook", "lakebase-health", "lakebase-health-fixer"):
        assert _sqlite(apps, application_name=app), app
    for app in ("psql", "checkout-service", "pg_sage_dashboard", None):
        assert not _sqlite(apps, application_name=app), app
    text = EXCL.tool_text_sql("query_text")
    assert "\\\\s" in text  # doubled for Spark's string literal
    assert _sqlite(text, query_text="/* pg_sage */ SELECT 1")
    assert not _sqlite(text, query_text="SELECT 1 /* pg_sage */") and not _sqlite(text, query_text=None)
    assert EXCL.tool_text_pg("s.query").startswith("(coalesce(s.query, '') ~ '^\\s*/\\*")


def test_query_scoped_checks_drop_tool_queries_by_role_and_query_id():
    s = parse_check({"id": "q", "title": "Q", "category": "c", "source": "delta", "explain": "e", "query_scoped": True,
                     "thresholds": {"warning": 0}, "sql": "SELECT 1 FROM system.lakebase.plan_history p "
                     "JOIN system.lakebase.pg_stat_statements_counters c JOIN system.lakebase.active_session_history a"},
                    stem="q")
    fake = FakeSql()
    DeltaExecutor(fake, EngineConfig(catalog="cat"), EXCL).run(s)
    out = fake.statements[-1][0]
    assert out.count("'pg_sage'") >= 3  # plan_history filter + both tool joins
    assert out.count("t.queryid = tq.queryid") == 2 and out.count("t.userid = tq.userid") == 2
    assert "coalesce(query, '') RLIKE" in out and "coalesce(query_text, '') RLIKE" in out


def test_inventory_statements_skip_tagged_text_inside_postgres():
    class S:
        def query(self, sql):
            self.sql = sql
            return []
    s = S()
    statements_rows(s, EXCL)
    assert "NOT (coalesce(s.query, '') ~ '^\\s*/\\*\\s*(pg_sage|lakebase-health)" in s.sql
    assert "{tool}" in STATEMENTS_SQL and not re.search(r"SELECT[^;]*\bs\.query\b[^;]*FROM pg_stat_statements", s.sql.split("WHERE")[0])


def test_engine_and_fixer_statements_lead_with_the_tag():
    assert tagged("SELECT 1") == "/* lakebase-health */ SELECT 1" and tagged(QUERY_TAG + "SELECT 1") == QUERY_TAG + "SELECT 1"
    conn = FakeConn({})
    w = NS(postgres=NS(generate_database_credential=lambda endpoint: NS(token="t")))
    with PostgresExecutor(w, "svc", connect=lambda **k: conn).session(ep(), "db") as s:
        s.query("SELECT 1 AS x")
    assert conn.executed[-1] == "/* lakebase-health */ SELECT 1 AS x"

    class Conn:
        def __init__(self):
            self.ran = []

        def execute(self, sql, params=None):
            self.ran.append((sql, params))

        def close(self):
            pass
    c = Conn()
    with FixerPg(w, "fixer", connect=lambda **k: c).session("projects/p/branches/b/endpoints/e", "h", "db", "analyze") as fx:
        fx.execute("ANALYZE public.t")
        fx.execute("SELECT 1 WHERE x = %s", ("y",))
    assert c.ran[-2:] == [("/* lakebase-health */ ANALYZE public.t", None), ("/* lakebase-health */ SELECT 1 WHERE x = %s", ("y",))]


def test_exclusions_reject_a_tag_that_would_break_the_regex():
    with pytest.raises(ValueError, match="tool tag"):
        tag_regex(["pg.sage"])
