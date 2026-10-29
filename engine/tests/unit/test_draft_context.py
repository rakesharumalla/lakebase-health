import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from lakebase_health.config import EngineConfig
from lakebase_health.fixer.draft import prompt
from lakebase_health.fixer.draft.context import NoQueryText, build_context, load_catalog, load_source, plan_context
from lakebase_health.fixer.draft.sqltext import normalize
from lakebase_health.fixer.draft.states import draft_token, finding_queryid
from lakebase_health.fixer.pg import FIXER_SETUP
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
QUERY = ("SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
         "AND o.status <> 'cancelled' AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
         "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) ORDER BY o.id")
PLAN = json.dumps([{"Plan": {"Node Type": "Index Scan", "Relation Name": "orders", "Alias": "o",
                             "Index Name": "orders_pkey", "Index Cond": "((o.id >= 1600004111) AND (o.id <= $2))",
                             "Filter": "((o.status <> 'cancelled'::text) AND (o.amount > (SubPlan 1)))",
                             "Plan Rows": 9, "Actual Rows": 1803,
                             "Plans": [{"Node Type": "Aggregate", "Strategy": "Plain", "Parent Relationship": "SubPlan",
                                        "Subplan Name": "SubPlan 1", "Plan Rows": 1}]},
                    "Query Text": "SELECT ... 1600004111 'cancelled'", "Execution Time": 512.5}])
FINDING = {"fingerprint": "f" * 32, "check_id": "top_db_time_query", "project_id": "p1", "branch_id": "br1",
           "endpoint_id": "ep-a", "object": "42", "status": "open", "measure": 62.5,
           "evidence": json.dumps({"queryid": "42", "calls": 400, "share_pct": 62.5, "query_text": "SELECT 'x'",
                                   "regressed": True})}


class Conn:
    """Catalog answers keyed by the first words of each statement."""
    ANSWERS = {
        "SELECT n.nspname": [("hc_test", "orders", 900000)],
        "SELECT a.attname": [("id", "integer", True), ("status", "text", True), ("amount", "numeric(12,2)", True)],
        "SELECT pg_get_constraintdef": [("PRIMARY KEY (id)",), ("CHECK ((status <> 'void'::text))",)],
        "SELECT pg_get_indexdef": [("CREATE UNIQUE INDEX orders_pkey ON hc_test.orders USING btree (id)",),
                                   ("CREATE INDEX orders_part ON hc_test.orders USING btree (id) WHERE (status = 'paid'::text)",)],
        "SELECT attname, n_distinct": [("status", 5.0, 0.0, 0.21)],
    }

    def __init__(self):
        self.ran = []

    def execute(self, stmt, args=None):
        self.ran.append((stmt, args))
        rows = next(v for k, v in self.ANSWERS.items() if stmt.startswith(k))
        return SimpleNamespace(fetchone=lambda: rows[0] if rows else None, fetchall=lambda: rows)


GENERIC = json.loads(PLAN)[0]
PGSS_QUERY = QUERY.replace("'cancelled'", "$3")
HISTORY_ROWS = [{"query_text": "<REDACTED>", "dbid": 16407}, {"query_text": QUERY, "dbid": 16407}]


class StatsConn:
    """A read-only draft_context session: pg_stat_statements (when installed in this database) and pg_database."""

    def __init__(self, db, installed, entry, reads_all=True):
        self.db, self.installed, self.entry, self.ran, self.reads_all = db, installed, entry, [], reads_all

    def execute(self, stmt, args=None):
        self.ran.append((stmt, args))
        if stmt.startswith("SELECT to_regclass('pg_stat_statements')"):
            rows = [(self.installed,)]
        elif "FROM pg_stat_statements s" in stmt:
            rows = [self.entry] if self.entry else []
        elif stmt.startswith("SELECT pg_has_role(current_user, 'pg_read_all_stats'"):
            rows = [(self.reads_all,)]
        elif stmt.startswith("SELECT datname FROM pg_database"):
            rows = [("databricks_postgres",)] if args == (16407,) else []
        else:
            rows = []
        return SimpleNamespace(fetchone=lambda: rows[0] if rows else None, fetchall=lambda: rows)


TGT = {"endpoint": "projects/x/branches/production/endpoints/primary", "host": "h",
       "databases": ["postgres", "databricks_postgres"]}


def source(entry=(PGSS_QUERY, "databricks_postgres", 16407, 1047, 942989), history=HISTORY_ROWS,
           installed=("databricks_postgres",), reads_all=True):
    opened = []

    @contextmanager
    def session(endpoint, host, db, kind):
        conn = StatsConn(db, db in installed, entry, reads_all)
        opened.append((db, kind, conn))
        yield conn
    sql = FakeSql([(r"raw_pg_stat_statements_counters", [{"dbid": 16407}]), (r"raw_plan_history", history)])
    return load_source(sql, CFG, session, TGT, FINDING), sql, opened


def test_query_text_is_the_pg_stat_statements_entry_for_the_findings_queryid_and_dbid():
    src, sql, opened = source()
    assert src == {"queryid": "42", "query": PGSS_QUERY, "database": "databricks_postgres", "dbid": 16407,
                   "mean_rows": 942989 / 1047, "origin": "pg_stat_statements"}
    assert [(db, kind) for db, kind, _ in opened] == [("postgres", "draft_context"), ("databricks_postgres", "draft_context")]
    [(stmt, args)] = [(s, a) for s, a in opened[1][2].ran if "FROM pg_stat_statements s" in s]
    assert args == (42, 16407)
    [counters] = sql.ran(r"raw_pg_stat_statements_counters")
    assert "dbid" in counters and "query" not in counters.split("FROM")[0]  # the masked column is never read
    assert not sql.ran(r"raw_plan_history")


def test_without_an_entry_readable_plan_history_text_is_the_fallback_and_its_parameters_stay_unused():
    src, sql, _ = source(entry=None)
    assert (src["query"], src["database"], src["origin"], src["mean_rows"]) == (QUERY, "databricks_postgres",
                                                                                "plan_history", None)
    [stmt] = sql.ran(r"raw_plan_history")
    assert "parameters" not in stmt and "plan_json" not in stmt and "<REDACTED" in stmt


def test_no_entry_and_no_readable_text_is_no_query_text():
    with pytest.raises(NoQueryText, match="no pg_stat_statements entry for queryid 42"):
        source(entry=None, history=[])
    with pytest.raises(NoQueryText, match="pg_stat_statements is not installed"):
        source(installed=(), history=[])
    # Live acceptance: the fixer's role was not in pg_read_all_stats, so the owner's statements were hidden from it
    with pytest.raises(NoQueryText, match="lacks pg_read_all_stats"):
        source(entry=None, history=[], reads_all=False)


def test_the_plan_for_the_model_is_the_generic_plan_with_literals_stripped():
    plan = plan_context(GENERIC)
    assert plan["Plan"]["Index Cond"] == "((o.id >= ?) AND (o.id <= $2))"
    assert "Query Text" not in plan and "Execution Time" not in plan
    assert plan_context(None) is None and plan_context({"x": 1}) is None


def test_the_model_payload_is_exactly_this_and_holds_no_values():
    norm, literals = normalize(QUERY)
    tables = load_catalog(Conn(), {("hc_test", "orders")})
    msgs = prompt.messages(build_context(FINDING, norm, True, plan_context(GENERIC), tables))
    assert [m["role"] for m in msgs] == ["system", "user"] and msgs[0]["content"] == prompt.SYSTEM
    assert json.loads(msgs[1]["content"]) == {
        "finding": {"check_id": "top_db_time_query", "measure": 62.5, "evidence": {"calls": 400, "share_pct": 62.5}},
        "query": ("SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
                  "AND o.status <> $3 AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
                  "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) ORDER BY o.id"),
        "parameters": ["$1", "$2", "$3"],
        "ordered": True,
        "plan": {"Plan": {"Node Type": "Index Scan", "Relation Name": "orders", "Alias": "o",
                          "Index Name": "orders_pkey", "Index Cond": "((o.id >= ?) AND (o.id <= $2))",
                          "Filter": "((o.status <> ?::text) AND (o.amount > (SubPlan ?)))",
                          "Plan Rows": 9, "Actual Rows": 1803,
                          "Plans": [{"Node Type": "Aggregate", "Strategy": "Plain", "Parent Relationship": "SubPlan",
                                     "Subplan Name": "SubPlan 1", "Plan Rows": 1}]}},
        "tables": [{"name": "hc_test.orders", "row_estimate": 900000,
                    "columns": [{"name": "id", "type": "integer", "not_null": True},
                                {"name": "status", "type": "text", "not_null": True},
                                {"name": "amount", "type": "numeric(12,2)", "not_null": True}],
                    "constraints": ["PRIMARY KEY (id)", "CHECK ((status <> ?::text))"],
                    "indexes": ["CREATE UNIQUE INDEX orders_pkey ON hc_test.orders USING btree (id)",
                                "CREATE INDEX orders_part ON hc_test.orders USING btree (id) WHERE (status = ?::text)"],
                    "column_stats": [{"column": "status", "n_distinct": 5.0, "null_frac": 0.0, "correlation": 0.21}]}],
    }
    payload = json.dumps(msgs)
    for value in ("1600004111", "1600006111", "1600200000", "cancelled", "void", "paid", "SELECT 'x'"):
        assert value not in payload, value
    assert literals == {3: "'cancelled'"}


def test_catalog_reads_resolve_names_through_the_search_path_and_never_read_values():
    conn = Conn()
    load_catalog(conn, {("", "orders")})
    assert all(args in (("orders",), ("hc_test", "orders")) for _, args in conn.ran)
    assert not any("most_common" in s or "histogram" in s for s, _ in conn.ran)


def test_catalog_session_is_read_only_and_short():
    setup = FIXER_SETUP("draft_context")
    assert "SET default_transaction_read_only = on" in setup and "SET statement_timeout = '30s'" in setup


def test_queryid_of_each_rewritable_check_and_the_attempt_token():
    assert finding_queryid("top_db_time_query", '{"queryid": "42"}', "42") == "42"
    assert finding_queryid("disk_spill", "{}", "-17@app") == "-17"
    assert finding_queryid("row_misestimate", '{"top_queries": [{"queryid": "7"}, {"queryid": "8"}]}', "ep-a") == "7"
    assert finding_queryid("missing_fk_index", '{"queryid": "42"}', "x") is None
    assert finding_queryid("plan_flip", "not json", "abc") is None
    assert draft_token("f" * 32, 2) != draft_token("f" * 32, 3)
