"""Normalized statement text for the app (spec §10): the analysis job copies pg_stat_statements.query of enrolled
branches into query_texts, replaced each run and kept 7 days, under the pii_masking column mask."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from lakebase_health import querytexts
from lakebase_health.config import EngineConfig
from lakebase_health.ddl import MASKED_COLUMNS, TABLE_DDL
from lakebase_health.models import EndpointInfo
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)


def ep(eid, enrolled=True, state="ACTIVE", opted_out=False, dbs=("postgres", "app")):
    return EndpointInfo(project_id="p1", project_name="hc", branch_id="br1", branch_name="production",
                        is_default_branch=True, is_protected=False, endpoint_id=eid, endpoint_name=f"n/{eid}",
                        endpoint_type="ENDPOINT_TYPE_READ_WRITE", current_state=state, host="h", min_cu=1, max_cu=2,
                        suspend_timeout_s=None, ha_min=1, restore_window_s=None, pg_native_login=False,
                        opted_out=opted_out, enrolled=enrolled, databases=list(dbs))


class Session:
    def __init__(self, db, rows):
        self.db, self.rows = db, rows

    def has_extension(self, name):
        return name == "pg_stat_statements" and self.db == "app"

    def query(self, sql):
        assert "FROM pg_stat_statements s" in sql
        return self.rows


class Postgres:
    def __init__(self, rows, fail=()):
        self.rows, self.fail, self.opened, self.active_checks = rows, set(fail), [], 0

    def still_active(self, e):
        self.active_checks += 1
        return True

    @contextmanager
    def session(self, e, db):
        if e.endpoint_id in self.fail:
            raise RuntimeError("connect failed")
        self.opened.append((e.endpoint_id, db))
        yield Session(db, self.rows)


ROWS = [{"dbid": 16407, "datname": "app", "queryid": -91, "query": "SELECT * FROM t WHERE a IN ($1,$2)"},
        {"dbid": 16407, "datname": "app", "queryid": -91, "query": "SELECT * FROM t WHERE a IN ($1,$2)"},
        {"dbid": 16407, "datname": "app", "queryid": 7, "query": "SET search_path = 'secret_schema'"},
        {"dbid": 16407, "datname": "app", "queryid": 8, "query": "SELECT 'unterminated"}]


def test_enrolled_active_computes_are_read_and_remaining_literals_never_stored():
    pg = Postgres(ROWS)
    rows, read, errors = querytexts.collect(pg, [ep("ep-a"), ep("ep-b", enrolled=False), ep("ep-c", state="IDLE"),
                                                  ep("ep-d", opted_out=True)], NOW)
    assert read == ["ep-a"] and errors == [] and pg.opened == [("ep-a", "postgres"), ("ep-a", "app")]
    assert rows == [
        {"project_id": "p1", "branch_id": "br1", "endpoint_id": "ep-a", "dbid": 16407, "datname": "app",
         "queryid": -91, "query": "SELECT * FROM t WHERE a IN ($1 ,$2 )", "captured_at": NOW},
        {"project_id": "p1", "branch_id": "br1", "endpoint_id": "ep-a", "dbid": 16407, "datname": "app",
         "queryid": 7, "query": "SET search_path = $1", "captured_at": NOW}]
    assert "secret_schema" not in str(rows) and "unterminated" not in str(rows)


def test_a_failing_compute_is_reported_and_keeps_its_previous_rows():
    pg = Postgres(ROWS, fail={"ep-a"})
    rows, read, errors = querytexts.collect(pg, [ep("ep-a")], NOW)
    assert rows == [] and read == [] and errors == ["query_texts ep-a: RuntimeError"]


def test_store_replaces_the_read_computes_rows_and_drops_rows_older_than_7_days():
    sql = FakeSql()
    querytexts.store(sql, CFG, [{"endpoint_id": "ep-a", "queryid": 1}], ["ep-a"], NOW)
    [(stmt, args)] = [(s, a) for s, a in sql.statements if s.startswith("DELETE")]
    assert "cat.lakebase_health.query_texts" in stmt and "array_contains(split(:eps, ','), endpoint_id)" in stmt
    assert args == {"eps": "ep-a", "cutoff": NOW - timedelta(days=7)}
    assert sql.tables["cat.lakebase_health.query_texts"] == [{"endpoint_id": "ep-a", "queryid": 1}]


def test_the_table_is_created_and_its_text_masked():
    assert "queryid BIGINT" in TABLE_DDL["query_texts"] and "dbid BIGINT" in TABLE_DDL["query_texts"]
    assert MASKED_COLUMNS["query_texts"] == ("query",)


def test_an_extension_schema_the_engine_cannot_use_is_named_in_the_error():
    """Live: pg_stat_statements was installed in hc_test, where the engine role has no USAGE (UndefinedTable)."""
    class UndefinedTable(Exception):
        pass

    class Hidden(Postgres):
        @contextmanager
        def session(self, e, db):
            s = Session(db, [])
            s.query = lambda sql: (_ for _ in ()).throw(UndefinedTable("relation does not exist"))
            yield s
    rows, read, errors = querytexts.collect(Hidden([]), [ep("ep-a")], NOW)
    assert errors == ["query_texts ep-a: UndefinedTable (pg_stat_statements is installed in a schema the engine "
                      "role has no USAGE on)"]
