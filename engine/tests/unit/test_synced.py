import json
from datetime import datetime, timezone

from lakebase_health.actions import plan_actions
from lakebase_health.checkspec import load_checks, parse_check, render_sql
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.models import Finding
from lakebase_health.synced import (SYNCED_MAP_SQL, SYNCED_TABLES_SQL, annotate, drops_synced_index, synced_map,
                                    synced_table_of)
from tests.unit.test_executors import EXCL, ep
from tests.unit.test_runner import FakePg, runner, spec

NOW = datetime(2026, 10, 29, tzinfo=timezone.utc)
SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}


class Session:
    def __init__(self, synced_rows, rows):
        self.synced_rows, self.rows, self.queries = synced_rows, rows, []

    def query(self, sql):
        self.queries.append(sql)
        return self.synced_rows if sql == SYNCED_MAP_SQL else self.rows

    def has_extension(self, name):
        return True

    def stats_window_hours(self):
        return 48.0


def test_synced_storage_is_a_db_system_partition_or_a_writer_owned_table():
    assert "pg_partition_root(c.oid)" in SYNCED_TABLES_SQL and "'__db_system'" in SYNCED_TABLES_SQL
    assert "databricks_writer_[0-9]+" in SYNCED_TABLES_SQL
    assert SYNCED_TABLES_SQL in SYNCED_MAP_SQL and "pg_index" in SYNCED_MAP_SQL


def test_the_synced_tables_token_is_rendered():
    s = parse_check({"id": "x", "title": "X", "category": "c", "source": "postgres", "explain": "e",
                     "thresholds": {"info": 0}, "sql": "SELECT 1 FROM {synced_tables} st"}, stem="x")
    assert render_sql(s, EXCL) == f"SELECT 1 FROM {SYNCED_TABLES_SQL} st"


def test_index_drop_checks_skip_synced_storage_and_the_review_check_covers_it():
    for i in ("unused_index", "duplicate_index", "redundant_index"):
        sql = render_sql(SPECS[i], EXCL)
        assert SYNCED_TABLES_SQL in sql and "st.relid IS NULL" in sql, i
    review = SPECS["synced_index_review"]
    assert review.thresholds == {"info": 0.0} and review.fix.kind == "manual" and not review.fix.actionable
    assert review.min_stats_hours == 24 and "parent_index" in review.sql and "pg_inherits" in review.sql


def test_synced_map_and_annotation():
    s = Session([{"ident": "__db_system.orders_p1", "synced_table": "public.orders"},
                 {"ident": "__db_system.orders_p1_idx", "synced_table": "public.orders"},
                 {"ident": None, "synced_table": "x"}], [])
    m = synced_map(s)
    assert m == {"__db_system.orders_p1": "public.orders", "__db_system.orders_p1_idx": "public.orders"}
    assert synced_table_of({"index_ident": "__db_system.orders_p1_idx"}, m) == "public.orders"
    rows = annotate([{"object": "__db_system.orders_p1"}, {"object": "public.other"}], m)
    assert rows == [{"object": "__db_system.orders_p1", "synced_table": "public.orders"}, {"object": "public.other"}]


def test_a_catalog_read_failure_maps_nothing():
    class Broken:
        def query(self, sql):
            raise RuntimeError("permission denied")
    assert synced_map(Broken()) == {}


def test_every_postgres_finding_on_synced_storage_names_the_synced_table():
    s = Session([{"ident": "__db_system.orders_p1", "synced_table": "public.orders"}],
                [{"object": "__db_system.orders_p1", "measure": 30.0}, {"object": "public.t", "measure": 30.0}])
    [o] = runner(pg=FakePg(s)).run([spec("dead", thresholds={"warning": 10})], [ep()], NOW, {}, set())
    by_obj = {r.object: r.evidence for r in o.results}
    assert by_obj["db:__db_system.orders_p1"]["synced_table"] == "public.orders"
    assert "synced_table" not in by_obj["db:public.t"]


def _finding(evidence, sql="DROP INDEX CONCURRENTLY __db_system.orders_p1_idx;"):
    return Finding("f1", "unused_index", "T", "indexing", "p", "b", "e", "db:__db_system.orders_p1_idx", "new",
                   "warning", "high", 1.0, NOW, NOW, NOW, 1, 0, json.dumps(evidence), None, "e", sql,
                   "CREATE INDEX CONCURRENTLY orders_p1_idx ON __db_system.orders_p1 USING btree (x);")


def test_no_drop_action_for_an_index_on_synced_storage():
    actions, events = plan_actions([_finding({"synced_table": "public.orders"})], {}, SPECS, NOW)
    assert actions == [] and events == []
    actions, _ = plan_actions([_finding({})], {}, SPECS, NOW)
    assert len(actions) == 1 and actions[0].sql.startswith("DROP INDEX")


def test_drops_synced_index():
    assert drops_synced_index({"synced_table": "public.orders"}, "  drop index concurrently x;")
    assert not drops_synced_index({"synced_table": "public.orders"}, "VACUUM (ANALYZE) x;")
    assert not drops_synced_index({}, "DROP INDEX x;") and not drops_synced_index(None, "DROP INDEX x;")
