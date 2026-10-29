import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.ddl import TABLE_DDL
from lakebase_health.exclusions import Exclusions
from lakebase_health import pg_inventory as inv
from tests.fakes import FakeSql
from tests.unit.test_executors import ep

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
CFG = EngineConfig(catalog="cat")
EXCL = Exclusions(("s",), ("r",), ("cloud_admin",), ("pg_catalog",), "^(pg_)", "^neon_",
                  query_users=("cloud_admin", "sp-engine"), query_user_regex="^databricks_writer_[0-9]+$")
TABLE = {"schema_name": "hc_test", "table_name": "orders", "synced_table": None, "total_bytes": 81920,
         "n_live_tup": 1000, "n_dead_tup": 10}
ACTIVITY = {"schema_name": "hc_test", "table_name": "orders", "synced_table": None, "n_live_tup": 1000,
            "n_tup_ins": 1500, "n_tup_upd": 10, "n_tup_del": 0, "seq_scan": 5, "idx_scan": 40}
PROBE_ALL = {"version": "1.25", "lfc_stats": True, "working_set": True, "throttling": True, "lsns": True,
             "perf_counters": True}


class Session:
    """Answers each inventory query by a marker in its text; `fail` maps a marker to an exception."""

    def __init__(self, probe=PROBE_ALL, fail=(), statements=True, io=True):
        self.probe, self.fail, self.statements, self.io, self.sql = probe, dict(fail), statements, io, []

    def has_extension(self, name):
        return self.statements if name == "pg_stat_statements" else False

    def query(self, sql):
        self.sql.append(sql)
        for marker, exc in self.fail.items():
            if marker in sql:
                raise exc
        if "pg_total_relation_size(s.relid) AS total_bytes" in sql:
            return [dict(TABLE)]
        if "am.amname AS method" in sql:
            return [{"schema_name": "hc_test", "table_name": "orders", "index_name": "orders_pkey", "method": "btree",
                     "index_bytes": 16384, "idx_scan": 40, "is_valid": True, "is_primary": True, "is_unique": True,
                     "synced_table": None, "parent_index": None}]
        if "s.n_tup_ins, s.n_tup_upd" in sql:
            return [dict(ACTIVITY)]
        if "FROM pg_settings" in sql:
            return [{"name": "work_mem", "setting": "4096", "unit": "kB", "source": "default", "context": "user"},
                    {"name": "track_io_timing", "setting": "off", "unit": None, "source": "default",
                     "context": "superuser"},
                    {"name": "max_wal_size", "setting": "1024", "unit": "MB", "source": "configuration file",
                     "context": "sighup"}]
        if "pg_stat_statements s" in sql:
            return [{"ranking": "time", "rank": 1, "queryid": "42", "role": "app", "datname": "db", "calls": 10,
                     "mean_ms": 2.0, "total_ms": 20.0, "shared_blks_hit": 1, "shared_blks_read": 2,
                     "temp_blks_written": 0}]
        if "to_regclass('pg_catalog.pg_stat_io')" in sql:
            return [{"present": self.io}]
        if "FROM pg_stat_io" in sql:
            return [{"backend_type": "client backend", "object": "relation", "context": "normal", "reads": 5,
                     "writes": 0, "extends": 0, "hits": 100, "evictions": 0, "reuses": None, "fsyncs": 0,
                     "read_time": None, "write_time": None, "stats_reset": NOW}]
        if "autovacuum_workers_busy" in sql:
            return [{"autovacuum_workers_busy": 3.0, "autovacuum_max_workers": 3.0, "oldest_xact_s": 12.0,
                     "uptime_s": 90000.0, "client_sessions": 4.0, "max_connections": 100.0,
                     "server_version_num": 170005.0, "server_version": "17.5"}]
        if "AS max_idle_s" in sql:
            return [{"role": "app", "application": "worker", "sessions": 2.0, "max_idle_s": 900.0}]
        if "FROM pg_extension e JOIN pg_namespace n" in sql:
            return [dict(self.probe)] if self.probe else []
        if "neon_get_lfc_stats()" in sql:
            return [{"lfc_key": "file_cache_hits", "lfc_value": 900}, {"lfc_key": "file_cache_misses", "lfc_value": 100}]
        if "approximate_working_set_size_seconds(900)" in sql:
            return [{"pages": 1000.0, "block_size": 8192.0}]
        if "backpressure_throttling_time()" in sql:
            return [{"us": 0.0}]
        if "pg_current_wal_lsn()" in sql:
            return [{"lsn": "0/2000"}]
        if "FROM backpressure_lsns()" in sql:
            return [{"received_lsn": "0/1000", "disk_consistent_lsn": "0/800", "remote_consistent_lsn": "0/400"}]
        if "FROM neon_perf_counters" in sql:
            return [{"metric": "getpage_wait_seconds_bucket", "bucket_le": 0.001, "value": 10.0},
                    {"metric": "getpage_wait_seconds_count", "bucket_le": None, "value": 12.0}]
        return []


class Pg:
    def __init__(self, session, suspended_after=None):
        self.s, self.suspended_after, self.opened = session, suspended_after, []

    def still_active(self, e):
        return self.suspended_after is None or len(self.opened) < self.suspended_after

    @contextmanager
    def session(self, e, db):
        self.opened.append(db)
        yield self.s


def metrics(rows):
    return {r["metric"]: r for r in rows}


def test_every_inventory_table_is_created_with_the_snapshot_key():
    for t in inv.INVENTORY_TABLES:
        assert TABLE_DDL[t].startswith("run_id STRING, collected_at TIMESTAMP, project_id STRING, branch_id STRING, "
                                       "endpoint_id STRING"), t
    assert "audit_events" in TABLE_DDL


def test_collects_every_table_and_never_reads_query_text():
    s = Session()
    out = inv.collect_endpoint(Pg(s), ep(databases=["db"]), EXCL, {}, False, NOW)
    assert out["inventory_tables"][0]["datname"] == "db" and out["inventory_indexes"][0]["method"] == "btree"
    assert out["inventory_statements"][0]["queryid"] == "42" and out["inventory_io"][0]["hits"] == 100
    joined = "\n".join(s.sql)
    assert not re.search(r"\bquery(_text)?\b", joined)  # queryid only, never the statement text
    stmt = next(q for q in s.sql if "pg_stat_statements s" in q)
    assert "coalesce(r.rolname, '') IN ('cloud_admin', 'sp-engine')" in stmt
    assert "coalesce(r.rolname, '') ~ '^databricks_writer_[0-9]+$'" in stmt


def test_settings_say_how_each_one_can_be_changed():
    out = inv.collect_endpoint(Pg(Session()), ep(), EXCL, {}, False, NOW)
    how = {r["name"]: r["how_to_change"] for r in out["inventory_settings"]}
    assert how == {"work_mem": "session", "track_io_timing": "support-managed", "max_wal_size": "support-managed"}
    assert inv.how_to_change("statement_timeout", "user") == "session"
    assert inv.how_to_change("log_connections", "superuser-backend") == "role or database"
    sql = inv.SETTINGS_SQL
    assert "source NOT IN ('default', 'override')" in sql and "{excluded_settings_regex}" in sql


def test_instance_queries_run_once_and_tables_per_database():
    s = Session()
    out = inv.collect_endpoint(Pg(s), ep(databases=["a", "b"]), EXCL, {}, False, NOW)
    assert [r["datname"] for r in out["inventory_tables"]] == ["a", "b"]
    assert sum("FROM pg_settings" in q for q in s.sql) == 1 and sum("pg_stat_statements s" in q for q in s.sql) == 1


def test_neon_functions_are_read_when_present_and_readable():
    m = metrics(inv.collect_endpoint(Pg(Session()), ep(), EXCL, {}, False, NOW)["inventory_endpoint"])
    assert m["source:neon"]["value"] == 1 and m["source:neon"]["note"] == "neon 1.25"
    assert m["neon.lfc.file_cache_hits"]["value"] == 900 and m["neon.working_set_15m_bytes"]["value"] == 8192000
    assert m["neon.lag_bytes.received_lsn"]["value"] == 0x1000 and m["source:neon_perf_counters"]["value"] == 1
    assert m["neon.perf.getpage_wait_seconds_bucket"]["dims"] == '{"le": 0.001}'
    assert m["autovacuum_workers_busy"]["value"] == 3 and m["server_version"]["note"] == "17.5"
    assert m["idle_clients"]["dims"] == '{"application": "worker", "max_idle_s": 900.0, "role": "app"}'


def test_neon_sources_without_privilege_are_recorded_not_available():
    probe = {**PROBE_ALL, "perf_counters": False}
    m = metrics(inv.collect_endpoint(Pg(Session(probe=probe)), ep(), EXCL, {}, False, NOW)["inventory_endpoint"])
    assert m["source:neon_perf_counters"] == {"metric": "source:neon_perf_counters", "value": 0.0, "dims": None,
                                              "note": "not available: no SELECT on neon_perf_counters"}
    assert "neon.perf.getpage_wait_seconds_bucket" not in m and m["source:neon_get_lfc_stats"]["value"] == 1


def test_no_neon_extension_and_failing_reads_fall_back_quietly():
    m = metrics(inv.neon_metrics(Session(probe=None)))
    assert m["source:neon"]["note"] == "not available: extension neon not installed"
    assert all(m[f"source:{n}"]["value"] == 0 for n, _, _ in inv.NEON_SOURCES)
    m = metrics(inv.neon_metrics(Session(fail={"pg_current_wal_lsn()": RuntimeError("recovery is in progress")})))
    assert m["source:backpressure_lsns"]["note"] == "not available: RuntimeError"
    assert m["source:backpressure_throttling_time"]["value"] == 1


def test_missing_pg_stat_io_and_pg_stat_statements_are_recorded():
    out = inv.collect_endpoint(Pg(Session(statements=False, io=False)), ep(), EXCL, {}, False, NOW)
    m = metrics(out["inventory_endpoint"])
    assert out["inventory_io"] == [] and out["inventory_statements"] == []
    assert m["source:pg_stat_io"]["note"] == "not available: Postgres older than 16"
    assert m["source:pg_stat_statements"]["value"] == 0


def test_activity_deltas_since_the_previous_snapshot():
    at = NOW - timedelta(minutes=15)
    prev = {None: at, ("db", "hc_test", "orders"): {"n_tup_ins": 500, "n_tup_upd": 10, "n_tup_del": 0,
                                                    "seq_scan": 5, "idx_scan": 30, "collected_at": at}}
    [r] = inv.activity_deltas([{**ACTIVITY, "datname": "db"}], prev, True, NOW)
    assert (r["ins_delta"], r["idx_delta"], r["window_s"]) == (1000, 10, 900.0)
    reset = {**prev, ("db", "hc_test", "orders"): {**prev[("db", "hc_test", "orders")], "n_tup_ins": 5000}}
    [r] = inv.activity_deltas([{**ACTIVITY, "datname": "db"}], reset, True, NOW)
    assert r["ins_delta"] is None and r["window_s"] is None


def test_a_new_synced_partition_counts_from_zero_but_not_on_the_first_snapshot():
    part = {**ACTIVITY, "datname": "db", "table_name": "orders_p2", "synced_table": "public.orders"}
    [r] = inv.activity_deltas([part], {None: NOW - timedelta(minutes=15)}, True, NOW)
    assert r["ins_delta"] == 1500 and r["window_s"] == 900.0
    [r] = inv.activity_deltas([part], {}, False, NOW)
    assert r["ins_delta"] is None
    [r] = inv.activity_deltas([{**part, "synced_table": None}], {None: NOW}, True, NOW)
    assert r["ins_delta"] is None


def test_a_synced_partition_recreated_under_the_same_name_counts_from_zero():
    # Live, Task 18: a SNAPSHOT full refresh recreated __db_system.partition_90322 (new oid, same name); its counters
    # restarted at n_tup_ins 10000 with seq_scan 3 < 4, so the name match read it as a stats reset and gave no delta.
    at = NOW - timedelta(minutes=15)
    key = ("db", "__db_system", "partition_90322")
    prev = {None: at, key: {"relid": 90322, "n_tup_ins": 10000, "n_tup_upd": 0, "n_tup_del": 0, "seq_scan": 4,
                            "idx_scan": 0, "collected_at": at}}
    part = {**ACTIVITY, "datname": "db", "schema_name": "__db_system", "table_name": "partition_90322",
            "synced_table": "lakebase_health_test.sync_dst", "relid": 90348, "n_live_tup": 10000,
            "n_tup_ins": 10000, "n_tup_upd": 0, "n_tup_del": 0, "seq_scan": 3, "idx_scan": 0}
    [r] = inv.activity_deltas([part], prev, True, NOW)
    assert (r["ins_delta"], r["seq_delta"], r["window_s"]) == (10000, 3, 900.0)


def test_a_recreated_ordinary_table_gives_no_delta_and_an_unchanged_relid_still_diffs():
    at = NOW - timedelta(minutes=15)
    prev = {None: at, ("db", "hc_test", "orders"): {"relid": 1, "n_tup_ins": 500, "n_tup_upd": 10, "n_tup_del": 0,
                                                    "seq_scan": 5, "idx_scan": 30, "collected_at": at}}
    [r] = inv.activity_deltas([{**ACTIVITY, "datname": "db", "relid": 2}], prev, True, NOW)
    assert r["ins_delta"] is None
    [r] = inv.activity_deltas([{**ACTIVITY, "datname": "db", "relid": 1}], prev, True, NOW)
    assert r["ins_delta"] == 1000
    old = {**prev, ("db", "hc_test", "orders"): {**prev[("db", "hc_test", "orders")], "relid": None}}
    [r] = inv.activity_deltas([{**ACTIVITY, "datname": "db", "relid": 2}], old, True, NOW)
    assert r["ins_delta"] == 1000  # snapshots written before relid existed still match by name


def test_activity_snapshots_record_the_relation_oid():
    from lakebase_health import ddl
    assert "s.relid::bigint AS relid" in inv.ACTIVITY_SQL
    assert "relid BIGINT" in ddl.TABLE_DDL["inventory_activity"]
    assert "relid BIGINT" in ddl.ADDED_COLUMNS["inventory_activity"]


def test_lsn_bytes():
    assert inv.lsn_bytes("16/B374D848") == (0x16 << 32) + 0xB374D848
    assert inv.lsn_bytes(None) is None and inv.lsn_bytes("zz/1") is None


def test_collect_inventory_writes_snapshots_skips_and_cleans_up():
    sql = FakeSql()
    endpoints = [ep(), ep(endpoint_id="e2", current_state="IDLE"), ep(endpoint_id="e3", enrolled=False),
                 ep(endpoint_id="e4", opted_out=True)]
    errors = []
    counts = inv.collect_inventory(sql, CFG, Pg(Session()), EXCL, endpoints, "run1", NOW, errors)
    assert counts == {"endpoints": 1, "skipped": 3} and errors == []
    [row] = sql.tables["cat.lakebase_health.inventory_tables"]
    assert row["run_id"] == "run1" and row["collected_at"] == NOW and row["endpoint_id"] == "e"
    assert row["project_id"] == "p" and row["branch_id"] == "b"
    deletes = sql.ran(r"DELETE FROM cat\.lakebase_health\.inventory_")
    assert len(deletes) == len(inv.INVENTORY_TABLES)
    assert all(a == {"cutoff": NOW - timedelta(days=7)} for s, a in sql.statements if s.startswith("DELETE"))


def test_a_compute_that_suspends_part_way_keeps_its_previous_snapshot():
    sql = FakeSql()
    pg = Pg(Session(), suspended_after=1)
    counts = inv.collect_inventory(sql, CFG, pg, EXCL, [ep(databases=["a", "b"])], "run1", NOW, [])
    assert counts == {"endpoints": 0, "skipped": 1} and pg.opened == ["a"]
    assert "cat.lakebase_health.inventory_tables" not in sql.tables


def test_an_endpoint_error_is_reported_and_the_others_still_collect():
    sql, errors = FakeSql(), []
    class Flaky(Pg):
        @contextmanager
        def session(self, e, db):
            if e.endpoint_id == "bad":
                raise RuntimeError("connection refused")
            yield self.s
    counts = inv.collect_inventory(sql, CFG, Flaky(Session()), EXCL, [ep(endpoint_id="bad"), ep()], "r", NOW, errors)
    assert counts["endpoints"] == 1 and errors == ["pg inventory bad: RuntimeError: connection refused"]


def test_a_write_failure_leaves_no_partial_snapshot_and_the_fleet_and_cleanup_continue():
    # I3: all-or-nothing per endpoint; one endpoint's failed insert does not stop the others or the cleanup
    class Sql(FakeSql):
        def insert_rows(self, table, rows):
            if table.endswith("inventory_settings") and any(r["endpoint_id"] == "bad" for r in rows):
                raise RuntimeError("warehouse hiccup")
            super().insert_rows(table, rows)

        def execute(self, stmt, args=None):
            super().execute(stmt, args)
            if stmt.startswith("DELETE") and "run_id = :run_id" in stmt:
                t = stmt.split()[2]
                self.tables[t] = [r for r in self.tables.get(t, [])
                                  if not (r["endpoint_id"] == args["endpoint_id"] and r["run_id"] == args["run_id"])]
    sql, errors = Sql(), []
    counts = inv.collect_inventory(sql, CFG, Pg(Session()), EXCL, [ep(endpoint_id="bad"), ep()], "r", NOW, errors)
    assert counts["endpoints"] == 1
    assert errors == ["pg inventory bad: write: RuntimeError: warehouse hiccup"]
    written = {r["endpoint_id"] for rows in sql.tables.values() for r in rows}
    assert written == {"e"}
    assert any(t.endswith("inventory_settings") for t in sql.tables)
    assert len(sql.ran(r"DELETE FROM \S+ WHERE collected_at < :cutoff")) == len(inv.INVENTORY_TABLES)


def test_previous_activity_reads_each_endpoints_last_snapshot_as_utc():
    naive = datetime(2026, 10, 29, 11, 45)
    sql = FakeSql(responses=[(r"QUALIFY collected_at = max\(collected_at\)",
                              [{"endpoint_id": "e", "datname": "db", "schema_name": "s", "table_name": "t",
                                "n_tup_ins": 1, "collected_at": naive}])])
    prev = inv.previous_activity(sql, CFG)
    assert prev["e"][None] == naive.replace(tzinfo=timezone.utc)
    assert prev["e"][("db", "s", "t")]["n_tup_ins"] == 1
