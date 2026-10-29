"""The report's telemetry checks: rendered SQL parses as Databricks SQL and reads the right tables.

Their semantics are exercised on the warehouse by the acceptance task (scripts/report_acceptance.py)."""
import pytest
import sqlglot

from lakebase_health.checkspec import load_checks
from lakebase_health.config import EngineConfig
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.executors.delta import rewrite_tables
from lakebase_health.jobs.run import CHECKS_DIR

SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
CFG = EngineConfig(catalog="cat", schema="lbh")
NEW_DELTA = ["autovacuum_saturation", "synced_full_refresh", "wal_burst", "getpage_latency_spike"]


def rendered(check_id):
    return rewrite_tables(SPECS[check_id].sql, CFG)


def test_engine_tables_are_rewritten_to_the_configured_schema():
    out = rewrite_tables("SELECT * FROM lakebase_health.inventory_endpoint e JOIN system.lakebase.plan_history p "
                         "JOIN lakebase_health.findings f", CFG)
    assert "cat.lbh.inventory_endpoint e" in out and "cat.lbh.raw_plan_history p" in out and "cat.lbh.findings f" in out
    assert rewrite_tables("SELECT * FROM cat.lakebase_health.findings", CFG) == "SELECT * FROM cat.lakebase_health.findings"


@pytest.mark.parametrize("check_id", NEW_DELTA)
def test_new_telemetry_check_sql_parses(check_id):
    sql = rendered(check_id)
    assert "lakebase_health." not in sql.replace("cat.lbh.", "")
    sqlglot.parse_one(sql, read="databricks")


def test_autovacuum_saturation_needs_three_busy_snapshots_and_an_hour_over_threshold():
    s = SPECS["autovacuum_saturation"]
    assert s.area == "vacuum_xid" and s.thresholds == {"warning": 1.0}
    assert "HAVING count(*) = 3" in s.sql and "busy >= max_workers" in s.sql
    assert "n_dead_tup > vacuum_threshold" in s.sql and "last_under" in s.sql
    assert s.fix.actionable and "autovacuum_vacuum_cost_limit" in s.fix.template


def test_synced_full_refresh_is_info_or_warning_when_it_overlaps_a_getpage_spike():
    s = SPECS["synced_full_refresh"]
    assert s.area == "write_path" and s.thresholds == {"info": 0.0, "warning": 1.0}
    assert "synced_table IS NOT NULL" in s.sql and "ins_delta >= 0.5 * greatest(n_live_tup, 1)" in s.sql
    assert "p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20" in s.sql and "THEN 2.0 ELSE 1.0" in s.sql
    assert "wal_bytes" in s.sql and "wal_fpi" in s.sql and s.fix.kind == "manual"


def test_wal_burst_counts_bursts_over_ten_times_the_median_and_a_gib():
    s = SPECS["wal_burst"]
    assert s.area == "write_path" and s.thresholds == {"info": 0.0, "warning": 3.0}
    assert "b.wal_bytes > 10 * m.median_bytes AND b.wal_bytes > 1073741824" in s.sql
    assert "system.lakebase.pg_stat_statements_counters" in s.sql and "top_wal_queryid" in s.sql
    assert "query AS" not in s.sql and "max(query)" not in s.sql  # names the statement by id only


def test_getpage_latency_spike_compares_five_minute_p99_with_the_24h_baseline():
    s = SPECS["getpage_latency_spike"]
    assert s.area == "storage_latency" and s.thresholds == {"info": 0.0, "warning": 2.0}
    assert "neon_getpage_wait_us / 1000.0 / neon_getpage_count" in s.sql and "p.execs >= 5" in s.sql
    assert "p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20" in s.sql and "query_text" not in s.sql


@pytest.mark.parametrize("check_id", ["wal_burst", "synced_full_refresh"])
def test_wal_aggregates_keep_platform_roles_so_the_sync_pipeline_counts(check_id):
    # I6: WAL volume is an endpoint property; the synced-table writer (databricks_writer_n) must count
    from lakebase_health.executors.delta import DeltaExecutor
    from tests.unit.test_executors import EXCL

    class Sql:
        def query(self, stmt):
            self.stmt = stmt
            return []
    sql = Sql()
    assert SPECS[check_id].query_scoped is False
    DeltaExecutor(sql, CFG, EXCL).run(SPECS[check_id])
    assert "cat.lbh.raw_pg_stat_statements_counters" in sql.stmt
    assert "LEFT ANTI JOIN" not in sql.stmt  # no platform-role (or tool) filter on the WAL sums
    sqlglot.parse_one(sql.stmt, read="databricks")


def test_wal_burst_labels_the_top_writer_by_role_and_names_the_sync_pipeline():
    s = SPECS["wal_burst"]
    assert "top_wal_role" in s.sql and "plan_history" in s.sql  # the role name of the top WAL writer's userid
    assert "'^databricks_writer_[0-9]+$'" in s.sql and "'synced-table pipeline ('" in s.sql
    assert "top_wal_source" in s.sql and "{top_wal_source}" in s.fix.template
    assert "synced-table pipeline" in s.explain


def test_getpage_spike_warns_only_for_three_of_six_consecutive_windows():
    # I7: one slow 5-minute window (a pageserver hiccup after a resume) is information; a warning needs at least 3
    # spike windows within 6 consecutive 5-minute windows (30 min)
    from lakebase_health.severity import classify
    s = SPECS["getpage_latency_spike"]
    assert s.measure == "measure" and s.thresholds == {"info": 0.0, "warning": 2.0}
    assert [classify(n, s.thresholds, s.direction) for n in (1, 2, 3, 6)] == ["info", "info", "warning", "warning"]
    assert "RANGE BETWEEN 1500 PRECEDING AND CURRENT ROW" in s.sql  # this window and the 5 before it
    assert "max(in_30_min)" in s.sql and "worst_p99_ms" in s.sql
    assert "3 of 6" in s.explain
