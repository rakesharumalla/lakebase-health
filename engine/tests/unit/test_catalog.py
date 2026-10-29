import re

from lakebase_health.checkspec import load_checks
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR

TELEMETRY = {
    "cpu_saturation", "lfc_hit_rate", "buffer_hit_rate", "working_set_vs_cache", "connection_utilization",
    "storage_utilization", "storage_runway", "write_backpressure", "cu_sizing", "rollback_ratio",
    "seq_scan_read_ratio", "deadlocks", "idle_in_tx_time", "top_db_time_query", "query_regression", "plan_flip",
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
    "commit_quorum_wait", "hot_row_lock", "statement_timeouts", "gin_cleanup_lock_wait", "connection_drops",
}
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
    "dead_tuples", "autovacuum_disabled", "autovacuum_scale_factor", "table_bloat", "index_bloat", "xid_age_table",
    "xmin_horizon_holder", "long_transaction", "idle_in_transaction", "blocked_session", "prepared_transaction",
    "replication_slot_retention", "sequence_exhaustion", "stale_statistics", "public_schema_create",
    "privileged_roles", "table_without_pk", "non_ssl_sessions", "guardrail_settings", "config_drift",
    "autovacuum_blocker", "gin_pending_list", "oversized_search_index",
}
API = {"backup_restore_window", "default_branch_unprotected", "primary_without_ha", "password_roles"}


def specs():
    return {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}


def test_catalog_ids():
    assert set(specs()) == TELEMETRY | POSTGRES | API


def test_sources_match_folders():
    s = specs()
    assert all(s[i].source == "delta" for i in TELEMETRY)
    assert all(s[i].source == "postgres" for i in POSTGRES)
    assert all(s[i].source == "api" for i in API)


def test_telemetry_checks_are_fleet_wide_and_located():
    for i in TELEMETRY:
        sql = specs()[i].sql
        assert ":project_id" not in sql, i
        for col in ("project_id", "branch_id", "endpoint_id", "object", "measure"):
            assert re.search(rf"\b{col}\b", sql), (i, col)


def test_postgres_checks_use_exclusions():
    for i in POSTGRES:
        sql = specs()[i].sql
        if "pg_replication_slots" in sql:
            assert "{excluded_slots}" in sql, i
        if "pg_stat_activity" in sql:
            assert "{excluded_users}" in sql, i
        if re.search(r"pg_(class|namespace|stat_user_tables|stat_user_indexes|statio_user_tables)\b", sql):
            assert "{excluded_schemas}" in sql, i


def test_actionable_fixes_have_rollback():
    for s in specs().values():
        if s.fix and s.fix.actionable:
            assert s.fix.rollback or s.fix.rollback_note, s.id


def test_unused_index_requires_agreement_from_replicas():
    assert specs()["unused_index"].replica_check == "intersect"


def test_incident_checks_cover_the_platform_vs_application_split():
    s = specs()
    assert "SyncRep" in s["commit_quorum_wait"].sql
    assert "blocker_class" in s["hot_row_lock"].sql and "SyncRep" in s["hot_row_lock"].sql
    assert "57014" in s["statement_timeouts"].sql
    assert "compute_mode" in s["connection_drops"].sql
    assert s["gin_pending_list"].requires_extension == "pgstattuple"


def test_gin_cleanup_wait_uses_wait_counters_not_session_samples():
    # Session history does not sample GIN cleanup (Lock/page) waits; the per-interval counters do.
    sql = specs()["gin_cleanup_lock_wait"].sql
    assert "system.lakebase.wait_event_counters" in sql and "active_session_history" not in sql
