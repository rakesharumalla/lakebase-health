TABLE_DDL = {
    "runs": "run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP, status STRING, step_seconds STRING, "
            "counts STRING, errors STRING",
    "ingest_watermarks": "source_table STRING, max_ts TIMESTAMP, last_run_id STRING, rows_added BIGINT, seconds DOUBLE",
    "inventory": "run_id STRING, captured_at TIMESTAMP, project_id STRING, project_name STRING, branch_id STRING, "
                 "branch_name STRING, is_default_branch BOOLEAN, is_protected BOOLEAN, endpoint_id STRING, "
                 "endpoint_name STRING, endpoint_type STRING, current_state STRING, host STRING, min_cu DOUBLE, "
                 "max_cu DOUBLE, suspend_timeout_s BIGINT, ha_min INT, restore_window_s BIGINT, "
                 "pg_native_login BOOLEAN, opted_out BOOLEAN, enrolled BOOLEAN, databases STRING, "
                 "password_roles STRING, last_pg_check_at TIMESTAMP",
    "check_results": "run_id STRING, ts TIMESTAMP, check_id STRING, fingerprint STRING, project_id STRING, "
                     "branch_id STRING, endpoint_id STRING, object STRING, measure DOUBLE, severity STRING, "
                     "confidence STRING, status STRING, error STRING, evidence STRING, query_text STRING",
    "findings": "fingerprint STRING, check_id STRING, title STRING, category STRING, project_id STRING, "
                "branch_id STRING, endpoint_id STRING, object STRING, status STRING, severity STRING, "
                "confidence STRING, measure DOUBLE, first_seen TIMESTAMP, last_seen TIMESTAMP, "
                "last_checked_at TIMESTAMP, occurrences BIGINT, clean_runs INT, evidence STRING, query_text STRING, "
                "explain STRING, candidate_sql STRING, rollback_sql STRING, suppressed_until TIMESTAMP, "
                "resolved_at TIMESTAMP, rollback_note STRING",
    "proposed_actions": "action_id STRING, fingerprint STRING, check_id STRING, kind STRING, sql STRING, "
                        "rollback_sql STRING, risk STRING, reversible BOOLEAN, verify_on_branch BOOLEAN, "
                        "state STRING, created_at TIMESTAMP, updated_at TIMESTAMP, expires_at TIMESTAMP, "
                        "rollback_note STRING",
    "shadow_log": "action_id STRING, evaluated_at TIMESTAMP, run_id STRING, would_allow_level STRING, "
                  "would_execute BOOLEAN, reason STRING, outcome_score DOUBLE",
    "ledger": "event_id STRING, action_id STRING, event_type STRING, actor STRING, payload STRING, ts TIMESTAMP, "
              "prev_hash STRING, hash STRING",
    "approvals": "approval_id STRING, action_id STRING, request STRING, sql_hash STRING, approver STRING, "
                 "requested_at TIMESTAMP, note STRING",
    "action_runs": "run_id STRING, approval_id STRING, action_id STRING, kind STRING, started_at TIMESTAMP, "
                   "ended_at TIMESTAMP, outcome STRING, report STRING, branch STRING, error STRING",
    "metrics_daily": "day DATE, project_id STRING, endpoint_id STRING, metric STRING, value DOUBLE",
}

# Columns added after the first release: CREATE TABLE IF NOT EXISTS leaves existing tables as they are
ADDED_COLUMNS = {
    "findings": ("rollback_note STRING",),
    "proposed_actions": ("rollback_note STRING",),
}

MASKED_COLUMNS = {
    "raw_plan_history": ("query_text", "plan_json", "parameters"),
    "raw_pg_stat_statements_counters": ("query",),
    "raw_postgres_logs": ("statement",),
    "raw_ddl_history": ("query_text", "ddl_json"),
    "findings": ("query_text",),
    "check_results": ("query_text",),
}


def ensure_schema(sql, cfg) -> None:
    sql.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    for name, cols in TABLE_DDL.items():
        sql.execute(f"CREATE TABLE IF NOT EXISTS {cfg.table(name)} ({cols})")
    for name, added in ADDED_COLUMNS.items():
        have = {str(r.get("col_name", "")).lower() for r in sql.query(f"SHOW COLUMNS IN {cfg.table(name)}")}
        if not have:
            continue  # nothing to compare against
        for col in added:
            if col.split()[0].lower() not in have:
                sql.execute(f"ALTER TABLE {cfg.table(name)} ADD COLUMNS ({col})")
    sql.execute(
        f"CREATE FUNCTION IF NOT EXISTS {cfg.table('mask_text')}(v STRING) RETURNS STRING "
        "RETURN CASE WHEN is_account_group_member('databricks_pii_access') THEN v ELSE '<REDACTED>' END")


def set_masks(sql, cfg, table: str) -> None:
    """SET MASK on every masked column of one table (by short name). Raises on any failure."""
    fn = cfg.table("mask_text")
    for col in MASKED_COLUMNS.get(table, ()):
        sql.execute(f"ALTER TABLE {cfg.table(table)} ALTER COLUMN {col} SET MASK {fn}")


def ensure_masks(sql, cfg) -> None:
    """Sets the mask_text column masks, or with cfg.pii_masking off removes them (idempotent either way)."""
    fn = cfg.table("mask_text")
    for table, cols in MASKED_COLUMNS.items():
        full = cfg.table(table)
        if not sql.table_exists(full):
            continue
        for col in cols:
            if cfg.pii_masking:
                sql.execute(f"ALTER TABLE {full} ALTER COLUMN {col} SET MASK {fn}")
                continue
            try:
                sql.execute(f"ALTER TABLE {full} ALTER COLUMN {col} DROP MASK")
            except Exception as e:  # tolerated only when the column has no mask (already dropped or never set)
                if any(m in str(e).upper() for m in ("PERMISSION", "PRIVILEGE", "NOT AUTHORIZED")):
                    raise  # masks would silently stay while the app shows text to everyone
