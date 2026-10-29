"""Inventory snapshots for the health report: one read-only pass per active, enrolled endpoint per analysis run.

Everything goes through the read-only Postgres executor (pg_monitor role, default_transaction_read_only), which never
wakes a compute: the state is re-read right before each connection, and an endpoint that suspends part-way keeps its
previous snapshot (nothing partial is written). Query text is never read. Rows older than 7 days are deleted.

The `neon` extension's functions are read when the extension is installed and the engine role may execute or select
them; each source records whether it was available (metric `source:<name>`, 1 or 0, with the reason in `note`).
Without them the report falls back to the system.lakebase telemetry (compute_counters, compute_gauges) for LFC,
working set and back-pressure, and to plan_history for getpage latency. Nothing here creates an extension or grants.
"""
import json
from datetime import timedelta, timezone

from .checkspec import render_text

RETENTION = timedelta(days=7)
TOP_TABLES = 25
TOP_STATEMENTS = 20
INVENTORY_TABLES = ("inventory_tables", "inventory_indexes", "inventory_settings", "inventory_statements",
                    "inventory_io", "inventory_activity", "inventory_endpoint")

KEY_SETTINGS = (
    "shared_buffers", "work_mem", "maintenance_work_mem", "effective_cache_size", "random_page_cost",
    "max_connections", "max_wal_size", "checkpoint_timeout", "default_statistics_target", "statement_timeout",
    "lock_timeout", "idle_in_transaction_session_timeout", "autovacuum_max_workers", "neon.file_cache_size_limit",
    "track_io_timing", "log_min_duration_statement", "shared_preload_libraries",
)
# Settings a customer cannot change on Lakebase, whatever their context says (spec: support-managed)
SUPPORT_MANAGED = frozenset({"track_io_timing", "log_min_duration_statement", "shared_preload_libraries",
                             "neon.file_cache_size_limit", "shared_buffers", "max_connections"})

_TOP = f"""SELECT s.relid FROM pg_stat_user_tables s
  WHERE s.schemaname NOT IN {{excluded_schemas}} ORDER BY pg_total_relation_size(s.relid) DESC LIMIT {TOP_TABLES}"""

TABLES_SQL = f"""
SELECT s.schemaname AS schema_name, s.relname AS table_name, st.synced_table,
       pg_total_relation_size(s.relid) AS total_bytes, pg_relation_size(s.relid) AS data_bytes,
       pg_indexes_size(s.relid) AS index_bytes,
       CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_total_relation_size(c.reltoastrelid) END AS toast_bytes,
       s.n_live_tup, s.n_dead_tup, s.seq_scan, coalesce(s.idx_scan, 0) AS idx_scan,
       io.heap_blks_read, io.heap_blks_hit,
       s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze,
       s.vacuum_count, s.autovacuum_count, s.analyze_count, s.autoanalyze_count,
       (coalesce(substring(array_to_string(c.reloptions, ',') FROM 'autovacuum_vacuum_threshold=([0-9]+)')::float8,
                 current_setting('autovacuum_vacuum_threshold')::float8)
        + coalesce(substring(array_to_string(c.reloptions, ',') FROM 'autovacuum_vacuum_scale_factor=([0-9.]+)')::float8,
                   current_setting('autovacuum_vacuum_scale_factor')::float8) * greatest(c.reltuples, 0))
         AS vacuum_threshold,
       CASE WHEN c.relkind = 'r' THEN age(c.relfrozenxid) END AS xid_age,
       (SELECT string_agg(o, ', ' ORDER BY o) FROM unnest(c.reloptions) o WHERE o LIKE 'autovacuum%')
         AS autovacuum_overrides
FROM ({_TOP}) t JOIN pg_stat_user_tables s ON s.relid = t.relid JOIN pg_class c ON c.oid = s.relid
LEFT JOIN pg_statio_user_tables io ON io.relid = s.relid
LEFT JOIN {{synced_tables}} st ON st.relid = s.relid
ORDER BY total_bytes DESC
"""

INDEXES_SQL = f"""
SELECT n.nspname AS schema_name, tc.relname AS table_name, ic.relname AS index_name, am.amname AS method,
       pg_relation_size(i.indexrelid) AS index_bytes, coalesce(s.idx_scan, 0) AS idx_scan,
       i.indisvalid AS is_valid, i.indisprimary AS is_primary, i.indisunique AS is_unique, st.synced_table,
       (SELECT quote_ident(pn.nspname) || '.' || quote_ident(pc.relname)
        FROM pg_inherits h JOIN pg_class pc ON pc.oid = h.inhparent JOIN pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE h.inhrelid = i.indexrelid) AS parent_index
FROM ({_TOP}) t JOIN pg_index i ON i.indrelid = t.relid
JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace JOIN pg_am am ON am.oid = ic.relam
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
LEFT JOIN {{synced_tables}} st ON st.relid = i.indrelid
ORDER BY index_bytes DESC LIMIT 200
"""

# Cumulative per-table counters; the deltas against the previous snapshot are computed in Python. Synced storage
# always comes first so a refreshed synced table is never cut by the LIMIT.
ACTIVITY_SQL = """
SELECT s.relid::bigint AS relid, s.schemaname AS schema_name, s.relname AS table_name, st.synced_table, s.n_live_tup,
       s.n_tup_ins, s.n_tup_upd, s.n_tup_del, s.seq_scan, coalesce(s.idx_scan, 0) AS idx_scan
FROM pg_stat_user_tables s LEFT JOIN {synced_tables} st ON st.relid = s.relid
WHERE s.schemaname NOT IN {excluded_schemas}
ORDER BY (st.relid IS NOT NULL) DESC, s.n_tup_ins + s.n_tup_upd + s.n_tup_del DESC LIMIT 200
"""

SETTINGS_SQL = """
SELECT name, setting, unit, source, context FROM pg_settings
WHERE name IN ({names}) OR (source NOT IN ('default', 'override') AND name !~ {excluded_settings_regex})
ORDER BY name LIMIT 300
"""

STATEMENTS_SQL = """
WITH s AS (
  SELECT s.queryid, r.rolname AS role, d.datname, sum(s.calls) AS calls, sum(s.total_exec_time) AS total_ms,
         sum(s.total_exec_time) / nullif(sum(s.calls), 0) AS mean_ms, sum(s.shared_blks_hit) AS shared_blks_hit,
         sum(s.shared_blks_read) AS shared_blks_read, sum(s.temp_blks_written) AS temp_blks_written
  FROM pg_stat_statements s JOIN pg_roles r ON r.oid = s.userid JOIN pg_database d ON d.oid = s.dbid
  WHERE s.queryid IS NOT NULL AND NOT {platform} AND NOT {tool}
  GROUP BY s.queryid, r.rolname, d.datname),
ranked AS (
  SELECT s.*, row_number() OVER (ORDER BY total_ms DESC) AS time_rank,
         row_number() OVER (ORDER BY shared_blks_read DESC) AS io_rank FROM s)
SELECT 'time' AS ranking, time_rank AS rank, queryid::text AS queryid, role, datname, calls, mean_ms, total_ms,
       shared_blks_hit, shared_blks_read, temp_blks_written FROM ranked WHERE time_rank <= {n}
UNION ALL
SELECT 'io', io_rank, queryid::text, role, datname, calls, mean_ms, total_ms,
       shared_blks_hit, shared_blks_read, temp_blks_written FROM ranked WHERE io_rank <= {n}
"""

IO_SQL = """
SELECT backend_type, object, context, reads, writes, extends, hits, evictions, reuses, fsyncs,
       CASE WHEN current_setting('track_io_timing') = 'on' THEN read_time END AS read_time,
       CASE WHEN current_setting('track_io_timing') = 'on' THEN write_time END AS write_time, stats_reset
FROM pg_stat_io
WHERE coalesce(reads, 0) + coalesce(writes, 0) + coalesce(extends, 0) + coalesce(hits, 0)
      + coalesce(evictions, 0) + coalesce(fsyncs, 0) > 0
"""

ENDPOINT_SQL = """
SELECT (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker')::float8
         AS autovacuum_workers_busy,
       current_setting('autovacuum_max_workers')::float8 AS autovacuum_max_workers,
       (SELECT extract(epoch FROM now() - min(xact_start)) FROM pg_stat_activity
        WHERE xact_start IS NOT NULL AND backend_type = 'client backend' AND pid <> pg_backend_pid())::float8
         AS oldest_xact_s,
       extract(epoch FROM now() - pg_postmaster_start_time())::float8 AS uptime_s,
       (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')::float8 AS client_sessions,
       current_setting('max_connections')::float8 AS max_connections,
       current_setting('server_version_num')::float8 AS server_version_num,
       current_setting('server_version') AS server_version
"""

IDLE_SQL = """
SELECT usename AS role, coalesce(nullif(application_name, ''), '(no application_name)') AS application,
       count(*)::float8 AS sessions, round(max(extract(epoch FROM now() - state_change)))::float8 AS max_idle_s
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND state = 'idle' AND pid <> pg_backend_pid()
  AND usename NOT IN {excluded_users} AND coalesce(application_name, '') NOT IN ('lakebase-health', 'lakebase-health-fixer')
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 50
"""

NEON_PROBE_SQL = """
SELECT e.extversion AS version,
  coalesce(has_function_privilege(to_regprocedure(quote_ident(n.nspname) || '.neon_get_lfc_stats()'), 'EXECUTE'),
           false) AS lfc_stats,
  coalesce(has_function_privilege(
           to_regprocedure(quote_ident(n.nspname) || '.approximate_working_set_size_seconds(integer)'), 'EXECUTE'),
           false) AS working_set,
  coalesce(has_function_privilege(to_regprocedure(quote_ident(n.nspname) || '.backpressure_throttling_time()'),
           'EXECUTE'), false) AS throttling,
  coalesce(has_function_privilege(to_regprocedure(quote_ident(n.nspname) || '.backpressure_lsns()'), 'EXECUTE'),
           false) AS lsns,
  coalesce(has_table_privilege(to_regclass(quote_ident(n.nspname) || '.neon_perf_counters'), 'SELECT'), false)
    AS perf_counters
FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'neon'
"""
NEON_LFC_SQL = "SELECT lfc_key, lfc_value FROM neon_get_lfc_stats() AS t(lfc_key text, lfc_value bigint)"
NEON_WORKING_SET_SQL = ("SELECT approximate_working_set_size_seconds(900)::float8 AS pages, "
                        "current_setting('block_size')::float8 AS block_size")
NEON_THROTTLING_SQL = "SELECT backpressure_throttling_time()::float8 AS us"
NEON_LSNS_SQL = "SELECT * FROM backpressure_lsns()"
CURRENT_LSN_SQL = "SELECT pg_current_wal_lsn()::text AS lsn"
NEON_PERF_SQL = "SELECT * FROM neon_perf_counters WHERE metric LIKE 'getpage%'"
# The five neon sources and the probe column that says whether the engine role may read each
NEON_SOURCES = (("neon_get_lfc_stats", "lfc_stats", "EXECUTE"),
                ("approximate_working_set_size_seconds", "working_set", "EXECUTE"),
                ("backpressure_throttling_time", "throttling", "EXECUTE"),
                ("backpressure_lsns", "lsns", "EXECUTE"),
                ("neon_perf_counters", "perf_counters", "SELECT"))
HAS_PG_STAT_IO_SQL = "SELECT to_regclass('pg_catalog.pg_stat_io') IS NOT NULL AS present"


class Suspended(Exception):
    """The compute suspended part-way: the endpoint keeps its previous snapshot."""


def how_to_change(name: str, context: str | None) -> str:
    if name in SUPPORT_MANAGED or context in ("postmaster", "sighup", "internal"):
        return "support-managed"
    return "session" if context == "user" else "role or database"


def lsn_bytes(lsn) -> int | None:
    """'16/B374D848' -> bytes; None for anything else."""
    if not isinstance(lsn, str) or "/" not in lsn:
        return None
    hi, lo = lsn.split("/", 1)
    try:
        return (int(hi, 16) << 32) + int(lo, 16)
    except ValueError:
        return None


def _num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _metric(name, value, note=None, dims=None) -> dict:
    return {"metric": name, "value": _num(value), "note": note,
            "dims": json.dumps(dims, sort_keys=True, default=str) if dims else None}


def _try(session, sql):
    """(rows, None) or ([], 'ErrorType'); one failed read never stops the snapshot."""
    try:
        return session.query(sql), None
    except Exception as e:
        return [], type(e).__name__


def neon_metrics(session) -> list:
    """The neon extension's endpoint-level values, or `source:*` rows saying why each one is not available."""
    probe, err = _try(session, NEON_PROBE_SQL)
    if err or not probe:
        why = f"not available: {err}" if err else "not available: extension neon not installed"
        return [_metric("source:neon", 0, why)] + [_metric(f"source:{n}", 0, why) for n, _, _ in NEON_SOURCES]
    p = probe[0]
    out = [_metric("source:neon", 1, f"neon {p.get('version')}")]
    readers = {
        "neon_get_lfc_stats": lambda: [_metric(f"neon.lfc.{r['lfc_key']}", r["lfc_value"])
                                       for r in session.query(NEON_LFC_SQL) if r.get("lfc_key")],
        "approximate_working_set_size_seconds": lambda: [
            _metric("neon.working_set_15m_bytes", r["pages"] * r["block_size"])
            for r in session.query(NEON_WORKING_SET_SQL) if r.get("pages") is not None],
        "backpressure_throttling_time": lambda: [_metric("neon.backpressure_throttling_us", r["us"])
                                                 for r in session.query(NEON_THROTTLING_SQL)],
        "backpressure_lsns": lambda: _lsn_lags(session),
        "neon_perf_counters": lambda: [
            _metric(f"neon.perf.{r['metric']}", r.get("value"),
                    dims={"le": r["bucket_le"]} if r.get("bucket_le") is not None else None)
            for r in session.query(NEON_PERF_SQL) if r.get("metric")],
    }
    for name, col, privilege in NEON_SOURCES:
        if not p.get(col):
            out.append(_metric(f"source:{name}", 0, f"not available: no {privilege} on {name}"))
            continue
        try:
            rows = readers[name]()
        except Exception as e:
            out.append(_metric(f"source:{name}", 0, f"not available: {type(e).__name__}"))
            continue
        out.append(_metric(f"source:{name}", 1))
        out.extend(rows)
    return out


def _lsn_lags(session) -> list:
    current = lsn_bytes((session.query(CURRENT_LSN_SQL) or [{}])[0].get("lsn"))  # raises on a replica: not read
    out = []
    for r in session.query(NEON_LSNS_SQL)[:1]:
        for k, v in r.items():
            b = lsn_bytes(v)
            if b is not None and current is not None:
                out.append(_metric(f"neon.lag_bytes.{k}", max(current - b, 0)))
    return out


def endpoint_metrics(session, excl) -> list:
    out = []
    rows, err = _try(session, ENDPOINT_SQL)
    for r in rows[:1]:
        for k, v in r.items():
            out.append(_metric(k, None, v) if k == "server_version" else _metric(k, v))
    if err:
        out.append(_metric("source:pg_stat_activity", 0, f"not available: {err}"))
    idle, err = _try(session, render_text(IDLE_SQL, excl))
    out += [_metric("idle_clients", r["sessions"], dims={"role": r["role"], "application": r["application"],
                                                          "max_idle_s": r["max_idle_s"]}) for r in idle]
    return out + neon_metrics(session)


def settings_rows(session, excl) -> list:
    names = ", ".join(f"'{n}'" for n in KEY_SETTINGS)
    rows = session.query(render_text(SETTINGS_SQL.replace("{names}", names), excl))
    return [{**r, "how_to_change": how_to_change(r["name"], r.get("context"))} for r in rows]


def statements_rows(session, excl) -> list:
    # The text is matched inside Postgres for the tool tag and never returned
    return session.query(STATEMENTS_SQL.format(platform=excl.platform_role_pg("r.rolname"),
                                               tool=excl.tool_text_pg("s.query"), n=TOP_STATEMENTS))


def io_rows(session) -> tuple:
    present, err = _try(session, HAS_PG_STAT_IO_SQL)
    if err or not present or not present[0].get("present"):
        return [], _metric("source:pg_stat_io", 0, f"not available: {err or 'Postgres older than 16'}")
    return session.query(IO_SQL), _metric("source:pg_stat_io", 1)


def activity_deltas(rows: list, prev: dict, had_previous: bool, now) -> list:
    """Cumulative counters -> deltas since the previous snapshot of the same endpoint. A counter that went backwards
    (stats reset) gives no delta. A relation whose oid changed under the same name was recreated and is treated as
    missing from the previous snapshot. A synced storage table missing from a previous snapshot (a new partition, or
    one recreated by a full refresh) counts from zero."""
    out = []
    for r in rows:
        p = prev.get((r.get("datname"), r["schema_name"], r["table_name"]))
        if p is not None and p.get("relid") is not None and r.get("relid") is not None and p["relid"] != r["relid"]:
            p = None
        if p is None and had_previous and r.get("synced_table"):
            p = {"n_tup_ins": 0, "n_tup_upd": 0, "n_tup_del": 0, "seq_scan": 0, "idx_scan": 0,
                 "collected_at": prev.get(None)}
        d = {"ins_delta": None, "upd_delta": None, "del_delta": None, "seq_delta": None, "idx_delta": None,
             "window_s": None}
        if p is not None:
            pairs = [("ins_delta", "n_tup_ins"), ("upd_delta", "n_tup_upd"), ("del_delta", "n_tup_del"),
                     ("seq_delta", "seq_scan"), ("idx_delta", "idx_scan")]
            diffs = {k: (r.get(c) or 0) - (p.get(c) or 0) for k, c in pairs}
            if all(v >= 0 for v in diffs.values()):
                d.update(diffs)
                at = p.get("collected_at")
                d["window_s"] = (now - at).total_seconds() if at is not None else None
        out.append({**r, **d})
    return out


def collect_endpoint(pg, ep, excl, prev: dict, had_previous: bool, now) -> dict:
    """{inventory table short name: rows} for one endpoint. Raises Suspended if the compute stops part-way."""
    out = {t: [] for t in INVENTORY_TABLES}
    instance_done, statements_done = False, False
    for db in ep.databases:
        if not pg.still_active(ep):  # re-read right before connecting: never wake a compute
            raise Suspended(ep.endpoint_id)
        with pg.session(ep, db) as s:
            out["inventory_tables"] += [{**r, "datname": db} for r in s.query(render_text(TABLES_SQL, excl))]
            out["inventory_indexes"] += [{**r, "datname": db} for r in s.query(render_text(INDEXES_SQL, excl))]
            activity = [{**r, "datname": db} for r in s.query(render_text(ACTIVITY_SQL, excl))]
            out["inventory_activity"] += activity_deltas(activity, prev, had_previous, now)
            if not statements_done and s.has_extension("pg_stat_statements"):
                out["inventory_statements"] = statements_rows(s, excl)
                statements_done = True
            if not instance_done:
                out["inventory_settings"] = settings_rows(s, excl)
                out["inventory_io"], io_source = io_rows(s)
                out["inventory_endpoint"] = endpoint_metrics(s, excl) + [io_source]
                instance_done = True
    if instance_done and not statements_done:
        out["inventory_endpoint"].append(_metric("source:pg_stat_statements", 0,
                                                 "not available: extension pg_stat_statements not installed"))
    return out


def previous_activity(sql, cfg) -> dict:
    """endpoint id -> {(datname, schema, table): counters, None: collected_at} from each endpoint's last snapshot."""
    rows = sql.query(f"SELECT * FROM {cfg.table('inventory_activity')} "
                     "QUALIFY collected_at = max(collected_at) OVER (PARTITION BY endpoint_id)")
    out = {}
    for r in rows:
        at = r.get("collected_at")  # Spark returns naive UTC timestamps
        at = at.replace(tzinfo=timezone.utc) if at is not None and at.tzinfo is None else at
        r = {**r, "collected_at": at}
        by = out.setdefault(r["endpoint_id"], {None: at})
        by[(r.get("datname"), r.get("schema_name"), r.get("table_name"))] = r
    return out


def write_snapshot(sql, cfg, snapshot: dict, key: dict, errors: list) -> bool:
    """All-or-nothing per endpoint: the snapshot is staged in memory by collect_endpoint, then written table by
    table; if any insert fails, this run's rows for the endpoint are removed again, so readers keep seeing the
    previous complete snapshot (Delta has no multi-table transaction)."""
    try:
        for table, rows in snapshot.items():
            sql.insert_rows(cfg.table(table), [{**key, **r} for r in rows])
        return True
    except Exception as e:
        errors.append(f"pg inventory {key['endpoint_id']}: write: {type(e).__name__}: {e}"[:500])
    for table in INVENTORY_TABLES:
        try:
            sql.execute(f"DELETE FROM {cfg.table(table)} WHERE endpoint_id = :endpoint_id AND run_id = :run_id",
                        {"endpoint_id": key["endpoint_id"], "run_id": key["run_id"]})
        except Exception as e:
            errors.append(f"pg inventory {key['endpoint_id']}: undo {table}: {type(e).__name__}: {e}"[:500])
    return False


def collect_inventory(sql, cfg, pg, excl, inventory, run_id, now, errors: list) -> dict:
    """Snapshot every active, enrolled, not opted-out endpoint; delete snapshots older than 7 days."""
    counts = {"endpoints": 0, "skipped": 0}
    try:
        prev_all = previous_activity(sql, cfg)
    except Exception as e:
        errors.append(f"pg inventory: previous snapshot: {type(e).__name__}: {e}"[:500])
        prev_all = {}
    for ep in inventory:
        if ep.opted_out or not ep.enrolled or not ep.active:
            counts["skipped"] += 1
            continue
        prev = prev_all.get(ep.endpoint_id, {})
        try:
            snapshot = collect_endpoint(pg, ep, excl, prev, bool(prev), now)
        except Suspended:
            counts["skipped"] += 1
            continue
        except Exception as e:
            errors.append(f"pg inventory {ep.endpoint_id}: {type(e).__name__}: {e}"[:500])
            continue
        key = {"run_id": run_id, "collected_at": now, "project_id": ep.project_id, "branch_id": ep.branch_id,
               "endpoint_id": ep.endpoint_id}
        if write_snapshot(sql, cfg, snapshot, key, errors):
            counts["endpoints"] += 1
    for table in INVENTORY_TABLES:
        try:
            sql.execute(f"DELETE FROM {cfg.table(table)} WHERE collected_at < :cutoff", {"cutoff": now - RETENTION})
        except Exception as e:
            errors.append(f"pg inventory cleanup {table}: {type(e).__name__}: {e}"[:500])
    return counts
