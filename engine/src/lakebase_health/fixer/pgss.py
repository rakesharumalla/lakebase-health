"""Normalized statement text from pg_stat_statements on the branch itself (spec §10).

system.lakebase masks query text and parameters for principals outside databricks_pii_access, so the fixer reads
pg_stat_statements directly (its role has pg_read_all_stats through pg_monitor), in read-only sessions. The text is
normalized by Postgres ($n for constants); $n placeholders are spaced for sqlglot."""
from .draft.sqltext import space_params

AVAILABLE_SQL = "SELECT to_regclass('pg_stat_statements') IS NOT NULL"
SCAN = 500  # statements read per database, busiest first, before the table filter


def _text(query) -> str:
    return space_params((query or "").strip().rstrip(";").strip())


def _mean_rows(calls, rows):
    return float(rows) / float(calls) if calls else None


def available(conn) -> bool:
    row = conn.execute(AVAILABLE_SQL).fetchone()
    return bool(row and row[0])


def reads_all(conn) -> bool:
    """Whether the session's role sees other roles' statements: without pg_read_all_stats (pg_monitor), Postgres
    hides their text and queryid."""
    row = conn.execute("SELECT pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')").fetchone()
    return bool(row and row[0])


def entry(conn, queryid, dbid=None) -> dict | None:
    """The top-level entry for queryid (in dbid when known, else the database with the most execution time)."""
    where = "s.queryid = %s AND s.toplevel" + (" AND s.dbid = %s" if dbid is not None else "")
    args = (int(queryid),) + ((int(dbid),) if dbid is not None else ())
    row = conn.execute("SELECT s.query, d.datname, s.dbid::bigint, s.calls, s.rows FROM pg_stat_statements s "
                       f"JOIN pg_database d ON d.oid = s.dbid WHERE {where} "
                       "ORDER BY s.total_exec_time DESC LIMIT 1", args).fetchone()
    if not row or not row[0]:
        return None
    query, datname, db, calls, rows = row
    return {"query": _text(query), "database": datname, "dbid": int(db), "calls": int(calls or 0),
            "mean_rows": _mean_rows(calls, rows)}


def statements(conn, database: str, scan: int = SCAN) -> list:
    """Top-level statements of one database, by total execution time; the fixer's own are left out."""
    rows = conn.execute("SELECT s.queryid::text, s.query, s.calls, s.rows, s.total_exec_time FROM pg_stat_statements s "
                        "WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = %s) AND s.toplevel "
                        "AND s.queryid IS NOT NULL AND s.userid <> (SELECT oid FROM pg_roles WHERE rolname = current_user) "
                        f"ORDER BY s.total_exec_time DESC LIMIT {int(scan)}", (database,)).fetchall()
    return [{"queryid": str(q), "query": _text(text), "calls": int(calls or 0), "mean_rows": _mean_rows(calls, n),
             "total_ms": float(total or 0)} for q, text, calls, n, total in rows]


def generic_plan(conn, query: str) -> dict | None:
    """EXPLAIN (FORMAT JSON, GENERIC_PLAN) of the normalized text: planned with $n unbound, never executed (PG 16+).
    None when Postgres cannot plan it (e.g. a parameter whose type it cannot infer)."""
    try:
        doc = conn.execute(f"EXPLAIN (FORMAT JSON, GENERIC_PLAN) {query}").fetchone()[0]
    except Exception:
        return None
    doc = doc[0] if isinstance(doc, list) and doc else doc
    return doc if isinstance(doc, dict) else None
