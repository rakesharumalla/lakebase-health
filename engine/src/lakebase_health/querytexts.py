"""Normalized statement text for the app (spec §10). system.lakebase masks query text for principals outside
databricks_pii_access, so the analysis job copies pg_stat_statements.query of enrolled branches into query_texts:
one row per (project, branch, endpoint, dbid, queryid), replaced for every compute read this run, kept 7 days.
The column carries the pii_masking mask like every other query-text column. Constants Postgres left in the text
(utility statements) become $k; text that cannot be tokenized is not stored."""
from datetime import timedelta

from .fixer.draft.sqltext import SqlTextError, normalize, space_params

KEEP_DAYS = 7
MAX_PER_COMPUTE = 5000
STATEMENTS_SQL = (
    "SELECT s.dbid::bigint AS dbid, d.datname, s.queryid::bigint AS queryid, s.query FROM pg_stat_statements s "
    "JOIN pg_database d ON d.oid = s.dbid WHERE s.queryid IS NOT NULL AND s.toplevel "
    f"ORDER BY s.total_exec_time DESC LIMIT {MAX_PER_COMPUTE}")


def _normalized(text):
    try:
        norm, _ = normalize(space_params((text or "").strip()))
    except SqlTextError:
        return None
    return norm or None


def collect(postgres, inventory, now) -> tuple:
    """(rows, endpoint ids read, errors). Only active, enrolled, not opted-out computes; never wakes one.
    pg_stat_statements is per compute and cluster-wide, so one database with the extension serves them all."""
    rows, read, errors = [], [], []
    for ep in inventory:
        if ep.opted_out or not ep.enrolled or not ep.active:
            continue
        got = None
        try:
            for db in ep.databases:
                if not postgres.still_active(ep):
                    break
                with postgres.session(ep, db) as s:
                    if s.has_extension("pg_stat_statements"):
                        got = s.query(STATEMENTS_SQL)
                        break
        except Exception as e:  # by type only: a message can carry statement text
            hint = (" (pg_stat_statements is installed in a schema the engine role has no USAGE on)"
                    if type(e).__name__ == "UndefinedTable" else "")
            errors.append(f"query_texts {ep.endpoint_id}: {type(e).__name__}{hint}")
            continue
        if got is None:
            continue
        read.append(ep.endpoint_id)
        seen = set()
        for r in got:
            key = (r["dbid"], r["queryid"])
            text = None if key in seen else _normalized(r["query"])
            seen.add(key)
            if text:
                rows.append({"project_id": ep.project_id, "branch_id": ep.branch_id, "endpoint_id": ep.endpoint_id,
                             "dbid": int(r["dbid"]), "datname": r["datname"], "queryid": int(r["queryid"]),
                             "query": text, "captured_at": now})
    return rows, read, errors


def store(sql, cfg, rows, read, now) -> None:
    sql.execute(f"DELETE FROM {cfg.table('query_texts')} "
                "WHERE array_contains(split(:eps, ','), endpoint_id) OR captured_at < :cutoff",
                {"eps": ",".join(read), "cutoff": now - timedelta(days=KEEP_DAYS)})
    sql.insert_rows(cfg.table("query_texts"), rows)
