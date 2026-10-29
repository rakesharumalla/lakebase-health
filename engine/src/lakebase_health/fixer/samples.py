"""The statements a fix affects, for branch verification (spec §10): pg_stat_statements text on the source branch, in
the finding's database, busiest first. Their parameter values are drawn from the data on the verification branch
(fixer/params.py); system.lakebase text and parameters are not used (masked outside databricks_pii_access)."""
import json
import re

from . import pgss
from .allowlist import QNAME


def _table_regex(table: str) -> str:
    """Java/Python regex for a table reference: bare or qualified with the table's own schema, never another
    schema's table of the same name, a longer identifier or an index named after the table."""
    parts = [p.strip('"') for p in table.split(".")]
    name = rf'"?{re.escape(parts[-1])}"?'
    schema = rf'(?:"?{re.escape(parts[0])}"?\s*\.\s*)?' if len(parts) > 1 else ""
    return rf'(?i)(?<![\w$."]){schema}{name}(?![\w$"])'


_ON_TABLE = re.compile(rf"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\b.*?\sON\s+(?:ONLY\s+)?({QNAME})", re.I | re.S | re.A)


def _on_table(index_def):
    m = _ON_TABLE.match(index_def or "")
    return m.group(1) if m else None


def sample_table(st, action: dict):
    """The table whose queries a fix affects. A DROP INDEX names only the index, so its table comes from the
    rollback (the index definition) or the finding's index_def evidence."""
    if st.table:
        return st.table
    table = _on_table(action.get("rollback_sql"))
    if table:
        return table
    try:
        ev = json.loads(action.get("evidence") or "{}")
    except (TypeError, ValueError):
        ev = {}
    return _on_table(ev.get("index_def")) if isinstance(ev, dict) else None


_MEASURABLE = re.compile(r"(?is)^(select|with|insert|update|delete|values|table)\b")


def affected_queries(conn, table, database, limit=10) -> list:
    """Up to `limit` statements of `database` that read or write `table`, by total execution time, from a read-only
    session on the source branch. [] without the table or without pg_stat_statements."""
    if not table or not pgss.available(conn):
        return []
    rx = re.compile(_table_regex(table))
    out = []
    for s in pgss.statements(conn, database):
        q = s["query"]
        if not rx.search(q) or ";" in q or not _MEASURABLE.match(q):
            continue  # several statements, EXPLAIN, SET or DDL: PREPARE accepts one SELECT/DML statement only
        out.append({"queryid": s["queryid"], "query": q, "mean_rows": s["mean_rows"],
                    "is_write": bool(re.match(r"(?is)^\s*(insert|update|delete)\b", q))})
        if len(out) == limit:
            break
    return out


def no_samples_reason(found: int, unsampled: list, table, database) -> str:
    """Why no statement could be measured, in counts and parameter numbers only (never text or values)."""
    if not table:
        return "could not tell which table the index is on"
    why = f"{found} statements in pg_stat_statements on database {database} mention {table}"
    if unsampled:
        why += f"; {len(unsampled)} could not be sampled: {'; '.join(unsampled[:5])}"
    return why
