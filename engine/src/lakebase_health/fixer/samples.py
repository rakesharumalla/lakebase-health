import json
import re

from .allowlist import QNAME

_PARAM = re.compile(r"\s*\$(\d+)\s*=\s*(?:'((?:[^']|'')*)'|(NULL))\s*(?:,|$)")


def parse_params(text):
    if not text:
        return []
    out, pos = [], 0
    while pos < len(text):
        m = _PARAM.match(text, pos)
        if not m:
            return None
        out.append(None if m.group(3) else m.group(2).replace("''", "'"))
        pos = m.end()
    return out


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
WINDOWS_HOURS = (24, 168)  # the last day first; a week when the last day has no usable sample


def affected_queries(sql, cfg, project_id, table, limit=10, per_query=3) -> list:
    if not table:
        return []
    for hours in WINDOWS_HOURS:
        out = _affected_queries(sql, cfg, project_id, table, limit, per_query, hours)
        if out:
            return out
    return []


def _affected_queries(sql, cfg, project_id, table, limit, per_query, hours) -> list:
    rows = sql.query(f"""
        WITH s AS (
          SELECT queryid, query_text, parameters, duration_ms,
                 row_number() OVER (PARTITION BY queryid ORDER BY ts DESC) AS rn
          FROM {cfg.raw('plan_history')}
          WHERE project_id = :p AND ts >= current_timestamp() - INTERVAL {int(hours)} HOURS
            AND NOT coalesce(truncated, false) AND query_text RLIKE :rx
        ), ranked AS (
          SELECT queryid, sum(duration_ms) AS total_ms FROM s GROUP BY queryid ORDER BY total_ms DESC LIMIT {int(limit)}
        )
        SELECT cast(s.queryid AS STRING) AS queryid, any_value(s.query_text) AS query,
               collect_list(s.parameters) AS params, max(r.total_ms) AS total_ms
        FROM s JOIN ranked r ON r.queryid = s.queryid
        WHERE s.rn <= {int(per_query)}
        GROUP BY s.queryid ORDER BY total_ms DESC""", {"p": project_id, "rx": _table_regex(table)})
    out = []
    for r in rows:
        q = (r["query"] or "").strip().rstrip(";").strip()
        if ";" in q or not _MEASURABLE.match(q):
            continue  # several statements, EXPLAIN, SET or DDL: PREPARE accepts one SELECT/DML statement only
        params = [p for p in (parse_params(x) for x in (r["params"] or [])) if p is not None]
        if not params:
            if re.search(r"\$\d", q):
                continue  # placeholders but no usable parameter set: unmeasurable, not an error
            params = [[]]
        out.append({"queryid": r["queryid"], "query": q, "params": params,
                    "is_write": bool(re.match(r"(?is)^\s*(insert|update|delete)\b", q))})
    return out


def no_samples_reason(sql, cfg, project_id, table) -> str:
    """Why affected_queries found nothing, in counts only (never query text): for the verification report."""
    if not table:
        return "could not tell which table the index is on"
    hours = WINDOWS_HOURS[-1]
    r = sql.query(f"""
        SELECT count_if(query_text RLIKE :rx) AS matched,
               count_if(query_text = '<REDACTED>' OR parameters = '<REDACTED>') AS redacted,
               count_if(query_text RLIKE :rx AND query_text RLIKE '[$][0-9]'
                        AND (parameters IS NULL OR parameters = '' OR parameters = '<REDACTED>')) AS placeholders
        FROM {cfg.raw('plan_history')}
        WHERE project_id = :p AND ts >= current_timestamp() - INTERVAL {int(hours)} HOURS
          AND NOT coalesce(truncated, false)""", {"p": project_id, "rx": _table_regex(table)})
    r = r[0] if r else {}
    matched, redacted, placeholders = (int(r.get(k) or 0) for k in ("matched", "redacted", "placeholders"))
    parts = [f"{matched} sampled plans in the last {hours // 24} days mention {table}"]
    if placeholders:
        parts.append(f"{placeholders} of them have placeholders without usable bind parameters")
    if redacted:
        parts.append(f"{redacted} plans in that window have redacted query text or parameters "
                     "(copied while PII masking was on, or the fixer is not in databricks_pii_access)")
    return "; ".join(parts)
