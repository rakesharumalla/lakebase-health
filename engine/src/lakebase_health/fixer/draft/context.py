"""The model's input: the normalized query, its generic plan and the catalog of the tables it reads (spec §10).
The query text comes from pg_stat_statements on the finding's branch, read by the fixer in a read-only session
(system.lakebase masks it for principals outside databricks_pii_access). Literal values never enter the context:
the query's literals become $k, plan and catalog literals become ?."""
import json

from .. import pgss
from .sqltext import params, space_params, strip_literals
from .states import finding_queryid

HISTORY_DAYS = 7
# Plan keys whose values are names, not expressions: kept as they are.
NAME_KEYS = frozenset({"Node Type", "Relation Name", "Schema", "Alias", "Index Name", "Join Type", "Strategy",
                       "Parent Relationship", "Subplan Name", "Scan Direction", "Sort Method", "Sort Space Type",
                       "Partial Mode", "Operation", "CTE Name", "Function Name", "Command"})


class NoQueryText(Exception):
    """No normalized text for the finding's query: nothing to draft from."""


def _redacted(v) -> bool:
    return v is None or str(v).startswith("<REDACTED")


def _num(v):
    return None if v is None else float(v)


def strip_plan(node):
    if isinstance(node, dict):
        return {k: (v if k in NAME_KEYS and isinstance(v, str) else strip_plan(v)) for k, v in node.items()}
    if isinstance(node, list):
        return [strip_plan(v) for v in node]
    if isinstance(node, str):
        return strip_literals(node)
    return node


def plan_context(doc):
    """The plan part of a generic EXPLAIN document, literals stripped; None without a plan."""
    return {"Plan": strip_plan(doc["Plan"])} if isinstance(doc, dict) and isinstance(doc.get("Plan"), dict) else None


def finding_dbid(sql, cfg, finding, qid):
    """The database the finding's query ran in, from the telemetry's dbid (never masked): the one with the most
    execution time in the last 7 days. None when the telemetry has no row for it."""
    rows = sql.query(f"""
        SELECT dbid FROM {cfg.raw('pg_stat_statements_counters')}
        WHERE project_id = :p AND endpoint_id = :e AND queryid = CAST(:q AS BIGINT)
          AND ts >= current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS AND dbid IS NOT NULL
        GROUP BY dbid ORDER BY sum(total_exec_time) DESC LIMIT 1""",
                     {"p": finding["project_id"], "e": finding["endpoint_id"], "q": qid})
    return int(rows[0]["dbid"]) if rows and rows[0].get("dbid") is not None else None


def _history_text(sql, cfg, finding, qid):
    """Fallback (spec §10): readable system.lakebase plan-history text, when it is not <REDACTED>. Its parameters and
    plan are never read."""
    rows = sql.query(f"""
        SELECT query_text, dbid FROM {cfg.raw('plan_history')}
        WHERE project_id = :p AND endpoint_id = :e AND queryid = CAST(:q AS BIGINT)
          AND ts >= current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS AND NOT coalesce(truncated, false)
          AND query_text IS NOT NULL AND query_text NOT LIKE '<REDACTED%'
        ORDER BY ts DESC LIMIT 20""", {"p": finding["project_id"], "e": finding["endpoint_id"], "q": qid})
    return next(({"query": r["query_text"].strip().rstrip(";").strip(), "dbid": r.get("dbid")} for r in rows
                 if not _redacted(r.get("query_text")) and r["query_text"].strip()), None)


def _datname(conn, dbid):
    if dbid is None:
        return None
    row = conn.execute("SELECT datname FROM pg_database WHERE oid = %s", (int(dbid),)).fetchone()
    return row[0] if row else None


def load_source(sql, cfg, session, tgt, finding) -> dict:
    """The finding's statement: {queryid, query, database, dbid, mean_rows, origin}. Read from pg_stat_statements on
    the finding's endpoint (cluster-wide, so any database with the extension serves), by queryid and dbid; readable
    plan-history text is the fallback. Raises NoQueryText."""
    qid = finding_queryid(finding.get("check_id"), finding.get("evidence"), finding.get("object"))
    if qid is None:
        raise NoQueryText("the finding names no query id")
    dbid = finding_dbid(sql, cfg, finding, qid)
    installed = False
    for db in tgt["databases"]:
        with session(tgt["endpoint"], tgt["host"], db, "draft_context") as conn:
            if not pgss.available(conn):
                continue
            installed = True
            e = pgss.entry(conn, qid, dbid)
            if e is not None:
                return {"queryid": qid, "query": e["query"], "database": e["database"], "dbid": e["dbid"],
                        "mean_rows": e["mean_rows"], "origin": "pg_stat_statements"}
            fb = _history_text(sql, cfg, finding, qid)
            database = _datname(conn, fb["dbid"]) if fb else None
            if fb and database:
                return {"queryid": qid, "query": space_params(fb["query"]), "database": database,
                        "dbid": int(fb["dbid"]), "mean_rows": None, "origin": "plan_history"}
            if not pgss.reads_all(conn):
                raise NoQueryText("the fixer's Postgres role lacks pg_read_all_stats (grant it pg_monitor on the "
                                  "branch), so pg_stat_statements hides other roles' statements from it")
            break
    if not installed:
        raise NoQueryText("pg_stat_statements is not installed in any database of the branch")
    raise NoQueryText(f"no pg_stat_statements entry for queryid {qid} on the finding's compute (statistics reset or "
                      "evicted) and no readable plan-history text")


def load_catalog(conn, tables) -> list:
    """Columns, constraints, indexes, row estimate and pg_stats summaries (no most-common values or histograms)."""
    out = []
    for schema, name in sorted(tables):
        rel = f"{schema}.{name}" if schema else name
        row = conn.execute("SELECT n.nspname, c.relname, c.reltuples::bigint FROM pg_class c "
                           "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass(%s)", (rel,)).fetchone()
        if not row:
            out.append({"name": rel, "missing": True})
            continue
        nsp, relname, tuples = row
        cols = conn.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull FROM pg_attribute a "
                            "WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped "
                            "ORDER BY a.attnum", (rel,)).fetchall()
        cons = conn.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = to_regclass(%s) "
                            "ORDER BY conname", (rel,)).fetchall()
        idx = conn.execute("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = to_regclass(%s) "
                           "ORDER BY indexrelid", (rel,)).fetchall()
        stats = conn.execute("SELECT attname, n_distinct, null_frac, correlation FROM pg_stats "
                             "WHERE schemaname = %s AND tablename = %s ORDER BY attname", (nsp, relname)).fetchall()
        out.append({"name": f"{nsp}.{relname}", "row_estimate": int(tuples),
                    "columns": [{"name": c[0], "type": c[1], "not_null": bool(c[2])} for c in cols],
                    "constraints": [strip_literals(c[0]) for c in cons],
                    "indexes": [strip_literals(i[0]) for i in idx],
                    "column_stats": [{"column": s[0], "n_distinct": _num(s[1]), "null_frac": _num(s[2]),
                                      "correlation": _num(s[3])} for s in stats]})
    return out


def _numbers(evidence) -> dict:
    try:
        ev = json.loads(evidence) if isinstance(evidence, str) else (evidence or {})
    except ValueError:
        return {}
    return {k: v for k, v in sorted(ev.items()) if isinstance(v, (int, float)) and not isinstance(v, bool)} \
        if isinstance(ev, dict) else {}


def build_context(finding, normalized_query, ordered, plan, tables) -> dict:
    """The user message: finding numbers, the normalized query, the stripped plan and the tables. No values."""
    return {"finding": {"check_id": finding["check_id"], "measure": _num(finding.get("measure")),
                        "evidence": _numbers(finding.get("evidence"))},
            "query": normalized_query, "parameters": [f"${n}" for n in sorted(params(normalized_query))],
            "ordered": ordered, "plan": plan, "tables": tables}
