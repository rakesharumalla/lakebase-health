from .allowlist import parse
from .pg import index_state, with_index_name


def _drop(conn, name: str) -> None:
    conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")


def _safe_drop(conn, name: str) -> str:
    try:
        _drop(conn, name)
        return ""
    except Exception as e:
        return f"; cleanup failed: {type(e).__name__}: {e}"


def execute(conn, stmt_sql: str, action_id: str) -> dict:
    st = parse(stmt_sql)  # NotAllowed propagates before anything runs
    name = None
    sql = stmt_sql.strip()
    if st.kind == "create_index":
        sql, name = with_index_name(sql, (st.table or "").strip(), action_id)
        if index_state(conn, name) != "absent":
            return {"outcome": "apply_failed", "index": None,
                    "error": f"index {name} already exists; refusing to claim it"}
    try:
        conn.execute(sql.rstrip(";"))
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:1000]
        if name and index_state(conn, name) == "invalid":
            err += _safe_drop(conn, name)  # leave nothing half-built; a valid index is kept for a human to judge
        return {"outcome": "apply_failed", "error": err[:1500], "index": None, "sqlstate": getattr(e, "sqlstate", None)}
    if name and index_state(conn, name) != "valid":
        err = "index was built but is not valid" + _safe_drop(conn, name)
        return {"outcome": "apply_failed", "error": err, "index": None}
    return {"outcome": "applied", "error": None, "index": name}


def recover(conn, stmt_sql: str, action_id: str) -> str:
    st = parse(stmt_sql)
    if st.kind == "create_index":
        _, name = with_index_name(stmt_sql.strip(), (st.table or "").strip(), action_id)
        state = index_state(conn, name)
        if state == "valid":
            return "applied"
        if state == "invalid":
            _drop(conn, name)
        return "apply_failed"
    if st.kind == "drop_index":
        return "applied" if index_state(conn, st.target) == "absent" else "apply_failed"
    return "apply_failed"  # ANALYZE / reloptions are short; re-request instead of guessing
