"""Branch verification of a rewrite: result equivalence by checksum, speed with the fixer's noise floor, and the
plan change (reported; a mismatch with the model's expectation is a warning, never a failure)."""
import statistics

from ..verify import MIN_INCREASE_MS, SLOWDOWN, SPEEDUP, _walk

# PostgreSQL plan node labels, as EXPLAIN (FORMAT JSON) names them; aggregates by strategy; sub/init plans.
NODE_NAMES = ("Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Bitmap Index Scan", "Tid Scan",
              "Subquery Scan", "Function Scan", "Values Scan", "CTE Scan", "WorkTable Scan", "Nested Loop",
              "Merge Join", "Hash Join", "Hash", "Materialize", "Memoize", "Sort", "Incremental Sort",
              "HashAggregate", "GroupAggregate", "MixedAggregate", "Aggregate", "WindowAgg", "Unique", "SetOp",
              "Append", "Merge Append", "Gather", "Gather Merge", "Limit", "Result", "SubPlan", "InitPlan")
TIMEOUT_FACTOR, TIMEOUT_MIN_MS, TIMEOUT_MAX_MS = 10, 5_000, 60_000  # the rewrite's statement timeout
BUDGET_S = 15 * 60  # the whole verification, all parameter sets
_AGG = {"Hashed": "HashAggregate", "Sorted": "GroupAggregate", "Mixed": "MixedAggregate"}


def _args(conn, values) -> str:
    from psycopg import sql as pgsql
    return ", ".join(pgsql.Literal(v).as_string(conn) for v in values)


def _execute(name, conn, values) -> str:
    return f"EXECUTE {name}({_args(conn, values)})" if values else f"EXECUTE {name}"


def _read_only(conn, name, stmt, values, fn, timeout_ms=None):
    """PREPAREs `stmt` as `name` in a read-only transaction, runs fn(cursor-producing EXECUTE text), always cleans up."""
    conn.execute("BEGIN READ ONLY")
    try:
        if timeout_ms is not None:
            conn.execute(f"SET LOCAL statement_timeout = '{int(timeout_ms)}ms'")
        conn.execute(f"PREPARE {name} AS {stmt}")
        return fn(_execute(name, conn, values))
    finally:
        try:
            conn.execute("ROLLBACK")
        finally:
            conn.execute("DEALLOCATE ALL")


def rewrite_timeout_ms(original_ms) -> int:
    """clamp(10 x the original's median, 5 s, 60 s); the ceiling when the original could not be timed."""
    if original_ms is None:
        return TIMEOUT_MAX_MS
    return int(min(max(TIMEOUT_FACTOR * float(original_ms), TIMEOUT_MIN_MS), TIMEOUT_MAX_MS))


def checksum(conn, q, values, ordered, timeout_ms=None) -> dict:
    """Row count and checksum of q's result: in result order when `ordered`, else as a multiset."""
    if ordered:
        stmt = ("SELECT count(*), md5(coalesce(string_agg(h, ',' ORDER BY n), '')) FROM "
                f"(SELECT md5(t::text) AS h, row_number() OVER () AS n FROM ({q}) AS t) AS s")
    else:
        stmt = ("SELECT count(*), coalesce(sum(('x' || substr(md5(t::text), 1, 15))::bit(60)::bigint), 0)::text "
                f"FROM ({q}) AS t")
    try:
        row = _read_only(conn, "lbh_eq", stmt, values, lambda ex: conn.execute(ex).fetchone(), timeout_ms)
        return {"rows": int(row[0]), "checksum": str(row[1])}
    except Exception as e:  # by type only: messages can carry values
        return {"rows": None, "checksum": None, "error": type(e).__name__}


def plan_shape(conn, q, values) -> dict:
    """Node labels of q's plan (EXPLAIN VERBOSE, not executed) and its query identifier when Postgres computes one."""
    try:
        doc = _read_only(conn, "lbh_pl", q, values,
                         lambda ex: conn.execute(f"EXPLAIN (VERBOSE, FORMAT JSON) {ex}").fetchone()[0][0])
    except Exception as e:
        return {"nodes": [], "queryid": None, "error": type(e).__name__}
    nodes = set()
    for n in _walk(doc["Plan"]):
        t = n["Node Type"]
        nodes.add(_AGG.get(n.get("Strategy"), t) if t == "Aggregate" else t)
        if n.get("Parent Relationship") in ("SubPlan", "InitPlan"):
            nodes.add(n["Parent Relationship"])
    qid = doc.get("Query Identifier")
    return {"nodes": sorted(nodes), "queryid": None if qid is None else str(qid)}


def mentioned(text) -> list:
    """Plan node labels named in free text, longest first so "Hash Join" is not also read as "Hash"."""
    t, found = (text or "").lower(), []
    for name in sorted(NODE_NAMES, key=len, reverse=True):
        if name.lower() in t:
            found.append(name)
            t = t.replace(name.lower(), " ")
    return sorted(found)


def _equivalence(sets, ordered) -> dict:
    bad = []
    for s in sets:
        o, r = s["original"], s["rewrite"]
        if o.get("error") or r.get("error"):
            bad.append(f"set {s['set']}: {'original' if o.get('error') else 'rewrite'} failed "
                       f"({o.get('error') or r.get('error')})")
        elif o["rows"] != r["rows"]:
            bad.append(f"set {s['set']}: {o['rows']} rows vs {r['rows']}")
        elif o["checksum"] != r["checksum"]:
            bad.append(f"set {s['set']}: same row count, different {'rows or order' if ordered else 'rows'}")
    how = "in order" if ordered else "in any order"
    if not bad and not any((s["original"].get("rows") or 0) > 0 for s in sets):  # 0 = 0 shows nothing
        return {"rule": "equivalence", "passed": False,
                "detail": f"no_informative_samples: the original returned no rows for any of {len(sets)} parameter sets"}
    return {"rule": "equivalence", "passed": bool(sets) and not bad,
            "detail": "; ".join(bad) if bad else f"{len(sets)} of {len(sets)} parameter sets: same rows {how}"}


def _speed(sets) -> dict:
    """Median across sets >= 1.2x faster; no set both > 10 % and > 1 ms slower (the fixer's noise floor)."""
    pairs = [(s["original_ms"], s["rewrite_ms"]) for s in sets]
    if not pairs or any(o is None or r is None for o, r in pairs):
        return {"rule": "speed", "passed": False, "detail": "a parameter set could not be timed"}
    b, a = statistics.median(o for o, _ in pairs), statistics.median(r for _, r in pairs)
    speedup = b / a if a > 0 else float("inf")
    slower = [f"set {s['set']} {o:g} -> {r:g} ms" for s, (o, r) in zip(sets, pairs)
              if r > o * SLOWDOWN and r - o > MIN_INCREASE_MS]
    detail = f"median {b:g} -> {a:g} ms, {speedup:.2f}x faster (needs {SPEEDUP}x)"
    if slower:
        detail += f"; slower: {'; '.join(slower)}"
    return {"rule": "speed", "passed": speedup >= SPEEDUP and not slower, "detail": detail}


def _plan(before, after, expected) -> tuple:
    b, a = set(before.get("nodes") or []), set(after.get("nodes") or [])
    removed, added = sorted(b - a), sorted(a - b)
    want = mentioned(expected)
    found = [n for n in want if n in added or n in removed]
    warning = not found
    detail = (f"removed {', '.join(removed) or 'nothing'}; added {', '.join(added) or 'nothing'}; "
              f"expected {', '.join(want) or 'no named plan node'}")
    if warning:
        detail += " (warning: the expected change is not in the new plan)"
    if before.get("error") or after.get("error"):
        detail += f"; EXPLAIN failed: {before.get('error') or after.get('error')}"
    plan = {"original_nodes": sorted(b), "rewrite_nodes": sorted(a), "removed": removed, "added": added,
            "expected_nodes": want, "found": found}
    return {"rule": "plan", "passed": True, "warning": warning, "detail": detail}, plan


def judge(sets, ordered, before, after, expected) -> dict:
    plan_rule, plan = _plan(before, after, expected)
    rules = [_equivalence(sets, ordered), _speed(sets), plan_rule]
    return {"passed": all(r["passed"] for r in rules), "rules": rules, "plan": plan}
