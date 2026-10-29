import re
import statistics

SPEEDUP = 1.2
SLOWDOWN = 1.10
MIN_INCREASE_MS = 1.0  # a slowdown below this is timing noise, whatever its ratio
WRITE_ROUNDS = 3
WRITE_SLOWDOWN = 1.25


def _walk(node):
    yield node
    for ch in node.get("Plans", []):
        yield from _walk(ch)


def measure(conn, q, params, repeats=3) -> dict:
    """Median warm execution of one sampled statement; writes are rolled back."""
    from psycopg import sql as pgsql
    times, nodes, indexes = [], set(), set()
    error = None
    for values in params:
        args = ", ".join(pgsql.Literal(v).as_string(conn) for v in values)
        conn.execute("BEGIN")
        try:
            conn.execute(f"PREPARE lbh_q AS {q}")
            execute = f"EXECUTE lbh_q({args})" if values else "EXECUTE lbh_q"
            conn.execute(execute if q.lower().startswith(("select", "with")) else f"EXPLAIN ANALYZE {execute}")  # warm-up
            for _ in range(repeats):
                plan = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {execute}").fetchone()[0][0]
                times.append(plan["Execution Time"])
                for n in _walk(plan["Plan"]):
                    nodes.add(n["Node Type"])
                    if n.get("Index Name"):
                        indexes.add(n["Index Name"])
        except Exception as e:  # recorded by type only: messages can carry query text or values
            error = type(e).__name__
        finally:
            try:
                conn.execute("ROLLBACK")
            finally:
                conn.execute("DEALLOCATE ALL")  # PREPARE is not transactional; clear it before the next sample
        if error:
            return {"ms": None, "nodes": [], "indexes": [], "error": error}
    return {"ms": statistics.median(times) if times else None, "nodes": sorted(nodes), "indexes": sorted(indexes)}


def _uses(m, index):
    def short(name):
        return (name or "").split(".")[-1].strip('"')
    return any(short(i) == short(index) for i in m.get("indexes", []))


def _plan_changed(b, a) -> bool:
    return set(b.get("nodes") or []) != set(a.get("nodes") or []) or \
        {i.split(".")[-1].strip('"') for i in b.get("indexes") or []} != \
        {i.split(".")[-1].strip('"') for i in a.get("indexes") or []}


def _read_regression(scope, target_index, before, after) -> dict:
    """A query regressed only when the change could have caused it (its plan changed, or its after plan uses the
    changed index) and it is both > 10 % and > 1 ms slower. Other slowdowns are the cold branch's noise band:
    reported with their numbers, never failing."""
    regressed, noise = [], []
    for k in scope:
        b, a = before[k]["ms"], after[k]["ms"]
        ratio = a / b
        if ratio <= SLOWDOWN:
            continue
        caused = _plan_changed(before[k], after[k]) or _uses(after[k], target_index)
        (regressed if caused and a - b > MIN_INCREASE_MS else noise).append(
            f"{k} {b:g}->{a:g} ms ({ratio:.2f}x{'' if caused else ', plan unchanged'})")
    worst = max((after[k]["ms"] / before[k]["ms"] for k in scope
                 if _plan_changed(before[k], after[k]) or _uses(after[k], target_index)), default=1.0)
    detail = f"{len(regressed)} regressed" + (f": {'; '.join(regressed)}" if regressed else "") + \
        f"; worst ratio among changed plans {worst:.2f}"
    if noise:
        detail += f"; noise band (not failing): {'; '.join(noise)}"
    return {"rule": "no_read_regression", "passed": not regressed, "detail": detail}


def measure_writes(measure_fn, conn, queries, rounds=WRITE_ROUNDS) -> dict:
    """Write probe: one warm repetition of every write query per round, alternating between the queries, for
    `rounds` rounds; each query's time is the median of its rounds (samples_ms keeps them for the report)."""
    out = {q["queryid"]: {"samples_ms": [], "nodes": set(), "indexes": set(), "error": None} for q in queries}
    for _ in range(rounds):
        for q in queries:
            r = out[q["queryid"]]
            if r["error"]:
                continue
            m = measure_fn(conn, q["query"], q["params"], repeats=1)
            if m.get("error") or m.get("ms") is None:
                r["error"] = m.get("error") or "no timing"
                continue
            r["samples_ms"].append(m["ms"])
            r["nodes"].update(m.get("nodes") or [])
            r["indexes"].update(m.get("indexes") or [])
    res = {}
    for k, r in out.items():
        base = {"nodes": sorted(r["nodes"]), "indexes": sorted(r["indexes"]), "samples_ms": r["samples_ms"]}
        res[k] = {**base, "ms": None, "error": r["error"]} if r["error"] else \
            {**base, "ms": statistics.median(r["samples_ms"])}
    return res


_UPDATE_SET = re.compile(r"(?is)^\s*UPDATE\s+.+?\s+SET\s+(.*?)(?:\s+(?:FROM|WHERE|RETURNING)\s|$)")
_IDENT = re.compile(r'"((?:[^"]|"")+)"|([A-Za-z_][A-Za-z0-9_$]*)')


def _top_level_split(text):
    parts, depth, quote, cur = [], 0, None, []
    for ch in text:
        if quote:
            quote = None if ch == quote else quote
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    parts.append("".join(cur))
    return parts


def update_set_columns(query: str) -> set:
    """Columns an UPDATE assigns (the left-hand sides of its SET list); empty for anything else."""
    m = _UPDATE_SET.match(query or "")
    if not m:
        return set()
    cols = set()
    for part in _top_level_split(m.group(1)):
        lhs = part.split("=", 1)[0].strip().strip("()")
        for q, plain in _IDENT.findall(lhs):
            cols.add(q.replace('""', '"') if q else plain)
    return cols


def _write_kind(query):
    m = re.match(r"(?is)^\s*(insert|update|delete)\b", query or "")
    return m.group(1).lower() if m else None


def _write_cost(writes, before, after, index_columns) -> dict:
    """Spec 5 write probe: judged are INSERTs and UPDATEs that change a column of the new index, each with a
    median of at least 1 ms. DELETE (Postgres maintains no secondary index on delete; vacuum does), UPDATEs of other
    columns and sub-millisecond statements are reported, not judged. Details carry query ids and timings only."""
    cols = {c.lower() for c in index_columns or []}
    judged, skipped = [], []
    for k in writes:
        b, a = before[k]["ms"], after[k]["ms"]
        line = f"{k} {b:g}->{a:g} ms ({a / b:.2f}x)"
        wk = _write_kind(before[k].get("query"))
        if wk == "delete":
            skipped.append(f"{line}: DELETE maintains no secondary index")
        elif wk == "update" and cols and not {c.lower() for c in update_set_columns(before[k]["query"])} & cols:
            skipped.append(f"{line}: UPDATE does not change an indexed column")
        elif b < MIN_INCREASE_MS:
            skipped.append(f"{line}: under 1 ms")
        else:
            judged.append((a / b, line))
    worst = max((r for r, _ in judged), default=None)
    detail = (f"judged: {'; '.join(l for _, l in judged)}; worst write ratio {worst:.2f}" if judged
              else "no judged write (write cost not measured)")
    if skipped:
        detail += f"; not judged: {'; '.join(skipped)}"
    return {"rule": "write_cost", "passed": worst is None or worst <= WRITE_SLOWDOWN, "detail": detail}


def judge(kind, target_index, before: dict, after: dict, index_columns=None) -> dict:
    rules = []
    if kind in ("analyze", "reloptions"):
        return {"passed": True, "rules": [{"rule": "kind", "passed": True, "detail": f"{kind}: no branch pass rule"}]}
    for k in sorted(set(before) | set(after)):
        err = before.get(k, {}).get("error") or after.get(k, {}).get("error")
        if err:
            rules.append({"rule": "query_error", "passed": False, "detail": f"query {k} failed: {err}"})
    common = [k for k in before if k in after and before[k].get("ms") and after[k].get("ms")]
    if not common:
        rules.append({"rule": "affected_queries", "passed": False,
                      "detail": "no sampled queries on this table could be measured"})
        return {"passed": False, "rules": rules}
    reads = [k for k in common if not before[k].get("is_write")]
    writes = [k for k in common if before[k].get("is_write")]
    scope = common if kind == "drop_index" else reads
    rules.append(_read_regression(scope, target_index, before, after))
    if kind == "create_index":
        users = [k for k in reads if _uses(after[k], target_index)]
        best = max((before[k]["ms"] / after[k]["ms"] for k in users), default=0.0)
        rules.append({"rule": "index_used_with_speedup", "passed": bool(users) and best >= SPEEDUP,
                      "detail": f"{len(users)} queries use it; best speed-up {best:.2f}x"})
        rules.append(_write_cost(writes, before, after, index_columns))
    if kind == "drop_index":
        used = [k for k in common if _uses(before[k], target_index)]
        rules.append({"rule": "index_unused_before", "passed": not used, "detail": f"{len(used)} plans used it"})
    return {"passed": all(r["passed"] for r in rules), "rules": rules}
