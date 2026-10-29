"""Parameter values for a normalized statement (pg_stat_statements text with $n), drawn from the data (spec §10).

Each $n is mapped to the column it is compared with (`=`, `<>`, `IN`, `BETWEEN`, `<`/`>`, `LIKE` prefix) or to
`LIMIT`/`OFFSET` (small integers). Up to 3 value sets are drawn from existing rows in read-only transactions with a
statement timeout, on the verification branch. The values stay in the job: they are never stored, logged or sent to
the model. A parameter bound to no column makes the statement unsampled."""
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .draft.sqltext import SqlTextError, is_cte_reference, params, space_params

MAX_SETS = 3
SAMPLE_ROWS = 1000           # TABLESAMPLE SYSTEM aims at about this many rows before the random pick
RANGE_ROWS = (100, 10000)    # a $lo..$hi range spans the statement's mean rows per call, clamped to this
DEFAULT_RANGE_ROWS = 1000
LIMITS = ("10", "25", "50")
OFFSETS = ("0", "5", "10")
LIKE_PREFIX = 3
TIMEOUT = "10s"
_COMPARE = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
_DENIED = {"42501"}


class Unsampled(Exception):
    """No value sets for the statement; the message names the parameter or table, never a value."""


@dataclass(frozen=True)
class Binding:
    param: int
    role: str            # eq | low | high | like | limit | offset
    table: str | None    # as written, exact names ("hc_test.orders")
    column: str | None   # exact name
    parts: tuple = ()    # (schema, name) or (name,), exact names


def _name(ident) -> str:
    return ident.name if getattr(ident, "quoted", False) else ident.name.lower()


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_key(t: exp.Table) -> tuple:
    parts = tuple(_name(p) for p in (t.args.get("db"), t.this) if p is not None and p.name)
    return parts


def _unwrap(node):
    while isinstance(node.parent, (exp.Cast, exp.Paren)):
        node = node.parent
    return node


def _bare(node):
    while isinstance(node, (exp.Cast, exp.Paren)):
        node = node.this
    return node


def _number(p) -> int | None:
    lit = p.this
    text = getattr(lit, "this", lit)
    return int(text) if str(text).isdigit() else None


def _role(node):
    """(role, column expression) for the parameter (unwrapped from casts), or (role, None) for LIMIT/OFFSET."""
    parent = node.parent
    if isinstance(parent, exp.Limit) and parent.args.get("expression") is node:
        return "limit", None
    if isinstance(parent, exp.Offset) and parent.args.get("expression") is node:
        return "offset", None
    if isinstance(parent, _COMPARE):
        right = parent.expression is node
        other = _bare(parent.this if right else parent.expression)
        if isinstance(parent, (exp.EQ, exp.NEQ)):
            return "eq", other
        lower = isinstance(parent, (exp.GT, exp.GTE))
        return ("low" if lower == right else "high"), other
    if isinstance(parent, exp.Between):
        if parent.args.get("low") is node:
            return "low", _bare(parent.this)
        if parent.args.get("high") is node:
            return "high", _bare(parent.this)
    if isinstance(parent, exp.In) and node in parent.expressions:
        return "eq", _bare(parent.this)
    if isinstance(parent, (exp.Like, exp.ILike)) and parent.expression is node:
        return "like", _bare(parent.this)
    return None, None


def _own_tables(select):
    """The FROM/JOIN tables of one SELECT, not those of its subqueries, CTE references excluded."""
    return [t for t in select.find_all(exp.Table) if t.find_ancestor(exp.Select) is select]


def _scope_tables(col):
    select = col.find_ancestor(exp.Select)
    return [] if select is None else [t for t in _own_tables(select) if not is_cte_reference(t)]


def _alias_key(col, qualifier):
    """The table a qualified column names, resolved like Postgres: the nearest SELECT that has a FROM item with that
    alias (or bare name), then the enclosing ones (correlated references). None for a CTE or derived table."""
    select = col.find_ancestor(exp.Select)
    while select is not None:
        for t in _own_tables(select):
            key = _table_key(t)
            if key and (t.alias or key[-1]).lower() == qualifier:
                return None if is_cte_reference(t) else key
        for sub in select.find_all(exp.Subquery):  # a derived table's alias hides the outer names too
            if sub.find_ancestor(exp.Select) is select and sub.alias and sub.alias.lower() == qualifier:
                return None
        select = select.find_ancestor(exp.Select)
    return None


def bindings(query: str, columns_of) -> list:
    """One Binding per $n (sorted), or Unsampled. `columns_of(table)` gives a table's column names (or None)."""
    try:
        text = space_params(query)
        tree = sqlglot.parse_one(text, read="postgres")
        wanted = params(text)
    except (ParseError, SqlTextError, ValueError) as e:
        raise Unsampled(f"cannot parse the statement ({type(e).__name__})") from e
    found = {}
    for p in tree.find_all(exp.Parameter):
        n = _number(p)
        if n is None or n in found:
            continue
        role, other = _role(_unwrap(p))
        if role in ("limit", "offset"):
            found[n] = Binding(n, role, None, None)
            continue
        if role is None or not isinstance(other, exp.Column):
            continue
        col = _name(other.this)
        if other.table:
            key = _alias_key(other, other.table.lower())
            candidates = [key] if key else []
        else:
            candidates = list(dict.fromkeys(_table_key(t) for t in _scope_tables(other)))
            if len(candidates) > 1:
                candidates = [k for k in candidates if col in (columns_of(".".join(k)) or ())]
        if len(candidates) != 1:
            continue
        key = candidates[0]
        known = columns_of(".".join(key))
        if known is not None and col not in known:
            continue
        found[n] = Binding(n, role, ".".join(key), col, key)
    for n in range(1, max(wanted, default=0) + 1):
        if n not in found:
            raise Unsampled(f"parameter ${n} is not bound to a column")
    return [found[n] for n in sorted(found)]


def _columns(conn, ref: str):
    rows = conn.execute("SELECT a.attname FROM pg_attribute a WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 "
                        "AND NOT a.attisdropped", (ref,)).fetchall()
    return {r[0] for r in rows} if rows else None


def _quoted_ref(parts) -> str:
    return ".".join(_quote(p) for p in parts)


def check(conn, query: str) -> list:
    """The bindings, with column names from the live catalog (no rows are read)."""
    cache = {}

    def columns_of(ref):
        if ref not in cache:
            cache[ref] = _columns(conn, _quoted_ref(ref.split(".")))
        return cache[ref]
    return bindings(query, columns_of)


def _width(mean_rows) -> int:
    if mean_rows is None:
        return DEFAULT_RANGE_ROWS
    return int(min(max(round(float(mean_rows)), RANGE_ROWS[0]), RANGE_ROWS[1]))


def _draw_sql(ref, cols, ranges, width, pct, n) -> str:
    q = [_quote(c) for c in cols]
    values = [f"s.{c}::text" for c in q]
    values += [f"(SELECT max(r.{_quote(c)}) FROM (SELECT x.{_quote(c)} FROM {ref} x WHERE x.{_quote(c)} >= s.{_quote(c)} "
               f"ORDER BY x.{_quote(c)} LIMIT {width}) r)::text" for c in ranges]
    sample = f" TABLESAMPLE SYSTEM ({pct:.4f})" if pct is not None else ""
    return (f"SELECT {', '.join(values)} FROM (SELECT {', '.join(q)} FROM {ref}{sample} "
            f"WHERE {' AND '.join(f'{c} IS NOT NULL' for c in q)} ORDER BY random() LIMIT {n}) s")


def _like(value: str) -> str:
    prefix = value[:LIKE_PREFIX]
    return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _draw(conn, parts, cols, ranges, width, n) -> list:
    ref = _quoted_ref(parts)
    row = conn.execute("SELECT c.reltuples::float8 FROM pg_class c WHERE c.oid = to_regclass(%s)", (ref,)).fetchone()
    tuples = max(float(row[0]) if row and row[0] is not None else 0.0, 1.0)
    pct = min(100.0, max(0.01, 100.0 * SAMPLE_ROWS / tuples))
    rows = conn.execute(_draw_sql(ref, cols, ranges, width, pct, n)).fetchall()
    if not rows:  # a sparse or tiny table: the sample can miss every row
        rows = conn.execute(_draw_sql(ref, cols, ranges, width, None, n)).fetchall()
    return [tuple(r) for r in rows]


def sample(conn, query: str, mean_rows=None, sets: int = MAX_SETS) -> list:
    """Up to `sets` distinct value lists ($1 first), or [[]] for a statement without parameters. Raises Unsampled.
    The catalog lookups and draws run in one read-only transaction with a statement timeout."""
    if not params(space_params(query)):
        return [[]]
    conn.execute("BEGIN READ ONLY")
    try:
        conn.execute(f"SET LOCAL statement_timeout = '{TIMEOUT}'")
        binds = check(conn, query)
        tables = _tables(binds)
        drawn = {}
        for parts, t in tables.items():
            try:
                drawn[parts] = _draw(conn, parts, t["cols"], t["ranges"], _width(mean_rows), sets)
            except Exception as e:  # by type only: a message can carry values
                if getattr(e, "sqlstate", None) in _DENIED:
                    raise
                raise Unsampled(f"sampling {'.'.join(parts)} failed ({type(e).__name__})") from e
            if not drawn[parts]:
                raise Unsampled(f"no rows with values in {'.'.join(parts)}")
    finally:
        conn.execute("ROLLBACK")
    return _sets(binds, tables, drawn, sets)


def _tables(binds) -> dict:
    """Per table: the columns to draw (one row serves every column of the table) and the range columns."""
    tables = {}
    for b in binds:
        if b.table is None:
            continue
        t = tables.setdefault(b.parts, {"cols": [], "ranges": []})
        if b.column not in t["cols"]:
            t["cols"].append(b.column)
        paired = any(o.role == "low" and o.column == b.column and o.parts == b.parts for o in binds)
        if b.role == "high" and paired and b.column not in t["ranges"]:
            t["ranges"].append(b.column)
    return tables


def _sets(binds, tables, drawn, sets) -> list:
    count = min([len(r) for r in drawn.values()] + [sets])
    out = []
    for i in range(count):
        values = []
        for b in binds:
            if b.role == "limit":
                values.append(LIMITS[i % len(LIMITS)])
            elif b.role == "offset":
                values.append(OFFSETS[i % len(OFFSETS)])
            else:
                t, row = tables[b.parts], drawn[b.parts][i]
                if b.role == "high" and b.column in t["ranges"]:
                    v = row[len(t["cols"]) + t["ranges"].index(b.column)]
                else:
                    v = row[t["cols"].index(b.column)]
                values.append(_like(v) if b.role == "like" else v)
        if values not in out:
            out.append(values)
    return out
