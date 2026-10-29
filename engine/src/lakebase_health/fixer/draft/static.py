"""Static checks of a model's rewrite against the normalized original, before any branch exists."""
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .sqltext import SqlTextError, has_comment, is_cte_reference, literals, params

# sqlglot's names for the functions a rewrite may call (it normalizes some: date_trunc -> timestamp_trunc,
# string_agg -> group_concat, bool_and -> logical_and). Functions the original calls are allowed as well.
SAFE_FUNCTIONS = frozenset({
    "count", "sum", "avg", "min", "max", "coalesce", "nullif", "greatest", "least", "cast", "lower", "upper",
    "length", "abs", "round", "floor", "ceil", "trim", "substring", "concat", "extract", "timestamp_trunc",
    "row_number", "rank", "dense_rank", "lag", "lead", "first_value", "last_value", "ntile", "percent_rank",
    "cume_dist", "array_agg", "array", "group_concat", "logical_and", "logical_or", "case", "if",
})
DENIED_FUNCTIONS = frozenset({
    "pg_sleep", "pg_sleep_for", "pg_sleep_until", "set_config", "current_setting", "pg_read_file",
    "pg_read_binary_file", "pg_ls_dir", "lo_import", "lo_export", "dblink", "dblink_exec", "nextval", "setval",
    "pg_terminate_backend", "pg_cancel_backend", "pg_advisory_lock", "pg_advisory_xact_lock", "pg_notify",
    "txid_current", "pg_reload_conf", "pg_switch_wal", "rand", "random",
})
# Literals a rewrite may add: EXISTS (SELECT 1), LIMIT 1, coalesce(x, 0).
ALLOWED_LITERALS = frozenset({"0", "1"})
_STRUCTURAL = (exp.Binary, exp.Connector, exp.Unary, exp.Predicate)


@dataclass(frozen=True)
class Shape:
    tables: frozenset      # (schema, name), lower-cased; schema "" when unqualified
    functions: frozenset
    columns: int | None    # None when the select list has a star
    ordered: bool          # top-level ORDER BY
    ctes: frozenset = frozenset()  # CTE names, lower-cased


class StaticError(ValueError):
    def __init__(self, check: str, detail: str):
        super().__init__(f"{check}: {detail}")
        self.check, self.detail = check, detail


def _function_names(tree) -> set:
    out = set()
    for f in tree.find_all(exp.Func):
        if isinstance(f, _STRUCTURAL):
            continue
        if isinstance(f, exp.Anonymous):
            out.add(f.name.lower())
        else:
            names = f.sql_names()
            out.add((names[0] if names else f.key).lower())
    return out


def _error_type(e: Exception) -> str:
    """Parse and tokenizer messages quote the SQL around the error, literal values included: the type only."""
    cause = e.__cause__ if isinstance(e, SqlTextError) and e.__cause__ is not None else e
    return type(cause).__name__


def shape(sql: str) -> Shape:
    """Parses one SELECT/WITH statement; raises StaticError otherwise."""
    try:
        trees = [t for t in sqlglot.parse(sql, read="postgres") if t is not None]
    except (ParseError, SqlTextError) as e:
        raise StaticError("parse", _error_type(e)) from e
    if len(trees) != 1:
        raise StaticError("one_statement", f"{len(trees)} statements")
    tree = trees[0]
    if not isinstance(tree, (exp.Select, exp.SetOperation)):
        raise StaticError("select_only", f"{type(tree).__name__} is not a SELECT")
    if any(True for _ in tree.find_all(exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        raise StaticError("data_modifying_cte", "INSERT, UPDATE, DELETE or MERGE inside the query")
    for s in tree.find_all(exp.Select):
        if s.args.get("locks"):
            raise StaticError("locking", "FOR UPDATE/SHARE is not allowed")
        if s.args.get("into"):
            raise StaticError("select_into", "SELECT INTO creates a table")
    ctes = frozenset(c.alias_or_name.lower() for c in tree.find_all(exp.CTE))
    tables = frozenset((t.db.lower(), t.name.lower()) for t in tree.find_all(exp.Table)
                       if t.name and not is_cte_reference(t))
    selects = tree.selects
    star = any(isinstance(s, exp.Star) or (isinstance(s, exp.Column) and isinstance(s.this, exp.Star)) for s in selects)
    return Shape(tables, frozenset(_function_names(tree)), None if star else len(selects),
                 tree.args.get("order") is not None, ctes)


def check(original: str, rewrite: str, visible=()) -> Shape:
    """`original` is the normalized original (literals as $k); `visible` the table names in the catalog context.
    Returns the rewrite's shape or raises StaticError."""
    try:
        if has_comment(rewrite):
            raise StaticError("comment", "comments (and hint comments) are not allowed")
        rp, op, lits = params(rewrite), params(original), literals(rewrite)
    except SqlTextError as e:
        raise StaticError("parse", _error_type(e)) from e
    o, r = shape(original), shape(rewrite)
    if rp != op:
        raise StaticError("parameters", f"uses {sorted(rp)}, the original uses {sorted(op)}")
    new = sorted({lit for lit in lits if lit not in ALLOWED_LITERALS})
    if new:
        raise StaticError("new_literal", f"{len(new)} literal(s) not in the original")
    known = {name for _, name in o.tables} | {str(v).rsplit(".", 1)[-1].strip('"').lower() for v in visible}
    shadowing = sorted(r.ctes & known)
    if shadowing:
        raise StaticError("cte_shadows_table", f"CTE {', '.join(shadowing)} has the name of a table")
    extra = sorted(".".join(p for p in t if p) for t in r.tables - o.tables)
    if extra:
        raise StaticError("other_table", f"reads {', '.join(extra)}, which the original does not")
    denied = sorted(r.functions & DENIED_FUNCTIONS)
    unsafe = sorted(r.functions - SAFE_FUNCTIONS - o.functions - DENIED_FUNCTIONS)
    if denied or unsafe:
        raise StaticError("unsafe_function", ", ".join(denied + unsafe))
    if o.columns is not None and r.columns is not None and o.columns != r.columns:
        raise StaticError("column_count", f"{r.columns} output columns, the original has {o.columns}")
    return r
