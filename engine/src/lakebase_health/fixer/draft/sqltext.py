"""Token-level helpers for query text: parameters, literals and their placeholders (sqlglot's Postgres tokenizer)."""
import hashlib
import re

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.errors import TokenError
from sqlglot.tokens import TokenType

LITERAL_TOKENS = {TokenType.STRING, TokenType.NUMBER, TokenType.BYTE_STRING, TokenType.HEREDOC_STRING,
                  TokenType.HEX_STRING, TokenType.BIT_STRING, TokenType.NATIONAL_STRING, TokenType.RAW_STRING,
                  TokenType.UNICODE_STRING}


class SqlTextError(ValueError):
    pass


_DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")
_PARAM_AT = re.compile(r"\$\d+")
_IDENT_CHAR = re.compile(r"[A-Za-z0-9_$]")


def space_params(sql: str) -> str:
    """A space after every $n that something other than whitespace follows (`IN ($1,$2)`, `$1||$2`): sqlglot's
    tokenizer reads `$1,$` as a dollar-quote tag. Strings, quoted identifiers, dollar quotes and comments are
    copied unchanged, so the statement means the same."""
    out, i, n = [], 0, len(sql)
    while i < n:
        c = sql[i]
        prev = sql[i - 1] if i else ""
        if c == "'" or (c in "eE" and sql[i + 1:i + 2] == "'" and not _IDENT_CHAR.match(prev)):
            backslash = c != "'"
            j = i + (2 if backslash else 1)
            while j < n:
                if backslash and sql[j] == "\\":
                    j += 2
                elif sql[j] == "'" and sql[j + 1:j + 2] == "'":
                    j += 2
                elif sql[j] == "'":
                    break
                else:
                    j += 1
            out.append(sql[i:j + 1])
            i = j + 1
        elif c == '"':
            j = sql.find('"', i + 1)
            j = n - 1 if j < 0 else j
            out.append(sql[i:j + 1])
            i = j + 1
        elif c == "-" and sql[i + 1:i + 2] == "-":
            j = sql.find("\n", i)
            j = n if j < 0 else j
            out.append(sql[i:j])
            i = j
        elif c == "/" and sql[i + 1:i + 2] == "*":
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append(sql[i:j])
            i = j
        elif c == "$" and not _IDENT_CHAR.match(prev) and _DOLLAR_TAG.match(sql, i):
            tag = _DOLLAR_TAG.match(sql, i).group(0)
            j = sql.find(tag, i + len(tag))
            j = n if j < 0 else j + len(tag)
            out.append(sql[i:j])
            i = j
        elif c == "$" and not _IDENT_CHAR.match(prev) and _PARAM_AT.match(sql, i):
            m = _PARAM_AT.match(sql, i)
            out.append(m.group(0))
            i = m.end()
            if i < n and not sql[i].isspace():
                out.append(" ")
        else:
            out.append(c)
            i += 1
    return "".join(out)


def tokens(sql: str) -> list:
    try:
        return Postgres().tokenize(sql)
    except TokenError as e:
        raise SqlTextError(f"cannot tokenize ({type(e).__name__})") from e  # the message quotes the SQL


def _scan(sql: str):
    """Yields (kind, start, end_inclusive, text): kind 'param' ($n, text = n), 'literal' or 'other'."""
    toks = tokens(sql)
    i = 0
    while i < len(toks):
        t = toks[i]
        nxt = toks[i + 1] if i + 1 < len(toks) else None
        if (t.token_type == TokenType.PARAMETER and nxt is not None and nxt.token_type == TokenType.NUMBER
                and nxt.start == t.end + 1 and nxt.text.isdigit()):
            yield "param", t.start, nxt.end, nxt.text
            i += 2
            continue
        yield ("literal" if t.token_type in LITERAL_TOKENS else "other"), t.start, t.end, sql[t.start:t.end + 1]
        i += 1


def params(sql: str) -> set:
    """The $n placeholders of a statement, as ints."""
    return {int(text) for kind, _, _, text in _scan(sql) if kind == "param"}


def literals(sql: str) -> list:
    return [text for kind, _, _, text in _scan(sql) if kind == "literal"]


def has_comment(sql: str) -> bool:
    return any(t.comments for t in tokens(sql))


def normalize(sql: str) -> tuple:
    """Every literal becomes a new $k (k after the largest $n), like pg_stat_statements. Returns (text, {k: literal})."""
    spans = list(_scan(sql))
    k = max([int(t) for kind, _, _, t in spans if kind == "param"], default=0)
    out, pos, values = [], 0, {}
    for kind, start, end, text in spans:
        if kind != "literal":
            continue
        k += 1
        out.append(sql[pos:start])
        out.append(f"${k}")
        values[k] = text
        pos = end + 1
    out.append(sql[pos:])
    return "".join(out), values


def denormalize(sql: str, values: dict) -> str:
    """Puts the original literal tokens back in place of their $k placeholders."""
    out, pos = [], 0
    for kind, start, end, text in _scan(sql):
        if kind == "param" and int(text) in values:
            out.append(sql[pos:start])
            out.append(values[int(text)])
            pos = end + 1
    out.append(sql[pos:])
    return "".join(out)


def strip_literals(text: str) -> str:
    """Literals become ?: for plan expressions and catalog definitions sent to the model. Untokenizable text -> ?."""
    try:
        spans = list(_scan(text))
    except SqlTextError:
        return "?"
    out, pos = [], 0
    for kind, start, end, _ in spans:
        if kind == "literal":
            out.append(text[pos:start])
            out.append("?")
            pos = end + 1
    out.append(text[pos:])
    return "".join(out)


def text_key(sql: str) -> str:
    """Identity of a statement's shape: normalized (literals as $k), whitespace collapsed, lower-cased."""
    norm, _ = normalize(sql)
    return hashlib.sha256(" ".join(norm.split()).lower().encode()).hexdigest()



def is_cte_reference(table) -> bool:
    """Whether a table reference names a CTE in scope, as Postgres resolves it: only unqualified names; a
    non-recursive WITH shows each CTE to the main body and to the CTEs after it, not to its own body or the ones
    before it (there the name is the real table); WITH RECURSIVE shows every CTE everywhere in the statement."""
    if table.args.get("db") or not table.name:
        return False
    name, child, node = table.name.lower(), table, table.parent
    while node is not None:
        w = node.args.get("with_") or node.args.get("with")
        if isinstance(w, exp.With):
            ctes = list(w.expressions)
            if child is w and not w.args.get("recursive"):  # inside one of this WITH's CTEs: earlier ones only
                ctes = ctes[:next((i for i, c in enumerate(ctes) if _within(table, c)), len(ctes))]
            if any(c.alias_or_name.lower() == name for c in ctes):
                return True
        child, node = node, node.parent
    return False


def _within(node, ancestor) -> bool:
    while node is not None:
        if node is ancestor:
            return True
        node = node.parent
    return False
