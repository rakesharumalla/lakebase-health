import re
from dataclasses import dataclass

IDENT = r'(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)'
COL = rf"{IDENT}(?:\s+(?:ASC|DESC))?"
QNAME = rf"{IDENT}(?:\.{IDENT})?"
_RULES = [
    ("create_index", re.compile(rf"^CREATE\s+INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:{IDENT}\s+)?ON\s+(?P<table>{QNAME})\s*(?:USING\s+\w+\s*)?\((?P<cols>{COL}(?:\s*,\s*{COL})*)\)$", re.I | re.S | re.A)),
    ("drop_index", re.compile(rf"^DROP\s+INDEX\s+CONCURRENTLY\s+(?:IF\s+EXISTS\s+)?(?P<target>{QNAME})$", re.I | re.A)),
    ("analyze", re.compile(rf"^ANALYZE\s+(?P<table>{QNAME})$", re.I | re.A)),
    ("reloptions", re.compile(rf"^ALTER\s+TABLE\s+(?P<table>{QNAME})\s+(?:SET|RESET)\s*\((?P<opts>[^;()]+)\)$", re.I | re.A)),
]


_WS = " \t\r\n\f\v"


class NotAllowed(ValueError):
    pass


@dataclass(frozen=True)
class Statement:
    kind: str
    target: str
    table: str | None


def _strip(sql: str) -> str:
    """Rejects comments and multiple statements outside quoted identifiers; returns the single statement."""
    out, i, quoted = [], 0, False
    while i < len(sql):
        ch = sql[i]
        if quoted:
            out.append(ch)
            if ch == '"':
                if i + 1 < len(sql) and sql[i + 1] == '"':
                    out.append('"')
                    i += 1
                else:
                    quoted = False
        elif ch == '"':
            quoted = True
            out.append(ch)
        elif ord(ch) > 127:
            raise NotAllowed("non-ASCII characters are only allowed inside quoted identifiers")
        elif sql.startswith("--", i) or sql.startswith("/*", i) or ch == "'" or ch == "$":
            raise NotAllowed("comments, string literals and dollar quoting are not allowed")
        elif ch == ";":
            if sql[i + 1:].strip(_WS):
                raise NotAllowed("only one statement is allowed")
            break
        else:
            out.append(ch)
        i += 1
    if quoted:
        raise NotAllowed("unterminated identifier")
    return "".join(out).strip(_WS)


def parse(sql: str) -> Statement:
    stmt = _strip(sql or "")
    for kind, rx in _RULES:
        m = rx.match(stmt)
        if not m:
            continue
        if kind == "create_index" and re.search(r"\bUNIQUE\b", stmt[: m.start("table")], re.I):
            raise NotAllowed("unique indexes are not allowed")
        if kind == "reloptions":
            opts = [o.split("=")[0].strip().lower() for o in m.group("opts").split(",")]
            if not opts or any(not o.startswith("autovacuum_") for o in opts):
                raise NotAllowed("only autovacuum_* table settings are allowed")
        g = m.groupdict()
        table = g.get("table")
        if kind == "analyze" and table.lower() == "verbose":
            raise NotAllowed("ANALYZE VERBOSE is not allowed")
        return Statement(kind, g.get("target") or table, table)
    raise NotAllowed("statement is not one of the allowed fix kinds")


def runnable(sql) -> bool:
    """Whether the fixer would run this statement. Empty or comment-only text never runs."""
    if not (sql or "").strip():
        return False
    try:
        parse(sql)
        return True
    except NotAllowed:
        return False
