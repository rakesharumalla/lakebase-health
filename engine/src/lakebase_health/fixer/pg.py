import re
from contextlib import contextmanager

from ..executors.postgres import SEARCH_PATH_SQL, tagged
from .allowlist import IDENT, parse

LONG = {"create_index", "drop_index"}
# Catalog and pg_stat_statements reads on the target branch, and the rewrite's verification on its branch (model SQL:
# the database keeps it read-only, and each statement sets a tighter SET LOCAL statement_timeout of its own).
READ_ONLY = {"draft_context", "stats", "query_rewrite"}


def FIXER_SETUP(kind: str) -> tuple:
    if kind in READ_ONLY:
        return (SEARCH_PATH_SQL, "SET default_transaction_read_only = on", "SET lock_timeout = '1s'",
                "SET statement_timeout = '30s'")
    return (SEARCH_PATH_SQL, "SET lock_timeout = '5s'",
            f"SET statement_timeout = '{'2h' if kind in LONG else '10min'}'")


CREDENTIAL_DENIED = "credential_denied"


class CredentialDenied(Exception):
    """The fixer may not generate a Postgres credential for the endpoint (not enrolled on the project)."""
    sqlstate = CREDENTIAL_DENIED


class TaggedConn:
    """The fixer's connection: every statement leads with the /* lakebase-health */ tag (executors.postgres)."""

    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        return self.conn.execute(tagged(sql)) if params is None else self.conn.execute(tagged(sql), params)

    def close(self):
        self.conn.close()

    def __getattr__(self, name):
        # everything else (adapters, connection, pgconn, info ...) is the wrapped connection's: psycopg's
        # Literal(v).as_string(conn) in verify.measure and the draft verify needs them
        return getattr(self.conn, name)


class FixerPg:
    def __init__(self, w, user: str, connect=None):
        self.w, self.user = w, user
        if connect is None:
            import psycopg
            connect = psycopg.connect
        self.connect = connect

    @contextmanager
    def session(self, endpoint_name: str, host: str, database: str, kind: str):
        from databricks.sdk.errors import PermissionDenied
        try:
            token = self.w.postgres.generate_database_credential(endpoint=endpoint_name).token
        except PermissionDenied as e:
            raise CredentialDenied() from e
        conn = self.connect(host=host, user=self.user, password=token, dbname=database, sslmode="require",
                            autocommit=True, application_name="lakebase-health-fixer", connect_timeout=10,
                            prepare_threshold=None)  # the endpoint does not keep auto-prepared statements
        try:
            for stmt in FIXER_SETUP(kind):
                conn.execute(stmt)
            yield TaggedConn(conn)
        finally:
            conn.close()


def index_state(conn, qualified_index: str) -> str:
    row = conn.execute("SELECT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass(%s)",
                       (qualified_index,)).fetchone()
    if not row:
        return "absent"
    return "valid" if row[0] else "invalid"


def fixer_index_name(action_id: str) -> str:
    return "lbh_" + action_id[:12].lower()


_CREATE_HEAD = re.compile(r"^CREATE\s+INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?", re.I | re.A)
_SCHEMA = re.compile(rf"^({IDENT})\.{IDENT}$", re.A)


def with_index_name(sql: str, schema_qualified_table: str, action_id: str) -> tuple:
    """Pins the index name a CREATE INDEX CONCURRENTLY will produce, so recovery can find exactly that index."""
    if parse(sql).kind != "create_index":
        raise ValueError("with_index_name only accepts CREATE INDEX CONCURRENTLY statements")
    s = sql.strip()
    head = _CREATE_HEAD.match(s)
    rest = s[head.end():]
    schema = _SCHEMA.match(schema_qualified_table)
    prefix = f"{schema.group(1)}." if schema else ""
    if re.match(r"ON\s", rest, re.I):
        name = fixer_index_name(action_id)
        return f"{s[:head.end()]}{name} {rest}", prefix + name
    name = re.match(IDENT, rest).group(0)
    return s, prefix + name
