from contextlib import contextmanager

SEARCH_PATH_SQL = (
    "DO $$ BEGIN PERFORM set_config('search_path', concat_ws(',', '\"$user\"', 'public', ("
    "SELECT string_agg(DISTINCT quote_ident(n.nspname), ',') FROM pg_extension e "
    "JOIN pg_namespace n ON n.oid = e.extnamespace WHERE n.nspname NOT IN ('public', 'pg_catalog'))), false); END $$"
)
SESSION_SETUP = (
    SEARCH_PATH_SQL,
    "SET default_transaction_read_only = on",
    "SET statement_timeout = '30s'",
    "SET lock_timeout = '1s'",
)
STATS_WINDOW_SQL = (
    "SELECT extract(epoch FROM now() - coalesce((SELECT stats_reset FROM pg_stat_database "
    "WHERE datname = current_database()), pg_postmaster_start_time()))::float8 / 3600.0 AS h"
)


class PgSession:
    def __init__(self, conn):
        self.conn = conn

    def query(self, sql: str) -> list:
        cur = self.conn.execute(sql)
        if cur.description is None:
            return []
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def has_extension(self, name: str) -> bool:
        return bool(self.query(f"SELECT 1 AS x FROM pg_extension WHERE extname = '{name}'"))

    def stats_window_hours(self) -> float | None:
        rows = self.query(STATS_WINDOW_SQL)
        return float(rows[0]["h"]) if rows and rows[0]["h"] is not None else None


class PostgresExecutor:
    def __init__(self, w, user: str, connect=None):
        self.w, self.user = w, user
        if connect is None:
            import psycopg
            connect = psycopg.connect
        self.connect = connect

    def still_active(self, ep) -> bool:
        """The compute's state now, not at inventory time: connecting to a suspended compute would wake it.
        When the state cannot be read, the answer is False (skip rather than risk a wake)."""
        try:
            state = self.w.postgres.get_endpoint(name=ep.endpoint_name).status.current_state
            return str(getattr(state, "value", state)) == "ACTIVE"
        except Exception:
            return False

    @contextmanager
    def session(self, ep, database: str):
        token = self.w.postgres.generate_database_credential(endpoint=ep.endpoint_name).token
        conn = self.connect(host=ep.host, user=self.user, password=token, dbname=database, sslmode="require",
                            autocommit=True, application_name="lakebase-health", connect_timeout=10,
                            prepare_threshold=None)  # the endpoint does not keep auto-prepared statements
        try:
            for stmt in SESSION_SETUP:
                conn.execute(stmt)
            yield PgSession(conn)
        finally:
            conn.close()
