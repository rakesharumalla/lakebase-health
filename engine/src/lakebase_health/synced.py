"""Synced tables (Unity Catalog tables synced into Lakebase).

Lakebase keeps a synced table's rows in storage partitions under the `__db_system` schema; the partition root
(`pg_partition_root()`) is the table the user sees. A table owned by a pipeline writer role (`databricks_writer_<n>`)
is a synced table too (an unpartitioned one is its own storage). Indexes on synced storage are managed by the sync:
Postgres refuses to drop a partition's index, and a refresh rebuilds them.
"""

SYNCED_SCHEMA = "__db_system"
WRITER_ROLE_RE = "^databricks_writer_[0-9]+$"

# relid -> synced table, for every relation that stores a synced table (a partition, or the table itself).
# Spliced into check SQL by checkspec.render_sql as the {synced_tables} token.
SYNCED_TABLES_SQL = (
    "(SELECT c.oid AS relid, quote_ident(rn.nspname) || '.' || quote_ident(r.relname) AS synced_table "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_class r ON r.oid = coalesce(pg_partition_root(c.oid), c.oid) "
    "JOIN pg_namespace rn ON rn.oid = r.relnamespace "
    "WHERE c.relkind IN ('r', 'p') "
    f"AND ((n.nspname = '{SYNCED_SCHEMA}' AND r.oid <> c.oid) OR pg_get_userbyid(r.relowner) ~ '{WRITER_ROLE_RE}'))"
)

# Every table and index identifier (quote_ident schema.name, the form check objects use) -> its synced table.
SYNCED_MAP_SQL = f"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, st.synced_table
FROM {SYNCED_TABLES_SQL} st JOIN pg_class c ON c.oid = st.relid JOIN pg_namespace n ON n.oid = c.relnamespace
UNION ALL
SELECT quote_ident(n.nspname) || '.' || quote_ident(ic.relname), st.synced_table
FROM {SYNCED_TABLES_SQL} st JOIN pg_index i ON i.indrelid = st.relid
JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_namespace n ON n.oid = ic.relnamespace
"""

# Result columns that may hold a table or index identifier, besides `object`
_IDENT_KEYS = ("object", "table_ident", "index_ident", "table_name")


def synced_map(session) -> dict:
    """{identifier: synced table} for one database; {} when it cannot be read (the checks still run)."""
    try:
        return {r["ident"]: r["synced_table"] for r in session.query(SYNCED_MAP_SQL)
                if r.get("ident") and r.get("synced_table")}
    except Exception:
        return {}


def synced_table_of(row: dict, synced: dict) -> str | None:
    for k in _IDENT_KEYS:
        v = row.get(k)
        if isinstance(v, str) and v in synced:
            return synced[v]
    return None


def annotate(rows: list, synced: dict) -> list:
    """Name the synced table on every result row about a synced table's storage (evidence.synced_table)."""
    if not synced:
        return rows
    out = []
    for row in rows:
        name = row.get("synced_table") or synced_table_of(row, synced)
        out.append({**row, "synced_table": name} if name else row)
    return out


def drops_synced_index(evidence, sql: str | None) -> bool:
    """A DROP INDEX on synced storage: never proposed as an action (Postgres refuses it; the sync rebuilds it)."""
    return bool(isinstance(evidence, dict) and evidence.get("synced_table") and sql
                and sql.lstrip().upper().startswith("DROP INDEX"))
