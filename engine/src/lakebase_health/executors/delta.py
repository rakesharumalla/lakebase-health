import re

from ..checkspec import render_sql

# Query telemetry that carries a role. plan_history names it; the others only have the role OID (userid), which is
# mapped to platform roles through the names plan_history recorded on the same branch.
_ROLE_MAP_DAYS = 8
_BY_USERID = ("pg_stat_statements_counters", "active_session_history")


def _tool_join(cfg, excl) -> str:
    """Drop a monitoring tool's statements: the (branch, role, query id) of every plan_history row from a tool's
    application or with a tool's leading tag comment, and every pg_stat_statements entry whose text has the tag.
    Keyed on the role too, so a customer running the same statement keeps it."""
    if not (excl.tool_tags or excl.tool_apps or excl.query_apps):
        return ""
    since = f"ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS"
    return (f" LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid, queryid FROM {cfg.raw('plan_history')} "
            f"WHERE {since} AND ({excl.tool_app_sql('application_name')} OR {excl.tool_text_sql('query_text')}) "
            f"UNION SELECT DISTINCT branch_id, userid, queryid FROM {cfg.raw('pg_stat_statements_counters')} "
            f"WHERE {since} AND {excl.tool_text_sql('query')}) tq "
            "ON t.branch_id = tq.branch_id AND t.userid = tq.userid AND t.queryid = tq.queryid")


def rewrite_tables(sql_text: str, cfg, excl=None) -> str:
    """Point system.lakebase.* at the ingested raw tables and lakebase_health.* at the engine's schema. With `excl`,
    query telemetry drops platform roles."""
    def table(m):
        name, raw = m.group(1), cfg.raw(m.group(1))
        if excl is None:
            return raw
        if name == "plan_history":
            return (f"(SELECT * FROM {raw} WHERE NOT ({excl.platform_role_sql('usename')} "
                    f"OR {excl.tool_app_sql('application_name')} OR {excl.tool_text_sql('query_text')}))")
        if name in _BY_USERID:
            return (f"(SELECT t.* FROM {raw} t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid "
                    f"FROM {cfg.raw('plan_history')} WHERE ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS "
                    f"AND {excl.platform_role_sql('usename')}) x ON t.branch_id = x.branch_id AND t.userid = x.userid"
                    f"{_tool_join(cfg, excl)})")
        return raw
    out = re.sub(r"\bsystem\.lakebase\.([a-z_]+)", table, sql_text)
    # The engine's own tables (inventory snapshots, findings), written as lakebase_health.<name> in check SQL
    return re.sub(r"(?<![\w.`])lakebase_health\.([a-z_]+)\b", lambda m: cfg.table(m.group(1)), out)


class DeltaExecutor:
    def __init__(self, sql, cfg, excl):
        self.sql, self.cfg, self.excl = sql, cfg, excl

    def run(self, spec) -> list:
        return self.sql.query(rewrite_tables(render_sql(spec, self.excl), self.cfg,
                                             self.excl if spec.query_scoped else None))
