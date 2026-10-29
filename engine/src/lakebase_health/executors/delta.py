import re

from ..checkspec import render_sql

# Query telemetry that carries a role. plan_history names it; the others only have the role OID (userid), which is
# mapped to platform roles through the names plan_history recorded on the same branch.
_ROLE_MAP_DAYS = 8
_BY_USERID = ("pg_stat_statements_counters", "active_session_history")


def rewrite_tables(sql_text: str, cfg, excl=None) -> str:
    """Point system.lakebase.* at the ingested raw tables. With `excl`, query telemetry drops platform roles."""
    def table(m):
        name, raw = m.group(1), cfg.raw(m.group(1))
        if excl is None:
            return raw
        if name == "plan_history":
            return (f"(SELECT * FROM {raw} WHERE NOT ({excl.platform_role_sql('usename')} "
                    f"OR {excl.platform_app_sql('application_name')}))")
        if name in _BY_USERID:
            return (f"(SELECT t.* FROM {raw} t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid "
                    f"FROM {cfg.raw('plan_history')} WHERE ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS "
                    f"AND {excl.platform_role_sql('usename')}) x ON t.branch_id = x.branch_id AND t.userid = x.userid)")
        return raw
    return re.sub(r"\bsystem\.lakebase\.([a-z_]+)", table, sql_text)


class DeltaExecutor:
    def __init__(self, sql, cfg, excl):
        self.sql, self.cfg, self.excl = sql, cfg, excl

    def run(self, spec) -> list:
        return self.sql.query(rewrite_tables(render_sql(spec, self.excl), self.cfg,
                                             self.excl if spec.query_scoped else None))
