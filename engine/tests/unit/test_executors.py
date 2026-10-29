from types import SimpleNamespace as NS

from lakebase_health.checkspec import parse_check, render_template
from lakebase_health.config import EngineConfig
from lakebase_health.exclusions import Exclusions
from lakebase_health.executors.api import API_CHECKS, ApiExecutor
from lakebase_health.executors.delta import DeltaExecutor, rewrite_tables
from lakebase_health.executors.postgres import SESSION_SETUP, PostgresExecutor
from lakebase_health.models import EndpointInfo
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
EXCL = Exclusions(("s",), ("r",), ("u",), ("pg_catalog",), "^(pg_)", "^neon_")


def ep(**kw):
    base = dict(project_id="p", project_name="P", branch_id="b", branch_name="production", is_default_branch=True,
                is_protected=False, endpoint_id="e", endpoint_name="projects/p/branches/production/endpoints/primary",
                endpoint_type="ENDPOINT_TYPE_READ_WRITE", current_state="ACTIVE", host="h", min_cu=1.0, max_cu=1.0,
                suspend_timeout_s=300, ha_min=1, restore_window_s=2 * 86400, pg_native_login=False, opted_out=False,
                enrolled=True, databases=["db"], password_roles=["legacy"])
    base.update(kw)
    return EndpointInfo(**base)


def test_rewrite_tables():
    out = rewrite_tables("SELECT * FROM system.lakebase.compute_gauges g JOIN system.lakebase.plan_history p", CFG)
    assert "cat.lakebase_health.raw_compute_gauges g" in out and "cat.lakebase_health.raw_plan_history p" in out


def test_delta_executor_runs_rewritten_sql():
    spec = parse_check({"id": "d", "title": "D", "category": "c", "source": "delta", "explain": "e",
                        "sql": "SELECT 1 AS measure FROM system.lakebase.compute_gauges", "thresholds": {"warning": 0}},
                       stem="d")
    sql = FakeSql(responses=[(r"raw_compute_gauges", [{"measure": 1}])])
    assert DeltaExecutor(sql, CFG, EXCL).run(spec) == [{"measure": 1}]


class FakeCursor:
    def __init__(self, rows, cols):
        self._rows, self.description = rows, [NS(name=c) for c in cols] if cols else None

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, answers):
        self.executed, self.answers, self.closed = [], answers, False

    def execute(self, stmt):
        self.executed.append(stmt)
        for key, (rows, cols) in self.answers.items():
            if key in stmt:
                return FakeCursor(rows, cols)
        return FakeCursor([], None)

    def close(self):
        self.closed = True


def test_postgres_session_is_read_only_and_uses_fresh_token():
    conn = FakeConn({"pg_extension": ([(1,)], ["x"]), "stats_reset": ([(30.0,)], ["h"])})
    calls = {}

    def connect(**kw):
        calls.update(kw)
        return conn

    w = NS(postgres=NS(generate_database_credential=lambda endpoint: NS(token=f"tok-{endpoint[-7:]}")))
    pg = PostgresExecutor(w, "svc", connect=connect)
    with pg.session(ep(), "db") as s:
        assert s.has_extension("pgstattuple") and s.stats_window_hours() == 30.0
    assert calls["password"] == "tok-primary" and calls["application_name"] == "lakebase-health"
    assert "prepare_threshold" in calls and calls["prepare_threshold"] is None  # no auto-prepared statements
    assert calls["dbname"] == "db" and calls["sslmode"] == "require"
    assert conn.executed[:len(SESSION_SETUP)] == list(SESSION_SETUP)
    assert "SET default_transaction_read_only = on" in SESSION_SETUP and conn.closed


def test_api_checks():
    rows = API_CHECKS["backup_restore_window"]([ep(), ep(endpoint_id="e2")])
    assert rows == [{"project_id": "p", "branch_id": None, "endpoint_id": None, "object": "P", "measure": 2.0,
                     "restore_window_days": 2.0}]
    assert API_CHECKS["default_branch_unprotected"]([ep()])[0]["measure"] == 1.0
    assert API_CHECKS["default_branch_unprotected"]([ep(is_protected=True)]) == []
    assert API_CHECKS["password_roles"]([ep()])[0]["object"] == "P/production:legacy"
    assert API_CHECKS["primary_without_ha"]([ep(ha_min=2)]) == []
    spec = parse_check({"id": "a", "title": "A", "category": "c", "source": "api", "explain": "e",
                        "function": "password_roles", "thresholds": {"warning": 0}}, stem="a",
                       api_functions=frozenset(API_CHECKS))
    assert ApiExecutor(EXCL).run(spec, [ep(opted_out=True)]) == []


def test_template_with_null_value_gives_no_sql():
    assert render_template("ALTER ROLE {role_ident} SET work_mem = '64MB';", {"role_ident": None}) is None


def test_api_executor_drops_excluded_platform_roles():
    spec = parse_check({"id": "a", "title": "A", "category": "c", "source": "api", "explain": "e",
                        "function": "password_roles", "thresholds": {"warning": 0}}, stem="a",
                       api_functions=frozenset(API_CHECKS))
    excl = Exclusions(("s",), ("authenticator",), ("u",), ("pg_catalog",), "^(pg_)", "^neon_")
    rows = ApiExecutor(excl).run(spec, [ep(password_roles=["authenticator", "legacy"])])
    assert [r["object"] for r in rows] == ["P/production:legacy"]


def test_still_active_reads_the_current_endpoint_state():
    from types import SimpleNamespace as NS

    from lakebase_health.executors.postgres import PostgresExecutor
    states = {"projects/p/branches/production/endpoints/primary": "ACTIVE"}

    def get_endpoint(name):
        if name not in states:
            raise RuntimeError("api down")
        return NS(status=NS(current_state=NS(value=states[name])))

    pex = PostgresExecutor(NS(postgres=NS(get_endpoint=get_endpoint)), "svc", connect=lambda **k: None)
    assert pex.still_active(ep()) is True
    states[ep().endpoint_name] = "SUSPENDED"
    assert pex.still_active(ep()) is False
    del states[ep().endpoint_name]
    assert pex.still_active(ep()) is False  # cannot tell: never risk waking it
