from lakebase_health.checkspec import load_checks, render_sql
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.runner import endpoint_sql
from tests.unit.test_executors import EXCL, ep
from tests.unit.test_runner import NOW, FakePg, runner

SPEC = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}["suspend_blockers"]
ROW = {"object": "worker as app_user", "role": "app_user", "application_name": "worker", "measure": 3.0,
       "idle_sessions": 3, "max_idle_s": 1800, "suspend_timeout_s": 300, "hours_since_start": 30}


class Session:
    def __init__(self, rows):
        self.rows, self.sql = rows, []

    def query(self, sql):
        self.sql.append(sql)
        return self.rows if "pg_stat_activity" in sql else []

    def has_extension(self, name):
        return True

    def stats_window_hours(self):
        return 48.0


def test_sql_groups_idle_clients_past_the_suspend_timeout_without_pids():
    sql = endpoint_sql(render_sql(SPEC, EXCL), ep(suspend_timeout_s=300))
    assert "make_interval(secs => 300)" in sql and "{suspend_timeout_s}" not in sql
    assert "pg_postmaster_start_time()" in sql and "interval '24 hours'" in sql
    assert "GROUP BY a.usename, a.application_name" in sql
    select = sql.split("FROM pg_stat_activity")[0]
    assert "pid" not in select.replace("pg_backend_pid", "")
    assert SPEC.area == "scale_to_zero" and SPEC.per_database is False


def test_raises_one_warning_per_role_and_application():
    s = Session([ROW])
    [o] = runner(pg=FakePg(s)).run([SPEC], [ep(suspend_timeout_s=300, databases=["a", "b"])], NOW, {}, set())
    assert o.status == "ok" and len(s.sql) == 3  # synced lookup per database opened, the check once
    [r] = o.results
    assert r.severity == "warning" and r.object == "worker as app_user" and r.evidence["idle_sessions"] == 3
    assert "300 s idle" in r.candidate_sql


def test_skipped_when_scale_to_zero_is_off():
    for off in (None, 0):
        [o] = runner(pg=FakePg(Session([ROW]))).run([SPEC], [ep(suspend_timeout_s=off)], NOW, {}, set())
        assert o.status == "skipped:scale_to_zero_off" and o.results == []


def test_a_busy_compute_is_not_a_suspend_blocker():
    # I5: a compute that is not suspending because it is busy is not blocked by its pool's idle sessions; the
    # check fires only when no client has run anything for longer than the suspend timeout
    sql = endpoint_sql(render_sql(SPEC, EXCL), ep(suspend_timeout_s=300))
    busy = sql.split("NOT EXISTS")[1]
    assert "b.backend_type = 'client backend'" in busy and "b.pid <> pg_backend_pid()" in busy
    assert "b.state IS DISTINCT FROM 'idle'" in busy  # active, idle in transaction, fastpath ...
    assert "now() - b.state_change <= make_interval(secs => 300)" in busy  # a query finished within the timeout
    assert "'lakebase-health'" in busy and "'databricks_monitor'" in busy  # our own and platform polling do not count
    assert "last_client_query_s" in sql
    assert "no client" in SPEC.explain and "CPU" in SPEC.explain
