from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from lakebase_health.checkspec import parse_check
from lakebase_health.models import FLEET
from lakebase_health.runner import Runner
from tests.unit.test_executors import EXCL, ep

NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)


def spec(id, source="postgres", **kw):
    d = {"id": id, "title": id, "category": "c", "source": source, "explain": "e", "sql": "SELECT 1",
         "thresholds": {"warning": 0}}
    if source == "api":
        d.pop("sql")
        d["function"] = kw.pop("function")
    d.update(kw)
    return parse_check(d, stem=id, api_functions=frozenset({"password_roles"}))


class FakeDelta:
    def __init__(self, rows=None, exc=None):
        self.rows, self.exc = rows or [], exc

    def run(self, s):
        if self.exc:
            raise self.exc
        return self.rows


class FakeSession:
    def __init__(self, rows, ext=True, window=48.0, exc=None):
        self.rows, self.ext, self.window, self.exc = rows, ext, window, exc

    def query(self, sql):
        if self.exc:
            raise self.exc
        return self.rows

    def has_extension(self, name):
        return self.ext

    def stats_window_hours(self):
        return self.window


class FakePg:
    def __init__(self, session=None, connect_exc=None, suspended_after=None):
        self.sess, self.connect_exc, self.opened = session, connect_exc, []
        self.suspended_after, self.state_checks = suspended_after, []  # suspends after N sessions

    def still_active(self, e):
        self.state_checks.append(e.endpoint_id)
        return self.suspended_after is None or len(self.opened) < self.suspended_after

    @contextmanager
    def session(self, e, db):
        if self.connect_exc:
            raise self.connect_exc
        self.opened.append((e.endpoint_id, db))
        yield self.sess


class FakeApi:
    def run(self, s, inv):
        return [{"project_id": "p", "branch_id": None, "endpoint_id": None, "object": "P:r", "measure": 1.0}]


def runner(delta=None, pg=None):
    return Runner(delta or FakeDelta(), pg or FakePg(FakeSession([])), FakeApi(), EXCL)


def test_delta_rows_become_results_with_fleet_scope():
    rows = [{"project_id": "p", "branch_id": "b", "endpoint_id": "e", "object": "e", "measure": 90.0,
             "query_text": "SELECT secret", "extra": 7}, {"project_id": "p", "branch_id": "b", "endpoint_id": "e2",
                                                          "object": "e2", "measure": 10.0}]
    s = spec("d", "delta", thresholds={"warning": 70, "critical": 85})
    [o] = runner(FakeDelta(rows)).run([s], [], NOW, {}, set())
    assert o.scope == FLEET and o.status == "ok" and len(o.results) == 1
    r = o.results[0]
    assert r.severity == "critical" and r.query_text == "SELECT secret"
    assert "query_text" not in r.evidence and r.evidence["extra"] == 7


def test_stale_source_status():
    s = spec("d", "delta", sql="SELECT 1 FROM system.lakebase.plan_history")
    [o] = runner().run([s], [], NOW, {}, {"plan_history"})
    assert o.status == "ok_stale_source"


def test_delta_error_and_timeout():
    s = spec("d", "delta")
    [o] = runner(FakeDelta(exc=RuntimeError("bad column"))).run([s], [], NOW, {}, set())
    assert o.status == "error" and "bad column" in o.error

    class QueryCanceled(Exception):
        pass

    [o] = runner(FakeDelta(exc=QueryCanceled("x"))).run([s], [], NOW, {}, set())
    assert o.status == "timeout"


def test_daily_checks_skip_until_due():
    s = spec("d", "delta", schedule="daily")
    [o] = runner().run([s], [], NOW, {"d": NOW - timedelta(hours=2)}, set())
    assert o.status == "skipped:not_scheduled"
    [o] = runner().run([s], [], NOW, {"d": NOW - timedelta(hours=24)}, set())
    assert o.status == "ok"


def test_postgres_skip_reasons_and_scope():
    s = spec("pgc")
    inv = [ep(endpoint_id="a", current_state="IDLE"), ep(endpoint_id="b", enrolled=False),
           ep(endpoint_id="c", opted_out=True), ep(endpoint_id="d", endpoint_type="ENDPOINT_TYPE_READ_ONLY"),
           ep(endpoint_id="e")]
    pg = FakePg(FakeSession([{"object": "o", "measure": 1}]))
    outcomes = runner(pg=pg).run([s], inv, NOW, {}, set())
    by_ep = {o.scope[1]: o.status for o in outcomes}
    assert by_ep == {"a": "skipped:compute_inactive", "b": "skipped:not_enrolled", "c": "skipped:opted_out",
                     "e": "ok"}
    assert pg.opened == [("e", "db")]
    ok = next(o for o in outcomes if o.status == "ok")
    assert ok.results[0].object == "db:o" and ok.results[0].endpoint_id == "e"


def test_guard_failure_lowers_confidence():
    s = spec("pgc", guards=["stats_window_hours >= 24"])
    pg = FakePg(FakeSession([{"object": "o", "measure": 1}], window=2.0))
    [o] = runner(pg=pg).run([s], [ep()], NOW, {}, set())
    assert o.results[0].confidence == "low"


def test_missing_extension_skips_and_emits_meta_finding():
    s = spec("bloat", requires_extension="pgstattuple")
    pg = FakePg(FakeSession([{"object": "o", "measure": 1}], ext=False))
    outcomes = runner(pg=pg).run([s], [ep()], NOW, {}, set())
    statuses = {o.check_id: o.status for o in outcomes}
    assert statuses == {"bloat": "skipped:extension_missing", "extension_missing": "ok"}
    meta = next(o for o in outcomes if o.check_id == "extension_missing")
    assert meta.results[0].object == "db:pgstattuple" and meta.results[0].severity == "info"


def test_connection_failure_marks_error_and_keeps_findings():
    s = spec("pgc")
    [o] = runner(pg=FakePg(connect_exc=OSError("connection refused"))).run([s], [ep()], NOW, {}, set())
    assert o.status == "error" and "connection refused" in o.error and o.results == []


def test_fix_templates_render_candidates():
    fix = {"kind": "ddl", "template": "DROP INDEX CONCURRENTLY {index_ident};", "rollback": "{index_def};",
           "risk": "high", "reversible": True}
    s = spec("pgc", fix=fix)
    pg = FakePg(FakeSession([{"object": "o", "measure": 1, "index_ident": "s.i", "index_def": "CREATE INDEX i"}]))
    [o] = runner(pg=pg).run([s], [ep()], NOW, {}, set())
    assert o.results[0].candidate_sql == "DROP INDEX CONCURRENTLY s.i;"
    assert o.results[0].rollback_sql == "CREATE INDEX i;"


def test_api_check_scope_is_fleet():
    s = spec("a", "api", function="password_roles")
    [o] = runner().run([s], [ep()], NOW, {}, set())
    assert o.scope == FLEET and o.results[0].object == "P:r"


class PerEndpointPg:
    """Answers each endpoint with its own rows; endpoints in `fail` raise on connect."""

    def __init__(self, rows_by_ep, window_by_ep=None, fail=(), suspended=()):
        self.rows_by_ep, self.window_by_ep, self.fail, self.opened = rows_by_ep, window_by_ep or {}, fail, []
        self.suspended = suspended

    def still_active(self, e):
        return e.endpoint_id not in self.suspended

    @contextmanager
    def session(self, e, db):
        if e.endpoint_id in self.fail:
            raise OSError("replica unreachable")
        self.opened.append(e.endpoint_id)
        yield FakeSession(self.rows_by_ep.get(e.endpoint_id, []), window=self.window_by_ep.get(e.endpoint_id, 48.0))


RO = "ENDPOINT_TYPE_READ_ONLY"


def test_replica_check_drops_objects_used_on_a_replica():
    s = spec("unused", replica_check="intersect")
    pg = PerEndpointPg({"e": [{"object": "i_used_on_replica", "measure": 1}, {"object": "i_unused", "measure": 1}],
                        "r1": [{"object": "i_unused", "measure": 1}]})
    inv = [ep(), ep(endpoint_id="r1", endpoint_type=RO)]
    [o] = runner(pg=pg).run([s], inv, NOW, {}, set())
    assert [r.object for r in o.results] == ["db:i_unused"]
    assert o.results[0].confidence == "high" and o.results[0].evidence["replicas_checked"] == ["r1"]
    assert sorted(pg.opened) == ["e", "r1"]


def test_replica_check_lowers_confidence_when_a_replica_cannot_be_read():
    s = spec("unused", replica_check="intersect", guards=["stats_window_hours >= 24"])
    inv = [ep(), ep(endpoint_id="r1", endpoint_type=RO, current_state="IDLE"),
           ep(endpoint_id="r2", endpoint_type=RO), ep(endpoint_id="r3", endpoint_type=RO)]
    pg = PerEndpointPg({"e": [{"object": "i", "measure": 1}], "r3": [{"object": "i", "measure": 1}]},
                       window_by_ep={"r3": 2.0}, fail=("r2",))
    [o] = runner(pg=pg).run([s], inv, NOW, {}, set())
    [r] = o.results
    assert r.confidence == "low"
    assert r.evidence["replicas_unverified"] == ["r1", "r2", "r3"]


def test_replica_check_ignores_replicas_on_other_branches_and_plain_checks_skip_replicas():
    s_rep, s_plain = spec("unused", replica_check="intersect"), spec("plain")
    inv = [ep(), ep(endpoint_id="r_other", branch_id="b2", endpoint_type=RO)]
    pg = PerEndpointPg({"e": [{"object": "i", "measure": 1}]})
    outcomes = runner(pg=pg).run([s_rep, s_plain], inv, NOW, {}, set())
    assert pg.opened == ["e"]
    assert all(len(o.results) == 1 and o.results[0].confidence == "high" for o in outcomes)


def test_replica_check_value_is_validated():
    import pytest
    from lakebase_health.checkspec import CheckSpecError
    with pytest.raises(CheckSpecError, match="replica_check"):
        spec("bad", replica_check="union")


def test_recent_replica_counters_still_veto_an_index_they_saw_used():
    s = spec("unused", replica_check="intersect", guards=["stats_window_hours >= 24"])
    pg = PerEndpointPg({"e": [{"object": "i_used", "measure": 1}, {"object": "i_unused", "measure": 1}],
                        "r1": [{"object": "i_unused", "measure": 1}]}, window_by_ep={"r1": 1.0})
    [o] = runner(pg=pg).run([s], [ep(), ep(endpoint_id="r1", endpoint_type=RO)], NOW, {}, set())
    assert [r.object for r in o.results] == ["db:i_unused"]
    assert o.results[0].confidence == "low" and o.results[0].evidence["replicas_unverified"] == ["r1"]


# Final review I5: the compute state is re-read right before each Postgres connection; a compute that suspended
# since the inventory is skipped (never woken) as skipped:compute_inactive.
def test_a_compute_suspended_since_the_inventory_is_skipped_without_connecting():
    pg = FakePg(FakeSession([{"object": "o", "measure": 1}]), suspended_after=0)
    [o] = runner(pg=pg).run([spec("pgc")], [ep()], NOW, {}, set())
    assert o.status == "skipped:compute_inactive" and o.results == [] and pg.opened == [] and pg.state_checks == ["e"]


def test_the_state_is_rechecked_before_each_database():
    pg = FakePg(FakeSession([{"object": "o", "measure": 1}]), suspended_after=1)
    [o] = runner(pg=pg).run([spec("pgc")], [ep(databases=["db1", "db2"])], NOW, {}, set())
    assert pg.opened == [("e", "db1")] and pg.state_checks == ["e", "e"]
    assert o.status == "skipped:compute_inactive" and o.results == []


def test_a_connection_failure_on_a_compute_that_suspended_is_compute_inactive():
    pg = FakePg(connect_exc=OSError("connection refused"))
    pg.still_active = lambda e, calls=[]: calls.append(1) or len(calls) == 1  # active, then suspended
    [o] = runner(pg=pg).run([spec("pgc")], [ep()], NOW, {}, set())
    assert o.status == "skipped:compute_inactive" and o.results == []


def test_a_replica_suspended_since_the_inventory_is_unverified_not_woken():
    s = spec("unused", replica_check="intersect")
    pg = PerEndpointPg({"e": [{"object": "i", "measure": 1}]}, suspended=("r1",))
    [o] = runner(pg=pg).run([s], [ep(), ep(endpoint_id="r1", endpoint_type=RO)], NOW, {}, set())
    assert pg.opened == ["e"] and o.results[0].evidence["replicas_unverified"] == ["r1"]


def test_counter_checks_do_not_run_on_young_statistics():
    young = spec("u", min_stats_hours=24, thresholds={"info": 0})
    plain = spec("p", thresholds={"info": 0})
    sess = FakeSession([{"object": "s.i", "measure": 5.0}], window=3.4)
    out = {o.check_id: o for o in runner(pg=FakePg(sess)).run([young, plain], [ep()], NOW, {}, set())}
    assert out["u"].status == "skipped:stats_young" and out["u"].results == []
    assert out["u"].error == "counters too young (reset 3 h ago)"
    assert out["p"].status == "ok" and len(out["p"].results) == 1


def test_counter_checks_run_once_statistics_are_old_enough_and_unknown_age_skips():
    s = spec("u", min_stats_hours=24, thresholds={"info": 0})
    [o] = runner(pg=FakePg(FakeSession([{"object": "s.i", "measure": 5.0}], window=30.0))).run([s], [ep()], NOW, {}, set())
    assert o.status == "ok" and len(o.results) == 1
    [o] = runner(pg=FakePg(FakeSession([{"object": "s.i", "measure": 5.0}], window=None))).run([s], [ep()], NOW, {}, set())
    assert o.status == "skipped:stats_young" and o.error == "counters too young (reset time unknown)"


def test_one_young_database_skips_the_endpoint():
    class TwoDbs(FakeSession):
        def __init__(self):
            super().__init__([{"object": "s.i", "measure": 5.0}])
            self.windows = iter([48.0, 2.0])

        def stats_window_hours(self):
            return next(self.windows)

    s = spec("u", min_stats_hours=24, thresholds={"info": 0})
    [o] = runner(pg=FakePg(TwoDbs())).run([s], [ep(databases=["a", "b"])], NOW, {}, set())
    assert o.status == "skipped:stats_young" and o.results == []


def test_min_stats_hours_is_validated():
    import pytest
    from lakebase_health.checkspec import CheckSpecError
    for bad in (0, -1, "24", True):
        with pytest.raises(CheckSpecError, match="min_stats_hours"):
            spec("u", min_stats_hours=bad)
    with pytest.raises(CheckSpecError, match="min_stats_hours"):
        spec("d", "delta", min_stats_hours=24)
