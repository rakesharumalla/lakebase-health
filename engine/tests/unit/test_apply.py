import pytest
from types import SimpleNamespace
from lakebase_health.fixer import apply as ap
from lakebase_health.fixer.allowlist import NotAllowed

CREATE = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);"
AID = "ABCDEF0123456789"
NAME = "hc_test.lbh_abcdef012345"


class Conn:
    def __init__(self, fail_on=None):
        self.ran, self.fail_on = [], fail_on

    def execute(self, sql, params=None):
        self.ran.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("canceling statement due to lock timeout")
        return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])


def states(monkeypatch, *seq):
    asked, seq = [], list(seq)
    def fake(c, i):
        asked.append(i)
        return seq.pop(0) if len(seq) > 1 else seq[0]
    monkeypatch.setattr(ap, "index_state", fake)
    return asked


def test_successful_apply_uses_deterministic_name(monkeypatch):
    asked = states(monkeypatch, "absent", "valid")
    conn = Conn()
    r = ap.execute(conn, CREATE, AID)
    assert r == {"outcome": "applied", "error": None, "index": NAME}
    assert "lbh_abcdef012345" in conn.ran[0] and conn.ran[0].startswith("CREATE INDEX CONCURRENTLY")
    assert set(asked) == {NAME}


def test_failed_create_drops_only_our_invalid_index(monkeypatch):
    asked = states(monkeypatch, "absent", "invalid")
    conn = Conn(fail_on="CREATE INDEX")
    r = ap.execute(conn, CREATE, AID)
    assert r["outcome"] == "apply_failed" and "lock timeout" in r["error"] and r["index"] is None
    assert conn.ran[-1] == f"DROP INDEX CONCURRENTLY IF EXISTS {NAME}"
    assert set(asked) == {NAME}


def test_failed_create_with_valid_index_is_kept(monkeypatch):
    states(monkeypatch, "absent", "valid")
    conn = Conn(fail_on="CREATE INDEX")
    r = ap.execute(conn, CREATE, AID)
    assert r["outcome"] == "apply_failed" and r["error"]
    assert not any(s.startswith("DROP") for s in conn.ran)


def test_success_but_not_valid_is_dropped(monkeypatch):
    states(monkeypatch, "absent", "invalid")
    conn = Conn()
    r = ap.execute(conn, CREATE, AID)
    assert r["outcome"] == "apply_failed"
    assert conn.ran[-1] == f"DROP INDEX CONCURRENTLY IF EXISTS {NAME}"


def test_not_allowed_raises_before_executing(monkeypatch):
    states(monkeypatch, "valid")
    conn = Conn()
    with pytest.raises(NotAllowed):
        ap.execute(conn, "DROP TABLE hc_test.orders", AID)
    assert conn.ran == []


def test_recover_create(monkeypatch):
    states(monkeypatch, "valid")
    assert ap.recover(Conn(), CREATE, AID) == "applied"
    asked = states(monkeypatch, "invalid")
    conn = Conn()
    assert ap.recover(conn, CREATE, AID) == "apply_failed"
    assert conn.ran[-1] == f"DROP INDEX CONCURRENTLY IF EXISTS {NAME}"
    assert set(asked) == {NAME}
    states(monkeypatch, "absent")
    conn = Conn()
    assert ap.recover(conn, CREATE, AID) == "apply_failed" and conn.ran == []


def test_recover_drop_and_others(monkeypatch):
    drop = "DROP INDEX CONCURRENTLY IF EXISTS hc_test.dup"
    states(monkeypatch, "absent")
    assert ap.recover(Conn(), drop, AID) == "applied"
    states(monkeypatch, "valid")
    assert ap.recover(Conn(), drop, AID) == "apply_failed"
    assert ap.recover(Conn(), "ANALYZE hc_test.orders", AID) == "apply_failed"


NAMED = "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_named ON hc_test.orders (customer_id);"


@pytest.mark.parametrize("sql", [CREATE, NAMED])
@pytest.mark.parametrize("state", ["valid", "invalid"])
def test_preexisting_index_is_refused_untouched(monkeypatch, sql, state):
    states(monkeypatch, state)
    conn = Conn()
    r = ap.execute(conn, sql, AID)
    assert r["outcome"] == "apply_failed" and r["index"] is None
    assert "already exists; refusing to claim it" in r["error"]
    assert conn.ran == []


def test_cleanup_failure_preserves_original_error(monkeypatch):
    states(monkeypatch, "absent", "invalid")
    conn = Conn(fail_on="CREATE INDEX")
    orig = conn.execute
    def ex(sql, params=None):
        if sql.startswith("DROP"):
            raise RuntimeError("drop boom")
        return orig(sql, params)
    conn.execute = ex
    r = ap.execute(conn, CREATE, AID)
    assert r["outcome"] == "apply_failed"
    assert "lock timeout" in r["error"] and "; cleanup failed: RuntimeError: drop boom" in r["error"]


def test_cleanup_failure_after_invalid_success(monkeypatch):
    states(monkeypatch, "absent", "invalid")
    conn = Conn(fail_on="DROP")
    r = ap.execute(conn, CREATE, AID)
    assert r["outcome"] == "apply_failed" and "not valid" in r["error"] and "cleanup failed" in r["error"]


def test_failure_reports_sqlstate(monkeypatch):
    states(monkeypatch, "absent")

    class Denied(Exception):
        sqlstate = "42501"

    class DeniedConn(Conn):
        def execute(self, sql, params=None):
            raise Denied("permission denied for table orders")

    r = ap.execute(DeniedConn(), CREATE, AID)
    assert r["outcome"] == "apply_failed" and r["sqlstate"] == "42501"
