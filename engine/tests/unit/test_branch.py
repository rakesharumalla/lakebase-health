from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.postgres import EndpointType

from lakebase_health.fixer.branch import branch_name, verification_branch


class Op:
    def __init__(self, value=None, raises=None):
        self.value, self.raises = value, raises

    def wait(self, timeout=None):
        if self.raises:
            raise self.raises
        return self.value


class FakePostgres:
    """Like Lakebase, a new branch comes with one read_write endpoint (unless default_endpoint=False)."""

    def __init__(self, fail_endpoint=False, fail_create_wait=False, fail_delete=None, default_endpoint=True):
        self.fail_create_wait, self.fail_delete = fail_create_wait, fail_delete
        self.created, self.deleted, self.endpoints, self.specs = [], [], [], []
        self.fail_endpoint, self.default_endpoint = fail_endpoint, default_endpoint
        self.existing, self.updates, self.masks = {}, [], []

    def create_branch(self, parent, branch, branch_id):
        self.created.append((parent, branch_id, branch.spec.ttl, branch.spec.source_branch))
        if self.fail_create_wait:
            return Op(raises=RuntimeError("create wait failed"))
        name = f"{parent}/branches/{branch_id}"
        self.existing[name] = [SimpleNamespace(name=f"{name}/endpoints/ep-default",
                                               status=SimpleNamespace(endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE))
                               ] if self.default_endpoint else []
        return Op()

    def list_endpoints(self, parent):
        return iter(self.existing.get(parent, []))

    def _endpoint(self, name, spec):
        self.endpoints.append((name, spec.autoscaling_limit_min_cu, spec.autoscaling_limit_max_cu))
        self.specs.append(spec)
        return Op(SimpleNamespace(name=name, status=SimpleNamespace(hosts=SimpleNamespace(host="h"))))

    def update_endpoint(self, name, endpoint, update_mask):
        if self.fail_endpoint:
            raise RuntimeError("endpoint update failed")
        self.updates.append(name)
        self.masks.append(update_mask.paths)
        return self._endpoint(name, endpoint.spec)

    def create_endpoint(self, parent, endpoint, endpoint_id):
        if self.fail_endpoint:
            raise RuntimeError("endpoint creation failed")
        if any(e.status.endpoint_type == EndpointType.ENDPOINT_TYPE_READ_WRITE for e in self.existing.get(parent, [])):
            raise RuntimeError("BadRequest: read_write endpoint already exists")
        return self._endpoint(f"{parent}/endpoints/{endpoint_id}", endpoint.spec)

    def delete_branch(self, name):
        self.deleted.append(name)
        if self.fail_delete:
            raise self.fail_delete
        return Op()


def test_branch_is_created_with_ttl_and_small_compute_and_always_deleted():
    pg = FakePostgres()
    w = SimpleNamespace(postgres=pg)
    with pytest.raises(RuntimeError):
        with verification_branch(w, "projects/hc", "projects/hc/branches/production", "abcdef0123456789") as b:
            assert b["host"] == "h" and b["branch"].endswith("/branches/lbh-verify-abcdef01")
            raise RuntimeError("verification blew up")
    assert pg.created[0][1] == "lbh-verify-abcdef01" and pg.created[0][2].seconds == 4 * 3600
    assert pg.endpoints[0][1:] == (1.0, 2.0)
    assert pg.deleted == ["projects/hc/branches/lbh-verify-abcdef01"]
    assert branch_name("abcdef0123456789") == "lbh-verify-abcdef01"


def test_branch_deleted_when_endpoint_creation_fails():
    pg = FakePostgres(fail_endpoint=True)
    with pytest.raises(RuntimeError, match="endpoint update failed"):
        with verification_branch(SimpleNamespace(postgres=pg), "projects/hc", "projects/hc/branches/production", "abcdef0123456789"):
            pass
    assert pg.deleted == ["projects/hc/branches/lbh-verify-abcdef01"]


def _run(pg, body=lambda b: None):
    with verification_branch(SimpleNamespace(postgres=pg), "projects/hc", "projects/hc/branches/production", "abcdef0123456789") as b:
        body(b)


def test_create_wait_failure_attempts_delete_and_raises_original():
    pg = FakePostgres(fail_create_wait=True, fail_delete=RuntimeError("delete boom"))
    with pytest.raises(RuntimeError, match="create wait failed"):
        _run(pg)
    assert pg.deleted == ["projects/hc/branches/lbh-verify-abcdef01"]


def test_create_wait_failure_with_notfound_on_cleanup_raises_original():
    pg = FakePostgres(fail_create_wait=True, fail_delete=NotFound("gone"))
    with pytest.raises(RuntimeError, match="create wait failed"):
        _run(pg)


def test_body_error_wins_over_delete_error():
    pg = FakePostgres(fail_delete=RuntimeError("delete boom"))

    def body(b):
        raise ValueError("body error")

    with pytest.raises(ValueError, match="body error"):
        _run(pg, body)


def test_delete_error_raised_when_body_succeeds():
    pg = FakePostgres(fail_delete=RuntimeError("delete boom"))
    with pytest.raises(RuntimeError, match="delete boom"):
        _run(pg)


def test_endpoint_scales_to_zero_after_300s():
    pg = FakePostgres()
    _run(pg)
    spec = pg.specs[0]
    assert spec.suspend_timeout_duration.seconds == 300
    assert not spec.no_suspension


def test_uses_the_branch_default_read_write_endpoint_and_resizes_it():
    pg = FakePostgres()
    _run(pg, lambda b: b["endpoint"].endswith("/endpoints/ep-default") or pytest.fail(b["endpoint"]))
    assert pg.updates == ["projects/hc/branches/lbh-verify-abcdef01/endpoints/ep-default"]
    assert set(pg.masks[0]) == {"spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu",
                                "spec.suspend_timeout_duration"}
    assert pg.endpoints[0][1:] == (1.0, 2.0)


def test_creates_an_endpoint_only_when_the_branch_has_none():
    pg = FakePostgres(default_endpoint=False)
    _run(pg)
    assert pg.updates == [] and pg.endpoints == [("projects/hc/branches/lbh-verify-abcdef01/endpoints/primary", 1.0, 2.0)]
