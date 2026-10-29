import logging
from contextlib import contextmanager

from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.errors import NotFound
from databricks.sdk.service.postgres import Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType
from google.protobuf.duration_pb2 import Duration

log = logging.getLogger(__name__)

# Durations are protobuf Duration in the installed SDK (not timedelta).
TTL = Duration(seconds=4 * 3600)
SUSPEND_AFTER = Duration(seconds=300)


def branch_name(action_id: str) -> str:
    return f"lbh-verify-{action_id[:8]}"


def _delete(w, name: str) -> None:
    try:
        w.postgres.delete_branch(name=name).wait()
    except NotFound:
        pass


def _cleanup_quietly(w, name: str) -> None:
    try:
        _delete(w, name)
    except Exception:
        log.exception("failed to delete verification branch %s; it will expire via TTL", name)


def _small_spec() -> EndpointSpec:
    return EndpointSpec(endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE, autoscaling_limit_min_cu=1.0,
                        autoscaling_limit_max_cu=2.0, suspend_timeout_duration=SUSPEND_AFTER)


def _verification_endpoint(w, branch: str):
    """Lakebase gives every new branch a read_write endpoint; resize that one. Create one only if there is none."""
    rw = next((e for e in w.postgres.list_endpoints(parent=branch)
               if e.status and e.status.endpoint_type == EndpointType.ENDPOINT_TYPE_READ_WRITE), None)
    if rw is None:
        return w.postgres.create_endpoint(parent=branch, endpoint_id="primary", endpoint=Endpoint(spec=_small_spec())).wait()
    mask = FieldMask(["spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu", "spec.suspend_timeout_duration"])
    return w.postgres.update_endpoint(name=rw.name, endpoint=Endpoint(name=rw.name, spec=_small_spec()),
                                      update_mask=mask).wait()


@contextmanager
def verification_branch(w, project: str, source_branch: str, action_id: str):
    bid = branch_name(action_id)
    name = f"{project}/branches/{bid}"
    try:
        w.postgres.create_branch(parent=project, branch=Branch(spec=BranchSpec(source_branch=source_branch, ttl=TTL)),
                                 branch_id=bid).wait()
    except BaseException:
        _cleanup_quietly(w, name)
        raise
    try:
        ep = _verification_endpoint(w, name)
        yield {"branch": name, "endpoint": ep.name, "host": ep.status.hosts.host}
    except BaseException:
        _cleanup_quietly(w, name)
        raise
    _delete(w, name)
