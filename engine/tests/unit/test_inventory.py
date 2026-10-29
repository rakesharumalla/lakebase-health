from enum import Enum
from types import SimpleNamespace as NS

from lakebase_health.inventory import build_inventory


class State(Enum):
    ACTIVE = "ACTIVE"
    IDLE = "IDLE"


class Auth(Enum):
    LAKEBASE_OAUTH_V1 = "LAKEBASE_OAUTH_V1"
    PG_PASSWORD_SCRAM_SHA_256 = "PG_PASSWORD_SCRAM_SHA_256"


def fake_w(tags=None, roles=("svc",), fail_project=False):
    project = NS(name="projects/p", uid="uid-p",
                 status=NS(display_name="P", history_retention_duration="604800s", enable_pg_native_login=False,
                           custom_tags=tags or []))
    branch = NS(name="projects/p/branches/production", uid="br-1", status=NS(default=True, is_protected=False))
    ep = NS(name="projects/p/branches/production/endpoints/primary", uid="ep-1",
            status=NS(endpoint_type=NS(value="ENDPOINT_TYPE_READ_WRITE"), current_state=State.ACTIVE,
                      hosts=NS(host="h"), autoscaling_limit_min_cu=1.0, autoscaling_limit_max_cu=4.0,
                      suspend_timeout_duration="300s", group=NS(min=2)))
    role_objs = [NS(status=NS(postgres_role=r, auth_method=Auth.LAKEBASE_OAUTH_V1)) for r in roles]
    role_objs.append(NS(status=NS(postgres_role="legacy", auth_method=Auth.PG_PASSWORD_SCRAM_SHA_256)))

    def list_branches(parent):
        if fail_project:
            raise RuntimeError("denied")
        return [branch]

    pg = NS(list_projects=lambda: [project], list_branches=list_branches, list_endpoints=lambda parent: [ep],
            list_roles=lambda parent: role_objs,
            list_databases=lambda parent: [NS(status=NS(postgres_database="databricks_postgres"))])
    return NS(postgres=pg)


def test_inventory_maps_fields():
    [e], errors = build_inventory(fake_w(), "svc")
    assert errors == []
    assert (e.project_id, e.branch_id, e.endpoint_id) == ("uid-p", "br-1", "ep-1")
    assert e.active and e.read_write and e.enrolled and not e.opted_out
    assert e.restore_window_s == 604800 and e.suspend_timeout_s == 300 and e.ha_min == 2
    assert e.databases == ["databricks_postgres"] and e.password_roles == ["legacy"]


def test_not_enrolled_without_role():
    [e], _ = build_inventory(fake_w(roles=("someone_else",)), "svc")
    assert not e.enrolled


def test_opt_out_tag():
    [e], _ = build_inventory(fake_w(tags=[NS(key="lakebase_health", value="OFF")]), "svc")
    assert e.opted_out


def test_project_error_is_isolated():
    endpoints, errors = build_inventory(fake_w(fail_project=True), "svc")
    assert endpoints == [] and "denied" in errors[0]


# Final review I4: verification branches are never inventoried; the branches that exist are reported per project
# (None for a project whose listing failed, so nothing on it is treated as gone).
def test_verification_branches_are_excluded_and_existing_branches_are_reported():
    w = fake_w()
    verify = NS(name="projects/p/branches/lbh-verify-6b7a38d0", uid="br-v", status=NS(default=False, is_protected=False))
    prod = w.postgres.list_branches("projects/p")[0]
    w.postgres.list_branches = lambda parent: [prod, verify]
    branches = {}
    endpoints, errors = build_inventory(w, "svc", branches_out=branches)
    assert [e.branch_id for e in endpoints] == ["br-1"] and errors == []
    assert branches == {"uid-p": {"br-1"}}


def test_a_failed_project_reports_unknown_branches():
    branches = {}
    build_inventory(fake_w(fail_project=True), "svc", branches_out=branches)
    assert branches == {"uid-p": None}
