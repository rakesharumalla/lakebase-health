from .models import EndpointInfo


def _val(x):
    return getattr(x, "value", x)


def _seconds(d):
    if d is None:
        return None
    if hasattr(d, "total_seconds"):
        return int(d.total_seconds())
    if hasattr(d, "seconds"):
        return int(d.seconds)
    s = str(d)
    return int(float(s[:-1])) if s.endswith("s") else int(float(s))


def _tags(tags):
    out = {}
    for t in tags or []:
        if isinstance(t, dict):
            out[str(t.get("key"))] = str(t.get("value"))
        else:
            out[str(getattr(t, "key", ""))] = str(getattr(t, "value", ""))
    return out


VERIFY_BRANCH_PREFIX = "lbh-verify-"  # the fixer's short-lived verification branches (fixer/branch.py)


def build_inventory(w, engine_role: str, branches_out: dict | None = None) -> tuple:
    """Endpoints of every project and branch, except the fixer's verification branches. `branches_out`, when given,
    receives {project uid: set of existing branch uids}, or None for a project whose listing failed."""
    endpoints, errors = [], []
    branches_out = {} if branches_out is None else branches_out
    for p in w.postgres.list_projects():
        branches_out[p.uid] = None
        try:
            ps = p.status
            opted_out = _tags(getattr(ps, "custom_tags", None)).get("lakebase_health", "").lower() == "off"
            seen = set()
            for b in w.postgres.list_branches(parent=p.name):
                if b.name.rsplit("/", 1)[-1].startswith(VERIFY_BRANCH_PREFIX):
                    continue
                seen.add(b.uid)
                roles = list(w.postgres.list_roles(parent=b.name))
                role_names = {r.status.postgres_role for r in roles if r.status}
                password_roles = sorted(r.status.postgres_role for r in roles
                                        if r.status and "PASSWORD" in str(_val(r.status.auth_method)))
                dbs = [d.status.postgres_database for d in w.postgres.list_databases(parent=b.name) if d.status]
                for e in w.postgres.list_endpoints(parent=b.name):
                    es = e.status
                    group = getattr(es, "group", None)
                    endpoints.append(EndpointInfo(
                        project_id=p.uid, project_name=getattr(ps, "display_name", None) or p.name,
                        branch_id=b.uid, branch_name=b.name.rsplit("/", 1)[-1],
                        is_default_branch=bool(getattr(b.status, "default", False)),
                        is_protected=bool(getattr(b.status, "is_protected", False)),
                        endpoint_id=e.uid, endpoint_name=e.name, endpoint_type=str(_val(es.endpoint_type)),
                        current_state=str(_val(es.current_state)),
                        host=getattr(getattr(es, "hosts", None), "host", None),
                        min_cu=es.autoscaling_limit_min_cu, max_cu=es.autoscaling_limit_max_cu,
                        suspend_timeout_s=_seconds(getattr(es, "suspend_timeout_duration", None)),
                        ha_min=int(group.min) if group is not None and group.min else 1,
                        restore_window_s=_seconds(getattr(ps, "history_retention_duration", None)),
                        pg_native_login=bool(getattr(ps, "enable_pg_native_login", False)),
                        opted_out=opted_out, enrolled=engine_role in role_names,
                        databases=dbs, password_roles=password_roles))
            branches_out[p.uid] = seen
        except Exception as ex:  # one project failing must not stop the inventory
            errors.append(f"{p.name}: {type(ex).__name__}: {ex}"[:500])
    return endpoints, errors
