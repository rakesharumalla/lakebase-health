from dataclasses import replace


def _projects(inventory):
    seen = {}
    for e in inventory:
        seen.setdefault(e.project_id, e)
    return list(seen.values())


def backup_restore_window(inventory):
    return [{"project_id": e.project_id, "branch_id": None, "endpoint_id": None, "object": e.project_name,
             "measure": e.restore_window_s / 86400.0, "restore_window_days": e.restore_window_s / 86400.0}
            for e in _projects(inventory) if e.restore_window_s is not None]


def default_branch_unprotected(inventory):
    seen, rows = set(), []
    for e in inventory:
        if e.is_default_branch and not e.is_protected and e.branch_id not in seen:
            seen.add(e.branch_id)
            rows.append({"project_id": e.project_id, "branch_id": e.branch_id, "endpoint_id": None,
                         "object": f"{e.project_name}/{e.branch_name}", "measure": 1.0})
    return rows


def primary_without_ha(inventory):
    return [{"project_id": e.project_id, "branch_id": e.branch_id, "endpoint_id": e.endpoint_id,
             "object": f"{e.project_name}/{e.branch_name}", "measure": 1.0, "ha_min": e.ha_min}
            for e in inventory if e.is_default_branch and e.read_write and e.ha_min < 2]


def password_roles(inventory):
    seen, rows = set(), []
    for e in inventory:
        for role in e.password_roles:
            key = (e.branch_id, role)
            if key not in seen:
                seen.add(key)
                rows.append({"project_id": e.project_id, "branch_id": e.branch_id, "endpoint_id": None,
                             "object": f"{e.project_name}/{e.branch_name}:{role}", "measure": 1.0})
    return rows


API_CHECKS = {f.__name__: f for f in (backup_restore_window, default_branch_unprotected, primary_without_ha,
                                       password_roles)}


class ApiExecutor:
    def __init__(self, excl):
        self.excl = excl

    def run(self, spec, inventory) -> list:
        excluded = set(self.excl.roles)
        visible = [replace(e, password_roles=[r for r in e.password_roles if r not in excluded])
                   for e in inventory if not e.opted_out]
        return API_CHECKS[spec.function](visible)
