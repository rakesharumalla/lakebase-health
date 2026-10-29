from dataclasses import dataclass, replace
from pathlib import Path

import yaml

KNOWN_TOKENS = frozenset({"excluded_slots", "excluded_roles", "excluded_users", "excluded_schemas",
                          "excluded_role_regex", "excluded_settings_regex"})


def _lit(value: str) -> str:
    if "'" in value:
        raise ValueError(f"exclusion value must not contain a quote: {value!r}")
    return f"'{value}'"


def _sql_list(items) -> str:
    return "(" + ", ".join(_lit(i) for i in items) + ")"


@dataclass(frozen=True)
class Exclusions:
    slots: tuple
    roles: tuple
    users: tuple
    schemas: tuple
    role_regex: str
    settings_regex: str
    # Query-scoped checks drop telemetry from these roles / application names (platform and engine traffic).
    query_users: tuple = ()
    query_user_regex: str | None = None
    query_apps: tuple = ()

    def with_query_users(self, users) -> "Exclusions":
        extra = tuple(u for u in users if u and u not in self.query_users)
        return replace(self, query_users=self.query_users + tuple(dict.fromkeys(extra)))

    def platform_role_sql(self, col: str) -> str:
        """SQL predicate, true when the role in `col` is a platform or engine role (NULL roles are customer)."""
        parts = []
        if self.query_users:
            parts.append(f"coalesce({col}, '') IN {_sql_list(self.query_users)}")
        if self.query_user_regex:
            parts.append(f"coalesce({col}, '') RLIKE {_lit(self.query_user_regex)}")
        return "(" + " OR ".join(parts) + ")" if parts else "(1 = 0)"

    def platform_app_sql(self, col: str) -> str:
        """SQL predicate, true when `col` is an application name the engine or fixer connects with."""
        return f"(coalesce({col}, '') IN {_sql_list(self.query_apps)})" if self.query_apps else "(1 = 0)"

    def tokens(self) -> dict:
        return {
            "excluded_slots": _sql_list(self.slots),
            "excluded_roles": _sql_list(self.roles),
            "excluded_users": _sql_list(self.users),
            "excluded_schemas": _sql_list(self.schemas),
            "excluded_role_regex": _lit(self.role_regex),
            "excluded_settings_regex": _lit(self.settings_regex),
        }


def load_exclusions(path: Path) -> Exclusions:
    d = yaml.safe_load(Path(path).read_text())
    return Exclusions(slots=tuple(d["slots"]), roles=tuple(d["roles"]), users=tuple(d["users"]),
                      schemas=tuple(d["schemas"]), role_regex=d["role_regex"], settings_regex=d["settings_regex"],
                      query_users=tuple(d.get("query_users") or ()), query_user_regex=d.get("query_user_regex"),
                      query_apps=tuple(d.get("query_apps") or ()))
