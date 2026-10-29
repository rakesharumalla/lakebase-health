import json
from dataclasses import asdict, dataclass, field
from datetime import datetime

FLEET = ("*", "*")


@dataclass
class CheckResult:
    check_id: str
    fingerprint: str
    project_id: str | None
    branch_id: str | None
    endpoint_id: str | None
    object: str
    measure: float
    severity: str
    confidence: str
    evidence: dict
    query_text: str | None = None
    candidate_sql: str | None = None
    rollback_sql: str | None = None
    title: str | None = None  # rendered title_template; None means the spec title
    rollback_note: str | None = None  # why there is no rollback (nothing to undo)


@dataclass
class CheckOutcome:
    check_id: str
    scope: tuple
    status: str
    error: str | None = None
    results: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status in ("ok", "ok_stale_source")


@dataclass
class Finding:
    fingerprint: str
    check_id: str
    title: str
    category: str
    project_id: str | None
    branch_id: str | None
    endpoint_id: str | None
    object: str
    status: str
    severity: str
    confidence: str
    measure: float | None
    first_seen: datetime
    last_seen: datetime
    last_checked_at: datetime
    occurrences: int
    clean_runs: int
    evidence: str
    query_text: str | None
    explain: str
    candidate_sql: str | None
    rollback_sql: str | None
    suppressed_until: datetime | None = None
    resolved_at: datetime | None = None
    rollback_note: str | None = None

    def to_row(self) -> dict:
        return asdict(self)

    @classmethod
    def from_row(cls, row: dict) -> "Finding":
        names = cls.__dataclass_fields__.keys()
        return cls(**{k: row.get(k) for k in names})


@dataclass
class EndpointInfo:
    project_id: str
    project_name: str
    branch_id: str
    branch_name: str
    is_default_branch: bool
    is_protected: bool
    endpoint_id: str
    endpoint_name: str
    endpoint_type: str
    current_state: str
    host: str | None
    min_cu: float | None
    max_cu: float | None
    suspend_timeout_s: int | None
    ha_min: int
    restore_window_s: int | None
    pg_native_login: bool
    opted_out: bool
    enrolled: bool
    databases: list = field(default_factory=list)
    password_roles: list = field(default_factory=list)

    @property
    def active(self) -> bool:
        return self.current_state == "ACTIVE"

    @property
    def read_write(self) -> bool:
        return self.endpoint_type.endswith("READ_WRITE")

    def to_row(self, run_id: str, captured_at: datetime) -> dict:
        row = asdict(self)
        row.update(run_id=run_id, captured_at=captured_at, databases=json.dumps(self.databases),
                   password_roles=json.dumps(self.password_roles), last_pg_check_at=None)
        return row


@dataclass
class Action:
    action_id: str
    fingerprint: str
    check_id: str
    kind: str
    sql: str
    rollback_sql: str | None
    risk: str
    reversible: bool
    verify_on_branch: bool
    state: str
    created_at: datetime
    updated_at: datetime
    expires_at: datetime
    rollback_note: str | None = None

    def to_row(self) -> dict:
        return asdict(self)

    @classmethod
    def from_row(cls, row: dict) -> "Action":
        return cls(**{k: row.get(k) for k in cls.__dataclass_fields__})
