# Lakebase Health Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the read-only Lakebase health engine. A scheduled Databricks job copies `system.lakebase` telemetry into Delta, inventories projects, runs YAML-defined checks (Delta, Postgres, API), and records findings, proposed actions, shadow-mode decisions and a hash-chained ledger.

**Architecture:** A Python package `lakebase_health` (in `engine/`), packaged as a wheel and run by one Lakeflow Job on serverless compute, deployed with a DABs bundle. Pure logic (check parsing, severity, lifecycle, actions, ledger) is isolated from I/O behind a small `Sql` protocol and executor classes, so all of it is unit-tested locally with fakes. Integration tests run against the seeded `healthcheck` project on the `my-profile` workspace.

**Tech Stack:** Python ≥ 3.10, PyYAML, psycopg 3, databricks-sdk ≥ 0.81, PySpark (provided by Databricks serverless), pytest, Databricks Asset Bundles.

**Spec:** `docs/superpowers/specs/2026-10-27-lakebase-health-engine-design.md`

## Global Constraints

- The engine never changes a customer database. Postgres sessions set `default_transaction_read_only = on`, `statement_timeout = '30s'`, `lock_timeout = '1s'`, `application_name = 'lakebase-health'`.
- Postgres checks run only on endpoints that are `ACTIVE`, read-write, enrolled (a Postgres role exists for the engine's identity), and not opted out (`lakebase_health=off` custom tag). A suspended compute is never woken.
- Telemetry checks never read `system.lakebase` directly. They read `<catalog>.<schema>.raw_<table>`. Ingestion processes **one table at a time**, never in parallel.
- Incremental ingest is `INSERT … SELECT … WHERE ts > :lo EXCEPT SELECT … FROM target WHERE ts > :lo`, with `lo = watermark − 10 minutes`. The first run is a 7-day backfill (`CREATE TABLE … CLUSTER BY (project_id, ts) AS SELECT`).
- All SQL must work in ANSI/strict mode: `NULLIF` on every divisor, `try_element_at`/`try_cast`, no unguarded array indexing.
- Severity words are exactly `info`, `warning`, `critical`. Finding status words are exactly `new`, `open`, `resolved`, `suppressed`.
- Outcome statuses are exactly `ok`, `ok_stale_source`, `skipped:compute_inactive`, `skipped:not_enrolled`, `skipped:extension_missing`, `skipped:opted_out`, `skipped:not_scheduled`, `error`, `timeout`.
- A finding resolves after **2 consecutive runs** in which its check ran OK for its scope and did not report it. `skipped:*`, `error` and `timeout` never resolve or change a finding.
- Platform exclusions live only in `checks/exclusions.yaml`: slots `wal_retention_slot`, `wal_proposer_slot`; users `cloud_admin`; roles `cloud_admin`, `databricks_control_plane`, `databricks_superuser`, `databricks_monitor`, plus regex `^(pg_|databricks_)`; schemas `pg_catalog`, `information_schema`, `pg_toast`, `hint_plan`, `sage`; settings regex `^(databricks|neon|neon_monitor|compute_ctl|lakebase)\.|^neon_`.
- Query text never goes into `evidence`. It goes only into the `query_text` column, which is masked.
- No external network calls other than Databricks APIs and Lakebase endpoints. No code is copied from pg_sage (AGPL-3.0); only ideas are borrowed.
- Default schema `lakebase_health`; the catalog is required at install. The default schedule is hourly (`0 0 * * * ?`, UTC). `max_concurrent_runs = 1`.

## Deviations from the spec (decided while planning)

1. **Object identity:** every check's SQL returns an `object` column; there is no `object_key` list. Simpler, and equivalent.
2. **Catalog size:** the spec says "36 checks" counting `HEALTH_CHECKS.md` sections. One file per finding-producing statement gives **57 files** (26 Delta, 27 Postgres, 4 API) covering the same sections plus the 5 borrowed rules.
3. **Two extra columns:** `findings.clean_runs` (needed for the "2 clean runs" rule) and `query_text` on `findings` and `check_results` (masked), so that evidence never holds text.

## Review Focus

1. **Spark returns naive timestamps** while the engine uses timezone-aware UTC. Comparing them must not raise `TypeError`. Pinned in Task 4 (`test_naive_timestamps_from_spark_are_treated_as_utc`).
2. **A check row with a NULL or NaN measure** must produce no finding and must not crash the run. Pinned in Task 3 (`test_classify_none_and_nan`).
3. **A fix template whose variable is NULL in the row** must produce no candidate SQL and no action, never the literal `None` in SQL. Pinned in Task 9 (`test_template_with_null_value_gives_no_sql`).
4. **Late-arriving telemetry within the overlap window** must be neither lost nor duplicated. Pinned in Task 7 (`test_incremental_uses_overlap_and_except`).
5. **A compute that suspends between inventory and connection** (connection error) must give status `error` for that endpoint and leave its findings untouched. Pinned in Task 10 (`test_connection_failure_marks_error_and_keeps_findings`).

---

## File structure

```
engine/
  pyproject.toml
  src/lakebase_health/
    __init__.py
    config.py            # EngineConfig, RAW_TABLES
    sql.py               # Sql protocol + SparkSql adapter
    models.py            # CheckResult, CheckOutcome, Finding, EndpointInfo, Action, FLEET
    exclusions.py        # Exclusions + tokens
    checkspec.py         # CheckSpec/FixSpec parse, validate, load, render_sql, render_template
    severity.py          # classify, guards_pass
    fingerprint.py       # fingerprint()
    lifecycle.py         # apply_run()
    actions.py           # plan_actions(), policy_level(), shadow_rows()
    ledger.py            # chain(), verify()
    ddl.py               # ensure_schema(), ensure_masks()
    ingest.py            # ingest_all()
    inventory.py         # build_inventory()
    executors/__init__.py
    executors/delta.py   # DeltaExecutor, rewrite_tables()
    executors/postgres.py# PostgresExecutor, PgSession
    executors/api.py     # API_CHECKS, ApiExecutor
    runner.py            # Runner, META_SPECS
    record.py            # persist(), load_last_ok()
    jobs/__init__.py
    jobs/run.py          # run_engine(), main()
    checks/exclusions.yaml
    checks/<category>/<id>.yaml   (57 files)
  tests/
    fakes.py
    unit/test_*.py
    integration/acceptance.py
databricks.yml
resources/engine_job.yml
README.md
```

---

### Task 1: Repository, package skeleton, `Sql` protocol and test fake

**Files:**
- Create: `.gitignore`, `engine/pyproject.toml`, `engine/src/lakebase_health/__init__.py`, `engine/src/lakebase_health/config.py`, `engine/src/lakebase_health/sql.py`, `engine/tests/__init__.py`, `engine/tests/unit/__init__.py`, `engine/tests/fakes.py`, `engine/tests/unit/test_fakes.py`

**Interfaces:**
- Produces: `EngineConfig(catalog, schema="lakebase_health", source_schema="system.lakebase", backfill_days=7, retention_days=90, overlap_minutes=10, engine_enabled=True, resolve_after_clean_runs=2)` with `.table(name) -> str` and `.raw(source) -> str`; `RAW_TABLES: tuple[str, ...]`; protocol `Sql` with `query(stmt, args=None) -> list[dict]`, `execute(stmt, args=None) -> None`, `table_exists(name) -> bool`, `insert_rows(table, rows) -> None`, `merge_rows(table, rows, keys) -> None`; `SparkSql(spark)`; test helper `FakeSql`.

- [ ] **Step 1: Initialise git and a virtualenv**

```bash
cd <repo>
git init -q
cat > .gitignore <<'EOF'
.venv/
__pycache__/
*.egg-info/
engine/dist/
engine/build/
.databricks/
.pytest_cache/
EOF
python3 -m venv engine/.venv
engine/.venv/bin/pip install -q pytest pyyaml "psycopg[binary]>=3.1" "databricks-sdk>=0.81.0"
```

- [ ] **Step 2: Write `engine/pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "lakebase_health"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = ["pyyaml>=6", "psycopg[binary]>=3.1", "databricks-sdk>=0.81.0"]

[project.scripts]
main = "lakebase_health.jobs.run:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
lakebase_health = ["checks/*.yaml", "checks/*/*.yaml"]

[tool.pytest.ini_options]
testpaths = ["tests/unit"]
pythonpath = ["src", "."]
```

- [ ] **Step 3: Write the failing test `engine/tests/unit/test_fakes.py`**

```python
from lakebase_health.config import EngineConfig, RAW_TABLES
from tests.fakes import FakeSql


def test_config_table_names():
    cfg = EngineConfig(catalog="c")
    assert cfg.table("findings") == "c.lakebase_health.findings"
    assert cfg.raw("compute_gauges") == "c.lakebase_health.raw_compute_gauges"
    assert len(RAW_TABLES) == 10


def test_fake_sql_records_and_answers():
    sql = FakeSql(responses=[(r"SELECT max\(ts\)", [{"m": 1}])])
    assert sql.query("SELECT max(ts) AS m FROM t") == [{"m": 1}]
    sql.execute("DELETE FROM t", {"a": 1})
    assert sql.statements[-1] == ("DELETE FROM t", {"a": 1})


def test_fake_sql_merge_and_select_star():
    sql = FakeSql()
    sql.merge_rows("c.s.t", [{"k": 1, "v": "a"}], ["k"])
    sql.merge_rows("c.s.t", [{"k": 1, "v": "b"}, {"k": 2, "v": "c"}], ["k"])
    assert sorted(r["v"] for r in sql.query("SELECT * FROM c.s.t")) == ["b", "c"]
    assert sql.table_exists("c.s.t")
```

- [ ] **Step 4: Run it and verify it fails**

Run: `cd engine && .venv/bin/pytest tests/unit/test_fakes.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'lakebase_health'`

- [ ] **Step 5: Write `config.py`, `sql.py`, `__init__.py`, `tests/fakes.py`**

`engine/src/lakebase_health/__init__.py`:
```python
"""Lakebase health engine."""
```

`engine/src/lakebase_health/config.py`:
```python
from dataclasses import dataclass

RAW_TABLES = (
    "active_session_history", "wait_event_counters", "plan_history", "pg_stat_statements_counters",
    "ddl_history", "postgres_logs", "compute_counters", "compute_gauges", "database_counters", "database_gauges",
)


@dataclass(frozen=True)
class EngineConfig:
    catalog: str
    schema: str = "lakebase_health"
    source_schema: str = "system.lakebase"
    backfill_days: int = 7
    retention_days: int = 90
    overlap_minutes: int = 10
    engine_enabled: bool = True
    resolve_after_clean_runs: int = 2

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def raw(self, source: str) -> str:
        return self.table(f"raw_{source}")
```

`engine/src/lakebase_health/sql.py`:
```python
from typing import Any, Mapping, Protocol


class Sql(Protocol):
    def query(self, stmt: str, args: Mapping[str, Any] | None = None) -> list[dict]: ...
    def execute(self, stmt: str, args: Mapping[str, Any] | None = None) -> None: ...
    def table_exists(self, name: str) -> bool: ...
    def insert_rows(self, table: str, rows: list[dict]) -> None: ...
    def merge_rows(self, table: str, rows: list[dict], keys: list[str]) -> None: ...


class SparkSql:
    """Sql over a SparkSession. Rows are written using the target table's own schema."""

    def __init__(self, spark):
        self.spark = spark

    def query(self, stmt, args=None):
        return [r.asDict(recursive=True) for r in self.spark.sql(stmt, args=dict(args) if args else None).collect()]

    def execute(self, stmt, args=None):
        self.spark.sql(stmt, args=dict(args) if args else None).collect()

    def table_exists(self, name):
        return self.spark.catalog.tableExists(name)

    def _df(self, table, rows):
        schema = self.spark.table(table).schema
        return self.spark.createDataFrame([tuple(r.get(f.name) for f in schema.fields) for r in rows], schema)

    def insert_rows(self, table, rows):
        if rows:
            self._df(table, rows).write.mode("append").saveAsTable(table)

    def merge_rows(self, table, rows, keys):
        if not rows:
            return
        view = "_lakebase_health_merge_src"
        self._df(table, rows).createOrReplaceTempView(view)
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        self.spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} "
                       f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *").collect()
```

`engine/tests/__init__.py` and `engine/tests/unit/__init__.py`: empty files.

`engine/tests/fakes.py`:
```python
import re


class FakeSql:
    """In-memory Sql. `responses` is a list of (regex, rows | callable(stmt, args) | Exception)."""

    def __init__(self, responses=None, existing_tables=()):
        self.statements = []
        self.responses = list(responses or [])
        self.tables = {}
        self.existing = set(existing_tables)

    def _answer(self, stmt, args):
        m = re.fullmatch(r"\s*SELECT \* FROM (\S+)\s*", stmt)
        if m and m.group(1) in self.tables:
            return [dict(r) for r in self.tables[m.group(1)]]
        for pattern, resp in self.responses:
            if re.search(pattern, stmt, re.S):
                if isinstance(resp, Exception):
                    raise resp
                return resp(stmt, args) if callable(resp) else [dict(r) for r in resp]
        return []

    def query(self, stmt, args=None):
        self.statements.append((stmt, dict(args or {})))
        return self._answer(stmt, args or {})

    def execute(self, stmt, args=None):
        self.statements.append((stmt, dict(args or {})))
        self._answer(stmt, args or {})

    def table_exists(self, name):
        return name in self.existing or name in self.tables

    def insert_rows(self, table, rows):
        self.tables.setdefault(table, []).extend(dict(r) for r in rows)

    def merge_rows(self, table, rows, keys):
        current = self.tables.setdefault(table, [])
        for row in rows:
            for i, old in enumerate(current):
                if all(old.get(k) == row.get(k) for k in keys):
                    current[i] = dict(row)
                    break
            else:
                current.append(dict(row))

    def ran(self, pattern):
        return [s for s, _ in self.statements if re.search(pattern, s, re.S)]
```

- [ ] **Step 6: Run the tests and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_fakes.py -q`
Expected: `3 passed`

- [ ] **Step 7: Commit**

```bash
git add .gitignore engine/pyproject.toml engine/src engine/tests
git commit -m "feat(engine): package skeleton, Sql protocol, FakeSql"
```

---

### Task 2: Shared models, exclusions, check spec parsing and validation

**Files:**
- Create: `engine/src/lakebase_health/models.py`, `engine/src/lakebase_health/exclusions.py`, `engine/src/lakebase_health/checkspec.py`, `engine/src/lakebase_health/checks/exclusions.yaml`
- Test: `engine/tests/unit/test_checkspec.py`

**Interfaces:**
- Consumes: none.
- Produces:
  - `models.FLEET = ("*", "*")`.
  - `models.CheckResult`, `models.CheckOutcome`, `models.Finding`, `models.EndpointInfo`, `models.Action` (fields exactly as below).
  - `exclusions.Exclusions` with `.tokens() -> dict[str, str]`; `exclusions.load_exclusions(path) -> Exclusions`; `exclusions.KNOWN_TOKENS: frozenset[str]`.
  - `checkspec.CheckSpec`, `checkspec.FixSpec` (`.actionable`), `checkspec.CheckSpecError`.
  - `checkspec.parse_check(data: dict, stem: str | None = None, api_functions: frozenset[str] = frozenset()) -> CheckSpec`.
  - `checkspec.load_checks(directory: Path, api_functions: frozenset[str]) -> list[CheckSpec]`.
  - `checkspec.render_sql(spec, excl) -> str`; `checkspec.render_template(template: str | None, row: dict) -> str | None`.

- [ ] **Step 1: Write the failing tests `engine/tests/unit/test_checkspec.py`**

```python
from pathlib import Path

import pytest

from lakebase_health.checkspec import CheckSpecError, load_checks, parse_check, render_sql, render_template
from lakebase_health.exclusions import Exclusions, load_exclusions

PKG = Path(__file__).resolve().parents[2] / "src" / "lakebase_health" / "checks"

BASE = {
    "id": "x", "title": "X", "category": "c", "source": "postgres", "explain": "e",
    "sql": "SELECT 1 AS measure, 'a' AS object FROM pg_class WHERE relnamespace NOT IN {excluded_schemas}",
    "thresholds": {"warning": 0},
}


def test_parse_minimal_check():
    spec = parse_check(dict(BASE), stem="x")
    assert spec.direction == "above" and spec.schedule == "every_run" and spec.fix is None


@pytest.mark.parametrize("patch,msg", [
    ({"source": "nope"}, "source"),
    ({"thresholds": {}}, "thresholds"),
    ({"thresholds": {"warning": 10, "critical": 5}}, "order"),
    ({"sql": "SELECT {excluded_bogus}"}, "unknown token"),
    ({"guards": ["stats_window ~ 3"]}, "guard"),
    ({"fix": {"kind": "ddl", "template": "X", "risk": "safe", "reversible": True}}, "rollback"),
])
def test_invalid_checks_are_rejected(patch, msg):
    with pytest.raises(CheckSpecError, match=msg):
        parse_check({**BASE, **patch}, stem="x")


def test_id_must_match_filename():
    with pytest.raises(CheckSpecError, match="filename"):
        parse_check(dict(BASE), stem="y")


def test_api_check_needs_known_function():
    data = {k: v for k, v in BASE.items() if k != "sql"} | {"source": "api", "function": "f"}
    with pytest.raises(CheckSpecError, match="function"):
        parse_check(data, stem="x", api_functions=frozenset({"g"}))
    assert parse_check(data, stem="x", api_functions=frozenset({"f"})).function == "f"


def test_below_direction_order():
    spec = parse_check({**BASE, "direction": "below", "thresholds": {"warning": 99, "critical": 95}}, stem="x")
    assert spec.thresholds == {"warning": 99.0, "critical": 95.0}


def test_render_sql_replaces_tokens():
    excl = Exclusions(slots=("s1",), roles=("r1",), users=("u1",), schemas=("pg_catalog", "sage"),
                      role_regex="^(pg_)", settings_regex="^neon_")
    out = render_sql(parse_check(dict(BASE), stem="x"), excl)
    assert "('pg_catalog', 'sage')" in out and "{" not in out


def test_render_template():
    assert render_template("DROP INDEX {i};", {"i": "a.b"}) == "DROP INDEX a.b;"
    assert render_template("DROP INDEX {i};", {"i": None}) is None
    assert render_template(None, {}) is None


def test_fix_actionable_rules():
    fix = {"kind": "ddl", "template": "A {x}", "rollback": "B", "risk": "safe", "reversible": True}
    assert parse_check({**BASE, "fix": fix}, stem="x").fix.actionable
    manual = {"kind": "manual", "template": "do x", "risk": "high", "reversible": False}
    assert not parse_check({**BASE, "fix": manual}, stem="x").fix.actionable


def test_packaged_exclusions_file_loads():
    excl = load_exclusions(PKG / "exclusions.yaml")
    assert "wal_retention_slot" in excl.slots and "sage" in excl.schemas and "cloud_admin" in excl.users


def test_load_checks_rejects_duplicate_ids(tmp_path):
    for d in ("a", "b"):
        (tmp_path / d).mkdir()
        (tmp_path / d / "x.yaml").write_text(
            "id: x\ntitle: X\ncategory: c\nsource: delta\nexplain: e\nsql: SELECT 1\nthresholds: {warning: 0}\n")
    with pytest.raises(CheckSpecError, match="duplicate"):
        load_checks(tmp_path, frozenset())
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_checkspec.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'lakebase_health.checkspec'`

- [ ] **Step 3: Write `models.py`**

```python
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

    def to_row(self) -> dict:
        return asdict(self)

    @classmethod
    def from_row(cls, row: dict) -> "Action":
        return cls(**{k: row.get(k) for k in cls.__dataclass_fields__})
```

- [ ] **Step 4: Write `exclusions.py` and `checks/exclusions.yaml`**

`engine/src/lakebase_health/exclusions.py`:
```python
from dataclasses import dataclass
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
                      schemas=tuple(d["schemas"]), role_regex=d["role_regex"], settings_regex=d["settings_regex"])
```

`engine/src/lakebase_health/checks/exclusions.yaml`:
```yaml
# Lakebase platform objects. Every check excludes these through {excluded_*} tokens.
slots: [wal_retention_slot, wal_proposer_slot]        # platform physical slots; wal_retention_slot is normally inactive
users: [cloud_admin]                                   # platform agents (exporter, monitor) connect as this user
roles: [cloud_admin, databricks_control_plane, databricks_superuser, databricks_monitor]
role_regex: '^(pg_|databricks_)'
schemas: [pg_catalog, information_schema, pg_toast, hint_plan, sage]   # sage = pg_sage's own schema
settings_regex: '^(databricks|neon|neon_monitor|compute_ctl|lakebase)\.|^neon_'
```

- [ ] **Step 5: Write `checkspec.py`**

```python
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from .exclusions import KNOWN_TOKENS, Exclusions

SOURCES = {"delta", "postgres", "api"}
SCHEDULES = {"every_run", "daily"}
SEVERITY_ORDER = ("info", "warning", "critical")
FIX_KINDS = {"ddl", "setting", "rewrite", "hint", "manual"}
ACTIONABLE_KINDS = {"ddl", "setting"}
RISKS = {"safe", "moderate", "high"}
GUARD_RE = re.compile(r"^\s*([a-z_]+)\s*(>=|<=|>|<|==)\s*(-?[0-9.]+)\s*$")
TOKEN_RE = re.compile(r"\{(excluded_[a-z_]+)\}")
VAR_RE = re.compile(r"\{([a-z_]+)\}")


class CheckSpecError(ValueError):
    pass


@dataclass(frozen=True)
class FixSpec:
    kind: str
    template: str | None
    rollback: str | None
    risk: str
    reversible: bool
    verify_on_branch: bool

    @property
    def actionable(self) -> bool:
        return self.kind in ACTIONABLE_KINDS and self.reversible and bool(self.template)


@dataclass(frozen=True)
class CheckSpec:
    id: str
    title: str
    category: str
    source: str
    explain: str
    thresholds: dict
    direction: str = "above"
    measure: str = "measure"
    sql: str | None = None
    function: str | None = None
    schedule: str = "every_run"
    guards: tuple = ()
    requires_extension: str | None = None
    fix: FixSpec | None = None

    def tables(self) -> set:
        return set(re.findall(r"system\.lakebase\.([a-z_]+)", self.sql or ""))


def _parse_fix(d, errors) -> FixSpec | None:
    if d is None:
        return None
    kind, risk = d.get("kind"), d.get("risk")
    if kind not in FIX_KINDS:
        errors.append(f"fix.kind must be one of {sorted(FIX_KINDS)}")
    if risk not in RISKS:
        errors.append(f"fix.risk must be one of {sorted(RISKS)}")
    reversible = bool(d.get("reversible", False))
    if kind in ACTIONABLE_KINDS and not d.get("template"):
        errors.append("fix.template is required for ddl/setting fixes")
    if kind in ACTIONABLE_KINDS and reversible and not d.get("rollback"):
        errors.append("fix.rollback is required when reversible is true")
    return FixSpec(kind=kind, template=d.get("template"), rollback=d.get("rollback"), risk=risk,
                   reversible=reversible, verify_on_branch=bool(d.get("verify_on_branch", False)))


def parse_check(data: dict, stem: str | None = None, api_functions: frozenset = frozenset()) -> CheckSpec:
    errors = []
    for key in ("id", "title", "category", "source", "explain"):
        if not data.get(key):
            errors.append(f"missing {key}")
    if stem is not None and data.get("id") != stem:
        errors.append(f"id {data.get('id')!r} must match filename {stem!r}")
    source = data.get("source")
    if source not in SOURCES:
        errors.append(f"source must be one of {sorted(SOURCES)}")
    if source in ("delta", "postgres") and not data.get("sql"):
        errors.append("sql is required for delta/postgres checks")
    if source == "api" and data.get("function") not in api_functions:
        errors.append(f"unknown api function {data.get('function')!r}")
    schedule = data.get("schedule", "every_run")
    if schedule not in SCHEDULES:
        errors.append(f"schedule must be one of {sorted(SCHEDULES)}")
    direction = data.get("direction", "above")
    if direction not in ("above", "below"):
        errors.append("direction must be above or below")
    thresholds = {k: float(v) for k, v in (data.get("thresholds") or {}).items()}
    if not thresholds or set(thresholds) - set(SEVERITY_ORDER):
        errors.append("thresholds must be a non-empty subset of info/warning/critical")
    else:
        vals = [thresholds[s] for s in SEVERITY_ORDER if s in thresholds]
        ordered = vals == sorted(vals) if direction == "above" else vals == sorted(vals, reverse=True)
        if not ordered:
            errors.append("thresholds out of order for direction")
    for token in TOKEN_RE.findall(data.get("sql") or ""):
        if token not in KNOWN_TOKENS:
            errors.append(f"unknown token {{{token}}}")
    guards = tuple(data.get("guards") or ())
    for g in guards:
        if not GUARD_RE.match(g):
            errors.append(f"bad guard {g!r}")
    fix = _parse_fix(data.get("fix"), errors)
    if errors:
        raise CheckSpecError(f"{data.get('id')}: " + "; ".join(errors))
    return CheckSpec(id=data["id"], title=data["title"], category=data["category"], source=source,
                     explain=data["explain"].strip(), thresholds=thresholds, direction=direction,
                     measure=data.get("measure", "measure"), sql=data.get("sql"), function=data.get("function"),
                     schedule=schedule, guards=guards, requires_extension=data.get("requires_extension"), fix=fix)


def load_checks(directory: Path, api_functions: frozenset) -> list:
    specs, seen = [], set()
    for path in sorted(Path(directory).rglob("*.yaml")):
        if path.name == "exclusions.yaml":
            continue
        spec = parse_check(yaml.safe_load(path.read_text()), stem=path.stem, api_functions=api_functions)
        if spec.id in seen:
            raise CheckSpecError(f"duplicate check id {spec.id}")
        seen.add(spec.id)
        specs.append(spec)
    return specs


def render_sql(spec: CheckSpec, excl: Exclusions) -> str:
    out = spec.sql or ""
    for name, value in excl.tokens().items():
        out = out.replace("{" + name + "}", value)
    return out


def render_template(template: str | None, row: dict) -> str | None:
    if not template:
        return None
    names = set(VAR_RE.findall(template))
    if any(row.get(n) is None for n in names):
        return None
    return template.format_map({n: row[n] for n in names})
```

- [ ] **Step 6: Run the tests and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_checkspec.py -q`
Expected: `15 passed`

- [ ] **Step 7: Commit**

```bash
git add engine/src/lakebase_health/{models,exclusions,checkspec}.py engine/src/lakebase_health/checks/exclusions.yaml engine/tests/unit/test_checkspec.py
git commit -m "feat(engine): models, exclusions, check spec parsing and validation"
```

---

### Task 3: Severity classification and guards

**Files:**
- Create: `engine/src/lakebase_health/severity.py`
- Test: `engine/tests/unit/test_severity.py`

**Interfaces:**
- Produces: `classify(measure, thresholds: dict, direction: str = "above") -> str | None`; `guards_pass(guards: tuple, ctx: dict) -> bool`; `SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}`.

- [ ] **Step 1: Write the failing tests**

```python
import math

from lakebase_health.severity import classify, guards_pass


def test_classify_above_is_strict():
    t = {"warning": 50, "critical": 75}
    assert classify(50, t) is None
    assert classify(74.55, t) == "warning"
    assert classify(76, t) == "critical"


def test_classify_below():
    t = {"warning": 99, "critical": 95}
    assert classify(100, t, "below") is None
    assert classify(97, t, "below") == "warning"
    assert classify(90, t, "below") == "critical"


def test_classify_info_only():
    assert classify(1, {"info": 0}) == "info"


def test_classify_none_and_nan():
    assert classify(None, {"warning": 0}) is None
    assert classify(math.nan, {"warning": 0}) is None
    assert classify("12.5", {"warning": 10}) == "warning"


def test_guards():
    assert guards_pass(("stats_window_hours >= 24",), {"stats_window_hours": 30})
    assert not guards_pass(("stats_window_hours >= 24",), {"stats_window_hours": 2})
    assert not guards_pass(("stats_window_hours >= 24",), {})
    assert guards_pass((), {})
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_severity.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `severity.py`**

```python
import math
import operator

from .checkspec import GUARD_RE

SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}
_OPS = {">=": operator.ge, "<=": operator.le, ">": operator.gt, "<": operator.lt, "==": operator.eq}


def classify(measure, thresholds: dict, direction: str = "above") -> str | None:
    if measure is None:
        return None
    try:
        m = float(measure)
    except (TypeError, ValueError):
        return None
    if math.isnan(m):
        return None
    for sev in ("critical", "warning", "info"):
        t = thresholds.get(sev)
        if t is None:
            continue
        if (direction == "above" and m > t) or (direction == "below" and m < t):
            return sev
    return None


def guards_pass(guards: tuple, ctx: dict) -> bool:
    for g in guards:
        name, op, value = GUARD_RE.match(g).groups()
        if ctx.get(name) is None or not _OPS[op](float(ctx[name]), float(value)):
            return False
    return True
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_severity.py -q`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/severity.py engine/tests/unit/test_severity.py
git commit -m "feat(engine): severity classification and guards"
```

---

### Task 4: Fingerprints and finding lifecycle

**Files:**
- Create: `engine/src/lakebase_health/fingerprint.py`, `engine/src/lakebase_health/lifecycle.py`
- Test: `engine/tests/unit/test_lifecycle.py`

**Interfaces:**
- Consumes: `models.CheckResult`, `CheckOutcome`, `Finding`, `FLEET`; `checkspec.CheckSpec`; `severity.SEVERITY_RANK`.
- Produces: `fingerprint(check_id, project_id, endpoint_id, obj) -> str` (32 hex chars); `apply_run(existing: dict[str, Finding], outcomes: list[CheckOutcome], specs: dict[str, CheckSpec], now: datetime, resolve_after: int = 2) -> list[Finding]`, returning every finding it created or changed.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import datetime, timedelta, timezone

from lakebase_health.checkspec import parse_check
from lakebase_health.fingerprint import fingerprint
from lakebase_health.lifecycle import apply_run
from lakebase_health.models import FLEET, CheckOutcome, CheckResult

NOW = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
SPEC = parse_check({"id": "c1", "title": "T", "category": "cat", "source": "postgres", "explain": "E",
                    "sql": "SELECT 1", "thresholds": {"warning": 0}}, stem="c1")
SPECS = {"c1": SPEC}


def res(obj="a", sev="warning", ep="ep1"):
    fp = fingerprint("c1", "p1", ep, obj)
    return CheckResult("c1", fp, "p1", "b1", ep, obj, 1.0, sev, "high", {"k": 1})


def run(existing, outcomes, now=NOW):
    changed = apply_run(existing, outcomes, SPECS, now)
    existing.update({f.fingerprint: f for f in changed})
    return changed


def test_fingerprint_is_stable_and_endpoint_specific():
    assert fingerprint("c", "p", "e1", "o") == fingerprint("c", "p", "e1", "o")
    assert fingerprint("c", "p", "e1", "o") != fingerprint("c", "p", "e2", "o")
    assert len(fingerprint("c", None, None, "o")) == 32


def test_new_then_open_then_resolved_after_two_clean_runs():
    findings = {}
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])])
    assert f.status == "new" and f.occurrences == 1
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])], NOW + timedelta(hours=1))
    assert f.status == "open" and f.occurrences == 2
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok")], NOW + timedelta(hours=2))
    assert f.status == "open" and f.clean_runs == 1
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok")], NOW + timedelta(hours=3))
    assert f.status == "resolved" and f.resolved_at == NOW + timedelta(hours=3)


def test_skipped_error_timeout_never_resolve():
    findings = {}
    run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])])
    for status in ("skipped:compute_inactive", "error", "timeout", "skipped:not_enrolled"):
        assert run(findings, [CheckOutcome("c1", ("p1", "ep1"), status)]) == []
    assert next(iter(findings.values())).status == "new"


def test_other_scope_does_not_resolve():
    findings = {}
    run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])])
    for _ in range(3):
        run(findings, [CheckOutcome("c1", ("p1", "ep2"), "ok")])
    assert next(iter(findings.values())).status == "new"


def test_fleet_scope_resolves_any_endpoint():
    findings = {}
    run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    assert next(iter(findings.values())).status == "resolved"


def test_resolved_finding_reopens_as_new():
    findings = {}
    run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    assert f.status == "new" and f.resolved_at is None and f.occurrences == 1


def test_suppression_until_expiry():
    findings = {}
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    f.status, f.suppressed_until = "suppressed", NOW + timedelta(days=1)
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])], NOW + timedelta(hours=1))
    assert f.status == "suppressed"
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])], NOW + timedelta(days=2))
    assert f.status == "open"


def test_duplicate_rows_keep_highest_severity():
    [f] = apply_run({}, [CheckOutcome("c1", FLEET, "ok", results=[res(sev="warning"), res(sev="critical")])], SPECS, NOW)
    assert f.severity == "critical"


def test_naive_timestamps_from_spark_are_treated_as_utc():
    findings = {}
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    f.status, f.suppressed_until = "suppressed", (NOW + timedelta(days=1)).replace(tzinfo=None)
    f.first_seen = f.first_seen.replace(tzinfo=None)
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])], NOW + timedelta(hours=1))
    assert f.status == "suppressed"
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_lifecycle.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `fingerprint.py` and `lifecycle.py`**

`engine/src/lakebase_health/fingerprint.py`:
```python
import hashlib


def fingerprint(check_id: str, project_id: str | None, endpoint_id: str | None, obj: str | None) -> str:
    raw = "\x1f".join([check_id, project_id or "", endpoint_id or "", obj or ""])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]
```

`engine/src/lakebase_health/lifecycle.py`:
```python
import json
from datetime import datetime, timezone

from .models import FLEET, CheckOutcome, Finding
from .severity import SEVERITY_RANK

ACTIVE = ("new", "open", "suppressed")


def _utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _covered(f: Finding, covered: set) -> bool:
    return (f.check_id, FLEET) in covered or (f.check_id, (f.project_id, f.endpoint_id)) in covered


def apply_run(existing: dict, outcomes: list, specs: dict, now: datetime, resolve_after: int = 2) -> list:
    reported = {}
    covered = set()
    for o in outcomes:
        if not o.ok:
            continue
        covered.add((o.check_id, tuple(o.scope)))
        for r in o.results:
            prev = reported.get(r.fingerprint)
            if prev is None or SEVERITY_RANK[r.severity] > SEVERITY_RANK[prev.severity]:
                reported[r.fingerprint] = r

    changed = []
    for fp, r in reported.items():
        spec = specs[r.check_id]
        f = existing.get(fp)
        fresh = dict(severity=r.severity, confidence=r.confidence, measure=r.measure,
                     evidence=json.dumps(r.evidence, default=str, sort_keys=True), query_text=r.query_text,
                     candidate_sql=r.candidate_sql, rollback_sql=r.rollback_sql, last_seen=now,
                     last_checked_at=now, clean_runs=0, title=spec.title, explain=spec.explain)
        if f is None or f.status == "resolved":
            f = Finding(fingerprint=fp, check_id=r.check_id, category=spec.category, project_id=r.project_id,
                        branch_id=r.branch_id, endpoint_id=r.endpoint_id, object=r.object, status="new",
                        first_seen=now, occurrences=1, suppressed_until=None, resolved_at=None, **fresh)
        else:
            for k, v in fresh.items():
                setattr(f, k, v)
            f.first_seen = _utc(f.first_seen)
            f.occurrences = (f.occurrences or 0) + 1
            until = _utc(f.suppressed_until)
            f.status = "suppressed" if until is not None and until > now else "open"
        changed.append(f)

    for fp, f in existing.items():
        if fp in reported or f.status not in ACTIVE or not _covered(f, covered):
            continue
        f.clean_runs = (f.clean_runs or 0) + 1
        f.last_checked_at = now
        if f.clean_runs >= resolve_after:
            f.status, f.resolved_at = "resolved", now
        changed.append(f)
    return changed
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_lifecycle.py -q`
Expected: `9 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/{fingerprint,lifecycle}.py engine/tests/unit/test_lifecycle.py
git commit -m "feat(engine): fingerprints and finding lifecycle"
```

---

### Task 5: Proposed actions, shadow mode and hash-chained ledger

**Files:**
- Create: `engine/src/lakebase_health/actions.py`, `engine/src/lakebase_health/ledger.py`
- Test: `engine/tests/unit/test_actions_ledger.py`

**Interfaces:**
- Consumes: `models.Finding`, `models.Action`; `checkspec.CheckSpec`.
- Produces:
  - `policy_level(risk, reversible, verify_on_branch) -> str`, one of `"L3"`, `"L2"`, `"L1"`.
  - `plan_actions(findings: list[Finding], existing: dict[str, Action], specs: dict, now) -> tuple[list[Action], list[dict]]`; events are dicts `{action_id, event_type, payload}`, with `event_type` in `proposed` / `expired`.
  - `shadow_rows(actions: list[Action], run_id, now) -> list[dict]`.
  - `chain(prev_hash: str | None, events: list[dict], now, actor="engine") -> list[dict]`, rows with keys `event_id, action_id, event_type, actor, payload, ts, prev_hash, hash`.
  - `verify(rows: list[dict]) -> bool`.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import datetime, timedelta, timezone

from lakebase_health.actions import plan_actions, policy_level, shadow_rows
from lakebase_health.checkspec import parse_check
from lakebase_health.ledger import chain, verify
from lakebase_health.models import Finding

NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
FIX = {"kind": "ddl", "template": "CREATE INDEX x;", "rollback": "DROP INDEX x;", "risk": "moderate",
       "reversible": True, "verify_on_branch": True}
SPECS = {
    "a": parse_check({"id": "a", "title": "A", "category": "c", "source": "postgres", "explain": "e",
                      "sql": "SELECT 1", "thresholds": {"warning": 0}, "fix": FIX}, stem="a"),
    "m": parse_check({"id": "m", "title": "M", "category": "c", "source": "postgres", "explain": "e",
                      "sql": "SELECT 1", "thresholds": {"warning": 0},
                      "fix": {"kind": "manual", "template": "do it", "risk": "high"}}, stem="m"),
}


def finding(check="a", status="new", sql="CREATE INDEX x;", fp="f1"):
    return Finding(fp, check, "T", "c", "p", "b", "e", "o", status, "warning", "high", 1.0, NOW, NOW, NOW, 1, 0,
                   "{}", None, "e", sql, "DROP INDEX x;")


def test_policy_levels():
    assert policy_level("safe", True, False) == "L3"
    assert policy_level("safe", True, True) == "L2"
    assert policy_level("moderate", True, True) == "L2"
    assert policy_level("high", True, True) == "L1"
    assert policy_level("safe", False, False) == "L1"


def test_new_actionable_finding_proposes_once():
    actions, events = plan_actions([finding()], {}, SPECS, NOW)
    assert len(actions) == 1 and actions[0].state == "proposed" and events[0]["event_type"] == "proposed"
    again, events2 = plan_actions([finding(status="open")], {actions[0].action_id: actions[0]}, SPECS, NOW)
    assert events2 == [] and again[0].expires_at == NOW + timedelta(days=7)


def test_manual_or_missing_sql_never_proposes():
    assert plan_actions([finding(check="m")], {}, SPECS, NOW) == ([], [])
    assert plan_actions([finding(sql=None)], {}, SPECS, NOW) == ([], [])


def test_resolved_finding_expires_action_and_changed_sql_replaces_it():
    [a], _ = plan_actions([finding()], {}, SPECS, NOW)
    expired, ev = plan_actions([finding(status="resolved")], {a.action_id: a}, SPECS, NOW)
    assert expired[0].state == "expired" and ev[0]["event_type"] == "expired"
    replaced, ev2 = plan_actions([finding(sql="CREATE INDEX y;")], {a.action_id: a}, SPECS, NOW)
    assert {x.state for x in replaced} == {"expired", "proposed"} and len(ev2) == 2


def test_shadow_rows():
    [a], _ = plan_actions([finding()], {}, SPECS, NOW)
    [row] = shadow_rows([a], "r1", NOW)
    assert row["would_allow_level"] == "L2" and row["would_execute"] is False


def test_ledger_chain_and_tamper_detection():
    rows = chain(None, [{"action_id": "a1", "event_type": "proposed", "payload": {"x": 1}}], NOW)
    rows += chain(rows[-1]["hash"], [{"action_id": "a1", "event_type": "expired", "payload": {}}], NOW)
    assert verify(rows) and rows[1]["prev_hash"] == rows[0]["hash"]
    rows[0]["payload"] = '{"x": 2}'
    assert not verify(rows)
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_actions_ledger.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `actions.py` and `ledger.py`**

`engine/src/lakebase_health/actions.py`:
```python
import hashlib
from datetime import timedelta

from .models import Action

ACTION_TTL = timedelta(days=7)


def policy_level(risk: str, reversible: bool, verify_on_branch: bool) -> str:
    if not reversible or risk == "high":
        return "L1"
    if risk == "safe" and not verify_on_branch:
        return "L3"
    return "L2"


def _action_id(fingerprint: str, sql: str) -> str:
    return hashlib.sha256(f"{fingerprint}\x1f{sql}".encode()).hexdigest()[:32]


def plan_actions(findings: list, existing: dict, specs: dict, now) -> tuple:
    by_fp = {}
    for a in existing.values():
        by_fp.setdefault(a.fingerprint, []).append(a)
    changed, events = [], []

    def expire(a, reason):
        a.state, a.updated_at = "expired", now
        changed.append(a)
        events.append({"action_id": a.action_id, "event_type": "expired", "payload": {"reason": reason}})

    for f in findings:
        current = [a for a in by_fp.get(f.fingerprint, []) if a.state == "proposed"]
        spec = specs.get(f.check_id)
        fix = spec.fix if spec else None
        wanted = None
        if f.status in ("new", "open") and fix is not None and fix.actionable and f.candidate_sql:
            wanted = _action_id(f.fingerprint, f.candidate_sql)
        for a in current:
            if a.action_id != wanted:
                expire(a, "finding_resolved" if wanted is None else "sql_changed")
        if wanted is None:
            continue
        keep = next((a for a in current if a.action_id == wanted), None)
        if keep is not None:
            keep.expires_at, keep.updated_at = now + ACTION_TTL, now
            changed.append(keep)
            continue
        a = Action(wanted, f.fingerprint, f.check_id, fix.kind, f.candidate_sql, f.rollback_sql, fix.risk,
                   fix.reversible, fix.verify_on_branch, "proposed", now, now, now + ACTION_TTL)
        changed.append(a)
        events.append({"action_id": a.action_id, "event_type": "proposed",
                       "payload": {"fingerprint": f.fingerprint, "check_id": f.check_id, "sql": a.sql,
                                   "rollback_sql": a.rollback_sql, "risk": a.risk}})
    return changed, events


def shadow_rows(actions: list, run_id: str, now) -> list:
    rows = []
    for a in actions:
        if a.state != "proposed":
            continue
        level = policy_level(a.risk, a.reversible, a.verify_on_branch)
        reason = {"L3": "safe and reversible: an L3 policy would apply it unattended",
                  "L2": "needs branch verification and one-click approval",
                  "L1": "high risk or irreversible: human-run script only"}[level]
        rows.append({"action_id": a.action_id, "evaluated_at": now, "run_id": run_id, "would_allow_level": level,
                     "would_execute": level == "L3", "reason": reason, "outcome_score": None})
    return rows
```

`engine/src/lakebase_health/ledger.py`:
```python
import hashlib
import json
import uuid


def _digest(prev_hash, event_id, action_id, event_type, actor, payload, ts) -> str:
    raw = "\x1f".join([prev_hash or "", event_id, action_id, event_type, actor, payload, ts.isoformat()])
    return hashlib.sha256(raw.encode()).hexdigest()


def chain(prev_hash, events: list, now, actor: str = "engine") -> list:
    rows = []
    for e in events:
        payload = json.dumps(e["payload"], sort_keys=True, default=str)
        event_id = uuid.uuid4().hex
        h = _digest(prev_hash, event_id, e["action_id"], e["event_type"], actor, payload, now)
        rows.append({"event_id": event_id, "action_id": e["action_id"], "event_type": e["event_type"], "actor": actor,
                     "payload": payload, "ts": now, "prev_hash": prev_hash, "hash": h})
        prev_hash = h
    return rows


def verify(rows: list) -> bool:
    prev = rows[0]["prev_hash"] if rows else None
    for r in rows:
        if r["prev_hash"] != prev:
            return False
        if _digest(r["prev_hash"], r["event_id"], r["action_id"], r["event_type"], r["actor"], r["payload"], r["ts"]) != r["hash"]:
            return False
        prev = r["hash"]
    return True
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_actions_ledger.py -q`
Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/{actions,ledger}.py engine/tests/unit/test_actions_ledger.py
git commit -m "feat(engine): proposed actions, shadow mode, hash-chained ledger"
```

---

### Task 6: Schema DDL and column masks

**Files:**
- Create: `engine/src/lakebase_health/ddl.py`
- Test: `engine/tests/unit/test_ddl.py`

**Interfaces:**
- Consumes: `EngineConfig`, `Sql`.
- Produces: `ensure_schema(sql, cfg) -> None` (idempotent); `ensure_masks(sql, cfg) -> None`; `MASKED_COLUMNS: dict[str, tuple[str, ...]]` (table short name → columns).

- [ ] **Step 1: Write the failing tests**

```python
from lakebase_health.config import EngineConfig
from lakebase_health.ddl import MASKED_COLUMNS, ensure_masks, ensure_schema
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
TABLES = ("runs", "ingest_watermarks", "inventory", "check_results", "findings", "proposed_actions",
          "shadow_log", "ledger", "metrics_daily")


def test_ensure_schema_creates_everything_idempotently():
    sql = FakeSql()
    ensure_schema(sql, CFG)
    assert sql.ran(r"CREATE SCHEMA IF NOT EXISTS cat\.lakebase_health")
    for t in TABLES:
        assert sql.ran(rf"CREATE TABLE IF NOT EXISTS cat\.lakebase_health\.{t} \("), t
    assert sql.ran(r"CREATE FUNCTION IF NOT EXISTS cat\.lakebase_health\.mask_text")
    assert all("IF NOT EXISTS" in s for s, _ in sql.statements)


def test_findings_has_clean_runs_and_query_text():
    sql = FakeSql()
    ensure_schema(sql, CFG)
    [ddl] = sql.ran(r"TABLE IF NOT EXISTS cat\.lakebase_health\.findings ")
    assert "clean_runs INT" in ddl and "query_text STRING" in ddl


def test_masks_only_on_existing_tables():
    sql = FakeSql(existing_tables={"cat.lakebase_health.raw_plan_history", "cat.lakebase_health.findings"})
    ensure_masks(sql, CFG)
    stmts = sql.ran("SET MASK")
    assert any("raw_plan_history ALTER COLUMN plan_json SET MASK cat.lakebase_health.mask_text" in s for s in stmts)
    assert any("findings ALTER COLUMN query_text" in s for s in stmts)
    assert not any("raw_postgres_logs" in s for s in stmts)
    assert set(MASKED_COLUMNS["raw_plan_history"]) == {"query_text", "plan_json", "parameters"}
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_ddl.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `ddl.py`**

```python
TABLE_DDL = {
    "runs": "run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP, status STRING, step_seconds STRING, "
            "counts STRING, errors STRING",
    "ingest_watermarks": "source_table STRING, max_ts TIMESTAMP, last_run_id STRING, rows_added BIGINT, seconds DOUBLE",
    "inventory": "run_id STRING, captured_at TIMESTAMP, project_id STRING, project_name STRING, branch_id STRING, "
                 "branch_name STRING, is_default_branch BOOLEAN, is_protected BOOLEAN, endpoint_id STRING, "
                 "endpoint_name STRING, endpoint_type STRING, current_state STRING, host STRING, min_cu DOUBLE, "
                 "max_cu DOUBLE, suspend_timeout_s BIGINT, ha_min INT, restore_window_s BIGINT, "
                 "pg_native_login BOOLEAN, opted_out BOOLEAN, enrolled BOOLEAN, databases STRING, "
                 "password_roles STRING, last_pg_check_at TIMESTAMP",
    "check_results": "run_id STRING, ts TIMESTAMP, check_id STRING, fingerprint STRING, project_id STRING, "
                     "branch_id STRING, endpoint_id STRING, object STRING, measure DOUBLE, severity STRING, "
                     "confidence STRING, status STRING, error STRING, evidence STRING, query_text STRING",
    "findings": "fingerprint STRING, check_id STRING, title STRING, category STRING, project_id STRING, "
                "branch_id STRING, endpoint_id STRING, object STRING, status STRING, severity STRING, "
                "confidence STRING, measure DOUBLE, first_seen TIMESTAMP, last_seen TIMESTAMP, "
                "last_checked_at TIMESTAMP, occurrences BIGINT, clean_runs INT, evidence STRING, query_text STRING, "
                "explain STRING, candidate_sql STRING, rollback_sql STRING, suppressed_until TIMESTAMP, "
                "resolved_at TIMESTAMP",
    "proposed_actions": "action_id STRING, fingerprint STRING, check_id STRING, kind STRING, sql STRING, "
                        "rollback_sql STRING, risk STRING, reversible BOOLEAN, verify_on_branch BOOLEAN, "
                        "state STRING, created_at TIMESTAMP, updated_at TIMESTAMP, expires_at TIMESTAMP",
    "shadow_log": "action_id STRING, evaluated_at TIMESTAMP, run_id STRING, would_allow_level STRING, "
                  "would_execute BOOLEAN, reason STRING, outcome_score DOUBLE",
    "ledger": "event_id STRING, action_id STRING, event_type STRING, actor STRING, payload STRING, ts TIMESTAMP, "
              "prev_hash STRING, hash STRING",
    "metrics_daily": "day DATE, project_id STRING, endpoint_id STRING, metric STRING, value DOUBLE",
}

MASKED_COLUMNS = {
    "raw_plan_history": ("query_text", "plan_json", "parameters"),
    "raw_pg_stat_statements_counters": ("query",),
    "raw_postgres_logs": ("statement",),
    "raw_ddl_history": ("query_text", "ddl_json"),
    "findings": ("query_text",),
    "check_results": ("query_text",),
}


def ensure_schema(sql, cfg) -> None:
    sql.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    for name, cols in TABLE_DDL.items():
        sql.execute(f"CREATE TABLE IF NOT EXISTS {cfg.table(name)} ({cols})")
    sql.execute(
        f"CREATE FUNCTION IF NOT EXISTS {cfg.table('mask_text')}(v STRING) RETURNS STRING "
        "RETURN CASE WHEN is_account_group_member('databricks_pii_access') THEN v ELSE '<REDACTED>' END")


def ensure_masks(sql, cfg) -> None:
    fn = cfg.table("mask_text")
    for table, cols in MASKED_COLUMNS.items():
        full = cfg.table(table)
        if not sql.table_exists(full):
            continue
        for col in cols:
            sql.execute(f"ALTER TABLE {full} ALTER COLUMN {col} SET MASK {fn}")
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_ddl.py -q`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/ddl.py engine/tests/unit/test_ddl.py
git commit -m "feat(engine): schema DDL and column masks"
```

---

### Task 7: Telemetry ingestion

**Files:**
- Create: `engine/src/lakebase_health/ingest.py`
- Test: `engine/tests/unit/test_ingest.py`

**Interfaces:**
- Consumes: `EngineConfig`, `RAW_TABLES`, `Sql`.
- Produces: `IngestResult(source, status, rows_added, seconds, error)` with `status` in `ok` / `error`; `ingest_all(sql, cfg, run_id, now, sources=RAW_TABLES) -> list[IngestResult]`.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.ingest import ingest_all
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
WM = datetime(2026, 10, 26, 23, tzinfo=timezone.utc)


def test_first_run_backfills_with_clustering():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 5}])])
    [r] = ingest_all(sql, CFG, "r1", NOW, sources=("compute_gauges",))
    [ctas] = sql.ran("CREATE TABLE")
    assert "cat.lakebase_health.raw_compute_gauges CLUSTER BY (project_id, ts)" in ctas
    assert "FROM system.lakebase.compute_gauges WHERE ts >= current_timestamp() - INTERVAL 7 DAYS" in ctas
    assert r.status == "ok"
    assert sql.tables["cat.lakebase_health.ingest_watermarks"][0]["max_ts"] == NOW


def test_incremental_uses_overlap_and_except():
    sql = FakeSql(existing_tables={"cat.lakebase_health.raw_compute_gauges"},
                  responses=[(r"FROM cat\.lakebase_health\.ingest_watermarks", [{"max_ts": WM}]),
                             (r"^INSERT INTO", [{"num_inserted_rows": 42}]),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 9}])])
    [r] = ingest_all(sql, CFG, "r1", NOW, sources=("compute_gauges",))
    [(stmt, args)] = [(s, a) for s, a in sql.statements if s.startswith("INSERT INTO")]
    assert "SELECT * FROM system.lakebase.compute_gauges WHERE ts > :lo" in stmt
    assert "EXCEPT SELECT * FROM cat.lakebase_health.raw_compute_gauges WHERE ts > :lo" in stmt
    assert args["lo"] == WM - timedelta(minutes=10)
    assert r.rows_added == 42
    assert not sql.ran("CREATE TABLE")


def test_retention_delete_runs():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    ingest_all(sql, CFG, "r1", NOW, sources=("compute_gauges",))
    assert sql.ran(r"DELETE FROM cat\.lakebase_health\.raw_compute_gauges WHERE ts < current_timestamp\(\) - INTERVAL 90 DAYS")


def test_one_table_failing_does_not_stop_others_or_advance_watermark():
    sql = FakeSql(responses=[(r"system\.lakebase\.plan_history", RuntimeError("boom")),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    results = ingest_all(sql, CFG, "r1", NOW, sources=("plan_history", "compute_gauges"))
    assert [r.status for r in results] == ["error", "ok"]
    assert "boom" in results[0].error
    wms = sql.tables["cat.lakebase_health.ingest_watermarks"]
    assert [w["source_table"] for w in wms] == ["compute_gauges"]


def test_tables_are_processed_sequentially_in_given_order():
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    ingest_all(sql, CFG, "r1", NOW, sources=("b_table", "a_table"))
    ctas = sql.ran("CREATE TABLE")
    assert "raw_b_table" in ctas[0] and "raw_a_table" in ctas[1]
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_ingest.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `ingest.py`**

```python
import time
from dataclasses import dataclass
from datetime import timedelta

from .config import RAW_TABLES


@dataclass
class IngestResult:
    source: str
    status: str
    rows_added: int | None
    seconds: float
    error: str | None = None


def _watermark(sql, cfg, source):
    rows = sql.query(f"SELECT max_ts FROM {cfg.table('ingest_watermarks')} WHERE source_table = :s", {"s": source})
    return rows[0]["max_ts"] if rows and rows[0].get("max_ts") is not None else None


def ingest_all(sql, cfg, run_id, now, sources=RAW_TABLES) -> list:
    results = []
    for src in sources:  # one table at a time: parallel statements made system-table compile ~10x slower
        t0 = time.monotonic()
        target, source = cfg.raw(src), f"{cfg.source_schema}.{src}"
        try:
            wm = _watermark(sql, cfg, src) if sql.table_exists(target) else None
            if not sql.table_exists(target):
                sql.execute(f"CREATE TABLE IF NOT EXISTS {target} CLUSTER BY (project_id, ts) AS "
                            f"SELECT * FROM {source} WHERE ts >= current_timestamp() - INTERVAL {cfg.backfill_days} DAYS")
                added = None
            else:
                if wm is None:
                    wm = sql.query(f"SELECT max(ts) AS max_ts, count(*) AS n FROM {target}")[0]["max_ts"]
                lo = wm - timedelta(minutes=cfg.overlap_minutes)
                out = sql.query(f"INSERT INTO {target} SELECT * FROM {source} WHERE ts > :lo "
                                f"EXCEPT SELECT * FROM {target} WHERE ts > :lo", {"lo": lo})
                added = out[0].get("num_inserted_rows") if out else None
            sql.execute(f"DELETE FROM {target} WHERE ts < current_timestamp() - INTERVAL {cfg.retention_days} DAYS")
            new_max = sql.query(f"SELECT max(ts) AS max_ts, count(*) AS n FROM {target}")[0]["max_ts"]
            secs = time.monotonic() - t0
            sql.merge_rows(cfg.table("ingest_watermarks"),
                           [{"source_table": src, "max_ts": new_max, "last_run_id": run_id,
                             "rows_added": added, "seconds": secs}], ["source_table"])
            results.append(IngestResult(src, "ok", added, secs))
        except Exception as e:  # isolate per table; watermark not advanced
            results.append(IngestResult(src, "error", None, time.monotonic() - t0, f"{type(e).__name__}: {e}"[:1000]))
    return results
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_ingest.py -q`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/ingest.py engine/tests/unit/test_ingest.py
git commit -m "feat(engine): sequential incremental telemetry ingestion"
```

---

### Task 8: Inventory from the Lakebase API

**Files:**
- Create: `engine/src/lakebase_health/inventory.py`
- Test: `engine/tests/unit/test_inventory.py`

**Interfaces:**
- Consumes: `models.EndpointInfo`; a `WorkspaceClient`-like object `w` with `w.postgres.list_projects()`, `list_branches(parent=)`, `list_endpoints(parent=)`, `list_roles(parent=)`, `list_databases(parent=)`.
- Produces: `build_inventory(w, engine_role: str) -> tuple[list[EndpointInfo], list[str]]` (endpoints, errors).

- [ ] **Step 1: Write the failing tests**

```python
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
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_inventory.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `inventory.py`**

```python
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


def build_inventory(w, engine_role: str) -> tuple:
    endpoints, errors = [], []
    for p in w.postgres.list_projects():
        try:
            ps = p.status
            opted_out = _tags(getattr(ps, "custom_tags", None)).get("lakebase_health", "").lower() == "off"
            for b in w.postgres.list_branches(parent=p.name):
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
        except Exception as ex:  # one project failing must not stop the inventory
            errors.append(f"{p.name}: {type(ex).__name__}: {ex}"[:500])
    return endpoints, errors
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_inventory.py -q`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/inventory.py engine/tests/unit/test_inventory.py
git commit -m "feat(engine): Lakebase inventory with enrollment and opt-out"
```

---

### Task 9: Executors (Delta, Postgres, API)

**Files:**
- Create: `engine/src/lakebase_health/executors/__init__.py`, `executors/delta.py`, `executors/postgres.py`, `executors/api.py`
- Test: `engine/tests/unit/test_executors.py`

**Interfaces:**
- Consumes: `EngineConfig`, `Sql`, `CheckSpec`, `render_sql`, `render_template`, `Exclusions`, `EndpointInfo`.
- Produces:
  - `rewrite_tables(sql_text, cfg) -> str`; `DeltaExecutor(sql, cfg, excl).run(spec) -> list[dict]`.
  - `PostgresExecutor(w, user, connect=None)` with `.session(ep, database)` (a context manager yielding `PgSession`); `PgSession.query(sql) -> list[dict]`, `.has_extension(name) -> bool`, `.stats_window_hours() -> float | None`; `SESSION_SETUP: tuple[str, ...]`.
  - `API_CHECKS: dict[str, Callable[[list[EndpointInfo]], list[dict]]]`; `ApiExecutor().run(spec, inventory) -> list[dict]`.

- [ ] **Step 1: Write the failing tests**

```python
from types import SimpleNamespace as NS

from lakebase_health.checkspec import parse_check, render_template
from lakebase_health.config import EngineConfig
from lakebase_health.exclusions import Exclusions
from lakebase_health.executors.api import API_CHECKS, ApiExecutor
from lakebase_health.executors.delta import DeltaExecutor, rewrite_tables
from lakebase_health.executors.postgres import SESSION_SETUP, PostgresExecutor
from lakebase_health.models import EndpointInfo
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
EXCL = Exclusions(("s",), ("r",), ("u",), ("pg_catalog",), "^(pg_)", "^neon_")


def ep(**kw):
    base = dict(project_id="p", project_name="P", branch_id="b", branch_name="production", is_default_branch=True,
                is_protected=False, endpoint_id="e", endpoint_name="projects/p/branches/production/endpoints/primary",
                endpoint_type="ENDPOINT_TYPE_READ_WRITE", current_state="ACTIVE", host="h", min_cu=1.0, max_cu=1.0,
                suspend_timeout_s=300, ha_min=1, restore_window_s=2 * 86400, pg_native_login=False, opted_out=False,
                enrolled=True, databases=["db"], password_roles=["legacy"])
    base.update(kw)
    return EndpointInfo(**base)


def test_rewrite_tables():
    out = rewrite_tables("SELECT * FROM system.lakebase.compute_gauges g JOIN system.lakebase.plan_history p", CFG)
    assert "cat.lakebase_health.raw_compute_gauges g" in out and "cat.lakebase_health.raw_plan_history p" in out


def test_delta_executor_runs_rewritten_sql():
    spec = parse_check({"id": "d", "title": "D", "category": "c", "source": "delta", "explain": "e",
                        "sql": "SELECT 1 AS measure FROM system.lakebase.compute_gauges", "thresholds": {"warning": 0}},
                       stem="d")
    sql = FakeSql(responses=[(r"raw_compute_gauges", [{"measure": 1}])])
    assert DeltaExecutor(sql, CFG, EXCL).run(spec) == [{"measure": 1}]


class FakeCursor:
    def __init__(self, rows, cols):
        self._rows, self.description = rows, [NS(name=c) for c in cols] if cols else None

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, answers):
        self.executed, self.answers, self.closed = [], answers, False

    def execute(self, stmt):
        self.executed.append(stmt)
        for key, (rows, cols) in self.answers.items():
            if key in stmt:
                return FakeCursor(rows, cols)
        return FakeCursor([], None)

    def close(self):
        self.closed = True


def test_postgres_session_is_read_only_and_uses_fresh_token():
    conn = FakeConn({"pg_extension": ([(1,)], ["x"]), "stats_reset": ([(30.0,)], ["h"])})
    calls = {}

    def connect(**kw):
        calls.update(kw)
        return conn

    w = NS(postgres=NS(generate_database_credential=lambda endpoint: NS(token=f"tok-{endpoint[-7:]}")))
    pg = PostgresExecutor(w, "svc", connect=connect)
    with pg.session(ep(), "db") as s:
        assert s.has_extension("pgstattuple") and s.stats_window_hours() == 30.0
    assert calls["password"] == "tok-primary" and calls["application_name"] == "lakebase-health"
    assert calls["dbname"] == "db" and calls["sslmode"] == "require"
    assert conn.executed[:len(SESSION_SETUP)] == list(SESSION_SETUP)
    assert "SET default_transaction_read_only = on" in SESSION_SETUP and conn.closed


def test_api_checks():
    rows = API_CHECKS["backup_restore_window"]([ep(), ep(endpoint_id="e2")])
    assert rows == [{"project_id": "p", "branch_id": None, "endpoint_id": None, "object": "P", "measure": 2.0,
                     "restore_window_days": 2.0}]
    assert API_CHECKS["default_branch_unprotected"]([ep()])[0]["measure"] == 1.0
    assert API_CHECKS["default_branch_unprotected"]([ep(is_protected=True)]) == []
    assert API_CHECKS["password_roles"]([ep()])[0]["object"] == "P/production:legacy"
    assert API_CHECKS["primary_without_ha"]([ep(ha_min=2)]) == []
    spec = parse_check({"id": "a", "title": "A", "category": "c", "source": "api", "explain": "e",
                        "function": "password_roles", "thresholds": {"warning": 0}}, stem="a",
                       api_functions=frozenset(API_CHECKS))
    assert ApiExecutor().run(spec, [ep(opted_out=True)]) == []


def test_template_with_null_value_gives_no_sql():
    assert render_template("ALTER ROLE {role_ident} SET work_mem = '64MB';", {"role_ident": None}) is None
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_executors.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write the executors**

`engine/src/lakebase_health/executors/__init__.py`: empty file.

`engine/src/lakebase_health/executors/delta.py`:
```python
import re

from ..checkspec import render_sql


def rewrite_tables(sql_text: str, cfg) -> str:
    return re.sub(r"\bsystem\.lakebase\.([a-z_]+)", lambda m: cfg.raw(m.group(1)), sql_text)


class DeltaExecutor:
    def __init__(self, sql, cfg, excl):
        self.sql, self.cfg, self.excl = sql, cfg, excl

    def run(self, spec) -> list:
        return self.sql.query(rewrite_tables(render_sql(spec, self.excl), self.cfg))
```

`engine/src/lakebase_health/executors/postgres.py`:
```python
from contextlib import contextmanager

SEARCH_PATH_SQL = (
    "DO $$ BEGIN PERFORM set_config('search_path', concat_ws(',', '\"$user\"', 'public', ("
    "SELECT string_agg(DISTINCT quote_ident(n.nspname), ',') FROM pg_extension e "
    "JOIN pg_namespace n ON n.oid = e.extnamespace WHERE n.nspname NOT IN ('public', 'pg_catalog'))), false); END $$"
)
SESSION_SETUP = (
    SEARCH_PATH_SQL,
    "SET default_transaction_read_only = on",
    "SET statement_timeout = '30s'",
    "SET lock_timeout = '1s'",
)
STATS_WINDOW_SQL = (
    "SELECT extract(epoch FROM now() - coalesce((SELECT stats_reset FROM pg_stat_database "
    "WHERE datname = current_database()), pg_postmaster_start_time()))::float8 / 3600.0 AS h"
)


class PgSession:
    def __init__(self, conn):
        self.conn = conn

    def query(self, sql: str) -> list:
        cur = self.conn.execute(sql)
        if cur.description is None:
            return []
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def has_extension(self, name: str) -> bool:
        return bool(self.query(f"SELECT 1 AS x FROM pg_extension WHERE extname = '{name}'"))

    def stats_window_hours(self) -> float | None:
        rows = self.query(STATS_WINDOW_SQL)
        return float(rows[0]["h"]) if rows and rows[0]["h"] is not None else None


class PostgresExecutor:
    def __init__(self, w, user: str, connect=None):
        self.w, self.user = w, user
        if connect is None:
            import psycopg
            connect = psycopg.connect
        self.connect = connect

    @contextmanager
    def session(self, ep, database: str):
        token = self.w.postgres.generate_database_credential(endpoint=ep.endpoint_name).token
        conn = self.connect(host=ep.host, user=self.user, password=token, dbname=database, sslmode="require",
                            autocommit=True, application_name="lakebase-health", connect_timeout=10)
        try:
            for stmt in SESSION_SETUP:
                conn.execute(stmt)
            yield PgSession(conn)
        finally:
            conn.close()
```

`engine/src/lakebase_health/executors/api.py`:
```python
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
    def run(self, spec, inventory) -> list:
        return API_CHECKS[spec.function]([e for e in inventory if not e.opted_out])
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_executors.py -q`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/executors engine/tests/unit/test_executors.py
git commit -m "feat(engine): Delta, Postgres (read-only) and API executors"
```

---

### Task 10: Check runner

**Files:**
- Create: `engine/src/lakebase_health/runner.py`
- Test: `engine/tests/unit/test_runner.py`

**Interfaces:**
- Consumes: executors from Task 9; `CheckSpec`, `render_sql`, `render_template`, `classify`, `guards_pass`, `fingerprint`, models.
- Produces: `Runner(delta, postgres, api, excl)` with `.run(specs, inventory, now, last_ok: dict[str, datetime], stale_sources: set[str]) -> list[CheckOutcome]`; `META_SPECS: dict[str, CheckSpec]` (ids `check_broken`, `extension_missing`); `DAILY_INTERVAL = timedelta(hours=23)`.

- [ ] **Step 1: Write the failing tests**

```python
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
    def __init__(self, session=None, connect_exc=None):
        self.sess, self.connect_exc, self.opened = session, connect_exc, []

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
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_runner.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `runner.py`**

```python
from datetime import timedelta

from .checkspec import parse_check, render_sql, render_template
from .fingerprint import fingerprint
from .models import FLEET, CheckOutcome, CheckResult
from .severity import classify, guards_pass

DAILY_INTERVAL = timedelta(hours=23)
_LOCATION = ("project_id", "branch_id", "endpoint_id", "object", "query_text")

META_SPECS = {
    "check_broken": parse_check({
        "id": "check_broken", "title": "A health check has failed 3 runs in a row", "category": "engine",
        "source": "delta", "sql": "SELECT 1", "thresholds": {"warning": 0},
        "explain": "The check's SQL errored on its last 3 runs (often a renamed column in a Beta system table). "
                   "Its findings are frozen until it runs again."}, stem="check_broken"),
    "extension_missing": parse_check({
        "id": "extension_missing", "title": "Extension needed by a health check is not installed",
        "category": "engine", "source": "postgres", "sql": "SELECT 1", "thresholds": {"info": 0},
        "explain": "Install it with CREATE EXTENSION IF NOT EXISTS <name>; to enable the dependent checks."},
        stem="extension_missing"),
}


def _timeout(e: Exception) -> bool:
    return type(e).__name__ in ("QueryCanceled", "TimeoutError")


def _status_for(e: Exception) -> str:
    return "timeout" if _timeout(e) else "error"


class Runner:
    def __init__(self, delta, postgres, api, excl):
        self.delta, self.postgres, self.api, self.excl = delta, postgres, api, excl

    def _results(self, spec, rows, loc=None, prefix="", ctx=None) -> list:
        out = []
        confidence = "high" if guards_pass(spec.guards, ctx or {}) else "low"
        for row in rows:
            sev = classify(row.get(spec.measure), spec.thresholds, spec.direction)
            if sev is None:
                continue
            place = dict(loc) if loc else {k: row.get(k) for k in ("project_id", "branch_id", "endpoint_id")}
            obj = prefix + str(row.get("object"))
            fix = spec.fix
            out.append(CheckResult(
                check_id=spec.id, fingerprint=fingerprint(spec.id, place["project_id"], place["endpoint_id"], obj),
                project_id=place["project_id"], branch_id=place["branch_id"], endpoint_id=place["endpoint_id"],
                object=obj, measure=float(row.get(spec.measure)), severity=sev, confidence=confidence,
                evidence={k: v for k, v in row.items() if k not in _LOCATION},
                query_text=row.get("query_text"),
                candidate_sql=render_template(fix.template, row) if fix else None,
                rollback_sql=render_template(fix.rollback, row) if fix else None))
        return out

    def _due(self, spec, now, last_ok) -> bool:
        last = last_ok.get(spec.id)
        return spec.schedule != "daily" or last is None or now - last >= DAILY_INTERVAL

    def run(self, specs, inventory, now, last_ok, stale_sources) -> list:
        outcomes = []
        for spec in specs:
            if spec.source == "postgres":
                continue
            if not self._due(spec, now, last_ok):
                outcomes.append(CheckOutcome(spec.id, FLEET, "skipped:not_scheduled"))
                continue
            try:
                rows = self.delta.run(spec) if spec.source == "delta" else self.api.run(spec, inventory)
                status = "ok_stale_source" if spec.tables() & set(stale_sources) else "ok"
                outcomes.append(CheckOutcome(spec.id, FLEET, status, results=self._results(spec, rows)))
            except Exception as e:
                outcomes.append(CheckOutcome(spec.id, FLEET, _status_for(e), f"{type(e).__name__}: {e}"[:1000]))
        pg_specs = [s for s in specs if s.source == "postgres"]
        if pg_specs:
            for ep in inventory:
                if ep.read_write:
                    outcomes.extend(self._run_endpoint(pg_specs, ep, now, last_ok))
        return outcomes

    def _run_endpoint(self, specs, ep, now, last_ok) -> list:
        scope = (ep.project_id, ep.endpoint_id)
        skip = ("skipped:opted_out" if ep.opted_out else "skipped:compute_inactive" if not ep.active
                else "skipped:not_enrolled" if not ep.enrolled else None)
        if skip:
            return [CheckOutcome(s.id, scope, skip) for s in specs]
        loc = {"project_id": ep.project_id, "branch_id": ep.branch_id, "endpoint_id": ep.endpoint_id}
        acc = {s.id: CheckOutcome(s.id, scope, "ok") for s in specs}
        missing = []
        for db in ep.databases:
            try:
                with self.postgres.session(ep, db) as session:
                    ctx = {"stats_window_hours": session.stats_window_hours()}
                    for spec in specs:
                        o = acc[spec.id]
                        if o.status != "ok":
                            continue
                        if not self._due(spec, now, last_ok):
                            o.status = "skipped:not_scheduled"
                            continue
                        if spec.requires_extension and not session.has_extension(spec.requires_extension):
                            o.status = "skipped:extension_missing"
                            missing.append({"object": f"{spec.requires_extension}", "measure": 1.0,
                                            "extension": spec.requires_extension, "needed_by": spec.id, "db": db})
                            continue
                        try:
                            rows = session.query(render_sql(spec, self.excl))
                            o.results.extend(self._results(spec, rows, loc, f"{db}:", ctx))
                        except Exception as e:
                            o.status, o.error = _status_for(e), f"{type(e).__name__}: {e}"[:1000]
            except Exception as e:  # connection failed (e.g. compute suspended since inventory)
                for o in acc.values():
                    if o.status == "ok":
                        o.status, o.error, o.results = "error", f"{type(e).__name__}: {e}"[:1000], []
        out = list(acc.values())
        if missing:
            meta_rows = {(m["db"], m["extension"]): m for m in missing}.values()
            results = []
            for m in meta_rows:
                results.extend(self._results(META_SPECS["extension_missing"], [m], loc, f"{m['db']}:"))
            out.append(CheckOutcome("extension_missing", scope, "ok", results=results))
        return out
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_runner.py -q`
Expected: `10 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/runner.py engine/tests/unit/test_runner.py
git commit -m "feat(engine): check runner with skip rules, guards and meta findings"
```

---

### Task 11: Recording results (findings, actions, shadow, ledger, metrics, check_broken)

**Files:**
- Create: `engine/src/lakebase_health/record.py`
- Test: `engine/tests/unit/test_record.py`

**Interfaces:**
- Consumes: `apply_run`, `plan_actions`, `shadow_rows`, `chain`, `META_SPECS`, models, `Sql`, `EngineConfig`.
- Produces:
  - `persist(sql, cfg, run_id, now, outcomes, specs: dict, inventory) -> dict`, returning a counts dict with keys `findings_new`, `findings_open`, `findings_resolved`, `actions_proposed`, `results`.
  - `load_last_ok(sql, cfg) -> dict[str, datetime]`.
  - `broken_outcome(sql, cfg, now) -> CheckOutcome | None`.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import datetime, timedelta, timezone

from lakebase_health.checkspec import parse_check
from lakebase_health.config import EngineConfig
from lakebase_health.fingerprint import fingerprint
from lakebase_health.ledger import verify
from lakebase_health.models import FLEET, CheckOutcome, CheckResult
from lakebase_health.record import load_last_ok, persist
from tests.fakes import FakeSql
from tests.unit.test_executors import ep

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
FIX = {"kind": "ddl", "template": "CREATE INDEX {n};", "rollback": "DROP INDEX {n};", "risk": "moderate",
       "reversible": True, "verify_on_branch": True}
SPEC = parse_check({"id": "c1", "title": "T", "category": "c", "source": "delta", "explain": "E", "sql": "SELECT 1",
                    "thresholds": {"warning": 0}, "fix": FIX}, stem="c1")
SPECS = {"c1": SPEC}
T = CFG.table


def outcome(with_result=True, status="ok"):
    res = [CheckResult("c1", fingerprint("c1", "p", "e", "o"), "p", "b", "e", "o", 1.0, "warning", "high",
                       {"k": 1}, "SELECT 1", "CREATE INDEX x;", "DROP INDEX x;")] if with_result else []
    return CheckOutcome("c1", FLEET, status, results=res)


def test_persist_writes_everything_and_is_idempotent():
    sql = FakeSql()
    c1 = persist(sql, CFG, "r1", NOW, [outcome()], SPECS, [ep()])
    assert c1["findings_new"] == 1 and c1["actions_proposed"] == 1
    assert len(sql.tables[T("inventory")]) == 1
    assert {r["status"] for r in sql.tables[T("check_results")]} == {"ok"}
    assert sql.tables[T("findings")][0]["query_text"] == "SELECT 1"
    assert '"k": 1' in sql.tables[T("findings")][0]["evidence"]
    assert sql.tables[T("proposed_actions")][0]["state"] == "proposed"
    assert sql.tables[T("shadow_log")][0]["would_allow_level"] == "L2"
    c2 = persist(sql, CFG, "r2", NOW + timedelta(hours=1), [outcome()], SPECS, [ep()])
    assert c2["findings_new"] == 0 and c2["findings_open"] == 1
    assert len(sql.tables[T("findings")]) == 1 and len(sql.tables[T("proposed_actions")]) == 1
    assert len(sql.tables[T("ledger")]) == 1 and verify(sql.tables[T("ledger")])


def test_resolution_expires_action_and_extends_ledger_chain():
    sql = FakeSql(responses=[(r"SELECT hash FROM", lambda s, a: sql.tables.get(T("ledger"), [])[-1:])])
    persist(sql, CFG, "r1", NOW, [outcome()], SPECS, [])
    persist(sql, CFG, "r2", NOW + timedelta(hours=1), [outcome(False)], SPECS, [])
    c = persist(sql, CFG, "r3", NOW + timedelta(hours=2), [outcome(False)], SPECS, [])
    assert c["findings_resolved"] == 1
    assert sql.tables[T("proposed_actions")][0]["state"] == "expired"
    ledger = sql.tables[T("ledger")]
    assert [e["event_type"] for e in ledger] == ["proposed", "expired"] and verify(ledger)


def test_status_rows_for_non_ok_outcomes():
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [CheckOutcome("c1", ("p", "e"), "skipped:compute_inactive")], SPECS, [])
    [row] = sql.tables[T("check_results")]
    assert row["status"] == "skipped:compute_inactive" and row["fingerprint"] is None


def test_check_broken_after_three_error_runs():
    history = [{"check_id": "c1", "status": "error", "run_ts": NOW - timedelta(hours=h)} for h in (0, 1, 2)]
    sql = FakeSql(responses=[(r"AS run_ts", history)])
    c = persist(sql, CFG, "r1", NOW, [CheckOutcome("c1", FLEET, "error", "boom")], SPECS, [])
    broken = [f for f in sql.tables[T("findings")] if f["check_id"] == "check_broken"]
    assert broken and broken[0]["object"] == "c1" and c["findings_new"] == 1


def test_metrics_daily_merge_runs():
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [], SPECS, [])
    assert sql.ran(r"MERGE INTO cat\.lakebase_health\.metrics_daily")


def test_sequence_metric_and_check_results_retention():
    seq = parse_check({"id": "sequence_exhaustion", "title": "S", "category": "c", "source": "postgres",
                       "explain": "E", "sql": "SELECT 1", "thresholds": {"warning": 50}}, stem="sequence_exhaustion")
    r = CheckResult("sequence_exhaustion", "fp", "p", "b", "e", "db:t.id", 74.5, "warning", "high", {})
    sql = FakeSql()
    persist(sql, CFG, "r1", NOW, [CheckOutcome("sequence_exhaustion", ("p", "e"), "ok", results=[r])],
            {"sequence_exhaustion": seq}, [])
    [m] = sql.tables[T("metrics_daily")]
    assert m["metric"] == "sequence_pct:db:t.id" and m["value"] == 74.5
    assert sql.ran(r"DELETE FROM cat\.lakebase_health\.check_results WHERE ts < current_timestamp\(\) - INTERVAL 90 DAYS")


def test_load_last_ok():
    sql = FakeSql(responses=[(r"max\(ts\) AS last_ok", [{"check_id": "c1", "last_ok": NOW}])])
    assert load_last_ok(sql, CFG) == {"c1": NOW}
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_record.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `record.py`**

```python
import json
from datetime import timezone

from .actions import plan_actions, shadow_rows
from .fingerprint import fingerprint
from .ledger import chain
from .lifecycle import apply_run
from .models import FLEET, Action, CheckOutcome, CheckResult, Finding
from .runner import META_SPECS

BROKEN_AFTER = 3
METRIC_CHECKS = {"sequence_exhaustion": "sequence_pct"}  # check id -> metrics_daily metric prefix

METRICS_SQL = """
MERGE INTO {md} md USING (
  SELECT current_date() AS day, project_id, endpoint_id, 'storage_bytes' AS metric, CAST(max(total_size) AS DOUBLE) AS value
  FROM {cg} WHERE ts >= current_date() AND total_size IS NOT NULL GROUP BY project_id, endpoint_id
  UNION ALL
  SELECT current_date(), project_id, endpoint_id, 'xid_age', CAST(max(oldest_frozen_xid_age) AS DOUBLE)
  FROM {dg} WHERE ts >= current_date() GROUP BY project_id, endpoint_id
  UNION ALL
  SELECT current_date(), project_id, endpoint_id, 'peak_backends', CAST(max(nb) AS DOUBLE)
  FROM (SELECT project_id, endpoint_id, ts, sum(numbackends) AS nb FROM {dg} WHERE ts >= current_date()
        GROUP BY project_id, endpoint_id, ts) GROUP BY project_id, endpoint_id
) s ON md.day = s.day AND md.project_id = s.project_id AND md.endpoint_id = s.endpoint_id AND md.metric = s.metric
WHEN MATCHED THEN UPDATE SET value = greatest(md.value, s.value)
WHEN NOT MATCHED THEN INSERT *
"""


def _utc(dt):
    return dt.replace(tzinfo=timezone.utc) if dt is not None and dt.tzinfo is None else dt


def load_last_ok(sql, cfg) -> dict:
    rows = sql.query(f"SELECT check_id, max(ts) AS last_ok FROM {cfg.table('check_results')} "
                     "WHERE status IN ('ok', 'ok_stale_source') GROUP BY check_id")
    return {r["check_id"]: _utc(r["last_ok"]) for r in rows}


def _result_rows(run_id, now, outcomes) -> list:
    rows = []
    for o in outcomes:
        base = {"run_id": run_id, "ts": now, "check_id": o.check_id, "status": o.status, "error": o.error}
        rows.append({**base, "fingerprint": None, "project_id": None if o.scope == FLEET else o.scope[0],
                     "branch_id": None, "endpoint_id": None if o.scope == FLEET else o.scope[1], "object": None,
                     "measure": None, "severity": None, "confidence": None, "evidence": None, "query_text": None})
        for r in o.results:
            rows.append({**base, "fingerprint": r.fingerprint, "project_id": r.project_id, "branch_id": r.branch_id,
                         "endpoint_id": r.endpoint_id, "object": r.object, "measure": r.measure,
                         "severity": r.severity, "confidence": r.confidence,
                         "evidence": json.dumps(r.evidence, default=str, sort_keys=True), "query_text": r.query_text})
    return rows


def broken_outcome(sql, cfg, now) -> CheckOutcome | None:
    rows = sql.query(
        f"SELECT check_id, status, run_ts FROM (SELECT check_id, run_id, max(ts) AS run_ts, "
        f"max(CASE WHEN status = 'error' THEN 'error' ELSE 'ok' END) AS status FROM {cfg.table('check_results')} "
        f"WHERE fingerprint IS NULL AND ts >= current_timestamp() - INTERVAL 7 DAYS GROUP BY check_id, run_id)")
    by_check = {}
    for r in rows:
        by_check.setdefault(r["check_id"], []).append(r)
    results = []
    for check_id, runs in by_check.items():
        last = sorted(runs, key=lambda r: _utc(r["run_ts"]), reverse=True)[:BROKEN_AFTER]
        if len(last) == BROKEN_AFTER and all(r["status"] == "error" for r in last):
            results.append(CheckResult("check_broken", fingerprint("check_broken", None, None, check_id), None, None,
                                       None, check_id, float(BROKEN_AFTER), "warning", "high",
                                       {"consecutive_error_runs": BROKEN_AFTER}))
    return CheckOutcome("check_broken", FLEET, "ok", results=results)


def persist(sql, cfg, run_id, now, outcomes, specs, inventory) -> dict:
    sql.insert_rows(cfg.table("inventory"), [e.to_row(run_id, now) for e in inventory])
    sql.insert_rows(cfg.table("check_results"), _result_rows(run_id, now, outcomes))

    all_specs = {**META_SPECS, **specs}
    meta = broken_outcome(sql, cfg, now)
    outcomes = list(outcomes) + ([meta] if meta else [])

    existing = {r["fingerprint"]: Finding.from_row(r) for r in sql.query(f"SELECT * FROM {cfg.table('findings')}")}
    before = {fp: f.status for fp, f in existing.items()}
    changed = apply_run(existing, outcomes, all_specs, now, cfg.resolve_after_clean_runs)
    sql.merge_rows(cfg.table("findings"), [f.to_row() for f in changed], ["fingerprint"])

    open_actions = {r["action_id"]: Action.from_row(r) for r in
                    sql.query(f"SELECT * FROM {cfg.table('proposed_actions')}") if r.get("state") == "proposed"}
    actions, events = plan_actions(changed, open_actions, all_specs, now)
    sql.merge_rows(cfg.table("proposed_actions"), [a.to_row() for a in actions], ["action_id"])
    active = [a for a in actions if a.state == "proposed"]
    sql.insert_rows(cfg.table("shadow_log"), shadow_rows(active, run_id, now))
    if events:
        last = sql.query(f"SELECT hash FROM {cfg.table('ledger')} ORDER BY ts DESC, event_id DESC LIMIT 1")
        sql.insert_rows(cfg.table("ledger"), chain(last[0]["hash"] if last else None, events, now))

    try:
        sql.execute(METRICS_SQL.format(md=cfg.table("metrics_daily"), cg=cfg.raw("compute_gauges"),
                                       dg=cfg.raw("database_gauges")))
    except Exception:  # raw tables may not exist yet on a failed first ingest
        pass
    metric_rows = [{"day": now.date(), "project_id": r.project_id, "endpoint_id": r.endpoint_id,
                    "metric": f"{METRIC_CHECKS[o.check_id]}:{r.object}", "value": r.measure}
                   for o in outcomes if o.check_id in METRIC_CHECKS for r in o.results]
    sql.merge_rows(cfg.table("metrics_daily"), metric_rows, ["day", "project_id", "endpoint_id", "metric"])
    sql.execute(f"DELETE FROM {cfg.table('check_results')} "
                f"WHERE ts < current_timestamp() - INTERVAL {cfg.retention_days} DAYS")

    def count(status, new_only=False):
        return sum(1 for f in changed if f.status == status and (not new_only or before.get(f.fingerprint) != status))

    return {"findings_new": count("new"), "findings_open": count("open"),
            "findings_resolved": count("resolved", new_only=True),
            "actions_proposed": sum(1 for e in events if e["event_type"] == "proposed"),
            "results": sum(len(o.results) for o in outcomes)}
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_record.py -q`
Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/record.py engine/tests/unit/test_record.py
git commit -m "feat(engine): persist findings, actions, shadow log, ledger and metrics"
```

---

### Task 12: Job entry point

**Files:**
- Create: `engine/src/lakebase_health/jobs/__init__.py`, `engine/src/lakebase_health/jobs/run.py`
- Test: `engine/tests/unit/test_run.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `run_engine(sql, w, cfg, connect=None, now=None, checks_dir=None) -> dict` (the final `runs` row); `main(argv=None) -> None`; `CHECKS_DIR: Path`.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import datetime, timezone
from types import SimpleNamespace as NS

from lakebase_health.config import EngineConfig
from lakebase_health.jobs.run import run_engine
from tests.fakes import FakeSql

NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)


def fake_w():
    pg = NS(list_projects=lambda: [])
    return NS(postgres=pg, current_user=NS(me=lambda: NS(user_name="svc")))


def write_check(tmp_path):
    (tmp_path / "exclusions.yaml").write_text(
        "slots: [s]\nusers: [u]\nroles: [r]\nrole_regex: '^(pg_)'\nschemas: [pg_catalog]\nsettings_regex: '^neon_'\n")
    (tmp_path / "x.yaml").write_text(
        "id: x\ntitle: X\ncategory: c\nsource: delta\nexplain: e\nthresholds: {warning: 0}\n"
        "sql: SELECT 'p' AS project_id, 'b' AS branch_id, 'e' AS endpoint_id, 'o' AS object, 1.0 AS measure "
        "FROM system.lakebase.compute_gauges\n")


def test_run_engine_end_to_end_with_fakes(tmp_path):
    write_check(tmp_path)
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}]),
                             (r"raw_compute_gauges$", [{"project_id": "p", "branch_id": "b", "endpoint_id": "e",
                                                        "object": "o", "measure": 1.0}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert run["status"] == "ok"
    assert sql.tables["cat.lakebase_health.findings"][0]["check_id"] == "x"
    [final] = sql.tables["cat.lakebase_health.runs"]
    assert final["status"] == "ok" and final["ended_at"] == NOW


def test_disabled_engine_does_nothing(tmp_path):
    write_check(tmp_path)
    sql = FakeSql()
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat", engine_enabled=False), now=NOW, checks_dir=tmp_path)
    assert run["status"] == "disabled"
    assert not sql.ran("system.lakebase") and "cat.lakebase_health.findings" not in sql.tables


def test_ingest_errors_are_reported_not_fatal(tmp_path):
    write_check(tmp_path)
    sql = FakeSql(responses=[(r"system\.lakebase\.", RuntimeError("no access")),
                             (r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert run["status"] == "ok_with_errors" and "no access" in run["errors"]
```

- [ ] **Step 2: Run them and verify they fail**

Run: `cd engine && .venv/bin/pytest tests/unit/test_run.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Write `jobs/run.py`**

`engine/src/lakebase_health/jobs/__init__.py`: empty file.

`engine/src/lakebase_health/jobs/run.py`:
```python
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from ..checkspec import load_checks
from ..config import EngineConfig
from ..ddl import ensure_masks, ensure_schema
from ..exclusions import load_exclusions
from ..executors.api import API_CHECKS, ApiExecutor
from ..executors.delta import DeltaExecutor
from ..executors.postgres import PostgresExecutor
from ..ingest import ingest_all
from ..inventory import build_inventory
from ..record import load_last_ok, persist
from ..runner import Runner

CHECKS_DIR = Path(__file__).resolve().parent.parent / "checks"


def run_engine(sql, w, cfg, connect=None, now=None, checks_dir=None) -> dict:
    injected_now = now is not None  # tests inject `now`; real runs stamp ended_at with the wall clock
    now = now or datetime.now(timezone.utc)
    checks_dir = Path(checks_dir or CHECKS_DIR)
    run = {"run_id": uuid.uuid4().hex, "started_at": now, "ended_at": None, "status": "running",
           "step_seconds": None, "counts": None, "errors": None}
    ensure_schema(sql, cfg)
    sql.insert_rows(cfg.table("runs"), [run])
    if not cfg.engine_enabled:
        run.update(status="disabled", ended_at=now)
        sql.merge_rows(cfg.table("runs"), [run], ["run_id"])
        return run

    errors, steps = [], {}
    t = time.monotonic()
    ingested = ingest_all(sql, cfg, run["run_id"], now)
    errors += [f"ingest {r.source}: {r.error}" for r in ingested if r.status != "ok"]
    stale = {r.source for r in ingested if r.status != "ok"}
    try:
        ensure_masks(sql, cfg)
    except Exception as e:
        errors.append(f"masks: {type(e).__name__}: {e}")
    steps["ingest"] = time.monotonic() - t

    t = time.monotonic()
    me = w.current_user.me().user_name
    inventory, inv_errors = build_inventory(w, me)
    errors += [f"inventory {m}" for m in inv_errors]
    steps["inventory"] = time.monotonic() - t

    t = time.monotonic()
    excl = load_exclusions(checks_dir / "exclusions.yaml")
    specs = load_checks(checks_dir, frozenset(API_CHECKS))
    runner = Runner(DeltaExecutor(sql, cfg, excl), PostgresExecutor(w, me, connect), ApiExecutor(), excl)
    outcomes = runner.run(specs, inventory, now, load_last_ok(sql, cfg), stale)
    errors += [f"check {o.check_id} {o.scope}: {o.error}" for o in outcomes if o.status in ("error", "timeout")]
    steps["checks"] = time.monotonic() - t

    t = time.monotonic()
    counts = persist(sql, cfg, run["run_id"], now, outcomes, {s.id: s for s in specs}, inventory)
    steps["record"] = time.monotonic() - t

    run.update(ended_at=now if injected_now else datetime.now(timezone.utc),
               status="ok_with_errors" if errors else "ok",
               step_seconds=json.dumps({k: round(v, 1) for k, v in steps.items()}),
               counts=json.dumps(counts), errors=json.dumps(errors)[:20000] if errors else None)
    sql.merge_rows(cfg.table("runs"), [run], ["run_id"])
    return run


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="lakebase-health")
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", default="lakebase_health")
    p.add_argument("--engine-enabled", default="true")
    a = p.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    from ..sql import SparkSql

    cfg = EngineConfig(catalog=a.catalog, schema=a.schema, engine_enabled=a.engine_enabled.lower() == "true")
    run = run_engine(SparkSql(SparkSession.builder.getOrCreate()), WorkspaceClient(), cfg)
    print(json.dumps(run, default=str))
```

- [ ] **Step 4: Run them and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_run.py -q`
Expected: `3 passed`

- [ ] **Step 5: Run the full unit suite**

Run: `cd engine && .venv/bin/pytest -q`
Expected: all tests pass (≈ 70)

- [ ] **Step 6: Commit**

```bash
git add engine/src/lakebase_health/jobs engine/tests/unit/test_run.py
git commit -m "feat(engine): job entry point orchestrating ingest, inventory, checks, record"
```

---

### Task 13: Check catalog, part 1: telemetry (Delta) checks

**Files:**
- Create: 26 files under `engine/src/lakebase_health/checks/telemetry/`
- Create: `engine/tests/unit/test_catalog.py`

**Interfaces:**
- Consumes: `load_checks`, `API_CHECKS`.
- Produces: check ids used by the integration manifest (Task 17).

Conventions for every telemetry check: the SQL reads `system.lakebase.*` (rewritten to `raw_*` at run time); it covers **all projects** (no `:project_id` filter); it outputs `project_id`, `branch_id`, `endpoint_id`, `object`, `measure`, and optionally `query_text` plus evidence columns; every divisor is wrapped in `NULLIF`. Windows: 24 h for capacity/performance checks, 2 h for incident checks (deadlocks, blocking, errors), 7 days for trends.

- [ ] **Step 1: Write the failing catalog test `engine/tests/unit/test_catalog.py`**

```python
import re

from lakebase_health.checkspec import load_checks
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR

TELEMETRY = {
    "cpu_saturation", "lfc_hit_rate", "buffer_hit_rate", "working_set_vs_cache", "connection_utilization",
    "storage_utilization", "storage_runway", "write_backpressure", "cu_sizing", "rollback_ratio",
    "seq_scan_read_ratio", "deadlocks", "idle_in_tx_time", "top_db_time_query", "query_regression", "plan_flip",
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
}
POSTGRES = set()
API = set()


def specs():
    return {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}


def test_catalog_ids():
    assert set(specs()) == TELEMETRY | POSTGRES | API


def test_sources_match_folders():
    s = specs()
    assert all(s[i].source == "delta" for i in TELEMETRY)
    assert all(s[i].source == "postgres" for i in POSTGRES)
    assert all(s[i].source == "api" for i in API)


def test_telemetry_checks_are_fleet_wide_and_located():
    for i in TELEMETRY:
        sql = specs()[i].sql
        assert ":project_id" not in sql, i
        for col in ("project_id", "branch_id", "endpoint_id", "object", "measure"):
            assert re.search(rf"\b{col}\b", sql), (i, col)


def test_postgres_checks_use_exclusions():
    for i in POSTGRES:
        sql = specs()[i].sql
        if "pg_replication_slots" in sql:
            assert "{excluded_slots}" in sql, i
        if "pg_stat_activity" in sql:
            assert "{excluded_users}" in sql, i
        if re.search(r"pg_(class|namespace|stat_user_tables|stat_user_indexes|statio_user_tables)\b", sql):
            assert "{excluded_schemas}" in sql, i


def test_actionable_fixes_have_rollback():
    for s in specs().values():
        if s.fix and s.fix.actionable:
            assert s.fix.rollback, s.id
```

- [ ] **Step 2: Run it and verify it fails**

Run: `cd engine && .venv/bin/pytest tests/unit/test_catalog.py -q`
Expected: FAIL, `test_catalog_ids` (no checks exist yet)

- [ ] **Step 3: Create the 26 telemetry check files**

`checks/telemetry/cpu_saturation.yaml`:
```yaml
id: cpu_saturation
title: "CPU saturated (p95 over 24 h)"
category: capacity
source: delta
thresholds: {warning: 70, critical: 85}
explain: >
  p95 of CPU used versus the compute's logical CPUs over the last 24 hours. Sustained high CPU slows every query.
  Fix the top CPU queries first (see top_db_time_query); if CPU is pinned at the autoscaling maximum, raise max CU.
sql: |
  WITH c AS (
    SELECT project_id, branch_id, endpoint_id, compute_id, ts,
           host_cpu_seconds / NULLIF(timestampdiff(MILLISECOND, previous_ts, ts) / 1000.0, 0) AS cores_used
    FROM system.lakebase.compute_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS),
  g AS (
    SELECT compute_id, date_trunc('MINUTE', ts) AS m, max(host_logical_cpus) AS cpus
    FROM system.lakebase.compute_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY compute_id, date_trunc('MINUTE', ts))
  SELECT c.project_id, c.branch_id, c.endpoint_id, c.endpoint_id AS object,
         100 * percentile_approx(c.cores_used / NULLIF(g.cpus, 0), 0.95) AS measure,
         100 * max(c.cores_used / NULLIF(g.cpus, 0)) AS max_cpu_pct
  FROM c JOIN g ON c.compute_id = g.compute_id AND date_trunc('MINUTE', c.ts) = g.m
  GROUP BY c.project_id, c.branch_id, c.endpoint_id
fix: {kind: manual, template: "Optimize the top CPU queries, or raise autoscaling_limit_max_cu on {object}.", risk: moderate}
```

`checks/telemetry/lfc_hit_rate.yaml`:
```yaml
id: lfc_hit_rate
title: "Local SSD cache hit rate below 99%"
category: capacity
source: delta
direction: below
thresholds: {warning: 99, critical: 95}
explain: >
  Share of page reads served from the compute's local file cache instead of remote storage (24 h). OLTP needs 99%+.
  If the working set exceeds the cache, raise min CU (the cache follows the current size) or reduce pages read.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         100.0 * sum(lfc_hits) / NULLIF(sum(lfc_hits) + sum(lfc_misses), 0) AS measure,
         sum(lfc_hits) AS hits, sum(lfc_misses) AS misses
  FROM system.lakebase.compute_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id
  HAVING sum(lfc_hits) + sum(lfc_misses) > 10000
fix: {kind: manual, template: "Raise min CU on {object} or index the queries reading the most pages.", risk: moderate}
```

`checks/telemetry/buffer_hit_rate.yaml`:
```yaml
id: buffer_hit_rate
title: "Postgres buffer cache hit rate low"
category: capacity
source: delta
direction: below
thresholds: {info: 95, warning: 90}
explain: >
  shared_buffers hit ratio per database (24 h). On Lakebase this is only the first cache tier in front of the
  local SSD cache, so judge lfc_hit_rate first; a low value here is informational unless the LFC rate is also low.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
         100.0 * sum(shared_blks_hit) / NULLIF(sum(shared_blks_hit) + sum(shared_blks_read), 0) AS measure
  FROM system.lakebase.database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, datname
  HAVING sum(shared_blks_hit) + sum(shared_blks_read) > 10000
```

`checks/telemetry/working_set_vs_cache.yaml`:
```yaml
id: working_set_vs_cache
title: "Working set larger than the compute cache"
category: capacity
source: delta
thresholds: {warning: 80, critical: 100}
explain: >
  Peak 60-minute working set as a share of the local cache size (both in bytes, 24 h). Above 100% the data a
  workload touches no longer fits in cache. Raise min/max CU or shrink the working set (unused indexes, bloat).
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         100.0 * max(lfc_working_set_size_60m) / NULLIF(max(lfc_size_limit), 0) AS measure,
         max(lfc_working_set_size_60m) AS working_set_bytes, max(lfc_size_limit) AS cache_bytes
  FROM system.lakebase.compute_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id
```

`checks/telemetry/connection_utilization.yaml`:
```yaml
id: connection_utilization
title: "Connections near the compute limit"
category: capacity
source: delta
thresholds: {warning: 70, critical: 85}
explain: >
  Peak open backends as a share of max_connections (24 h). Near the limit, new connections fail (53300). Use a
  client-side pool; with autoscaling the limit follows min(max CU, 8 x min CU), so raise min CU if needed.
sql: |
  WITH conn AS (
    SELECT project_id, branch_id, endpoint_id, compute_id, ts, sum(numbackends) AS backends
    FROM system.lakebase.database_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY project_id, branch_id, endpoint_id, compute_id, ts),
  lim AS (
    SELECT compute_id, date_trunc('MINUTE', ts) AS m, max(max_connections) AS max_conn
    FROM system.lakebase.compute_gauges WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY compute_id, date_trunc('MINUTE', ts))
  SELECT conn.project_id, conn.branch_id, conn.endpoint_id, conn.endpoint_id AS object,
         100.0 * max(conn.backends / NULLIF(lim.max_conn, 0)) AS measure,
         max(conn.backends) AS peak_backends, max(lim.max_conn) AS max_connections
  FROM conn JOIN lim ON conn.compute_id = lim.compute_id AND date_trunc('MINUTE', conn.ts) = lim.m
  GROUP BY conn.project_id, conn.branch_id, conn.endpoint_id
```

`checks/telemetry/storage_utilization.yaml`:
```yaml
id: storage_utilization
title: "Storage near its limit"
category: capacity
source: delta
thresholds: {warning: 70, critical: 85}
explain: >
  Used storage as a share of the limit (latest reading, 24 h). Write performance drops at the quota. Drop unused
  or duplicate indexes, reclaim bloat, archive cold data, or shorten the restore window.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         100.0 * max_by(total_size, ts) / NULLIF(max_by(total_size_limit, ts), 0) AS measure,
         max_by(total_size, ts) AS used_bytes, max_by(total_size_limit, ts) AS limit_bytes
  FROM system.lakebase.compute_gauges
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND total_size IS NOT NULL
  GROUP BY project_id, branch_id, endpoint_id
```

`checks/telemetry/storage_runway.yaml`:
```yaml
id: storage_runway
title: "Storage projected to fill soon"
category: forecast
source: delta
direction: below
thresholds: {warning: 90, critical: 30}
explain: >
  Days until storage reaches its limit at the 7-day linear growth rate. Act before the runway is gone.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         (max_by(total_size_limit, ts) - max_by(total_size, ts))
           / NULLIF(regr_slope(total_size, unix_timestamp(ts)) * 86400, 0) AS measure,
         regr_slope(total_size, unix_timestamp(ts)) * 86400 AS growth_bytes_per_day
  FROM system.lakebase.compute_gauges
  WHERE ts >= current_timestamp() - INTERVAL 7 DAYS AND total_size IS NOT NULL
  GROUP BY project_id, branch_id, endpoint_id
  HAVING regr_slope(total_size, unix_timestamp(ts)) > 0
```

`checks/telemetry/write_backpressure.yaml`:
```yaml
id: write_backpressure
title: "Storage throttled writes"
category: capacity
source: delta
thresholds: {warning: 0, critical: 600}
explain: >
  Seconds in the last 24 h during which storage throttled writes because WAL volume exceeded what it could absorb.
  Batch bulk loads, prefer COPY, drop unused indexes, and split huge UPDATE/DELETE statements.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         sum(backpressure_throttling_seconds) AS measure, sum(host_disk_written_bytes) AS disk_written_bytes
  FROM system.lakebase.compute_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id
```

`checks/telemetry/cu_sizing.yaml`:
```yaml
id: cu_sizing
title: "Compute size does not match the workload"
category: capacity
source: delta
thresholds: {info: 0, warning: 1}
explain: >
  Combines 7-day p95 CPU, local cache hit rate and working set. measure = 2 when undersized (p95 CPU > 85%,
  working set > cache, or cache hit < 95%) and 1 when oversized (CPU < 20%, working set < 40%, hit rate >= 99%).
sql: |
  WITH cpu AS (
    SELECT c.project_id, c.branch_id, c.endpoint_id,
           percentile_approx(c.host_cpu_seconds / NULLIF(timestampdiff(MILLISECOND, c.previous_ts, c.ts) / 1000.0, 0)
                             / NULLIF(g.host_logical_cpus, 0), 0.95) AS p95_cpu,
           100.0 * sum(c.lfc_hits) / NULLIF(sum(c.lfc_hits) + sum(c.lfc_misses), 0) AS lfc_hit_pct
    FROM system.lakebase.compute_counters c
    JOIN system.lakebase.compute_gauges g
      ON c.compute_id = g.compute_id AND date_trunc('MINUTE', c.ts) = date_trunc('MINUTE', g.ts)
    WHERE c.ts >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY c.project_id, c.branch_id, c.endpoint_id),
  mem AS (
    SELECT endpoint_id, percentile_approx(lfc_working_set_size_60m / NULLIF(lfc_size_limit, 0), 0.95) AS p95_ws
    FROM system.lakebase.compute_gauges WHERE ts >= current_timestamp() - INTERVAL 7 DAYS GROUP BY endpoint_id)
  SELECT cpu.project_id, cpu.branch_id, cpu.endpoint_id, cpu.endpoint_id AS object,
         100 * cpu.p95_cpu AS p95_cpu_pct, cpu.lfc_hit_pct, 100 * mem.p95_ws AS p95_ws_pct,
         CASE WHEN cpu.p95_cpu > 0.85 OR mem.p95_ws > 1.0 OR cpu.lfc_hit_pct < 95 THEN 2
              WHEN cpu.p95_cpu < 0.20 AND mem.p95_ws < 0.40 AND cpu.lfc_hit_pct >= 99 THEN 1 ELSE 0 END AS measure
  FROM cpu JOIN mem ON cpu.endpoint_id = mem.endpoint_id
fix: {kind: manual, template: "measure 2: raise CU (min CU if cache-bound). measure 1: lower min CU / enable scale-to-zero on non-prod.", risk: moderate}
```

`checks/telemetry/rollback_ratio.yaml`:
```yaml
id: rollback_ratio
title: "High transaction rollback ratio"
category: throughput
source: delta
thresholds: {warning: 1, critical: 5}
explain: >
  Share of transactions rolled back per database (24 h). Look up the cause in postgres_errors: serialization
  failures, deadlocks, constraint violations, or ORMs rolling back read-only transactions.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
         100.0 * sum(xact_rollback) / NULLIF(sum(xact_commit) + sum(xact_rollback), 0) AS measure,
         (sum(xact_commit) + sum(xact_rollback)) / NULLIF(sum(timestampdiff(SECOND, previous_ts, ts)), 0) AS avg_tps
  FROM system.lakebase.database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, datname
  HAVING sum(xact_commit) + sum(xact_rollback) > 1000
```

`checks/telemetry/seq_scan_read_ratio.yaml`:
```yaml
id: seq_scan_read_ratio
title: "Database reads far more rows than it uses"
category: indexing
source: delta
thresholds: {warning: 10, critical: 100}
explain: >
  rows_returned (rows read, dominated by sequential scans) divided by rows_fetched (rows from index scans) per
  database over 24 h. A high ratio means missing indexes; see seq_scan_heavy and top_db_time_query.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
         sum(rows_returned) / NULLIF(sum(rows_fetched), 0) AS measure,
         sum(rows_returned) AS rows_returned, sum(rows_fetched) AS rows_fetched
  FROM system.lakebase.database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, datname
  HAVING sum(rows_returned) > 1000000
```

`checks/telemetry/deadlocks.yaml`:
```yaml
id: deadlocks
title: "Deadlocks detected"
category: incidents
source: delta
thresholds: {warning: 0, critical: 2}
explain: >
  Deadlocks in the last 2 hours per database. Lock rows in a consistent order, keep transactions short, retry on
  40P01, and index foreign keys (missing_fk_index) to narrow lock scope. Details are in postgres_logs (40P01).
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object, sum(deadlocks) AS measure
  FROM system.lakebase.database_counters WHERE ts >= current_timestamp() - INTERVAL 2 HOURS
  GROUP BY project_id, branch_id, endpoint_id, datname
```

`checks/telemetry/idle_in_tx_time.yaml`:
```yaml
id: idle_in_tx_time
title: "Sessions spend a lot of time idle in transaction"
category: incidents
source: delta
thresholds: {warning: 5, critical: 20}
explain: >
  Share of session time spent idle inside an open transaction (24 h). Such sessions hold locks and block vacuum.
  Set idle_in_transaction_session_timeout (30-60 s) and fix application transaction scoping.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
         100.0 * sum(idle_in_transaction_time_ms) / NULLIF(sum(session_time_ms), 0) AS measure,
         sum(sessions_killed) AS sessions_killed, sum(sessions_fatal) AS sessions_fatal
  FROM system.lakebase.database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, datname
  HAVING sum(session_time_ms) > 600000
```

`checks/telemetry/top_db_time_query.yaml`:
```yaml
id: top_db_time_query
title: "One query dominates database time"
category: performance
source: delta
thresholds: {warning: 25, critical: 50}
explain: >
  Share of the endpoint's total execution time spent in one normalized query (24 h, at least 60 s total). Optimize
  it first: check its plan (disk_spill, row_misestimate) and indexes (seq_scan_heavy, missing_fk_index).
sql: |
  WITH q AS (
    SELECT project_id, branch_id, endpoint_id, queryid, max(query) AS query_text, sum(calls) AS calls,
           sum(total_exec_time) AS total_ms, sum(temp_blks_written) AS temp_blks_written
    FROM system.lakebase.pg_stat_statements_counters
    WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND toplevel
    GROUP BY project_id, branch_id, endpoint_id, queryid)
  SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, query_text, calls,
         total_ms / NULLIF(calls, 0) AS mean_ms, temp_blks_written,
         100.0 * total_ms / NULLIF(sum(total_ms) OVER (PARTITION BY endpoint_id), 0) AS measure
  FROM q
  QUALIFY total_ms > 60000
```

`checks/telemetry/query_regression.yaml`:
```yaml
id: query_regression
title: "Query got slower than its baseline"
category: performance
source: delta
thresholds: {warning: 1.5, critical: 2}
explain: >
  Mean latency in the last 24 h divided by the previous 6 days, per endpoint and query (>= 50 calls in each
  window). Correlate with recent DDL and statistics; fix with ANALYZE / CREATE STATISTICS before pinning plans.
sql: |
  WITH w AS (
    SELECT project_id, branch_id, endpoint_id, queryid, max(query) AS query_text,
           CASE WHEN ts >= current_timestamp() - INTERVAL 24 HOURS THEN 'recent' ELSE 'baseline' END AS win,
           sum(calls) AS calls, sum(total_exec_time) AS t
    FROM system.lakebase.pg_stat_statements_counters
    WHERE ts >= current_timestamp() - INTERVAL 7 DAYS AND toplevel
    GROUP BY project_id, branch_id, endpoint_id, queryid,
             CASE WHEN ts >= current_timestamp() - INTERVAL 24 HOURS THEN 'recent' ELSE 'baseline' END)
  SELECT r.project_id, r.branch_id, r.endpoint_id, CAST(r.queryid AS STRING) AS object, r.query_text,
         b.t / NULLIF(b.calls, 0) AS baseline_mean_ms, r.t / NULLIF(r.calls, 0) AS recent_mean_ms,
         r.calls AS recent_calls,
         (r.t / NULLIF(r.calls, 0)) / NULLIF(b.t / NULLIF(b.calls, 0), 0) AS measure
  FROM w r JOIN w b ON r.endpoint_id = b.endpoint_id AND r.queryid = b.queryid
                   AND r.win = 'recent' AND b.win = 'baseline'
  WHERE r.calls >= 50 AND b.calls >= 50
```

`checks/telemetry/plan_flip.yaml`:
```yaml
id: plan_flip
title: "Query switches between fast and slow plans"
category: performance
source: delta
thresholds: {warning: 2, critical: 10}
explain: >
  Ratio between the slowest and fastest median latency of a query's plan shapes on the same endpoint (7 days,
  >= 5 executions per shape). Fix statistics first; pin the good plan with pg_hint_plan only as a last resort.
sql: |
  WITH p AS (
    SELECT project_id, branch_id, endpoint_id, queryid, plan_fingerprint, max(query_text) AS query_text,
           count(*) AS execs, percentile_approx(duration_ms, 0.5) AS p50_ms
    FROM system.lakebase.plan_history WHERE ts >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY project_id, branch_id, endpoint_id, queryid, plan_fingerprint HAVING count(*) >= 5)
  SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, max(query_text) AS query_text,
         count(*) AS plan_shapes, max(p50_ms) / NULLIF(min(p50_ms), 0) AS measure
  FROM p GROUP BY project_id, branch_id, endpoint_id, queryid HAVING count(*) > 1
fix: {kind: manual, template: "ANALYZE the tables of query {object}; if the bad plan persists, pin the good one with a pg_hint_plan hint.", risk: moderate}
```

`checks/telemetry/disk_spill.yaml`:
```yaml
id: disk_spill
title: "Query spills sorts or hashes to disk"
category: performance
source: delta
thresholds: {warning: 0, critical: 100}
explain: >
  Executions in the last 24 h whose plan sorted on disk or used more than one hash batch, per query and role.
  Raise work_mem for that role (not globally), or add an index that delivers the sort order.
sql: |
  SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) || '@' || usename AS object, usename,
         concat('"', replace(usename, '"', '""'), '"') AS role_ident, max(query_text) AS query_text,
         count(*) AS measure, max(temp_blks_written) AS max_temp_blks,
         max(CASE WHEN plan_json RLIKE '"Sort Space Type":\\s*"Disk"' THEN 1 ELSE 0 END) AS disk_sort,
         max(CASE WHEN plan_json RLIKE '"Hash Batches":\\s*([2-9]|[1-9][0-9]+)' THEN 1 ELSE 0 END) AS hash_spill
  FROM system.lakebase.plan_history
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND analyze_enabled AND usename IS NOT NULL
    AND (plan_json RLIKE '"Sort Space Type":\\s*"Disk"' OR plan_json RLIKE '"Hash Batches":\\s*([2-9]|[1-9][0-9]+)')
  GROUP BY project_id, branch_id, endpoint_id, queryid, usename
fix:
  kind: setting
  template: "ALTER ROLE {role_ident} SET work_mem = '64MB';"
  rollback: "ALTER ROLE {role_ident} RESET work_mem;"
  risk: moderate
  reversible: true
  verify_on_branch: true
```

`checks/telemetry/row_misestimate.yaml`:
```yaml
id: row_misestimate
title: "Planner row estimates are far off"
category: performance
source: delta
thresholds: {warning: 10, critical: 100}
explain: >
  Largest ratio between estimated and actual rows for a read query in the last 24 h (writes are excluded because
  their top node returns 0 rows). Run ANALYZE; add CREATE STATISTICS for correlated columns.
sql: |
  SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, max(query_text) AS query_text,
         count(*) AS execs,
         max(greatest(greatest(actual_rows, 1) / greatest(plan_rows, 1),
                      greatest(plan_rows, 1) / greatest(actual_rows, 1))) AS measure
  FROM system.lakebase.plan_history
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND analyze_enabled
    AND NOT plan_json RLIKE '"Node Type":\\s*"ModifyTable"'
  GROUP BY project_id, branch_id, endpoint_id, queryid
fix: {kind: manual, template: "ANALYZE the tables of query {object}; consider CREATE STATISTICS (dependencies, ndistinct) on correlated columns.", risk: safe}
```

`checks/telemetry/sort_limit_waste.yaml`:
```yaml
id: sort_limit_waste
title: "Sort reads far more rows than the LIMIT returns"
category: performance
source: delta
thresholds: {warning: 10, critical: 1000}
explain: >
  For plans topped by Limit with a Sort beneath, the largest row count read by a plan node divided by the rows the
  LIMIT returned (24 h). An index matching ORDER BY lets Postgres stop early instead of sorting everything.
sql: |
  WITH s AS (
    SELECT project_id, branch_id, endpoint_id, queryid, query_text,
           regexp_extract_all(plan_json, '"Node Type":\\s*"([^"]+)"', 1) AS types,
           transform(regexp_extract_all(plan_json, '"Actual Rows":\\s*([0-9.]+)', 1), x -> try_cast(x AS DOUBLE)) AS rws
    FROM system.lakebase.plan_history
    WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND analyze_enabled)
  SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, max(query_text) AS query_text,
         count(*) AS execs, max(array_max(rws)) AS max_rows_read, max(try_element_at(rws, 1)) AS limit_rows,
         max(array_max(rws) / greatest(try_element_at(rws, 1), 1)) AS measure
  FROM s
  WHERE try_element_at(types, 1) = 'Limit'
    AND (array_contains(slice(types, 2, 3), 'Sort') OR array_contains(slice(types, 2, 3), 'Incremental Sort'))
  GROUP BY project_id, branch_id, endpoint_id, queryid
fix: {kind: manual, template: "Create an index matching the ORDER BY of query {object}, verified on a branch.", risk: moderate}
```

`checks/telemetry/storage_wait.yaml`:
```yaml
id: storage_wait
title: "Query waits on remote storage"
category: performance
source: delta
thresholds: {warning: 10, critical: 30}
explain: >
  Average share of a query's duration spent waiting on pageserver reads after local cache misses (24 h, >= 10
  executions). Index it so it reads fewer pages, enlarge the cache (min CU), or pg_prewarm hot tables.
sql: |
  SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, max(query_text) AS query_text,
         count(*) AS execs, avg(duration_ms) AS avg_ms,
         100 * avg(neon_getpage_wait_us / 1000.0 / NULLIF(duration_ms, 0)) AS measure
  FROM system.lakebase.plan_history WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, queryid HAVING count(*) >= 10
```

`checks/telemetry/lock_wait_share.yaml`:
```yaml
id: lock_wait_share
title: "Lock waits are a large share of wait time"
category: incidents
source: delta
thresholds: {warning: 10, critical: 25}
explain: >
  Share of wait time in the Lock class (24 h), excluding client think-time and platform background waits
  (Activity, Extension:Neon*). See blocked_session and missing_fk_index.
sql: |
  WITH w AS (
    SELECT project_id, branch_id, endpoint_id, wait_class_name, sum(wait_event_time) AS t
    FROM system.lakebase.wait_event_counters
    WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND wait_class_name NOT IN ('Activity', 'Client')
      AND NOT (wait_class_name = 'Extension' AND wait_event_name RLIKE '^Neon')
    GROUP BY project_id, branch_id, endpoint_id, wait_class_name)
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         100.0 * sum(CASE WHEN wait_class_name = 'Lock' THEN t ELSE 0 END) / NULLIF(sum(t), 0) AS measure,
         sum(t) AS total_wait_s
  FROM w GROUP BY project_id, branch_id, endpoint_id HAVING sum(t) > 60
```

`checks/telemetry/lock_blocking_history.yaml`:
```yaml
id: lock_blocking_history
title: "Sessions were blocked by other sessions"
category: incidents
source: delta
thresholds: {warning: 60, critical: 300}
explain: >
  Longest time (seconds) a sampled session spent blocked by another session in the last 2 hours. Usually an
  idle-in-transaction session holding a row lock. Set idle_in_transaction_session_timeout and lock_timeout.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         max(query_age_ms) / 1000.0 AS measure, count(DISTINCT pid) AS blocked_pids, count(*) AS samples
  FROM system.lakebase.active_session_history
  WHERE ts >= current_timestamp() - INTERVAL 2 HOURS AND blocking_pid IS NOT NULL AND blocking_pid <> 0
  GROUP BY project_id, branch_id, endpoint_id
```

`checks/telemetry/postgres_errors.yaml`:
```yaml
id: postgres_errors
title: "Infrastructure-level Postgres errors"
category: incidents
source: delta
thresholds: {warning: 0, critical: 10}
explain: >
  Count of resource, operator, system or internal errors (SQLSTATE 53*, 57P*, 58*, XX*), idle-in-transaction
  terminations (25P03) and lock-not-available (55P03) in the last 2 hours. Deadlocks are covered separately.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || sqlstate AS object, sqlstate,
         count(*) AS measure, max(regexp_replace(message, '[0-9]+', 'N')) AS message_pattern, max(ts) AS last_seen
  FROM system.lakebase.postgres_logs
  WHERE ts >= current_timestamp() - INTERVAL 2 HOURS AND severity IN ('ERROR', 'FATAL', 'PANIC')
    AND (sqlstate RLIKE '^(53|57P|58|XX)' OR sqlstate IN ('25P03', '55P03'))
  GROUP BY project_id, branch_id, endpoint_id, sqlstate
```

`checks/telemetry/xid_runway.yaml`:
```yaml
id: xid_runway
title: "Transaction ID age is high"
category: maintenance
source: delta
thresholds: {warning: 200000000, critical: 1000000000}
explain: >
  Current age of the oldest unfrozen transaction ID per database, with days until 1.5 billion at the 7-day rate.
  Remove freeze blockers (xmin_horizon_holder, replication_slot_retention) and VACUUM (FREEZE) the oldest tables.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object,
         max_by(oldest_frozen_xid_age, ts) AS measure,
         (1500000000 - max_by(oldest_frozen_xid_age, ts))
           / NULLIF(regr_slope(oldest_frozen_xid_age, unix_timestamp(ts)) * 86400, 0) AS days_to_1_5b,
         max_by(oldest_mxid_age, ts) AS mxid_age
  FROM system.lakebase.database_gauges WHERE ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY project_id, branch_id, endpoint_id, datname
```

`checks/telemetry/replica_lag.yaml`:
```yaml
id: replica_lag
title: "Read replica lags behind the primary"
category: replication
source: delta
thresholds: {warning: 1, critical: 30}
explain: >
  p95 replication lag in seconds for read replicas over 24 h. Size the replica like the primary, shorten
  long replica queries, and smooth bulk writes on the primary.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object,
         percentile_approx(replication_lag_time_ms, 0.95) / 1000.0 AS measure,
         max(replication_lag_bytes) AS max_lag_bytes
  FROM system.lakebase.compute_gauges
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND compute_mode <> 'primary'
    AND replication_lag_time_ms IS NOT NULL
  GROUP BY project_id, branch_id, endpoint_id
```

`checks/telemetry/auth_failures.yaml`:
```yaml
id: auth_failures
title: "Repeated authentication failures"
category: security
source: delta
thresholds: {warning: 5, critical: 50}
explain: >
  Failed logins (28P01, 28000) per user in the last 24 h. Check for expired OAuth tokens in clients, or for
  password guessing against roles with native passwords.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || coalesce(usename, '?') AS object,
         count(*) AS measure, max(ts) AS last_seen
  FROM system.lakebase.postgres_logs
  WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND sqlstate IN ('28P01', '28000')
  GROUP BY project_id, branch_id, endpoint_id, usename
```

- [ ] **Step 4: Run the catalog test and verify it passes**

Run: `cd engine && .venv/bin/pytest tests/unit/test_catalog.py -q`
Expected: `5 passed`

- [ ] **Step 5: Note on SQL validation**

Telemetry SQL can only run against the Delta copy (`raw_*`), which the first engine run creates in Task 17. Task 17's acceptance script fails if any check reports status `error` in the latest run, so every telemetry query is executed and validated there. Do not query `system.lakebase` directly to test these queries; each statement takes minutes to compile.

- [ ] **Step 6: Commit**

```bash
git add engine/src/lakebase_health/checks/telemetry engine/tests/unit/test_catalog.py
git commit -m "feat(checks): 26 telemetry checks"
```

---

### Task 14: Check catalog, part 2: Postgres checks

**Files:**
- Create: 27 files under `engine/src/lakebase_health/checks/postgres/`
- Modify: `engine/tests/unit/test_catalog.py` (fill `POSTGRES`)

Conventions: SQL runs in the target database with the engine's read-only session. It outputs `object` and `measure` (`float8`). Identifiers used in fix templates are built with `quote_ident` (never `::regclass::text`, which depends on the search path). Every query referencing catalog objects filters `{excluded_schemas}`; activity queries filter `{excluded_users}`; slot queries filter `{excluded_slots}`. Query text goes only into a `query_text` column.

- [ ] **Step 1: Set the expected ids in `test_catalog.py`**

```python
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
    "dead_tuples", "autovacuum_disabled", "autovacuum_scale_factor", "table_bloat", "index_bloat", "xid_age_table",
    "xmin_horizon_holder", "long_transaction", "idle_in_transaction", "blocked_session", "prepared_transaction",
    "replication_slot_retention", "sequence_exhaustion", "stale_statistics", "public_schema_create",
    "privileged_roles", "table_without_pk", "non_ssl_sessions", "guardrail_settings", "config_drift",
    "autovacuum_blocker",
}
```

- [ ] **Step 2: Run the catalog test and verify it fails**

Run: `cd engine && .venv/bin/pytest tests/unit/test_catalog.py -q`
Expected: FAIL, `test_catalog_ids`

- [ ] **Step 3: Create the 27 Postgres check files**

`checks/postgres/unused_index.yaml`:
```yaml
id: unused_index
title: "Index never used"
category: indexing
source: postgres
guards: ["stats_window_hours >= 24"]
thresholds: {info: 0, warning: 104857600, critical: 1073741824}
explain: >
  Non-unique, non-constraint index with zero scans since statistics reset (measure = size in bytes). It costs
  writes and storage. Only trust this when the statistics window covers a full business cycle, and check replicas.
sql: |
  SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS object,
         quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS index_ident,
         s.relname AS table_name, s.idx_scan, pg_relation_size(s.indexrelid)::float8 AS measure,
         pg_get_indexdef(s.indexrelid) AS index_def
  FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
  WHERE s.idx_scan = 0 AND NOT i.indisunique AND NOT i.indisprimary AND i.indisvalid
    AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid)
    AND s.schemaname NOT IN {excluded_schemas}
fix:
  kind: ddl
  template: "DROP INDEX CONCURRENTLY {index_ident};"
  rollback: "{index_def};"
  risk: high
  reversible: true
  verify_on_branch: true
```

`checks/postgres/duplicate_index.yaml`:
```yaml
id: duplicate_index
title: "Duplicate index"
category: indexing
source: postgres
thresholds: {warning: 0, critical: 1073741824}
explain: >
  Two or more indexes with identical table, columns, operator classes, expressions and predicate. Keep one (unique
  and primary-key indexes are always kept) and drop the rest.
sql: |
  WITH d AS (
    SELECT i.indexrelid, i.indisunique, i.indisprimary,
           i.indrelid::text || '|' || i.indclass::text || '|' || i.indkey::text || '|' ||
           coalesce(i.indexprs::text, '') || '|' || coalesce(i.indpred::text, '') AS sig,
           quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, n.nspname
    FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname NOT IN {excluded_schemas}),
  r AS (SELECT d.*, row_number() OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS rn,
               first_value(ident) OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS kept
        FROM d)
  SELECT ident AS object, ident AS index_ident, kept AS kept_index,
         pg_relation_size(indexrelid)::float8 AS measure, pg_get_indexdef(indexrelid) AS index_def
  FROM r WHERE rn > 1 AND NOT indisunique AND NOT indisprimary
fix:
  kind: ddl
  template: "DROP INDEX CONCURRENTLY {index_ident};"
  rollback: "{index_def};"
  risk: high
  reversible: true
  verify_on_branch: true
```

`checks/postgres/redundant_index.yaml`:
```yaml
id: redundant_index
title: "Index covered by a wider index"
category: indexing
source: postgres
thresholds: {warning: 0, critical: 1073741824}
explain: >
  A non-unique index whose columns are a leading prefix of another valid index on the same table with the same
  access method, e.g. (a) next to (a, b). The wider index serves the same lookups.
sql: |
  SELECT quote_ident(na.nspname) || '.' || quote_ident(ca.relname) AS object,
         quote_ident(na.nspname) || '.' || quote_ident(ca.relname) AS index_ident,
         quote_ident(nb.nspname) || '.' || quote_ident(cb.relname) AS covered_by,
         pg_relation_size(a.indexrelid)::float8 AS measure, pg_get_indexdef(a.indexrelid) AS index_def
  FROM pg_index a
  JOIN pg_index b ON a.indrelid = b.indrelid AND a.indexrelid <> b.indexrelid
  JOIN pg_class ca ON ca.oid = a.indexrelid JOIN pg_namespace na ON na.oid = ca.relnamespace
  JOIN pg_class cb ON cb.oid = b.indexrelid JOIN pg_namespace nb ON nb.oid = cb.relnamespace
  WHERE ca.relam = cb.relam AND NOT a.indisunique AND NOT a.indisprimary AND b.indisvalid
    AND a.indpred IS NULL AND b.indpred IS NULL AND a.indexprs IS NULL AND b.indexprs IS NULL
    AND b.indkey::text LIKE a.indkey::text || ' %'
    AND na.nspname NOT IN {excluded_schemas}
fix:
  kind: ddl
  template: "DROP INDEX CONCURRENTLY {index_ident};"
  rollback: "{index_def};"
  risk: high
  reversible: true
  verify_on_branch: true
```

`checks/postgres/invalid_index.yaml`:
```yaml
id: invalid_index
title: "Invalid index left by a failed build"
category: indexing
source: postgres
thresholds: {warning: 0}
explain: >
  An index marked invalid (usually a failed CREATE INDEX CONCURRENTLY). It is maintained on every write but never
  used for reads. Drop it, or rebuild it with REINDEX INDEX CONCURRENTLY after fixing the cause.
sql: |
  SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
         quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS index_ident, 1.0::float8 AS measure
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE NOT i.indisvalid AND n.nspname NOT IN {excluded_schemas}
fix: {kind: manual, template: "DROP INDEX CONCURRENTLY {index_ident};  -- or: REINDEX INDEX CONCURRENTLY {index_ident};", risk: moderate}
```

`checks/postgres/missing_fk_index.yaml`:
```yaml
id: missing_fk_index
title: "Foreign key without a supporting index"
category: indexing
source: postgres
thresholds: {warning: 0, critical: 1073741824}
explain: >
  The referencing columns of a foreign key have no index (measure = child table size). Every DELETE or key UPDATE
  on the parent then scans the child while holding locks, a common cause of lock waits and deadlocks.
sql: |
  SELECT quote_ident(n.nspname) || '.' || quote_ident(t.relname) || ' (' || cols.list || ')' AS object,
         quote_ident(n.nspname) || '.' || quote_ident(t.relname) AS child_table, cols.list AS fk_columns,
         quote_ident(n.nspname) AS index_schema,
         quote_ident(left(t.relname || '_' || cols.names || '_fk_idx', 63)) AS index_name,
         c.conname AS constraint_name, pg_relation_size(t.oid)::float8 AS measure
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace
  CROSS JOIN LATERAL (
    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY array_position(c.conkey, a.attnum)) AS list,
           string_agg(a.attname, '_' ORDER BY array_position(c.conkey, a.attnum)) AS names
    FROM pg_attribute a WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)) cols
  WHERE c.contype = 'f' AND n.nspname NOT IN {excluded_schemas}
    AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.conrelid
                    AND (i.indkey::text || ' ') LIKE array_to_string(c.conkey, ' ') || ' %')
fix:
  kind: ddl
  template: "CREATE INDEX CONCURRENTLY {index_name} ON {child_table} ({fk_columns});"
  rollback: "DROP INDEX CONCURRENTLY IF EXISTS {index_schema}.{index_name};"
  risk: moderate
  reversible: true
  verify_on_branch: true
```

`checks/postgres/seq_scan_heavy.yaml`:
```yaml
id: seq_scan_heavy
title: "Table read mostly by large sequential scans"
category: indexing
source: postgres
guards: ["stats_window_hours >= 24"]
thresholds: {warning: 10000, critical: 1000000}
explain: >
  Average rows read per sequential scan on a table with more than 10k live rows. Find the queries scanning it
  (top_db_time_query) and index their filter and join columns; verify the index on a branch.
sql: |
  SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object, seq_scan, idx_scan, n_live_tup,
         seq_tup_read, (seq_tup_read::float8 / NULLIF(seq_scan, 0)) AS measure,
         pg_relation_size(relid)::float8 AS table_bytes
  FROM pg_stat_user_tables
  WHERE n_live_tup > 10000 AND seq_scan > 0 AND schemaname NOT IN {excluded_schemas}
fix: {kind: manual, template: "Index the filter columns of the queries scanning {object}.", risk: moderate}
```

`checks/postgres/dead_tuples.yaml`:
```yaml
id: dead_tuples
title: "High share of dead tuples"
category: maintenance
source: postgres
thresholds: {warning: 10, critical: 20}
explain: >
  Dead tuples as a share of all tuples on tables with more than 10k rows. Vacuum is falling behind or blocked;
  check autovacuum_blocker first, then tune autovacuum per table.
sql: |
  SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
         quote_ident(schemaname) || '.' || quote_ident(relname) AS table_ident,
         n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count,
         (100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0))::float8 AS measure
  FROM pg_stat_user_tables
  WHERE n_live_tup + n_dead_tup > 10000 AND schemaname NOT IN {excluded_schemas}
fix:
  kind: ddl
  template: "VACUUM (ANALYZE) {table_ident};"
  rollback: "-- VACUUM changes no data; nothing to roll back"
  risk: safe
  reversible: true
```

`checks/postgres/autovacuum_disabled.yaml`:
```yaml
id: autovacuum_disabled
title: "Autovacuum disabled on a table"
category: maintenance
source: postgres
thresholds: {warning: 0}
explain: >
  The table has autovacuum_enabled = false, so dead tuples accumulate and transaction IDs are never frozen by
  autovacuum. Re-enable it unless a documented process vacuums this table.
sql: |
  SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
         quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_ident,
         array_to_string(c.reloptions, ',') AS reloptions, 1.0::float8 AS measure
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'm') AND c.reloptions @> ARRAY['autovacuum_enabled=false']
    AND n.nspname NOT IN {excluded_schemas}
fix:
  kind: ddl
  template: "ALTER TABLE {table_ident} RESET (autovacuum_enabled);"
  rollback: "ALTER TABLE {table_ident} SET (autovacuum_enabled = false);"
  risk: safe
  reversible: true
```

`checks/postgres/autovacuum_scale_factor.yaml`:
```yaml
id: autovacuum_scale_factor
title: "Large table needs per-table autovacuum tuning"
category: maintenance
source: postgres
thresholds: {warning: 10, critical: 30}
explain: >
  A table with more than 1M live rows, default autovacuum scale factor and more than 10% dead tuples. The default
  0.2 waits for 20% of the table to be dead; lower it per table.
sql: |
  SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.relname) AS object,
         quote_ident(s.schemaname) || '.' || quote_ident(s.relname) AS table_ident, s.n_live_tup, s.n_dead_tup,
         (100.0 * s.n_dead_tup / NULLIF(s.n_live_tup + s.n_dead_tup, 0))::float8 AS measure
  FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
  WHERE s.n_live_tup > 1000000 AND s.schemaname NOT IN {excluded_schemas}
    AND coalesce(array_to_string(c.reloptions, ','), '') NOT LIKE '%autovacuum_vacuum_scale_factor%'
    AND NOT coalesce(c.reloptions @> ARRAY['autovacuum_enabled=false'], false)
fix:
  kind: ddl
  template: "ALTER TABLE {table_ident} SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.01);"
  rollback: "ALTER TABLE {table_ident} RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_scale_factor);"
  risk: safe
  reversible: true
```

`checks/postgres/table_bloat.yaml`:
```yaml
id: table_bloat
title: "Table bloat"
category: maintenance
source: postgres
schedule: daily
requires_extension: pgstattuple
thresholds: {warning: 20, critical: 40}
explain: >
  Approximate free space share in the 15 largest tables over 10 MB (pgstattuple_approx; runs daily because it
  reads pages). VACUUM makes space reusable but does not shrink; shrinking needs VACUUM FULL in a window.
sql: |
  SELECT quote_ident(t.schemaname) || '.' || quote_ident(t.relname) AS object,
         pg_table_size(t.relid)::float8 AS table_bytes, a.dead_tuple_percent::float8 AS dead_pct,
         a.approx_free_percent::float8 AS measure
  FROM (SELECT relid, schemaname, relname FROM pg_stat_user_tables
        WHERE schemaname NOT IN {excluded_schemas} AND pg_table_size(relid) > 10485760
        ORDER BY pg_table_size(relid) DESC LIMIT 15) t
  CROSS JOIN LATERAL pgstattuple_approx(t.relid) a
fix: {kind: manual, template: "Plan VACUUM FULL {object} in a maintenance window (ACCESS EXCLUSIVE lock), then tune autovacuum.", risk: high}
```

`checks/postgres/index_bloat.yaml`:
```yaml
id: index_bloat
title: "B-tree index bloat"
category: maintenance
source: postgres
schedule: daily
requires_extension: pgstattuple
direction: below
thresholds: {warning: 70, critical: 50}
explain: >
  Average leaf density of B-tree indexes over 100 MB (pgstatindex; daily). A fresh B-tree is about 90% dense.
  Rebuild online with REINDEX INDEX CONCURRENTLY.
sql: |
  SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS object,
         quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS index_ident,
         pg_relation_size(s.indexrelid)::float8 AS index_bytes, x.leaf_fragmentation::float8 AS leaf_fragmentation,
         x.avg_leaf_density::float8 AS measure
  FROM pg_stat_user_indexes s
  JOIN pg_class c ON c.oid = s.indexrelid JOIN pg_am am ON am.oid = c.relam AND am.amname = 'btree'
  CROSS JOIN LATERAL pgstatindex(s.indexrelid) x
  WHERE pg_relation_size(s.indexrelid) > 104857600 AND s.schemaname NOT IN {excluded_schemas}
fix:
  kind: ddl
  template: "REINDEX INDEX CONCURRENTLY {index_ident};"
  rollback: "-- REINDEX rebuilds the index in place; nothing to roll back"
  risk: moderate
  reversible: true
```

`checks/postgres/xid_age_table.yaml`:
```yaml
id: xid_age_table
title: "Table with old unfrozen transaction IDs"
category: maintenance
source: postgres
thresholds: {warning: 200000000, critical: 1000000000}
explain: >
  age(relfrozenxid) of a table above 100M. Freeze the oldest tables first; remove freeze blockers if age keeps rising.
sql: |
  SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object,
         quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_ident,
         age(c.relfrozenxid)::float8 AS measure, mxid_age(c.relminmxid)::float8 AS mxid_age
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'm') AND n.nspname NOT IN {excluded_schemas} AND age(c.relfrozenxid) > 100000000
fix:
  kind: ddl
  template: "VACUUM (FREEZE) {table_ident};"
  rollback: "-- VACUUM FREEZE changes no data; nothing to roll back"
  risk: safe
  reversible: true
```

`checks/postgres/xmin_horizon_holder.yaml`:
```yaml
id: xmin_horizon_holder
title: "Session holds back vacuum and freezing"
category: maintenance
source: postgres
thresholds: {warning: 50000000, critical: 200000000}
explain: >
  A session whose snapshot (backend_xmin) is very old in transaction IDs. Vacuum cannot remove rows newer than it
  and freezing stalls. Find the owner and end the transaction.
sql: |
  SELECT 'pid ' || pid AS object, pid, usename, application_name, state, age(backend_xmin)::float8 AS measure,
         extract(epoch FROM now() - xact_start)::float8 AS xact_age_s, left(query, 200) AS query_text
  FROM pg_stat_activity
  WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid() AND usename NOT IN {excluded_users}
fix: {kind: manual, template: "After confirming with the session owner: SELECT pg_terminate_backend({pid});", risk: high}
```

`checks/postgres/long_transaction.yaml`:
```yaml
id: long_transaction
title: "Long-running transaction"
category: incidents
source: postgres
thresholds: {warning: 300, critical: 1800}
explain: >
  An active transaction open for more than 5 minutes (seconds). Long transactions hold locks and block vacuum.
  Set statement_timeout for application roles and split long batch jobs.
sql: |
  SELECT 'pid ' || pid AS object, pid, usename, application_name, wait_event_type, wait_event,
         extract(epoch FROM now() - xact_start)::float8 AS measure, left(query, 200) AS query_text
  FROM pg_stat_activity
  WHERE backend_type = 'client backend' AND xact_start IS NOT NULL AND state = 'active'
    AND pid <> pg_backend_pid() AND usename NOT IN {excluded_users}
fix: {kind: manual, template: "Investigate pid {pid}; cancel with SELECT pg_cancel_backend({pid}); if safe.", risk: high}
```

`checks/postgres/idle_in_transaction.yaml`:
```yaml
id: idle_in_transaction
title: "Session idle inside an open transaction"
category: incidents
source: postgres
thresholds: {warning: 60, critical: 300}
explain: >
  A session that opened a transaction and went idle (seconds in that state). It holds its locks and snapshot,
  blocking writers and vacuum. Lower idle_in_transaction_session_timeout and fix the application.
sql: |
  SELECT 'pid ' || pid AS object, pid, usename, application_name,
         extract(epoch FROM now() - state_change)::float8 AS measure,
         extract(epoch FROM now() - xact_start)::float8 AS xact_age_s, left(query, 200) AS query_text
  FROM pg_stat_activity
  WHERE state LIKE 'idle in transaction%' AND pid <> pg_backend_pid() AND usename NOT IN {excluded_users}
fix: {kind: manual, template: "After confirming with the session owner: SELECT pg_terminate_backend({pid});", risk: high}
```

`checks/postgres/blocked_session.yaml`:
```yaml
id: blocked_session
title: "Session blocked by another session"
category: incidents
source: postgres
thresholds: {warning: 60, critical: 300}
explain: >
  A session waiting on a lock held by another session (seconds blocked). Resolve the blocker first; durable fixes
  are idle_in_transaction_session_timeout, lock_timeout for DDL and short transactions.
sql: |
  SELECT 'pid ' || a.pid AS object, a.pid AS blocked_pid, b.pid AS blocking_pid, b.state AS blocking_state,
         a.usename, extract(epoch FROM now() - a.query_start)::float8 AS measure, left(a.query, 200) AS query_text
  FROM pg_stat_activity a
  CROSS JOIN LATERAL unnest(pg_blocking_pids(a.pid)) AS bp(pid)
  JOIN pg_stat_activity b ON b.pid = bp.pid
  WHERE a.pid <> pg_backend_pid() AND a.usename NOT IN {excluded_users}
fix: {kind: manual, template: "Blocking pid {blocking_pid} ({blocking_state}); end it if safe: SELECT pg_terminate_backend({blocking_pid});", risk: high}
```

`checks/postgres/prepared_transaction.yaml`:
```yaml
id: prepared_transaction
title: "Forgotten prepared transaction"
category: maintenance
source: postgres
thresholds: {warning: 3600, critical: 86400}
explain: >
  A two-phase (prepared) transaction older than an hour (seconds). It holds locks and blocks vacuum until it is
  committed or rolled back by its transaction manager.
sql: |
  SELECT gid AS object, owner, database, extract(epoch FROM now() - prepared)::float8 AS measure,
         age(transaction)::float8 AS xid_age
  FROM pg_prepared_xacts
fix: {kind: manual, template: "If no transaction manager owns it: ROLLBACK PREPARED '{object}';", risk: high}
```

`checks/postgres/replication_slot_retention.yaml`:
```yaml
id: replication_slot_retention
title: "Inactive replication slot retains WAL"
category: replication
source: postgres
thresholds: {warning: 0, critical: 1073741824}
explain: >
  An inactive replication slot (measure = WAL bytes retained) also holds back xmin/catalog_xmin. Lakebase platform
  slots are excluded. Confirm no consumer (Lakehouse Sync, CDC) owns it before dropping.
sql: |
  SELECT slot_name AS object, slot_type, plugin, age(xmin)::float8 AS xmin_age,
         age(catalog_xmin)::float8 AS catalog_xmin_age,
         pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::float8 AS measure
  FROM pg_replication_slots
  WHERE NOT active AND slot_name NOT IN {excluded_slots}
fix: {kind: manual, template: "Confirm no consumer owns slot {object}, then: SELECT pg_drop_replication_slot('{object}');", risk: high}
```

`checks/postgres/sequence_exhaustion.yaml`:
```yaml
id: sequence_exhaustion
title: "Sequence or identity column nearing its limit"
category: forecast
source: postgres
thresholds: {warning: 50, critical: 75}
explain: >
  Share of the usable range consumed, measured against the column type (an int4 column fails at 2^31-1 even with a
  bigint sequence). est_days_left uses the insert rate since statistics reset. Migrate to bigint, rehearsed on a branch.
sql: |
  WITH seq_cols AS (
    SELECT n.nspname AS seq_schema, sc.relname AS seq_name,
           quote_ident(tn.nspname) || '.' || quote_ident(t.relname) AS table_ident, a.attname AS column_name,
           format_type(a.atttypid, a.atttypmod) AS column_type, t.oid AS table_oid,
           (CASE a.atttypid WHEN 'int2'::regtype THEN 32767 WHEN 'int4'::regtype THEN 2147483647
                            ELSE 9223372036854775807 END)::numeric AS column_max
    FROM pg_depend d
    JOIN pg_class sc ON sc.oid = d.objid AND sc.relkind = 'S'
    JOIN pg_namespace n ON n.oid = sc.relnamespace
    JOIN pg_class t ON t.oid = d.refobjid JOIN pg_namespace tn ON tn.oid = t.relnamespace
    JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
    WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
      AND tn.nspname NOT IN {excluded_schemas})
  SELECT sc.table_ident || '.' || quote_ident(sc.column_name) AS object, sc.column_type, ps.last_value,
         least(sc.column_max, ps.max_value::numeric) AS effective_max,
         (100.0 * ps.last_value / least(sc.column_max, ps.max_value::numeric))::float8 AS measure,
         ((least(sc.column_max, ps.max_value::numeric) - ps.last_value)
           / NULLIF(t.n_tup_ins / NULLIF(extract(epoch FROM now() - coalesce(
               (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()),
               pg_postmaster_start_time())), 0), 0) / 86400)::float8 AS est_days_left
  FROM seq_cols sc
  JOIN pg_sequences ps ON ps.schemaname = sc.seq_schema AND ps.sequencename = sc.seq_name
  LEFT JOIN pg_stat_user_tables t ON t.relid = sc.table_oid
  WHERE ps.last_value IS NOT NULL
fix: {kind: manual, template: "Migrate {object} to bigint: add a bigint column, backfill in batches, swap in a window; rehearse on a branch.", risk: high}
```

`checks/postgres/stale_statistics.yaml`:
```yaml
id: stale_statistics
title: "Table statistics are missing or stale"
category: performance
source: postgres
thresholds: {warning: 20, critical: 100}
explain: >
  Rows modified since the last ANALYZE as a share of live rows (100 when never analyzed). Stale statistics give
  the planner bad row estimates. Run ANALYZE; it is cheap and safe.
sql: |
  SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
         quote_ident(schemaname) || '.' || quote_ident(relname) AS table_ident,
         n_live_tup, n_mod_since_analyze, last_analyze, last_autoanalyze,
         (CASE WHEN last_analyze IS NULL AND last_autoanalyze IS NULL THEN 100.0
               ELSE 100.0 * n_mod_since_analyze / NULLIF(n_live_tup, 0) END)::float8 AS measure
  FROM pg_stat_user_tables
  WHERE n_live_tup > 1000 AND schemaname NOT IN {excluded_schemas}
fix:
  kind: ddl
  template: "ANALYZE {table_ident};"
  rollback: "-- ANALYZE only refreshes planner statistics; nothing to roll back"
  risk: safe
  reversible: true
```

`checks/postgres/public_schema_create.yaml`:
```yaml
id: public_schema_create
title: "Any role can create objects in a schema"
category: security
source: postgres
thresholds: {warning: 0}
explain: >
  PUBLIC holds CREATE on the schema, so every role can create objects there (an object-squatting and
  privilege-escalation risk). Revoke it and grant CREATE only to owning roles.
sql: |
  SELECT quote_ident(nspname) AS object, quote_ident(nspname) AS schema_ident, 1.0::float8 AS measure
  FROM pg_namespace
  WHERE nspname NOT IN {excluded_schemas} AND nspname NOT LIKE 'pg\_%'
    AND has_schema_privilege('public', oid, 'CREATE')
fix:
  kind: ddl
  template: "REVOKE CREATE ON SCHEMA {schema_ident} FROM PUBLIC;"
  rollback: "GRANT CREATE ON SCHEMA {schema_ident} TO PUBLIC;"
  risk: moderate
  reversible: true
```

`checks/postgres/privileged_roles.yaml`:
```yaml
id: privileged_roles
title: "Login role with powerful attributes"
category: security
source: postgres
thresholds: {info: 0, warning: 1}
explain: >
  A login role (not platform-managed) with BYPASSRLS, REPLICATION or CREATEROLE (measure = number of these).
  Keep such roles few and audited; use group roles for least privilege.
sql: |
  SELECT quote_ident(rolname) AS object, rolcreaterole, rolcreatedb, rolbypassrls, rolreplication,
         (rolbypassrls::int + rolreplication::int + rolcreaterole::int)::float8 AS measure
  FROM pg_roles
  WHERE rolcanlogin AND rolname !~ {excluded_role_regex} AND rolname NOT IN {excluded_roles}
    AND (rolbypassrls OR rolreplication OR rolcreaterole)
```

`checks/postgres/table_without_pk.yaml`:
```yaml
id: table_without_pk
title: "Table without a primary key"
category: schema
source: postgres
thresholds: {info: -1, warning: 104857600}
explain: >
  Tables without a primary key cannot be safely replicated or synced (logical replication, CDC) and are prone to
  duplicates. measure = table size; warning above 100 MB.
sql: |
  SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS object, pg_relation_size(c.oid)::float8 AS measure
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND n.nspname NOT IN {excluded_schemas}
    AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = c.oid AND contype = 'p')
fix: {kind: manual, template: "Add a primary key to {object} (or a unique NOT NULL key).", risk: moderate}
```

`checks/postgres/non_ssl_sessions.yaml`:
```yaml
id: non_ssl_sessions
title: "Client sessions without SSL"
category: security
source: postgres
thresholds: {warning: 0, critical: 10}
explain: >
  Remote client sessions not using SSL (platform agents on localhost are excluded). Always connect with
  sslmode=require.
sql: |
  SELECT coalesce(a.usename, '?') || '@' || coalesce(host(a.client_addr), 'local') AS object,
         count(*)::float8 AS measure
  FROM pg_stat_ssl s JOIN pg_stat_activity a USING (pid)
  WHERE a.backend_type = 'client backend' AND NOT s.ssl AND a.usename NOT IN {excluded_users}
    AND a.client_addr IS DISTINCT FROM '127.0.0.1' AND a.client_addr IS DISTINCT FROM '::1'
  GROUP BY 1
```

`checks/postgres/guardrail_settings.yaml`:
```yaml
id: guardrail_settings
title: "Guardrail setting missing or too loose"
category: config
source: postgres
thresholds: {info: 0, warning: 1}
explain: >
  One result per setting. idle_in_transaction_session_timeout above 60 s (Lakebase default is 5 min) is a warning
  with a database-level fix; statement_timeout = 0 and track_io_timing = off are informational.
sql: |
  SELECT current_database() || ':' || name AS object, name, setting,
         quote_ident(current_database()) AS db_ident,
         CASE name  -- nested CASE: Postgres does not guarantee AND evaluation order, and setting::bigint fails on 'off'
           WHEN 'idle_in_transaction_session_timeout' THEN CASE WHEN setting = '0' THEN 2.0
                                                              WHEN setting::bigint > 60000 THEN 2.0 ELSE 0.0 END
           WHEN 'statement_timeout' THEN CASE WHEN setting = '0' THEN 1.0 ELSE 0.0 END
           WHEN 'track_io_timing' THEN CASE WHEN setting = 'off' THEN 1.0 ELSE 0.0 END
           ELSE 0.0 END AS measure,
         CASE WHEN name = 'idle_in_transaction_session_timeout'
              THEN 'ALTER DATABASE ' || quote_ident(current_database()) || ' SET idle_in_transaction_session_timeout = ''60s'';' END AS fix_sql,
         CASE WHEN name = 'idle_in_transaction_session_timeout'
              THEN 'ALTER DATABASE ' || quote_ident(current_database()) || ' RESET idle_in_transaction_session_timeout;' END AS fix_rollback
  FROM pg_settings
  WHERE name IN ('idle_in_transaction_session_timeout', 'statement_timeout', 'track_io_timing')
fix:
  kind: setting
  template: "{fix_sql}"
  rollback: "{fix_rollback}"
  risk: moderate
  reversible: true
```

`checks/postgres/config_drift.yaml`:
```yaml
id: config_drift
title: "Role or database setting override"
category: config
source: postgres
thresholds: {info: 0}
explain: >
  A setting overridden with ALTER ROLE / ALTER DATABASE (platform roles and settings excluded). The setting value
  is part of the finding identity, so a changed value appears as a new finding and the old one resolves.
sql: |
  SELECT coalesce(r.rolname, '*') || '@' || coalesce(d.datname, '*') || ':' || cfg AS object,
         coalesce(r.rolname, '*') AS role_name, coalesce(d.datname, '*') AS database_name, cfg AS setting,
         1.0::float8 AS measure
  FROM pg_db_role_setting s
  LEFT JOIN pg_roles r ON r.oid = s.setrole LEFT JOIN pg_database d ON d.oid = s.setdatabase
  CROSS JOIN LATERAL unnest(s.setconfig) AS cfg
  WHERE coalesce(r.rolname, '') !~ {excluded_role_regex} AND coalesce(r.rolname, '') NOT IN {excluded_roles}
    AND split_part(cfg, '=', 1) !~ {excluded_settings_regex}
```

`checks/postgres/autovacuum_blocker.yaml`:
```yaml
id: autovacuum_blocker
title: "Dead tuples pile up because something blocks vacuum"
category: maintenance
source: postgres
thresholds: {warning: 10, critical: 30}
explain: >
  Links a table's dead tuples (measure = dead %) to the blocker preventing cleanup: autovacuum disabled on the
  table, an old snapshot from a long transaction, a replication slot, or a prepared transaction.
sql: |
  WITH dead AS (
    SELECT relid, quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
           (100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0))::float8 AS dead_pct
    FROM pg_stat_user_tables WHERE n_live_tup + n_dead_tup > 10000 AND schemaname NOT IN {excluded_schemas}),
  blockers AS (
    SELECT 'long_transaction' AS kind, max(age(backend_xmin))::float8 AS xmin_age FROM pg_stat_activity
      WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid() AND usename NOT IN {excluded_users}
    UNION ALL
    SELECT 'replication_slot', max(greatest(age(xmin), age(catalog_xmin)))::float8 FROM pg_replication_slots
      WHERE slot_name NOT IN {excluded_slots}
    UNION ALL
    SELECT 'prepared_transaction', max(age(transaction))::float8 FROM pg_prepared_xacts),
  x AS (
    SELECT d.object, d.dead_pct AS measure,
           CASE WHEN c.reloptions @> ARRAY['autovacuum_enabled=false'] THEN 'autovacuum_disabled'
                ELSE (SELECT kind FROM blockers WHERE xmin_age > 1000000 ORDER BY xmin_age DESC LIMIT 1) END AS blocker
    FROM dead d JOIN pg_class c ON c.oid = d.relid WHERE d.dead_pct > 10)
  SELECT object, measure, blocker FROM x WHERE blocker IS NOT NULL
fix: {kind: manual, template: "Remove the blocker ({blocker}) first; see the matching finding for {object}.", risk: moderate}
```

- [ ] **Step 4: Run the catalog tests and verify they pass**

Run: `cd engine && .venv/bin/pytest tests/unit/test_catalog.py -q`
Expected: `5 passed`

- [ ] **Step 5: Run every Postgres check against the seeded test database**

```bash
cd engine && .venv/bin/python - <<'EOF'
import json, subprocess
from pathlib import Path
from lakebase_health.checkspec import load_checks, render_sql
from lakebase_health.exclusions import load_exclusions
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.executors.postgres import SESSION_SETUP
from lakebase_health.jobs.run import CHECKS_DIR
import psycopg
cli = lambda *a: json.loads(subprocess.check_output(["databricks", *a, "--profile", "my-profile", "-o", "json"]))
EP = "projects/healthcheck/branches/production/endpoints/primary"
host = cli("postgres", "get-endpoint", EP)["status"]["hosts"]["host"]
tok = cli("postgres", "generate-database-credential", EP)["token"]
user = cli("current-user", "me")["userName"]
excl = load_exclusions(CHECKS_DIR / "exclusions.yaml")
with psycopg.connect(host=host, user=user, password=tok, dbname="databricks_postgres", sslmode="require", autocommit=True) as c:
    for s in SESSION_SETUP: c.execute(s)
    for spec in load_checks(CHECKS_DIR, frozenset(API_CHECKS)):
        if spec.source != "postgres": continue
        try:
            cur = c.execute(render_sql(spec, excl)); rows = cur.fetchall() if cur.description else []
            print(f"OK   {spec.id}: {len(rows)} rows")
        except Exception as e:
            print(f"FAIL {spec.id}: {e}")
EOF
```
Expected: 27 `OK` lines, zero `FAIL`. Fix any failing SQL before committing.

- [ ] **Step 6: Commit**

```bash
git add engine/src/lakebase_health/checks/postgres engine/tests/unit/test_catalog.py
git commit -m "feat(checks): 27 Postgres checks incl. pg_sage-inspired rules"
```

---

### Task 15: Check catalog, part 3: API checks

**Files:**
- Create: 4 files under `engine/src/lakebase_health/checks/api/`
- Modify: `engine/tests/unit/test_catalog.py` (fill `API`)

- [ ] **Step 1: Set the expected ids**

```python
API = {"backup_restore_window", "default_branch_unprotected", "primary_without_ha", "password_roles"}
```

- [ ] **Step 2: Run the catalog test and verify it fails**

Run: `cd engine && .venv/bin/pytest tests/unit/test_catalog.py -q`
Expected: FAIL, `test_catalog_ids`

- [ ] **Step 3: Create the API check files**

`checks/api/backup_restore_window.yaml`:
```yaml
id: backup_restore_window
title: "Point-in-time restore window is short"
category: backup
source: api
function: backup_restore_window
direction: below
thresholds: {warning: 7, critical: 3}
explain: >
  The project's restore window in days (2-30, default 7). Production should keep at least 7 days; regulated
  workloads 14-30. Change it in project Settings > History window.
```

`checks/api/default_branch_unprotected.yaml`:
```yaml
id: default_branch_unprotected
title: "Default (production) branch is not protected"
category: backup
source: api
function: default_branch_unprotected
thresholds: {warning: 0}
explain: >
  The project's default branch can be reset or deleted. Protect it (update-branch spec.is_protected = true).
```

`checks/api/primary_without_ha.yaml`:
```yaml
id: primary_without_ha
title: "Primary compute has no high-availability secondary"
category: backup
source: api
function: primary_without_ha
thresholds: {info: 0}
explain: >
  The default branch's read-write compute runs without HA secondaries. For production, configure an HA group
  (min >= 2) so a failure fails over automatically.
```

`checks/api/password_roles.yaml`:
```yaml
id: password_roles
title: "Role uses a native Postgres password"
category: security
source: api
function: password_roles
thresholds: {warning: 0}
explain: >
  The role authenticates with a Postgres password instead of Databricks OAuth. Prefer OAuth roles, which leave no
  password to leak or rotate.
```

- [ ] **Step 4: Run the full unit suite**

Run: `cd engine && .venv/bin/pytest -q`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/checks/api engine/tests/unit/test_catalog.py
git commit -m "feat(checks): 4 API checks for backup, HA and auth posture"
```

---

### Task 16: Bundle, job definition and install README

**Files:**
- Create: `databricks.yml`, `resources/engine_job.yml`, `README.md`

- [ ] **Step 1: Write `databricks.yml`**

```yaml
bundle:
  name: lakebase-health

include:
  - resources/*.yml

variables:
  catalog:
    description: Catalog that will hold the lakebase_health schema (required)
  schema:
    description: Schema for derived tables
    default: lakebase_health
  schedule:
    description: Quartz cron for the engine (default hourly; ingest takes ~20 min, so keep >= 30 min)
    default: "0 0 * * * ?"
  engine_enabled:
    description: Set to "false" to stop all engine work
    default: "true"

artifacts:
  engine:
    type: whl
    path: ./engine
    build: python3 -m pip wheel . --no-deps -w dist

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production
```

- [ ] **Step 2: Write `resources/engine_job.yml`**

```yaml
resources:
  jobs:
    lakebase_health_engine:
      name: lakebase-health-engine
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: ${var.schedule}
        timezone_id: UTC
      tasks:
        - task_key: run
          environment_key: engine
          python_wheel_task:
            package_name: lakebase_health
            entry_point: main
            parameters: ["--catalog", "${var.catalog}", "--schema", "${var.schema}",
                         "--engine-enabled", "${var.engine_enabled}"]
      environments:
        - environment_key: engine
          spec:
            environment_version: "3"
            dependencies:
              - ./engine/dist/*.whl
              - psycopg[binary]>=3.1
              - databricks-sdk>=0.81.0
              - pyyaml>=6
```

- [ ] **Step 3: Validate the bundle**

Run: `databricks bundle validate --var catalog=my_catalog --profile my-profile`
Expected: `Validation OK!`. If `environment_version` is rejected by this CLI version, replace it with `client: "3"` and re-validate.

- [ ] **Step 4: Write `README.md` (install and grants)**

````markdown
# Lakebase Health

A read-only engine that checks the health of every Lakebase project in your account and proposes fixes.
It never changes a database.

## Install

1. Pick the identity the job runs as (`run_as` a service principal for production).
2. An **account admin and metastore admin** grants that identity read on the telemetry, and adds it to the
   PII group so query text and plans are visible:
   ```sql
   GRANT USE SCHEMA ON SCHEMA system.lakebase TO `<sp-application-id>`;
   GRANT SELECT ON SCHEMA system.lakebase TO `<sp-application-id>`;
   ```
   Add the SP to the account group `databricks_pii_access`.
3. Grant it `USE CATALOG` and `CREATE SCHEMA` on the target catalog.
4. Deploy: `databricks bundle deploy --var catalog=<catalog> -t prod`.

## Enroll a project for Postgres checks (optional, per project)

Telemetry and API checks cover every project automatically. For catalog checks (indexes, vacuum, sequences,
locks), create a Postgres role for the SP on the project's branch and grant monitoring roles:
```sql
GRANT pg_monitor, pg_read_all_stats TO "<sp-application-id>";
```
The engine only connects to computes that are already running; it never wakes a suspended compute.

## Opt a project out

Add the custom tag `lakebase_health = off` to the project.

## Results

Tables in `<catalog>.lakebase_health`: `findings` (current state), `proposed_actions`, `shadow_log`, `ledger`,
`check_results`, `inventory`, `runs`, `metrics_daily`, `raw_*` (telemetry copy, 90 days).
````

- [ ] **Step 5: Commit**

```bash
git add databricks.yml resources/engine_job.yml README.md
git commit -m "feat(bundle): DABs bundle, hourly serverless job, install README"
```

---

### Task 17: Deploy and run the acceptance tests on the `healthcheck` project

**Files:**
- Create: `engine/tests/integration/__init__.py`, `engine/tests/integration/acceptance.py`

**Interfaces:**
- Consumes: the deployed job `lakebase-health-engine`; `scripts/seed_test_workload.sql`, `scripts/workload.py`, `scripts/pg.sh`.
- Modify: `scripts/seed_test_workload.sql:1-9` (Step 2)

- [ ] **Step 1: Write `engine/tests/integration/acceptance.py`**

```python
"""Acceptance checks for the engine against the seeded healthcheck project (spec section 7).

Usage: engine/.venv/bin/python engine/tests/integration/acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555
Prerequisite: bundle deployed (`databricks bundle deploy --var catalog=my_catalog`), seed + workload run.
"""
import argparse
import json
import subprocess
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

EXPECTED = {  # planted problem -> check id(s); at least one must have an active finding on the project
    "duplicate index": ["duplicate_index"],
    "redundant prefix index": ["redundant_index"],
    "unused indexes": ["unused_index"],
    "invalid index": ["invalid_index"],
    "FK without index": ["missing_fk_index"],
    "seq scans dominate": ["seq_scan_heavy", "seq_scan_read_ratio", "top_db_time_query"],
    "int4 sequence at 75%": ["sequence_exhaustion"],
    "autovacuum disabled": ["autovacuum_disabled"],
    "dead tuples": ["dead_tuples"],
    "table without PK": ["table_without_pk"],
    "PUBLIC CREATE": ["public_schema_create"],
    "deadlocks": ["deadlocks"],
    "idle-in-tx blocking": ["lock_blocking_history", "blocked_session", "idle_in_transaction"],
    "disk spills": ["disk_spill"],
    "write back-pressure": ["write_backpressure"],
    "never analyzed (borrowed)": ["stale_statistics"],
    "sort vs LIMIT (borrowed)": ["sort_limit_waste"],
    "autovacuum blocker (borrowed)": ["autovacuum_blocker"],
}
MUST_NOT_FIND = ["wal_retention_slot", "wal_proposer_slot", "cloud_admin", "fsync", "full_page_writes", "sage."]


def q(w, wh, sql):
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wh, wait_timeout="50s",
                                                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE)
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        r = w.statement_execution.get_statement(r.statement_id)
    assert r.status.state == StatementState.SUCCEEDED, r.status.error
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []


def run_job(profile):
    t = time.time()
    subprocess.check_call(["databricks", "bundle", "run", "lakebase_health_engine", "--profile", profile])
    return time.time() - t


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid"):
        ap.add_argument(a, required=True)
    a = ap.parse_args()
    w = WorkspaceClient(profile=a.profile)
    t = f"{a.catalog}.lakebase_health"
    failures = []

    secs = run_job(a.profile)
    print(f"run 1: {secs / 60:.1f} min")
    if secs > 30 * 60:
        failures.append(f"criterion 2: run took {secs / 60:.1f} min (> 30)")

    active = q(w, a.warehouse, f"SELECT check_id, object FROM {t}.findings WHERE project_id = '{a.project_uid}' "
                               "AND status IN ('new', 'open')")
    ids = {r["check_id"] for r in active}
    for problem, checks in EXPECTED.items():
        ok = any(c in ids for c in checks)
        print(("PASS " if ok else "FAIL ") + f"{problem}: {checks}")
        if not ok:
            failures.append(f"criterion 1: missed {problem}")
    for bad in MUST_NOT_FIND:
        hits = [r for r in active if bad in (r["object"] or "")]
        if hits:
            failures.append(f"criterion 1: must-not-find {bad!r} in {hits[:3]}")

    errs = q(w, a.warehouse, f"SELECT check_id, endpoint_id, error FROM {t}.check_results WHERE run_id = "
                             f"(SELECT run_id FROM {t}.runs ORDER BY started_at DESC LIMIT 1) "
                             "AND status IN ('error', 'timeout')")
    for e in errs:
        failures.append(f"check error: {e['check_id']} on {e['endpoint_id']}: {(e['error'] or '')[:200]}")
    print(f"check errors in latest run: {len(errs)}")

    before = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.findings")[0]["n"]
    actions_before = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.proposed_actions WHERE state = 'proposed'")[0]["n"]
    run_job(a.profile)
    after = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.findings")[0]["n"]
    actions_after = q(w, a.warehouse, f"SELECT count(*) AS n FROM {t}.proposed_actions WHERE state = 'proposed'")[0]["n"]
    dupes = q(w, a.warehouse, f"SELECT fingerprint FROM {t}.findings GROUP BY fingerprint HAVING count(*) > 1")
    if dupes or int(after) < int(before) or int(actions_after) > int(actions_before) + 5:
        failures.append(f"criterion 3: findings {before}->{after}, proposed {actions_before}->{actions_after}, dupes {len(dupes)}")
    print(f"idempotency: findings {before}->{after}, proposed actions {actions_before}->{actions_after}")

    print("\n".join(["", "FAILURES:"] + failures) if failures else "\nALL AUTOMATED ACCEPTANCE CHECKS PASSED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Make the seed script safe to re-run**

`scripts/seed_test_workload.sql` currently starts with `DROP SCHEMA IF EXISTS hc_test CASCADE`. On the test database the extensions `pg_stat_statements` and `pgstattuple` live in `hc_test`, and their objects are owned by the platform (`must be owner`), so dropping the schema fails. Replace the first five lines (through `SET search_path = hc_test;`) with:

```sql
-- Seeds a test database with deliberate anti-patterns so every health check has something to find.
-- Safe to re-run: drops and recreates the test tables (not the schema, which may hold platform-owned extensions).
CREATE SCHEMA IF NOT EXISTS hc_test;
DROP TABLE IF EXISTS hc_test.orders, hc_test.customers, hc_test.audit_events CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pgstattuple SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_hint_plan;
SET search_path = hc_test;
```
(`CREATE EXTENSION IF NOT EXISTS` is a no-op where the extension already exists in another schema.)

- [ ] **Step 3: Confirm the deploying identity is enrolled on the test project**

The job runs as the deploying user in `dev` mode, who owns `healthcheck` and therefore has a Postgres role.
Run: `databricks postgres list-roles projects/healthcheck/branches/production --profile my-profile -o json`
Expected: a role whose `status.postgres_role` is `user@example.com`.

- [ ] **Step 4: Deploy and run once to backfill (before creating incident telemetry)**

The first run copies 7 days of all 10 telemetry tables and can take well over an hour (one table took 22 min). Incident checks look back 2 hours, so the workload must run **after** this backfill.

```bash
databricks bundle deploy --var catalog=my_catalog --profile my-profile
databricks bundle run lakebase_health_engine --profile my-profile
databricks experimental aitools tools query "SELECT status, step_seconds, errors FROM my_catalog.lakebase_health.runs ORDER BY started_at DESC LIMIT 1" --profile my-profile
```
Expected: the run status is `ok` or `ok_with_errors` with no ingest errors; `step_seconds.ingest` is recorded.

- [ ] **Step 5: Re-seed and generate recent incident telemetry**

```bash
cd <repo>
PROFILE=my-profile EP=projects/healthcheck/branches/production/endpoints/primary scripts/pg.sh -q -f scripts/seed_test_workload.sql
PROFILE=my-profile EP=projects/healthcheck/branches/production/endpoints/primary scripts/pg.sh -q -c "CREATE UNIQUE INDEX CONCURRENTLY orders_status_uq ON hc_test.orders(status)"
python3 scripts/workload.py --profile my-profile --endpoint projects/healthcheck/branches/production/endpoints/primary --minutes 12
```
Expected: the seed completes; the unique index fails with "is duplicated" (intended, leaving an invalid index); the workload prints `workload done`.

- [ ] **Step 6: Run the acceptance script (criteria 1, 2, 3)**

Run immediately after Step 5, so the deadlocks and blocking are inside the 2-hour incident window:
```bash
engine/.venv/bin/python engine/tests/integration/acceptance.py --profile my-profile --warehouse 0123456789abcdef \
  --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555
```
Expected: `ALL AUTOMATED ACCEPTANCE CHECKS PASSED`. These runs are incremental, so criterion 2 (< 30 min) applies to them.

- [ ] **Step 7: Criterion 5 (resolution): fix one planted problem and confirm it resolves within 2 runs**

```bash
PROFILE=my-profile EP=projects/healthcheck/branches/production/endpoints/primary scripts/pg.sh -q -c "DROP INDEX hc_test.customers_email_idx2"
databricks bundle run lakebase_health_engine --profile my-profile
databricks bundle run lakebase_health_engine --profile my-profile
databricks experimental aitools tools query "SELECT check_id, object, status FROM my_catalog.lakebase_health.findings WHERE check_id = 'duplicate_index'" --profile my-profile
```
Expected: the `duplicate_index` finding for `customers_email_idx2` has `status = resolved`; other findings are unchanged.

- [ ] **Step 8: Criterion 6 (no writes): the engine's sessions refuse writes**

```bash
cd engine && .venv/bin/python - <<'EOF'
import json, subprocess, psycopg
from lakebase_health.executors.postgres import SESSION_SETUP
cli = lambda *a: json.loads(subprocess.check_output(["databricks", *a, "--profile", "my-profile", "-o", "json"]))
EP = "projects/healthcheck/branches/production/endpoints/primary"
c = psycopg.connect(host=cli("postgres", "get-endpoint", EP)["status"]["hosts"]["host"], user=cli("current-user", "me")["userName"],
                    password=cli("postgres", "generate-database-credential", EP)["token"], dbname="databricks_postgres",
                    sslmode="require", autocommit=True)
for s in SESSION_SETUP: c.execute(s)
try:
    c.execute("CREATE TABLE hc_test.should_fail(x int)"); print("FAIL: write succeeded")
except psycopg.errors.ReadOnlySqlTransaction:
    print("PASS: session is read-only")
EOF
```
Expected: `PASS: session is read-only`

- [ ] **Step 9: Criterion 4 (no wake): a suspended compute stays suspended**

```bash
databricks postgres update-endpoint projects/healthcheck/branches/production/endpoints/primary spec.disabled \
  --json '{"spec": {"disabled": true}}' --profile my-profile
databricks postgres get-endpoint projects/healthcheck/branches/production/endpoints/primary --profile my-profile -o json | python3 -c "import json,sys; s=json.load(sys.stdin)['status']; print(s.get('current_state'))"
databricks api get /api/2.0/postgres/projects/healthcheck --profile my-profile | python3 -c "import json,sys; print(json.load(sys.stdin)['status']['compute_last_active_time'])"
databricks bundle run lakebase_health_engine --profile my-profile
databricks api get /api/2.0/postgres/projects/healthcheck --profile my-profile | python3 -c "import json,sys; print(json.load(sys.stdin)['status']['compute_last_active_time'])"
databricks experimental aitools tools query "SELECT DISTINCT status FROM my_catalog.lakebase_health.check_results WHERE run_id = (SELECT run_id FROM my_catalog.lakebase_health.runs ORDER BY started_at DESC LIMIT 1) AND endpoint_id = 'ep-sample-primary-a1b2c3d4'" --profile my-profile
databricks postgres update-endpoint projects/healthcheck/branches/production/endpoints/primary spec.disabled \
  --json '{"spec": {"disabled": false}}' --profile my-profile
```
Expected: the state is not `ACTIVE` before the run; `compute_last_active_time` is identical before and after the run; the endpoint's check statuses are only `skipped:compute_inactive`. The endpoint is re-enabled at the end.

- [ ] **Step 10: Commit**

```bash
git add engine/tests/integration
git commit -m "test(engine): acceptance tests against the seeded healthcheck project"
```
