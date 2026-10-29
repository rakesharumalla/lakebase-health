# Lakebase Health Fix Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a project owner or manager verify a proposed fix on a copy-on-write branch, approve it, have a separate fixer identity apply it, and get it confirmed or rolled back after the engine's next runs.

**Architecture:** The app records approval requests in an append-only `approvals` Delta table and triggers a `lakebase-health-fixer` Lakeflow job that runs as a dedicated fixer service principal. The job claims actions with compare-and-set state updates, verifies on a temporary branch using query samples from the telemetry copy, applies allowlisted DDL to the target branch, and writes `action_runs` and per-action hash-chained ledger events. The engine's hourly run performs the post-apply check.

**Tech Stack:** Python 3.11+ (`lakebase_health` package, psycopg 3, databricks-sdk ≥ 0.81, Spark SQL on serverless), pytest; app: AppKit/TypeScript, Vitest; Databricks Asset Bundles.

**Spec:** `docs/superpowers/specs/2026-10-28-lakebase-health-fix-loop-design.md` (engine: `2026-10-27-lakebase-health-engine-design.md`; app: `2026-10-28-lakebase-health-app-design.md`).

## Global Constraints

- Release-1 fix kinds only: `CREATE INDEX CONCURRENTLY` (non-unique), `DROP INDEX CONCURRENTLY [IF EXISTS]`, `ANALYZE <table>`, `ALTER TABLE <table> SET|RESET (autovacuum_… [, …])`; anything else → `blocked`.
- Every verify, apply and rollback requires an approval row; no unattended apply; L3 stays shadow-only.
- Approvers: project owner (`status.owner`) or users for whom `GET /api/2.0/permissions/database-projects/{uid}` succeeds with their token; checked on display and again on every click.
- The fixer executes only `proposed_actions.sql` / `rollback_sql`, after checking the approved SHA-256 equals the current SQL hash, the action is not expired and (for verify/apply) its finding is open.
- Fixer session: `lock_timeout = 5s`; `statement_timeout` 2 h for index builds, 10 min otherwise; `application_name = 'lakebase-health-fixer'`; autocommit; one action per project at a time.
- Verification branch: `lbh-verify-<action_id[:8]>`, TTL 4 h, endpoint 1–2 CU with scale to zero, deleted when done; verification never connects to the target compute.
- Pass rules: create index — ≥ 1 affected query's plan uses the index with median speed-up ≥ 1.2×, no affected query > 10 % slower, sampled write queries on the table ≤ 25 % slower; drop index — no affected plan uses it before, no affected query > 10 % slower after; `ANALYZE` always passes; autovacuum settings are not branch-verified.
- Post-apply: after 2 engine runs, `confirmed` if the finding is resolved/improving, affected queries' mean time ≤ 10 % worse than the 24 h before apply and write-wait share not higher; else `regressed` + critical finding `fix_regressed` + rollback card; too little traffic → `confirmed` (low confidence).
- Reports store query IDs and timings only, never query text or parameters.
- The engine SP stays read-only; the app SP gains only INSERT on `approvals` and `CAN_MANAGE_RUN` on the fixer job.
- Profile `my-profile`, catalog `my_catalog`, warehouse `0123456789abcdef`, test project `healthcheck` (uid `11111111-2222-4333-8444-555555555555`). Commit messages end with `Co-authored-by: Isaac <no-reply@databricks.com>`; never `--no-verify`.

## Decisions recorded in this plan

- **Ledger chains per action.** With two writers (engine, fixer) a single global chain forks when both read the same last hash. `chain()` links each event to the previous event of the *same action*; `verify()` checks each action's chain. Cost if wrong: a global ordering proof is lost (timestamps remain).
- **Engine must not reset fixer states.** `persist()` today loads only `proposed` actions, so a verified/applied action would be re-proposed under the same `action_id`. The engine will load all actions and leave any non-`proposed`, non-terminal state alone.
- **Write-penalty probe = sampled DML.** A generic synthetic insert is impossible without knowing each table's constraints, so the write rule uses the INSERT/UPDATE/DELETE statements sampled on that table in `raw_plan_history`; with none sampled the report says "write cost not measured".

## Review Focus

1. Approval races: two clicks (or two sweeps) for the same action must produce exactly one claim and one execution; the loser records nothing destructive.
2. Stale approvals: the SQL changed, the action expired, or the finding resolved between click and sweep — the approval must be consumed as `stale_approval`, never executed.
3. Interrupted apply: a job killed during `CREATE INDEX CONCURRENTLY` must be recovered on the next sweep (valid index → `applied`; invalid index → dropped, `apply_failed`) and never left `applying` forever.
4. Hostile SQL in `proposed_actions` (multiple statements, comments hiding a second statement, `DROP TABLE`, quoted identifiers with `;`) must be `blocked` by the allowlist.
5. Branch leaks: a verification that errors at any point must still delete its branch.

Each item has a test in the task that owns the code (Tasks 2, 3, 6, 7, 8).

## File Structure

```
engine/src/lakebase_health/
  ledger.py                 # modify: per-action chains
  record.py                 # modify: load all actions, keep fixer states; fill outcome_score
  ddl.py                    # modify: approvals, action_runs tables
  fixer/__init__.py
  fixer/allowlist.py        # parse & accept the four statement kinds
  fixer/states.py           # transition table, legal requests per state
  fixer/queue.py            # pending approvals, staleness, compare-and-set claim, state writes, ledger
  fixer/pg.py               # writable fixer sessions with guards; index catalog helpers
  fixer/branch.py           # create/delete verification branches
  fixer/samples.py          # affected queries + parameters from the telemetry copy
  fixer/verify.py           # measurements and pass rules -> report
  fixer/apply.py            # apply / rollback / recover
  fixer/postcheck.py        # confirmed | regressed rules
  jobs/fix.py               # fixer job entry point
engine/tests/unit/test_ledger_per_action.py, test_record_states.py, test_allowlist.py, test_states.py,
  test_queue.py, test_fixer_pg.py, test_branch.py, test_samples.py, test_verify.py, test_apply.py,
  test_postcheck.py, test_fix_job.py
resources/fixer_job.yml     # job, run_as fixer SP, 15-min schedule
app/server/writes/insert_approval.sql
app/server/lib/approvals.ts # approver check, request validation, insert, job trigger
app/server/tests/approvals.test.ts
app/client/src/views/ActionCard.tsx (+ test), Actions.tsx (+ test)
scripts/fixer_grants.sh, scripts/fix_acceptance.py
```

---

### Task 1: Engine prerequisites — per-action ledger, persist keeps fixer states, new tables

**Files:**
- Modify: `engine/src/lakebase_health/ledger.py`, `engine/src/lakebase_health/record.py:74-100`, `engine/src/lakebase_health/ddl.py`
- Test: `engine/tests/unit/test_ledger_per_action.py`, `engine/tests/unit/test_record_states.py`

**Interfaces:**
- Produces: `chain(last_by_action: dict[str, str | None], events, now, actor="engine") -> list[dict]`; `verify(rows) -> bool` (per action); `last_hashes(sql, cfg, action_ids) -> dict`; tables `approvals(approval_id, action_id, request, sql_hash, approver, requested_at, note)`, `action_runs(run_id, approval_id, action_id, kind, started_at, ended_at, outcome, report, branch, error)`.

- [ ] **Step 1: Write the failing tests**

```python
# engine/tests/unit/test_ledger_per_action.py
from datetime import datetime, timezone
from lakebase_health.ledger import chain, verify

NOW = datetime(2026, 10, 28, tzinfo=timezone.utc)


def test_events_link_to_the_previous_event_of_the_same_action():
    rows = chain({"a": "h_a", "b": None}, [
        {"action_id": "a", "event_type": "verify_requested", "payload": {}},
        {"action_id": "b", "event_type": "proposed", "payload": {}},
        {"action_id": "a", "event_type": "verified", "payload": {}},
    ], NOW, actor="fixer")
    assert rows[0]["prev_hash"] == "h_a" and rows[1]["prev_hash"] is None
    assert rows[2]["prev_hash"] == rows[0]["hash"]
    assert all(r["actor"] == "fixer" for r in rows)


def test_verify_checks_each_action_chain_and_detects_tampering():
    rows = chain({}, [{"action_id": "a", "event_type": "x", "payload": {"n": 1}},
                      {"action_id": "b", "event_type": "y", "payload": {}},
                      {"action_id": "a", "event_type": "z", "payload": {}}], NOW)
    assert verify(rows)
    rows[2]["payload"] = '{"n": 2}'
    assert not verify(rows)
```

```python
# engine/tests/unit/test_record_states.py
from datetime import datetime, timedelta, timezone
from lakebase_health.actions import plan_actions
from lakebase_health.models import Action

NOW = datetime(2026, 10, 28, tzinfo=timezone.utc)


def action(state):
    return Action("aid", "fp", "missing_fk_index", "ddl", "CREATE INDEX CONCURRENTLY i ON t (c);", "DROP INDEX CONCURRENTLY IF EXISTS i;",
                  "moderate", True, True, state, NOW, NOW, NOW + timedelta(days=7))


class F:
    fingerprint, check_id, status, candidate_sql, rollback_sql = "fp", "missing_fk_index", "open", "CREATE INDEX CONCURRENTLY i ON t (c);", None


class Fix:
    kind, risk, reversible, verify_on_branch, actionable = "ddl", "moderate", True, True, True


class Spec:
    fix = Fix()


def test_actions_in_fixer_states_are_never_re_proposed_or_expired():
    for state in ("verifying", "verified", "applying", "applied", "regressed", "rolling_back"):
        changed, events = plan_actions([F()], {"aid": action(state)}, {"missing_fk_index": Spec()}, NOW)
        assert changed == [] and events == [], state
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_ledger_per_action.py tests/unit/test_record_states.py -v`
Expected: FAIL — `chain()` takes a hash, not a dict; `plan_actions` re-proposes `aid` (changed non-empty).

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/ledger.py — replace chain() and verify()
def chain(last_by_action: dict, events: list, now, actor: str = "engine") -> list:
    """Each event links to the previous event of the same action (engine and fixer both write)."""
    last = dict(last_by_action)
    rows = []
    for e in events:
        payload = json.dumps(e["payload"], sort_keys=True, default=str)
        event_id = uuid.uuid4().hex
        prev = last.get(e["action_id"])
        h = _digest(prev, event_id, e["action_id"], e["event_type"], actor, payload, now)
        rows.append({"event_id": event_id, "action_id": e["action_id"], "event_type": e["event_type"], "actor": actor,
                     "payload": payload, "ts": now, "prev_hash": prev, "hash": h})
        last[e["action_id"]] = h
    return rows


def verify(rows: list) -> bool:
    by_action: dict = {}
    for r in rows:
        by_action.setdefault(r["action_id"], []).append(r)
    for chain_rows in by_action.values():
        prev = chain_rows[0]["prev_hash"]
        for r in chain_rows:
            if r["prev_hash"] != prev:
                return False
            if _digest(r["prev_hash"], r["event_id"], r["action_id"], r["event_type"], r["actor"], r["payload"], r["ts"]) != r["hash"]:
                return False
            prev = r["hash"]
    return True


def last_hashes(sql, cfg, action_ids) -> dict:
    if not action_ids:
        return {}
    ids = ",".join(f"'{a}'" for a in action_ids if all(c in "0123456789abcdef" for c in a))
    rows = sql.query(f"SELECT action_id, max_by(hash, ts) AS hash FROM {cfg.table('ledger')} WHERE action_id IN ({ids}) GROUP BY action_id")
    return {r["action_id"]: r["hash"] for r in rows}
```

In `actions.plan_actions`, the `existing` dict now contains every action; skip fingerprints that already have an action in a fixer state:
```python
FIXER_STATES = {"verifying", "verified", "verify_failed", "applying", "applied", "apply_failed", "regressed",
                "rolling_back", "rollback_failed", "blocked"}
# at the top of the loop over findings:
        if any(a.state in FIXER_STATES for a in by_fp.get(f.fingerprint, [])):
            continue
```
In `record.persist`, load all actions (`SELECT * FROM proposed_actions` without the state filter), and replace the ledger write with:
```python
        ids = sorted({e["action_id"] for e in events})
        sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, ids), events, now))
```
Add to `ddl.TABLE_DDL`:
```python
    "approvals": "approval_id STRING, action_id STRING, request STRING, sql_hash STRING, approver STRING, "
                 "requested_at TIMESTAMP, note STRING",
    "action_runs": "run_id STRING, approval_id STRING, action_id STRING, kind STRING, started_at TIMESTAMP, "
                   "ended_at TIMESTAMP, outcome STRING, report STRING, branch STRING, error STRING",
```
Update existing callers/tests of `chain(prev_hash, …)` (`test_actions_ledger.py`, `record.py`) to the dict form.

- [ ] **Step 4: Run the whole engine suite**

Run: `cd engine && .venv/bin/python -m pytest -q`
Expected: all pass (89 existing adjusted + new).

- [ ] **Step 5: Commit**

```bash
git add engine
git commit -m "feat(engine): per-action ledger chains, keep fixer states, approvals and action_runs tables

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: SQL allowlist

**Files:**
- Create: `engine/src/lakebase_health/fixer/__init__.py` (empty), `engine/src/lakebase_health/fixer/allowlist.py`
- Test: `engine/tests/unit/test_allowlist.py`

**Interfaces:**
- Produces: `@dataclass(frozen=True) class Statement: kind: str  # create_index|drop_index|analyze|reloptions; target: str  # schema-qualified object; table: str | None`; `parse(sql: str) -> Statement` (raises `NotAllowed(reason)`).

- [ ] **Step 1: Write the failing tests**

```python
# engine/tests/unit/test_allowlist.py
import pytest
from lakebase_health.fixer.allowlist import NotAllowed, parse

OK = [
    ("CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);", "create_index", "hc_test.orders"),
    ('CREATE INDEX CONCURRENTLY "ix" ON "hc_test"."orders" USING btree ("customer_id")', "create_index", '"hc_test"."orders"'),
    ("DROP INDEX CONCURRENTLY IF EXISTS hc_test.customers_email_idx2;", "drop_index", "hc_test.customers_email_idx2"),
    ("ANALYZE hc_test.orders;", "analyze", "hc_test.orders"),
    ("ALTER TABLE hc_test.orders SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.01);", "reloptions", "hc_test.orders"),
    ("ALTER TABLE hc_test.orders RESET (autovacuum_enabled);", "reloptions", "hc_test.orders"),
]
BAD = [
    "CREATE UNIQUE INDEX CONCURRENTLY ON t (c)",
    "CREATE INDEX ON t (c)",                                  # not concurrent
    "DROP INDEX hc_test.i",                                   # not concurrent
    "DROP TABLE hc_test.orders",
    "ANALYZE",                                                # whole database
    "ALTER TABLE t SET (fillfactor = 50)",                    # not autovacuum
    "ALTER TABLE t SET (autovacuum_enabled = false); DROP TABLE t",
    "CREATE INDEX CONCURRENTLY ON t (c) /* ; */; DELETE FROM t",
    "ANALYZE t -- \n; DROP TABLE t",
    'DROP INDEX CONCURRENTLY "a;b"',                          # quoted ; is fine in name but only one statement allowed
    "VACUUM FULL t",
]


@pytest.mark.parametrize("sql,kind,table", OK)
def test_allowed(sql, kind, table):
    s = parse(sql)
    assert s.kind == kind and (s.table or s.target) == table


@pytest.mark.parametrize("sql", BAD)
def test_rejected(sql):
    if sql == 'DROP INDEX CONCURRENTLY "a;b"':
        assert parse(sql).kind == "drop_index"
        return
    with pytest.raises(NotAllowed):
        parse(sql)
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_allowlist.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/fixer/allowlist.py
import re
from dataclasses import dataclass

IDENT = r'(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)'
QNAME = rf"{IDENT}(?:\.{IDENT})?"
_RULES = [
    ("create_index", re.compile(rf"^CREATE\s+INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:{IDENT}\s+)?ON\s+(?P<table>{QNAME})\s*(?:USING\s+\w+\s*)?\((?P<cols>[^;]*)\)(?:\s+WHERE\s+[^;]+)?$", re.I | re.S)),
    ("drop_index", re.compile(rf"^DROP\s+INDEX\s+CONCURRENTLY\s+(?:IF\s+EXISTS\s+)?(?P<target>{QNAME})$", re.I)),
    ("analyze", re.compile(rf"^ANALYZE\s+(?P<table>{QNAME})$", re.I)),
    ("reloptions", re.compile(rf"^ALTER\s+TABLE\s+(?P<table>{QNAME})\s+(?:SET|RESET)\s*\((?P<opts>[^;()]+)\)$", re.I)),
]


class NotAllowed(ValueError):
    pass


@dataclass(frozen=True)
class Statement:
    kind: str
    target: str
    table: str | None


def _strip(sql: str) -> str:
    """Rejects comments and multiple statements outside quoted identifiers; returns the single statement."""
    out, i, quoted = [], 0, False
    while i < len(sql):
        ch = sql[i]
        if quoted:
            out.append(ch)
            if ch == '"':
                if i + 1 < len(sql) and sql[i + 1] == '"':
                    out.append('"')
                    i += 1
                else:
                    quoted = False
        elif ch == '"':
            quoted = True
            out.append(ch)
        elif sql.startswith("--", i) or sql.startswith("/*", i) or ch == "'" or ch == "$":
            raise NotAllowed("comments, string literals and dollar quoting are not allowed")
        elif ch == ";":
            if sql[i + 1:].strip():
                raise NotAllowed("only one statement is allowed")
            break
        else:
            out.append(ch)
        i += 1
    if quoted:
        raise NotAllowed("unterminated identifier")
    return "".join(out).strip()


def parse(sql: str) -> Statement:
    stmt = _strip(sql or "")
    for kind, rx in _RULES:
        m = rx.match(stmt)
        if not m:
            continue
        if kind == "create_index" and re.search(r"\bUNIQUE\b", stmt[: m.start("table")], re.I):
            raise NotAllowed("unique indexes are not allowed")
        if kind == "reloptions":
            opts = [o.split("=")[0].strip().lower() for o in m.group("opts").split(",")]
            if not opts or any(not o.startswith("autovacuum_") for o in opts):
                raise NotAllowed("only autovacuum_* table settings are allowed")
        g = m.groupdict()
        table = g.get("table")
        return Statement(kind, g.get("target") or table, table)
    raise NotAllowed("statement is not one of the allowed fix kinds")
```
Note: the `WHERE` of a partial index can contain string literals; `_strip` rejects any `'`, so partial indexes with literals are blocked in release 1 (the engine does not propose them).

- [ ] **Step 4: Run to verify it passes**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_allowlist.py -q`
Expected: PASS (17 cases).

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer engine/tests/unit/test_allowlist.py
git commit -m "feat(fixer): allowlist for release-1 fix statements

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: Action states, approval queue and compare-and-set claims

**Files:**
- Create: `engine/src/lakebase_health/fixer/states.py`, `engine/src/lakebase_health/fixer/queue.py`
- Test: `engine/tests/unit/test_states.py`, `engine/tests/unit/test_queue.py`

**Interfaces:**
- Consumes: `chain`, `last_hashes` (Task 1); `parse`, `NotAllowed` (Task 2).
- Produces:
  - `states.REQUEST_FROM: dict[str, set[str]]` (request → states it is legal from), `states.START: dict[str, str]` (request → in-progress state), `states.can_request(request, state, verify_on_branch) -> bool`, `TERMINAL`.
  - `sql_hash(text: str) -> str` (sha256 hex).
  - `@dataclass class Work: approval: dict; action: dict; finding_status: str | None`
  - `pending(sql, cfg) -> list[Work]` (unconsumed approvals, oldest first, joined to the action and its finding).
  - `staleness(work, now) -> str | None` (`"sql_changed"|"expired"|"finding_resolved"|"illegal_state"|None`).
  - `claim(sql, cfg, action_id, from_state, to_state, now) -> bool`
  - `set_state(sql, cfg, action_id, to_state, now)`
  - `record_run(sql, cfg, row: dict)` and `events(sql, cfg, rows: list[dict], now)` (ledger, actor `"fixer"`).

- [ ] **Step 1: Write the failing tests**

```python
# engine/tests/unit/test_states.py
from lakebase_health.fixer.states import can_request


def test_legal_requests():
    assert can_request("verify", "proposed", True)
    assert can_request("verify", "verify_failed", True)
    assert can_request("apply", "verified", True)
    assert can_request("apply", "proposed", False)            # not branch-verifiable
    assert not can_request("apply", "proposed", True)         # must verify first
    assert can_request("apply", "apply_failed", True)
    assert can_request("rollback", "regressed", True) and can_request("rollback", "applied", True)
    assert can_request("reject", "verified", True)
    for bad in [("verify", "applied"), ("apply", "applying"), ("rollback", "proposed"), ("reject", "applied")]:
        assert not can_request(bad[0], bad[1], True), bad
```

```python
# engine/tests/unit/test_queue.py
from datetime import datetime, timedelta, timezone
from lakebase_health.config import EngineConfig
from lakebase_health.fixer.queue import Work, claim, sql_hash, staleness
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
SQL = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);"


def work(request="verify", state="proposed", approved_sql=SQL, current_sql=SQL, expires=NOW + timedelta(days=1), finding="open"):
    return Work({"approval_id": "ap1", "action_id": "a1", "request": request, "sql_hash": sql_hash(approved_sql)},
                {"action_id": "a1", "state": state, "sql": current_sql, "rollback_sql": "DROP INDEX CONCURRENTLY IF EXISTS x;",
                 "verify_on_branch": True, "expires_at": expires}, finding)


def test_fresh_approval_is_not_stale():
    assert staleness(work(), NOW) is None


def test_stale_reasons():
    assert staleness(work(current_sql=SQL.replace("customer_id", "status")), NOW) == "sql_changed"
    assert staleness(work(expires=NOW - timedelta(seconds=1)), NOW) == "expired"
    assert staleness(work(finding="resolved"), NOW) == "finding_resolved"
    assert staleness(work(request="apply", state="proposed"), NOW) == "illegal_state"


def test_rollback_hashes_the_rollback_sql_and_ignores_finding_state():
    w = work(request="rollback", state="regressed", approved_sql="DROP INDEX CONCURRENTLY IF EXISTS x;", finding="resolved")
    assert staleness(w, NOW) is None


def test_claim_is_compare_and_set():
    sql = FakeSql(responses=[(r"^UPDATE cat\.lakebase_health\.proposed_actions SET state = :to_state", [{"num_affected_rows": 1}])])
    assert claim(sql, CFG, "a1", "proposed", "verifying", NOW)
    stmt, args = sql.statements[-1]
    assert "WHERE action_id = :id AND state = :from_state" in stmt and args["from_state"] == "proposed"
    lost = FakeSql(responses=[(r"^UPDATE", [{"num_affected_rows": 0}])])
    assert not claim(lost, CFG, "a1", "proposed", "verifying", NOW)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_states.py tests/unit/test_queue.py -q`
Expected: FAIL — modules not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/fixer/states.py
REQUEST_FROM = {
    "verify": {"proposed", "verify_failed"},
    "apply": {"verified", "apply_failed"},
    "rollback": {"regressed", "applied", "confirmed", "rollback_failed"},
    "reject": {"proposed", "verified", "verify_failed", "apply_failed"},
}
START = {"verify": "verifying", "apply": "applying", "rollback": "rolling_back", "reject": "rejected"}
TERMINAL = {"rejected", "expired", "rolled_back", "confirmed", "blocked"}


def can_request(request: str, state: str, verify_on_branch: bool) -> bool:
    if request == "apply" and state == "proposed" and not verify_on_branch:
        return True
    return state in REQUEST_FROM.get(request, set())
```

```python
# engine/src/lakebase_health/fixer/queue.py
import hashlib
from dataclasses import dataclass
from datetime import timezone

from ..ledger import chain, last_hashes
from .states import can_request


def sql_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode()).hexdigest()


@dataclass
class Work:
    approval: dict
    action: dict
    finding_status: str | None


def _utc(dt):
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def pending(sql, cfg) -> list:
    rows = sql.query(f"""
        SELECT ap.approval_id, ap.action_id, ap.request, ap.sql_hash, ap.approver, ap.requested_at, ap.note,
               a.state, a.sql, a.rollback_sql, a.verify_on_branch, a.expires_at, a.fingerprint, a.check_id, a.kind,
               f.status AS finding_status, f.project_id, f.branch_id, f.endpoint_id, f.object, f.evidence
        FROM {cfg.table('approvals')} ap
        JOIN {cfg.table('proposed_actions')} a ON a.action_id = ap.action_id
        LEFT JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint
        WHERE ap.approval_id NOT IN (SELECT approval_id FROM {cfg.table('action_runs')} WHERE approval_id IS NOT NULL)
        ORDER BY ap.requested_at""")
    keys_ap = ("approval_id", "action_id", "request", "sql_hash", "approver", "requested_at", "note")
    return [Work({k: r[k] for k in keys_ap}, {k: v for k, v in r.items() if k not in keys_ap or k == "action_id"},
                 r.get("finding_status")) for r in rows]


def staleness(w: Work, now) -> str | None:
    a, ap = w.action, w.approval
    target = a.get("rollback_sql") if ap["request"] == "rollback" else a.get("sql")
    if ap["request"] != "reject" and ap["sql_hash"] != sql_hash(target):
        return "sql_changed"
    if ap["request"] in ("verify", "apply") and _utc(a.get("expires_at")) and _utc(a["expires_at"]) <= now:
        return "expired"
    if ap["request"] in ("verify", "apply") and w.finding_status not in ("new", "open"):
        return "finding_resolved"
    if not can_request(ap["request"], a.get("state"), bool(a.get("verify_on_branch"))):
        return "illegal_state"
    return None


def claim(sql, cfg, action_id, from_state, to_state, now) -> bool:
    out = sql.query(f"UPDATE {cfg.table('proposed_actions')} SET state = :to_state, updated_at = :now "
                    "WHERE action_id = :id AND state = :from_state",
                    {"to_state": to_state, "now": now, "id": action_id, "from_state": from_state})
    return bool(out) and int(out[0].get("num_affected_rows") or 0) == 1


def set_state(sql, cfg, action_id, to_state, now) -> None:
    sql.execute(f"UPDATE {cfg.table('proposed_actions')} SET state = :to_state, updated_at = :now WHERE action_id = :id",
                {"to_state": to_state, "now": now, "id": action_id})


def record_run(sql, cfg, row: dict) -> None:
    sql.insert_rows(cfg.table("action_runs"), [row])


def events(sql, cfg, rows: list, now) -> None:
    if rows:
        ids = sorted({e["action_id"] for e in rows})
        sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, ids), rows, now, actor="fixer"))
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_states.py tests/unit/test_queue.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer engine/tests/unit/test_states.py engine/tests/unit/test_queue.py
git commit -m "feat(fixer): action states, approval queue, stale checks and compare-and-set claims

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: Fixer Postgres sessions and index catalog helpers

**Files:**
- Create: `engine/src/lakebase_health/fixer/pg.py`
- Test: `engine/tests/unit/test_fixer_pg.py`

**Interfaces:**
- Produces: `FIXER_SETUP(kind) -> tuple[str, ...]`; `class FixerPg: __init__(w, user, connect=None); session(endpoint_name, host, database, kind) -> contextmanager[conn]`; `index_state(conn, qualified_index) -> "valid"|"invalid"|"absent"`; `created_index_name(conn, table, columns) -> str | None` (finds the index a `CREATE INDEX … ON t (cols)` produced, newest first).

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/unit/test_fixer_pg.py
from types import SimpleNamespace
from lakebase_health.fixer.pg import FIXER_SETUP, FixerPg, index_state


class Conn:
    def __init__(self, rows=None):
        self.ran, self.rows, self.closed = [], rows or [], False

    def execute(self, sql, params=None):
        self.ran.append(sql)
        return SimpleNamespace(fetchall=lambda: self.rows, fetchone=lambda: self.rows[0] if self.rows else None)

    def close(self):
        self.closed = True


def test_guards_per_kind_and_writable_session():
    assert "SET statement_timeout = '2h'" in FIXER_SETUP("create_index")
    assert "SET statement_timeout = '10min'" in FIXER_SETUP("analyze")
    for k in ("create_index", "analyze"):
        assert "SET lock_timeout = '5s'" in FIXER_SETUP(k)
        assert not any("read_only" in s for s in FIXER_SETUP(k))
    conn = Conn()
    w = SimpleNamespace(postgres=SimpleNamespace(generate_database_credential=lambda endpoint: SimpleNamespace(token="t")))
    seen = {}
    pg = FixerPg(w, "fixer-sp", connect=lambda **kw: (seen.update(kw), conn)[1])
    with pg.session("projects/p/branches/b/endpoints/primary", "h", "db", "drop_index"):
        pass
    assert seen["application_name"] == "lakebase-health-fixer" and seen["autocommit"] is True and conn.closed


def test_index_state():
    assert index_state(Conn([(True,)]), "s.i") == "valid"
    assert index_state(Conn([(False,)]), "s.i") == "invalid"
    assert index_state(Conn([]), "s.i") == "absent"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_fixer_pg.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/fixer/pg.py
from contextlib import contextmanager

from ..executors.postgres import SEARCH_PATH_SQL

LONG = {"create_index", "drop_index"}


def FIXER_SETUP(kind: str) -> tuple:
    return (SEARCH_PATH_SQL, "SET lock_timeout = '5s'",
            f"SET statement_timeout = '{'2h' if kind in LONG else '10min'}'")


class FixerPg:
    def __init__(self, w, user: str, connect=None):
        self.w, self.user = w, user
        if connect is None:
            import psycopg
            connect = psycopg.connect
        self.connect = connect

    @contextmanager
    def session(self, endpoint_name: str, host: str, database: str, kind: str):
        token = self.w.postgres.generate_database_credential(endpoint=endpoint_name).token
        conn = self.connect(host=host, user=self.user, password=token, dbname=database, sslmode="require",
                            autocommit=True, application_name="lakebase-health-fixer", connect_timeout=10)
        try:
            for stmt in FIXER_SETUP(kind):
                conn.execute(stmt)
            yield conn
        finally:
            conn.close()


def index_state(conn, qualified_index: str) -> str:
    row = conn.execute("SELECT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass(%s)",
                       (qualified_index,)).fetchone()
    if not row:
        return "absent"
    return "valid" if row[0] else "invalid"


def created_index_name(conn, table: str, columns: str) -> str | None:
    rows = conn.execute(
        "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE i.indrelid = to_regclass(%s) AND pg_get_indexdef(i.indexrelid) ILIKE %s "
        "ORDER BY c.oid DESC LIMIT 1", (table, f"%({columns.strip()})%")).fetchall()
    return rows[0][0] if rows else None
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_fixer_pg.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/pg.py engine/tests/unit/test_fixer_pg.py
git commit -m "feat(fixer): guarded writable sessions and index catalog helpers

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: Verification branches

**Files:**
- Create: `engine/src/lakebase_health/fixer/branch.py`
- Test: `engine/tests/unit/test_branch.py`

**Interfaces:**
- Produces: `branch_name(action_id) -> str` (`lbh-verify-<8>`); `@contextmanager verification_branch(w, project, source_branch, action_id) -> yields {"branch": name, "endpoint": endpoint_name, "host": host}`; always deletes the branch on exit, including on exceptions.

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/unit/test_branch.py
from types import SimpleNamespace
import pytest
from lakebase_health.fixer.branch import branch_name, verification_branch


class Op:
    def __init__(self, value=None):
        self.value = value

    def wait(self, timeout=None):
        return self.value


class FakePostgres:
    def __init__(self):
        self.created, self.deleted, self.endpoints = [], [], []

    def create_branch(self, parent, branch, branch_id):
        self.created.append((parent, branch_id, branch.spec.ttl, branch.spec.source_branch))
        return Op()

    def create_endpoint(self, parent, endpoint, endpoint_id):
        self.endpoints.append((parent, endpoint.spec.autoscaling_limit_min_cu, endpoint.spec.autoscaling_limit_max_cu))
        return Op(SimpleNamespace(name=f"{parent}/endpoints/{endpoint_id}", status=SimpleNamespace(hosts=SimpleNamespace(host="h"))))

    def delete_branch(self, name):
        self.deleted.append(name)
        return Op()


def test_branch_is_created_with_ttl_and_small_compute_and_always_deleted():
    pg = FakePostgres()
    w = SimpleNamespace(postgres=pg)
    with pytest.raises(RuntimeError):
        with verification_branch(w, "projects/hc", "projects/hc/branches/production", "abcdef0123456789") as b:
            assert b["host"] == "h" and b["branch"].endswith("/branches/lbh-verify-abcdef01")
            raise RuntimeError("verification blew up")
    assert pg.created[0][1] == "lbh-verify-abcdef01" and pg.created[0][2] is not None
    assert pg.endpoints[0][1:] == (1.0, 2.0)
    assert pg.deleted == ["projects/hc/branches/lbh-verify-abcdef01"]
    assert branch_name("abcdef0123456789") == "lbh-verify-abcdef01"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_branch.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/fixer/branch.py
from contextlib import contextmanager
from datetime import timedelta

from databricks.sdk.service.postgres import Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType

TTL = timedelta(hours=4)


def branch_name(action_id: str) -> str:
    return f"lbh-verify-{action_id[:8]}"


@contextmanager
def verification_branch(w, project: str, source_branch: str, action_id: str):
    bid = branch_name(action_id)
    name = f"{project}/branches/{bid}"
    w.postgres.create_branch(parent=project, branch=Branch(spec=BranchSpec(source_branch=source_branch, ttl=TTL)),
                             branch_id=bid).wait()
    try:
        ep = w.postgres.create_endpoint(
            parent=name, endpoint_id="primary",
            endpoint=Endpoint(spec=EndpointSpec(endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                                                autoscaling_limit_min_cu=1.0, autoscaling_limit_max_cu=2.0))).wait()
        yield {"branch": name, "endpoint": ep.name, "host": ep.status.hosts.host}
    finally:
        w.postgres.delete_branch(name=name).wait()
```
Confirm the field types once against the installed SDK: `cd engine && .venv/bin/python -c "from databricks.sdk.service.postgres import BranchSpec, EndpointType; import inspect; print(BranchSpec.__dataclass_fields__['ttl'].type, list(EndpointType))"`. If `ttl` is typed as a `Duration`/string rather than `timedelta`, pass the matching type and ledger it. If branch creation already provisions a `primary` endpoint (check the live call in Task 11), use `get_endpoint` instead of `create_endpoint` and ledger it.

- [ ] **Step 4: Run to verify it passes**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_branch.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/branch.py engine/tests/unit/test_branch.py
git commit -m "feat(fixer): verification branches that are always deleted

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: Affected queries, measurements and pass rules

**Files:**
- Create: `engine/src/lakebase_health/fixer/samples.py`, `engine/src/lakebase_health/fixer/verify.py`
- Test: `engine/tests/unit/test_samples.py`, `engine/tests/unit/test_verify.py`

**Interfaces:**
- Consumes: `Statement` (Task 2).
- Produces:
  - `parse_params(text: str | None) -> list[str] | None` (`"$1 = 'a', $2 = 'b''c'"` → `["a", "b'c"]`; `None` for unparseable).
  - `affected_queries(sql, cfg, project_id, table: str, limit=10, per_query=3) -> list[{"queryid", "query", "params": list[list[str]], "is_write": bool}]` (from `raw_plan_history`, last 24 h, ranked by total `duration_ms`, non-truncated samples).
  - `measure(conn, q, params, repeats=3) -> {"ms": float, "nodes": list[str], "indexes": list[str]}` (median of warm runs, inside a rolled-back transaction).
  - `judge(kind, target_index, before: dict, after: dict) -> {"passed": bool, "rules": list[{"rule", "passed", "detail"}]}` where `before/after` map `queryid` → measure result plus `is_write`.

- [ ] **Step 1: Write the failing tests**

```python
# engine/tests/unit/test_samples.py
from lakebase_health.fixer.samples import parse_params


def test_parse_params():
    assert parse_params("$1 = '110448'") == ["110448"]
    assert parse_params("$1 = 'user36', $2 = 'it''s', $3 = 'a, b'") == ["user36", "it's", "a, b"]
    assert parse_params("") == [] and parse_params(None) == []
    assert parse_params("$1 = NULL") == [None]
    assert parse_params("garbage") is None
```

```python
# engine/tests/unit/test_verify.py
from lakebase_health.fixer.verify import judge

Q = lambda ms, idx=(), write=False: {"ms": ms, "nodes": [], "indexes": list(idx), "is_write": write}


def test_create_index_passes_with_speedup_and_bounded_write_cost():
    before = {"1": Q(100), "2": Q(10), "3": Q(5, write=True)}
    after = {"1": Q(20, ["hc_test.ix"]), "2": Q(10.5), "3": Q(6, write=True)}
    r = judge("create_index", "hc_test.ix", before, after)
    assert r["passed"], r


def test_create_index_fails_when_unused_or_a_query_regresses_or_writes_get_expensive():
    assert not judge("create_index", "hc_test.ix", {"1": Q(100)}, {"1": Q(95)})["passed"]                       # not used
    assert not judge("create_index", "hc_test.ix", {"1": Q(100), "2": Q(10)}, {"1": Q(20, ["hc_test.ix"]), "2": Q(12)})["passed"]
    assert not judge("create_index", "hc_test.ix", {"1": Q(100), "3": Q(5, write=True)},
                     {"1": Q(20, ["hc_test.ix"]), "3": Q(7, write=True)})["passed"]


def test_drop_index_rules():
    assert judge("drop_index", "hc_test.dup", {"1": Q(10, ["hc_test.other"])}, {"1": Q(10.4)})["passed"]
    assert not judge("drop_index", "hc_test.dup", {"1": Q(10, ["hc_test.dup"])}, {"1": Q(10)})["passed"]
    assert not judge("drop_index", "hc_test.dup", {"1": Q(10)}, {"1": Q(12)})["passed"]


def test_analyze_always_passes_and_no_queries_is_reported():
    assert judge("analyze", None, {}, {})["passed"]
    r = judge("create_index", "hc_test.ix", {}, {})
    assert not r["passed"] and any(x["rule"] == "affected_queries" for x in r["rules"])
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_samples.py tests/unit/test_verify.py -q`
Expected: FAIL — modules not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/fixer/samples.py
import re

_PARAM = re.compile(r"\s*\$(\d+)\s*=\s*(?:'((?:[^']|'')*)'|(NULL))\s*(?:,|$)")


def parse_params(text):
    if not text:
        return []
    out, pos = [], 0
    while pos < len(text):
        m = _PARAM.match(text, pos)
        if not m:
            return None
        out.append(None if m.group(3) else m.group(2).replace("''", "'"))
        pos = m.end()
    return out


def _table_regex(table: str) -> str:
    parts = [p.strip('"') for p in table.split(".")]
    name = re.escape(parts[-1])
    return rf"(?i)\b{name}\b"


def affected_queries(sql, cfg, project_id, table, limit=10, per_query=3) -> list:
    rows = sql.query(f"""
        WITH s AS (
          SELECT queryid, query_text, parameters, duration_ms,
                 row_number() OVER (PARTITION BY queryid ORDER BY ts DESC) AS rn
          FROM {cfg.raw('plan_history')}
          WHERE project_id = :p AND ts >= current_timestamp() - INTERVAL 24 HOURS
            AND NOT coalesce(truncated, false) AND query_text RLIKE :rx
        ), ranked AS (
          SELECT queryid, sum(duration_ms) AS total_ms FROM s GROUP BY queryid ORDER BY total_ms DESC LIMIT {int(limit)}
        )
        SELECT cast(s.queryid AS STRING) AS queryid, any_value(s.query_text) AS query,
               collect_list(s.parameters) AS params, max(r.total_ms) AS total_ms
        FROM s JOIN ranked r ON r.queryid = s.queryid
        WHERE s.rn <= {int(per_query)}
        GROUP BY s.queryid ORDER BY total_ms DESC""", {"p": project_id, "rx": _table_regex(table)})
    out = []
    for r in rows:
        params = [p for p in (parse_params(x) for x in (r["params"] or [])) if p is not None] or [[]]
        q = (r["query"] or "").strip().rstrip(";")
        out.append({"queryid": r["queryid"], "query": q, "params": params,
                    "is_write": bool(re.match(r"(?is)^\s*(insert|update|delete)\b", q))})
    return out
```

```python
# engine/src/lakebase_health/fixer/verify.py
import statistics

SPEEDUP = 1.2
SLOWDOWN = 1.10
WRITE_SLOWDOWN = 1.25


def _walk(node):
    yield node
    for ch in node.get("Plans", []):
        yield from _walk(ch)


def measure(conn, q, params, repeats=3) -> dict:
    """Median warm execution of one sampled statement; writes are rolled back."""
    from psycopg import sql as pgsql
    times, nodes, indexes = [], set(), set()
    for values in params:
        args = ", ".join(pgsql.Literal(v).as_string(conn) for v in values)
        conn.execute("BEGIN")
        try:
            conn.execute(f"PREPARE lbh_q AS {q}")
            execute = f"EXECUTE lbh_q({args})" if values else "EXECUTE lbh_q"
            conn.execute(execute if q.lower().startswith(("select", "with")) else f"EXPLAIN ANALYZE {execute}")  # warm-up
            for _ in range(repeats):
                plan = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {execute}").fetchone()[0][0]
                times.append(plan["Execution Time"])
                for n in _walk(plan["Plan"]):
                    nodes.add(n["Node Type"])
                    if n.get("Index Name"):
                        indexes.add(n["Index Name"])
        finally:
            conn.execute("ROLLBACK")
            conn.execute("DEALLOCATE ALL")  # PREPARE is not transactional; clear it before the next sample
    return {"ms": statistics.median(times) if times else None, "nodes": sorted(nodes), "indexes": sorted(indexes)}


def _uses(m, index):
    short = (index or "").split(".")[-1].strip('"')
    return any(i.strip('"') == short for i in m.get("indexes", []))


def judge(kind, target_index, before: dict, after: dict) -> dict:
    rules = []
    if kind in ("analyze", "reloptions"):
        return {"passed": True, "rules": [{"rule": "kind", "passed": True, "detail": f"{kind}: no branch pass rule"}]}
    common = [k for k in before if k in after and before[k].get("ms") and after[k].get("ms")]
    if not common:
        return {"passed": False, "rules": [{"rule": "affected_queries", "passed": False,
                                            "detail": "no sampled queries on this table could be measured"}]}
    reads = [k for k in common if not before[k].get("is_write")]
    writes = [k for k in common if before[k].get("is_write")]
    worst = max((after[k]["ms"] / before[k]["ms"] for k in reads), default=1.0)
    rules.append({"rule": "no_read_regression", "passed": worst <= SLOWDOWN, "detail": f"worst ratio {worst:.2f}"})
    if kind == "create_index":
        users = [k for k in reads if _uses(after[k], target_index)]
        best = max((before[k]["ms"] / after[k]["ms"] for k in users), default=0.0)
        rules.append({"rule": "index_used_with_speedup", "passed": bool(users) and best >= SPEEDUP,
                      "detail": f"{len(users)} queries use it; best speed-up {best:.2f}x"})
        wworst = max((after[k]["ms"] / before[k]["ms"] for k in writes), default=None)
        rules.append({"rule": "write_cost", "passed": wworst is None or wworst <= WRITE_SLOWDOWN,
                      "detail": "write cost not measured" if wworst is None else f"worst write ratio {wworst:.2f}"})
    if kind == "drop_index":
        used = [k for k in common if _uses(before[k], target_index)]
        rules.append({"rule": "index_unused_before", "passed": not used, "detail": f"{len(used)} plans used it"})
    return {"passed": all(r["passed"] for r in rules), "rules": rules}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_samples.py tests/unit/test_verify.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer engine/tests/unit/test_samples.py engine/tests/unit/test_verify.py
git commit -m "feat(fixer): affected-query sampling, branch measurements and pass rules

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: Apply, rollback and recovery

**Files:**
- Create: `engine/src/lakebase_health/fixer/apply.py`
- Test: `engine/tests/unit/test_apply.py`

**Interfaces:**
- Consumes: `parse` (Task 2), `index_state`, `created_index_name` (Task 4).
- Produces: `execute(conn, stmt_sql: str) -> {"outcome": "applied"|"apply_failed", "error": str|None, "index": str|None}` (for create: on error, drops any invalid index it produced); `recover(conn, stmt_sql) -> "applied"|"apply_failed"` (for actions stuck in `applying`).

- [ ] **Step 1: Write the failing tests**

```python
# engine/tests/unit/test_apply.py
from types import SimpleNamespace
from lakebase_health.fixer import apply as ap

CREATE = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);"


class Conn:
    def __init__(self, fail_on=None):
        self.ran, self.fail_on = [], fail_on

    def execute(self, sql, params=None):
        self.ran.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("canceling statement due to lock timeout")
        return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])


def test_successful_apply(monkeypatch):
    monkeypatch.setattr(ap, "created_index_name", lambda c, t, cols: "hc_test.orders_customer_id_idx")
    monkeypatch.setattr(ap, "index_state", lambda c, i: "valid")
    r = ap.execute(Conn(), CREATE)
    assert r == {"outcome": "applied", "error": None, "index": "hc_test.orders_customer_id_idx"}


def test_failed_create_drops_the_invalid_index(monkeypatch):
    monkeypatch.setattr(ap, "created_index_name", lambda c, t, cols: "hc_test.orders_customer_id_idx")
    monkeypatch.setattr(ap, "index_state", lambda c, i: "invalid")
    conn = Conn(fail_on="CREATE INDEX")
    r = ap.execute(conn, CREATE)
    assert r["outcome"] == "apply_failed" and "lock timeout" in r["error"]
    assert conn.ran[-1] == "DROP INDEX CONCURRENTLY IF EXISTS hc_test.orders_customer_id_idx"


def test_recover_interrupted_apply(monkeypatch):
    monkeypatch.setattr(ap, "created_index_name", lambda c, t, cols: "hc_test.ix")
    monkeypatch.setattr(ap, "index_state", lambda c, i: "valid")
    assert ap.recover(Conn(), CREATE) == "applied"
    monkeypatch.setattr(ap, "index_state", lambda c, i: "invalid")
    conn = Conn()
    assert ap.recover(conn, CREATE) == "apply_failed" and conn.ran[-1].startswith("DROP INDEX CONCURRENTLY IF EXISTS")
    monkeypatch.setattr(ap, "index_state", lambda c, i: "absent")
    assert ap.recover(Conn(), "DROP INDEX CONCURRENTLY IF EXISTS hc_test.dup") == "applied"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_apply.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/fixer/apply.py
import re

from .allowlist import parse
from .pg import created_index_name, index_state


def _cols(sql: str) -> str:
    m = re.search(r"\(([^()]*)\)", sql)
    return m.group(1) if m else ""


def execute(conn, stmt_sql: str) -> dict:
    st = parse(stmt_sql)
    try:
        conn.execute(stmt_sql.strip().rstrip(";"))
    except Exception as e:  # leave nothing half-built
        err = f"{type(e).__name__}: {e}"[:1000]
        if st.kind == "create_index":
            ix = created_index_name(conn, st.table, _cols(stmt_sql))
            if ix and index_state(conn, ix) == "invalid":
                conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {ix}")
        return {"outcome": "apply_failed", "error": err, "index": None}
    ix = created_index_name(conn, st.table, _cols(stmt_sql)) if st.kind == "create_index" else None
    if ix and index_state(conn, ix) != "valid":
        conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {ix}")
        return {"outcome": "apply_failed", "error": "index was built but is not valid", "index": None}
    return {"outcome": "applied", "error": None, "index": ix}


def recover(conn, stmt_sql: str) -> str:
    st = parse(stmt_sql)
    if st.kind == "create_index":
        ix = created_index_name(conn, st.table, _cols(stmt_sql))
        state = index_state(conn, ix) if ix else "absent"
        if state == "valid":
            return "applied"
        if state == "invalid":
            conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {ix}")
        return "apply_failed"
    if st.kind == "drop_index":
        return "applied" if index_state(conn, st.target) == "absent" else "apply_failed"
    return "apply_failed"  # ANALYZE / reloptions are short; re-request instead of guessing
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_apply.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/apply.py engine/tests/unit/test_apply.py
git commit -m "feat(fixer): apply with cleanup of invalid indexes, recovery of interrupted applies

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: Fixer job orchestration

**Files:**
- Create: `engine/src/lakebase_health/jobs/fix.py`
- Modify: `engine/pyproject.toml` (`[project.scripts] fix = "lakebase_health.jobs.fix:main"`)
- Test: `engine/tests/unit/test_fix_job.py`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: `run_fixer(sql, w, cfg, user, deps=None, now=None) -> dict` (counts per outcome) where `deps` overrides `{"pg", "branch", "samples", "measure", "judge", "execute", "recover", "target"}` for tests; `main(argv)`.
- Behaviour per pending approval (oldest first): stale → `action_runs.outcome = "stale:<reason>"` + `stale_approval` event; `reject` → claim to `rejected`; `verify` → claim `verifying`, allowlist (`blocked` on `NotAllowed`), measure on branch before/after applying the SQL on the branch, `verified|verify_failed`; `apply`/`rollback` → claim, run on the target branch's read-write endpoint, `applied|apply_failed` / `rolled_back|rollback_failed`. Also: actions in `applying`/`rolling_back` with `updated_at` older than the statement timeout → `recover`. One action per project per sweep.

- [ ] **Step 1: Write the failing tests**

```python
# engine/tests/unit/test_fix_job.py
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from lakebase_health.config import EngineConfig
from lakebase_health.fixer.queue import Work, sql_hash
from lakebase_health.jobs import fix

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
SQL = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);"


def work(request="verify", state="proposed", sql=SQL, project="p1", aid="a1", apid="ap1"):
    return Work({"approval_id": apid, "action_id": aid, "request": request, "sql_hash": sql_hash(sql), "approver": "owner@x"},
                {"action_id": aid, "state": state, "sql": sql, "rollback_sql": "DROP INDEX CONCURRENTLY IF EXISTS hc_test.ix;",
                 "verify_on_branch": True, "expires_at": NOW + timedelta(days=1), "project_id": project,
                 "branch_id": "br1", "kind": "ddl", "check_id": "missing_fk_index"}, "open")


class Recorder:
    def __init__(self, items, claim_ok=True):
        self.items, self.claim_ok, self.states, self.runs, self.events = items, claim_ok, [], [], []


def deps(rec, judged=True, branch_error=None):
    @contextmanager
    def branch(w, project, source, aid):
        if branch_error:
            raise branch_error
        yield {"branch": "b", "endpoint": "e", "host": "h"}

    @contextmanager
    def session(endpoint, host, db, kind):
        yield object()

    return {
        "pending": lambda sql, cfg: rec.items,
        "stuck": lambda sql, cfg, now: [],
        "claim": lambda sql, cfg, aid, f, t, now: rec.claim_ok and (rec.states.append((aid, f, t)) or True),
        "set_state": lambda sql, cfg, aid, t, now: rec.states.append((aid, None, t)),
        "record_run": lambda sql, cfg, row: rec.runs.append(row),
        "events": lambda sql, cfg, rows, now: rec.events.extend(rows),
        "branch": branch, "session": session,
        "target": lambda w, w_action: {"project": "projects/x", "branch": "projects/x/branches/production",
                                       "endpoint": "projects/x/branches/production/endpoints/primary", "host": "h", "database": "db"},
        "samples": lambda sql, cfg, project_id, table: [{"queryid": "1", "query": "SELECT 1", "params": [[]], "is_write": False}],
        "measure": lambda conn, q, params: {"ms": 10.0, "nodes": [], "indexes": []},
        "judge": lambda kind, ix, before, after: {"passed": judged, "rules": []},
        "execute": lambda conn, s: {"outcome": "applied", "error": None, "index": "hc_test.ix"},
        "recover": lambda conn, s: "applied",
    }


def test_verify_success_records_report_and_events():
    rec = Recorder([work()])
    out = fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert ("a1", "proposed", "verifying") in rec.states and ("a1", None, "verified") in rec.states
    assert rec.runs[0]["outcome"] == "verified" and rec.runs[0]["approval_id"] == "ap1"
    assert [e["event_type"] for e in rec.events] == ["verify_started", "verified"]
    assert out["verified"] == 1


def test_lost_claim_does_nothing_destructive():
    rec = Recorder([work()], claim_ok=False)
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.runs[0]["outcome"] == "skipped:claim_lost" and not any(t == "verified" for _, _, t in rec.states)


def test_stale_approval_is_consumed_not_executed():
    w = work()
    w.action["sql"] = SQL.replace("customer_id", "status")
    rec = Recorder([w])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert rec.runs[0]["outcome"] == "stale:sql_changed" and rec.states == []
    assert rec.events[0]["event_type"] == "stale_approval"


def test_disallowed_sql_is_blocked():
    rec = Recorder([work(sql="DROP TABLE hc_test.orders")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert ("a1", None, "blocked") in rec.states and rec.runs[0]["outcome"] == "blocked"


def test_branch_failure_marks_verify_failed():
    rec = Recorder([work()])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec, branch_error=RuntimeError("branch limit")), now=NOW)
    assert ("a1", None, "verify_failed") in rec.states and "branch limit" in rec.runs[0]["error"]


def test_one_action_per_project_per_sweep():
    rec = Recorder([work(aid="a1", apid="ap1"), work(aid="a2", apid="ap2")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert [r["approval_id"] for r in rec.runs] == ["ap1"]


def test_apply_runs_on_target():
    rec = Recorder([work(request="apply", state="verified")])
    fix.run_fixer(None, None, CFG, "fixer", deps=deps(rec), now=NOW)
    assert ("a1", "verified", "applying") in rec.states and ("a1", None, "applied") in rec.states
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_fix_job.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/jobs/fix.py
import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone

from ..config import EngineConfig
from ..fixer import apply as applymod
from ..fixer import branch as branchmod
from ..fixer import queue, samples, verify
from ..fixer.allowlist import NotAllowed, parse
from ..fixer.pg import FixerPg

STUCK_AFTER = {"applying": timedelta(hours=2, minutes=15), "rolling_back": timedelta(hours=2, minutes=15)}


def _stuck(sql, cfg, now):
    rows = sql.query(f"SELECT a.*, f.project_id, f.branch_id FROM {cfg.table('proposed_actions')} a "
                     f"LEFT JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint "
                     "WHERE a.state IN ('applying', 'rolling_back')")
    return [r for r in rows if now - r["updated_at"].replace(tzinfo=timezone.utc) > STUCK_AFTER[r["state"]]]


def _target(w, action):
    proj = next(p for p in w.postgres.list_projects() if p.uid == action["project_id"])
    branch = next(b for b in w.postgres.list_branches(parent=proj.name) if b.uid == action["branch_id"])
    ep = next(e for e in w.postgres.list_endpoints(parent=branch.name) if str(e.status.endpoint_type).endswith("READ_WRITE"))
    db = next(iter(w.postgres.list_databases(parent=branch.name))).status.postgres_database
    return {"project": proj.name, "branch": branch.name, "endpoint": ep.name, "host": ep.status.hosts.host, "database": db}


def default_deps(sql, w, user):
    pg = FixerPg(w, user)
    return {"pending": queue.pending, "stuck": _stuck, "claim": queue.claim, "set_state": queue.set_state,
            "record_run": queue.record_run, "events": queue.events, "branch": branchmod.verification_branch,
            "session": pg.session, "target": _target, "samples": samples.affected_queries, "measure": verify.measure,
            "judge": verify.judge, "execute": applymod.execute, "recover": applymod.recover}


def _run_row(work, kind, now):
    return {"run_id": uuid.uuid4().hex, "approval_id": work.approval["approval_id"], "action_id": work.action["action_id"],
            "kind": kind, "started_at": now, "ended_at": None, "outcome": None, "report": None, "branch": None, "error": None}


def _verify(d, sql, w, cfg, work, st, tgt):
    a = work.action
    queries = d["samples"](sql, cfg, a["project_id"], st.table or st.target)
    report = {"queries": [], "rules": []}
    with d["branch"](w, tgt["project"], tgt["branch"], a["action_id"]) as b:
        report["branch"] = b["branch"]
        with d["session"](b["endpoint"], b["host"], tgt["database"], st.kind) as conn:
            before = {q["queryid"]: {**d["measure"](conn, q["query"], q["params"]), "is_write": q["is_write"]} for q in queries}
            res = d["execute"](conn, a["sql"])
            if res["outcome"] != "applied":
                raise RuntimeError(f"fix failed on the branch: {res['error']}")
            after = {q["queryid"]: {**d["measure"](conn, q["query"], q["params"]), "is_write": q["is_write"]} for q in queries}
    target_index = res.get("index") or st.target
    verdict = d["judge"](st.kind, target_index, before, after)
    report["rules"] = verdict["rules"]
    report["queries"] = [{"queryid": k, "before_ms": before[k].get("ms"), "after_ms": after.get(k, {}).get("ms"),
                          "indexes_after": after.get(k, {}).get("indexes", [])} for k in before]
    return verdict["passed"], report


def _handle(d, sql, w, cfg, work, now, counts):
    a, ap = work.action, work.approval
    run = _run_row(work, ap["request"], now)
    stale = queue.staleness(work, now)
    if stale:
        run.update(outcome=f"stale:{stale}", ended_at=now)
        d["record_run"](sql, cfg, run)
        d["events"](sql, cfg, [{"action_id": a["action_id"], "event_type": "stale_approval",
                                "payload": {"approval_id": ap["approval_id"], "reason": stale}}], now)
        counts["stale"] = counts.get("stale", 0) + 1
        return
    start = {"verify": "verifying", "apply": "applying", "rollback": "rolling_back", "reject": "rejected"}[ap["request"]]
    if not d["claim"](sql, cfg, a["action_id"], a["state"], start, now):
        run.update(outcome="skipped:claim_lost", ended_at=now)
        d["record_run"](sql, cfg, run)
        return
    events = [{"action_id": a["action_id"], "event_type": f"{ap['request']}_started" if ap["request"] != "reject" else "rejected",
               "payload": {"approval_id": ap["approval_id"], "approver": ap.get("approver")}}]
    if ap["request"] == "reject":
        run.update(outcome="rejected", ended_at=now)
        d["record_run"](sql, cfg, run)
        d["events"](sql, cfg, events, now)
        return
    stmt_sql = a["rollback_sql"] if ap["request"] == "rollback" else a["sql"]
    try:
        st = parse(stmt_sql)
    except NotAllowed as e:
        d["set_state"](sql, cfg, a["action_id"], "blocked", now)
        run.update(outcome="blocked", error=str(e), ended_at=now)
        d["record_run"](sql, cfg, run)
        d["events"](sql, cfg, events + [{"action_id": a["action_id"], "event_type": "blocked", "payload": {"reason": str(e)}}], now)
        counts["blocked"] = counts.get("blocked", 0) + 1
        return
    try:
        tgt = d["target"](w, a)
        if ap["request"] == "verify":
            passed, report = _verify(d, sql, w, cfg, work, st, tgt)
            outcome = "verified" if passed else "verify_failed"
            run.update(report=json.dumps(report, default=str), branch=report.get("branch"))
        else:
            with d["session"](tgt["endpoint"], tgt["host"], tgt["database"], st.kind) as conn:
                res = d["execute"](conn, stmt_sql)
            ok = res["outcome"] == "applied"
            outcome = ("applied" if ok else "apply_failed") if ap["request"] == "apply" else ("rolled_back" if ok else "rollback_failed")
            run.update(error=res["error"], report=json.dumps({"index": res.get("index")}))
    except Exception as e:
        outcome = {"verify": "verify_failed", "apply": "apply_failed", "rollback": "rollback_failed"}[ap["request"]]
        run.update(error=f"{type(e).__name__}: {e}"[:1000])
    d["set_state"](sql, cfg, a["action_id"], outcome, now)
    run.update(outcome=outcome, ended_at=datetime.now(timezone.utc) if now is None else now)
    d["record_run"](sql, cfg, run)
    d["events"](sql, cfg, events + [{"action_id": a["action_id"], "event_type": outcome,
                                     "payload": {"run_id": run["run_id"], "error": run["error"]}}], now)
    counts[outcome] = counts.get(outcome, 0) + 1


def run_fixer(sql, w, cfg, user, deps=None, now=None) -> dict:
    now = now or datetime.now(timezone.utc)
    d = {**(default_deps(sql, w, user) if deps is None else {}), **(deps or {})}
    counts: dict = {}
    for row in d["stuck"](sql, cfg, now):
        stmt_sql = row["rollback_sql"] if row["state"] == "rolling_back" else row["sql"]
        tgt = d["target"](w, row)
        with d["session"](tgt["endpoint"], tgt["host"], tgt["database"], parse(stmt_sql).kind) as conn:
            res = d["recover"](conn, stmt_sql)
        outcome = res if row["state"] == "applying" else ("rolled_back" if res == "applied" else "rollback_failed")
        d["set_state"](sql, cfg, row["action_id"], outcome, now)
        d["events"](sql, cfg, [{"action_id": row["action_id"], "event_type": outcome, "payload": {"recovered": True}}], now)
    busy = set()
    for work in d["pending"](sql, cfg):
        project = work.action.get("project_id")
        if project in busy:
            continue
        busy.add(project)
        _handle(d, sql, w, cfg, work, now, counts)
    return counts


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="lakebase_health")
    a = ap.parse_args(argv)
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    from ..ddl import ensure_schema
    from ..sql import SparkSql
    sql, w, cfg = SparkSql(SparkSession.builder.getOrCreate()), WorkspaceClient(), EngineConfig(catalog=a.catalog, schema=a.schema)
    ensure_schema(sql, cfg)
    print(json.dumps(run_fixer(sql, w, cfg, w.current_user.me().user_name)))
```
Add `fix = "lakebase_health.jobs.fix:main"` under `[project.scripts]`.

- [ ] **Step 4: Run to verify it passes**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_fix_job.py -q && .venv/bin/python -m pytest -q`
Expected: PASS; whole engine suite green.

- [ ] **Step 5: Commit**

```bash
git add engine
git commit -m "feat(fixer): fixer job orchestrating verify, apply, rollback, recovery and stale approvals

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Post-apply check in the engine run

**Files:**
- Create: `engine/src/lakebase_health/fixer/postcheck.py`
- Modify: `engine/src/lakebase_health/jobs/run.py` (call after `persist`), `engine/src/lakebase_health/runner.py` (`META_SPECS["fix_regressed"]`)
- Test: `engine/tests/unit/test_postcheck.py`

**Interfaces:**
- Produces: `decide(finding_status, before_ms, after_ms, before_lock_share, after_lock_share, calls_after) -> ("confirmed"|"regressed", confidence, reasons)`; `run_postcheck(sql, cfg, now) -> list[dict]` (updates `applied` actions with ≥ 2 engine runs since apply; writes `action_runs` kind `postcheck`, ledger events, `shadow_log.outcome_score`, and a `fix_regressed` finding for regressions).

- [ ] **Step 1: Write the failing tests**

```python
# engine/tests/unit/test_postcheck.py
from lakebase_health.fixer.postcheck import decide


def test_confirmed_when_finding_resolved_and_queries_not_slower():
    assert decide("resolved", 10.0, 10.5, 0.1, 0.1, 500)[0] == "confirmed"


def test_regressed_when_queries_slower_or_lock_share_up():
    assert decide("resolved", 10.0, 11.5, 0.1, 0.1, 500)[0] == "regressed"
    assert decide("resolved", 10.0, 10.0, 0.1, 0.3, 500)[0] == "regressed"
    assert decide("open", 10.0, 10.0, 0.1, 0.1, 500)[0] == "regressed"


def test_low_traffic_is_confirmed_with_low_confidence():
    assert decide("resolved", None, None, None, None, 3)[:2] == ("confirmed", "low")
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_postcheck.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

```python
# engine/src/lakebase_health/fixer/postcheck.py
import json
import uuid

from ..ledger import chain, last_hashes

MIN_CALLS = 50


def decide(finding_status, before_ms, after_ms, before_lock_share, after_lock_share, calls_after):
    reasons = []
    if finding_status in ("new", "open"):
        reasons.append("finding still open")
    if calls_after is None or calls_after < MIN_CALLS or before_ms is None or after_ms is None:
        return ("regressed", "high", reasons) if reasons else ("confirmed", "low", ["too little traffic to compare"])
    if after_ms > before_ms * 1.10:
        reasons.append(f"affected queries {after_ms / before_ms:.2f}x slower")
    if before_lock_share is not None and after_lock_share is not None and after_lock_share > before_lock_share + 0.05:
        reasons.append("lock wait share rose")
    return ("regressed", "high", reasons) if reasons else ("confirmed", "high", [])


def run_postcheck(sql, cfg, now) -> list:
    due = sql.query(f"""
        SELECT a.action_id, a.fingerprint, a.updated_at AS applied_at, f.status AS finding_status, f.project_id, f.endpoint_id
        FROM {cfg.table('proposed_actions')} a JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint
        WHERE a.state = 'applied'
          AND (SELECT count(*) FROM {cfg.table('runs')} r WHERE r.started_at > a.updated_at AND r.status IN ('ok', 'ok_with_errors')) >= 2""")
    out = []
    for d in due:
        m = sql.query(f"""
            SELECT sum(CASE WHEN ts < :t THEN total_exec_time END) / NULLIF(sum(CASE WHEN ts < :t THEN calls END), 0) AS before_ms,
                   sum(CASE WHEN ts >= :t THEN total_exec_time END) / NULLIF(sum(CASE WHEN ts >= :t THEN calls END), 0) AS after_ms,
                   sum(CASE WHEN ts >= :t THEN calls END) AS calls_after
            FROM {cfg.raw('pg_stat_statements_counters')}
            WHERE endpoint_id = :ep AND ts >= :t - INTERVAL 24 HOURS
              AND queryid IN (SELECT queryid FROM {cfg.raw('plan_history')} WHERE endpoint_id = :ep AND ts >= :t - INTERVAL 24 HOURS)""",
                      {"t": d["applied_at"], "ep": d["endpoint_id"]})[0]
        locks = sql.query(f"""
            SELECT sum(CASE WHEN ts < :t AND wait_class_name = 'Lock' THEN wait_event_time END) / NULLIF(sum(CASE WHEN ts < :t THEN wait_event_time END), 0) AS b,
                   sum(CASE WHEN ts >= :t AND wait_class_name = 'Lock' THEN wait_event_time END) / NULLIF(sum(CASE WHEN ts >= :t THEN wait_event_time END), 0) AS a
            FROM {cfg.raw('wait_event_counters')} WHERE endpoint_id = :ep AND ts >= :t - INTERVAL 24 HOURS
              AND NOT (wait_class_name IN ('Extension', 'Activity') OR wait_event_name LIKE '%Idle%')""",
                          {"t": d["applied_at"], "ep": d["endpoint_id"]})[0]
        f = lambda v: None if v is None else float(v)
        outcome, confidence, reasons = decide(d["finding_status"], f(m["before_ms"]), f(m["after_ms"]), f(locks["b"]), f(locks["a"]),
                                              None if m["calls_after"] is None else int(m["calls_after"]))
        sql.execute(f"UPDATE {cfg.table('proposed_actions')} SET state = :s, updated_at = :now WHERE action_id = :id AND state = 'applied'",
                    {"s": outcome, "now": now, "id": d["action_id"]})
        report = {"confidence": confidence, "reasons": reasons, "before_ms": f(m["before_ms"]), "after_ms": f(m["after_ms"])}
        sql.insert_rows(cfg.table("action_runs"), [{"run_id": uuid.uuid4().hex, "approval_id": None, "action_id": d["action_id"],
                                                    "kind": "postcheck", "started_at": now, "ended_at": now, "outcome": outcome,
                                                    "report": json.dumps(report), "branch": None, "error": None}])
        sql.execute(f"UPDATE {cfg.table('shadow_log')} SET outcome_score = :s WHERE action_id = :id",
                    {"s": 1.0 if outcome == "confirmed" else 0.0, "id": d["action_id"]})
        ev = [{"action_id": d["action_id"], "event_type": outcome, "payload": report}]
        sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, [d["action_id"]]), ev, now))
        out.append({**d, "outcome": outcome, **report})
    return out
```
In `jobs/run.py`, after `persist(...)`: `regressions = [r for r in run_postcheck(sql, cfg, now) if r["outcome"] == "regressed"]`, and pass them to a second `persist`-style write of meta findings using `META_SPECS["fix_regressed"]` (critical; object = action_id; evidence = reasons). Add to `runner.META_SPECS`:
```python
    "fix_regressed": parse_check({
        "id": "fix_regressed", "title": "An applied fix made things worse", "category": "engine", "source": "delta",
        "sql": "SELECT 1", "thresholds": {"critical": 0},
        "explain": "After the fix was applied, the affected queries got slower, lock waits rose or the finding stayed "
                   "open. Review the evidence and approve the rollback in the app."}, stem="fix_regressed"),
```
Add a unit test in `test_run.py` that a regressed postcheck result produces a `fix_regressed` outcome with severity `critical` (use the existing `run_engine` test harness with `run_postcheck` monkeypatched to return one regressed row).

- [ ] **Step 4: Run to verify it passes**

Run: `cd engine && .venv/bin/python -m pytest -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add engine
git commit -m "feat(engine): post-apply check confirms or flags regressed fixes

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 10: Bundle — fixer job, variables, grants

**Files:**
- Create: `resources/fixer_job.yml`, `scripts/fixer_grants.sh`
- Modify: `databricks.yml` (variable `fixer_sp`), `resources/app.yml` (env `FIXER_JOB_ID`), `README.md` (fix-loop install and enrolment)

**Interfaces:**
- Produces: job `lakebase_health_fixer` (resource key) named `lakebase-health-fixer`, `run_as` `${var.fixer_sp}`, schedule every 15 minutes, `max_concurrent_runs: 1`, entry point `fix`; app env `FIXER_JOB_ID=${resources.jobs.lakebase_health_fixer.id}`.

- [ ] **Step 1: Write the resources**

```yaml
# resources/fixer_job.yml
resources:
  jobs:
    lakebase_health_fixer:
      name: lakebase-health-fixer
      max_concurrent_runs: 1
      run_as:
        service_principal_name: ${var.fixer_sp}
      schedule:
        quartz_cron_expression: "0 0/15 * * * ?"
        timezone_id: UTC
      tasks:
        - task_key: fix
          environment_key: engine
          python_wheel_task:
            package_name: lakebase_health
            entry_point: fix
            parameters: ["--catalog", "${var.catalog}", "--schema", "${var.schema}"]
      environments:
        - environment_key: engine
          spec:
            environment_version: "3"
            dependencies:
              - ../engine/dist/*.whl
              - psycopg[binary]>=3.1
              - databricks-sdk>=0.81.0
              - pyyaml>=6
```
Add to `databricks.yml` variables: `fixer_sp: {description: Application ID of the fixer service principal (writes fixes; see README)}`. Add to `resources/app.yml` env: `- name: FIXER_JOB_ID` / `value: ${resources.jobs.lakebase_health_fixer.id}`.

```bash
# scripts/fixer_grants.sh — run as the catalog owner after deploy
set -euo pipefail
: "${PROFILE:?}" "${CATALOG:?}" "${FIXER_SP:?}" "${APP_SP:?}" "${WAREHOUSE:?}"
q() { databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{\"warehouse_id\":\"$WAREHOUSE\",\"statement\":\"$1\",\"wait_timeout\":\"50s\"}" >/dev/null; }
q "GRANT USE CATALOG ON CATALOG $CATALOG TO \`$FIXER_SP\`"
q "GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA $CATALOG.lakebase_health TO \`$FIXER_SP\`"
q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.approvals TO \`$APP_SP\`"
JOB=$(databricks jobs list --profile "$PROFILE" -o json | python3 -c "import json,sys;print(next(j['job_id'] for j in json.load(sys.stdin) if j['settings']['name'].endswith('lakebase-health-fixer')))")
databricks permissions update jobs "$JOB" --profile "$PROFILE" --json "{\"access_control_list\":[{\"service_principal_name\":\"$APP_SP\",\"permission_level\":\"CAN_MANAGE_RUN\"}]}" >/dev/null
echo "grants done (job $JOB)"
```

- [ ] **Step 2: Validate**

Run: `databricks bundle validate --var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 --var warehouse_id=0123456789abcdef --var fixer_sp=<fixer app id> --profile my-profile`
Expected: `Validation OK!` (use the fixer SP created in Task 12 Step 1; until then validate with a placeholder UUID and expect only a run_as identity error at deploy, not validate).

- [ ] **Step 3: README**

Add "Fix loop" to `README.md`: create the fixer SP; add it to `databricks_pii_access`; run `scripts/fixer_grants.sh`; per project, give the fixer SP `CAN_USE` (branch creation) and a Postgres role that is a member of the role owning the target tables:
```sql
-- on the project's branch, as the table owner's admin
GRANT <owner_role> TO "<fixer-sp-application-id>";
```
and explain that approvals require the project owner or `CAN_MANAGE`.

- [ ] **Step 4: Commit**

```bash
git add resources/fixer_job.yml scripts/fixer_grants.sh databricks.yml resources/app.yml README.md
git commit -m "feat(bundle): fixer job, fixer_sp variable, grants script and enrolment docs

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 11: App — approver check, approval requests, cards and Actions view

**Files:**
- Create: `app/server/writes/insert_approval.sql`, `app/server/queries/project_actions.sql`, `app/server/queries/action_runs.sql`, `app/server/lib/approvals.ts`, `app/server/tests/approvals.test.ts`, `app/client/src/views/ActionCard.tsx`, `app/client/src/views/ActionCard.test.tsx`, `app/client/src/views/Actions.tsx`
- Modify: `app/server/tests/readonly.test.ts` (allow `server/writes/insert_approval.sql` only), `app/server/lib/handlers.ts` (actions + request handler), `app/server/routes.ts`, `app/server/server.ts` (writer + job trigger), `app/server/lib/sql.ts` (`createWriter`), `app/client/src/views/Finding.tsx` (embed card), `app/client/src/App.tsx` (route `/projects/:id/actions`)

**Interfaces:**
- Consumes: `Access`, `createAccess` (`Http`), `createRunner`, handlers pattern.
- Produces:
  - `canManage(http, token, project: {uid, project_id, owner}, userName) -> Promise<boolean>` (owner match, or `GET /api/2.0/permissions/database-projects/{uid}` → 200).
  - `legalRequests(state, verifyOnBranch) -> ("verify"|"apply"|"rollback"|"reject")[]` (mirror of `states.can_request`).
  - Routes: `GET /api/projects/:id/actions` → `{ actions: Row[], canManage: boolean }`; `GET /api/actions/:id` → `{ action, runs, canManage, requests }`; `POST /api/actions/:id/requests` body `{ request, note? }` → `202 { approvalId }` | `403` (not a manager) | `409` (illegal for state) | `404`.
  - `insert_approval.sql`: `INSERT INTO {catalog}.lakebase_health.approvals (approval_id, action_id, request, sql_hash, approver, requested_at, note) VALUES (:approval_id, :action_id, :request, :sql_hash, :approver, current_timestamp(), :note)`.

- [ ] **Step 1: Write the failing server tests**

```ts
// app/server/tests/approvals.test.ts
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { canManage, legalRequests } from "../lib/approvals";
import { createHandlers } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const AID = "0123456789abcdef0123456789abcdef";
const SQL = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);";

describe("canManage", () => {
  it("accepts the owner, or a 200 on the project ACL read", async () => {
    const http = async (path: string) => ({ status: path.endsWith(`/${HC}`) ? 200 : 403, json: {} });
    expect(await canManage(http, "t", { uid: "x", owner: "me@x.com" }, "me@x.com")).toBe(true);
    expect(await canManage(http, "t", { uid: HC, owner: "o@x.com" }, "me@x.com")).toBe(true);
    expect(await canManage(http, "t", { uid: "other", owner: "o@x.com" }, "me@x.com")).toBe(false);
  });
});

describe("legalRequests", () => {
  it("mirrors the fixer's state rules", () => {
    expect(legalRequests("proposed", true)).toEqual(["verify", "reject"]);
    expect(legalRequests("proposed", false)).toEqual(["verify", "apply", "reject"]);
    expect(legalRequests("verified", true)).toEqual(["apply", "reject"]);
    expect(legalRequests("regressed", true)).toEqual(["rollback"]);
    expect(legalRequests("applying", true)).toEqual([]);
  });
});

describe("request handler", () => {
  function setup(manager: boolean, state = "proposed") {
    const writes: Record<string, unknown>[] = [];
    let triggered = 0;
    const h = createHandlers({
      access: { async resolve() { return { userKey: "me@x.com", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: false }; }, refresh() {} },
      runner: { async run(name: string) {
        return name === "action" ? [{ action_id: AID, project_id: HC, state, sql: SQL, rollback_sql: "DROP INDEX CONCURRENTLY IF EXISTS i;", verify_on_branch: "true" }] : [];
      } },
      scheduleMinutes: 60,
      approvals: {
        canManage: async () => manager,
        insert: async (row) => { writes.push(row); },
        trigger: async () => { triggered++; },
        newId: () => "ap-1",
      },
    });
    return { h, writes, triggered: () => triggered };
  }
  const ctx = { token: "t", user: "me@x.com", now: new Date("2026-10-28T12:00:00Z") };

  it("records a manager's verify request with the SQL hash and triggers the fixer", async () => {
    const s = setup(true);
    expect(await s.h.request(ctx, AID, { request: "verify" })).toEqual({ approvalId: "ap-1" });
    expect(s.writes[0]).toMatchObject({ action_id: AID, request: "verify", approver: "me@x.com",
      sql_hash: createHash("sha256").update(SQL).digest("hex") });
    expect(s.triggered()).toBe(1);
  });

  it("refuses non-managers with 403 and illegal requests with 409", async () => {
    await expect(setup(false).h.request(ctx, AID, { request: "verify" })).rejects.toMatchObject({ status: 403 });
    await expect(setup(true).h.request(ctx, AID, { request: "apply" })).rejects.toMatchObject({ status: 409 });
    await expect(setup(true).h.request(ctx, AID, { request: "drop" })).rejects.toMatchObject({ status: 400 });
  });
});
```
Also add a scoped `server/queries/action.sql`:
```sql
SELECT a.action_id, a.fingerprint, a.check_id, a.kind, a.sql, a.rollback_sql, a.risk, a.reversible, a.verify_on_branch,
       a.state, a.created_at, a.updated_at, a.expires_at, f.project_id, f.title, f.object
FROM {catalog}.lakebase_health.proposed_actions a
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
WHERE a.action_id = :action_id AND array_contains(split(:project_ids, ','), f.project_id)
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && npx vitest run server/tests/approvals.test.ts`
Expected: FAIL — cannot find `../lib/approvals`.

- [ ] **Step 3: Implement**

```ts
// app/server/lib/approvals.ts
import type { Http } from "./access";

export type RequestKind = "verify" | "apply" | "rollback" | "reject";
const FROM: Record<RequestKind, string[]> = {
  verify: ["proposed", "verify_failed"],
  apply: ["verified", "apply_failed"],
  rollback: ["regressed", "applied", "confirmed", "rollback_failed"],
  reject: ["proposed", "verified", "verify_failed", "apply_failed"],
};
const ORDER: RequestKind[] = ["verify", "apply", "rollback", "reject"];

export const isRequestKind = (v: unknown): v is RequestKind => typeof v === "string" && v in FROM;

export function legalRequests(state: string, verifyOnBranch: boolean): RequestKind[] {
  return ORDER.filter((r) => FROM[r].includes(state) || (r === "apply" && state === "proposed" && !verifyOnBranch));
}

/** Owner, or anyone allowed to read the project's ACL (only CAN_MANAGE can). */
export async function canManage(http: Http, token: string, project: { uid: string; owner?: string }, userName: string) {
  if (project.owner && project.owner === userName) return true;
  const r = await http(`/api/2.0/permissions/database-projects/${encodeURIComponent(project.uid)}`, token);
  return r.status === 200;
}
```
Extend `createHandlers` deps with optional `approvals?: { canManage(ctx: Ctx, projectId: string): Promise<boolean>; insert(row: Record<string, string>): Promise<void>; trigger(): Promise<void>; newId(): string }` and add:
```ts
    async request(ctx: Ctx, rawId: string, body: unknown) {
      if (!approvals) throw new HttpError(404, "not found");
      const a = await who(ctx);
      const id = typeof rawId === "string" && /^[0-9a-f]{32}$/.test(rawId) ? rawId : null;
      if (!id) throw new HttpError(400, "invalid action id");
      const req = typeof body === "object" && body !== null && "request" in body ? body.request : undefined;
      if (!isRequestKind(req)) throw new HttpError(400, "invalid request");
      const [action] = await runner.run("action", a.projectIds, { action_id: id });
      if (!action) throw new HttpError(404, "not found");
      if (!(await approvals.canManage(ctx, String(action.project_id)))) throw new HttpError(403, "only the project owner or a manager can approve");
      const state = String(action.state);
      if (!legalRequests(state, action.verify_on_branch === true || action.verify_on_branch === "true").includes(req))
        throw new HttpError(409, `cannot ${req} an action that is ${state}`);
      const target = req === "rollback" ? String(action.rollback_sql ?? "") : String(action.sql ?? "");
      const approvalId = approvals.newId();
      const note = typeof body === "object" && body !== null && "note" in body && typeof body.note === "string" ? body.note.slice(0, 500) : "";
      await approvals.insert({ approval_id: approvalId, action_id: id, request: req,
        sql_hash: createHash("sha256").update(target).digest("hex"), approver: ctx.user ?? "", note });
      await approvals.trigger();
      return { approvalId };
    },
```
(`import { createHash } from "node:crypto"` and `isRequestKind`, `legalRequests` from `./approvals`.) Add `createWriter(file, exec)` in `sql.ts` that loads `server/writes/insert_approval.sql`, substitutes `{catalog}` and binds the row as STRING params. In `server.ts`: `approvals.canManage` resolves the project (`GET /api/2.0/postgres/projects/{project_id}` with the user token for `status.owner` and the SCIM `/Me` userName already resolved by access; extend `Access` with `userName`), `insert` uses the writer with the SP client, `trigger` calls `sp.jobs.runNow({ job_id: Number(process.env.FIXER_JOB_ID) })` (confirm the JS SDK method name in `node_modules/@databricks/sdk-experimental/dist/apis/jobs/api.d.ts`). Route: `app.post("/api/actions/:id/requests", express.json(), (req, res) => send(res, () => h.request(ctxOf(req), param(req.params.id), req.body)))` returning 202.

Update `readonly.test.ts`: every file in `server/queries` and `config/queries` stays SELECT/WITH; `server/writes` may contain exactly one file whose statement starts with `INSERT INTO {catalog}.lakebase_health.approvals`.

Client: `ActionCard({ action, runs, canManage, requests, onRequest })` shows state, the latest verification report (rules with pass/fail, per-query before/after ms), and buttons only for `requests` when `canManage` (labels "Verify on a branch", "Apply", "Roll back", "Reject"); when not a manager it shows "Only the project owner or a manager can approve." `Actions.tsx` lists a project's actions with state and links. Component test (happy-dom): buttons appear only for a manager and only for legal requests; clicking calls `onRequest("verify")`.

- [ ] **Step 4: Run tests**

Run: `cd app && npm test && npm run typecheck && npx eslint client server`
Expected: all pass, clean.

- [ ] **Step 5: Commit**

```bash
git add app
git commit -m "feat(app): approval cards and requests for verify, apply, rollback and reject

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 12: Deploy and acceptance on healthcheck

**Files:**
- Create: `scripts/fix_acceptance.py`

**Interfaces:**
- Consumes: everything.
- Produces: output `ALL FIX-LOOP ACCEPTANCE CHECKS PASSED` for criteria 1–5.

- [ ] **Step 1: Create and enrol the fixer SP (confirm with the user first)**

Ask the user to confirm, then: `databricks service-principals create --display-name lakebase-health-fixer`; add it to `databricks_pii_access` if that group exists (record otherwise — verification then sees masked query text and the plan rule falls back to `affected_queries` failing); `CAN_USE` on `healthcheck`; Postgres role `lakebase-health-fixer` on `projects/healthcheck/branches/production` and `GRANT <owner of hc_test tables> TO "<fixer app id>"` (find the owner with `SELECT tableowner FROM pg_tables WHERE schemaname='hc_test'`).

- [ ] **Step 2: Deploy and grant**

```bash
databricks bundle deploy --var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 \
  --var warehouse_id=0123456789abcdef --var fixer_sp=<fixer app id> --profile my-profile
PROFILE=my-profile CATALOG=my_catalog FIXER_SP=<fixer app id> APP_SP=00000000-0000-4000-8000-0000000000b1 \
  WAREHOUSE=0123456789abcdef bash scripts/fixer_grants.sh
```
Expected: deploy succeeds; `grants done`.

- [ ] **Step 3: Write `scripts/fix_acceptance.py`**

The script (profile, warehouse, catalog, project uid, test-SP id/secret env as arguments) does, using the app's HTTP API as the owner (CLI token) and as the test SP:
1. Finds the open `missing_fk_index` action on `hc_test.orders`; POSTs `verify`; waits for the fixer job run; asserts state `verified`, a report rule `index_used_with_speedup` passed, and no `lbh-verify-*` branch remains (`databricks postgres list-branches`). POSTs `apply`; waits; asserts state `applied` and, via `scripts/pg.sh`, a valid index on `hc_test.orders (customer_id)`.
2. Same for the `duplicate_index` action on `hc_test.customers_email_idx2` (re-create it first if a previous test dropped it): verify → rule `index_unused_before` passed → apply → index absent.
3. Runs the engine job twice; asserts both actions `confirmed` and their findings `resolved`.
4. Forced regression: inserts an `action_runs` + state change only via the documented test hook — set `proposed_actions.state = 'applied'` and `updated_at` back 3 hours for a third action (the `redundant_index` one after applying it), then runs `run_postcheck` locally with `decide` monkeypatched to return `regressed`; asserts `fix_regressed` finding (critical) and that `GET /api/actions/<id>` lists `rollback`; POSTs `rollback`; waits; asserts `rolled_back` and the index restored.
5. As the test SP (Postgres role holder, not a manager): `GET /api/actions/<id>` shows `canManage: false`; `POST …/requests` returns 403. Overwrites one proposed action's `sql` with `DROP TABLE hc_test.audit_events` (as the owner via the warehouse), requests verify as the owner with the tampered hash (the app computes the hash from the current SQL), and asserts the action becomes `blocked` and the table still exists.
6. Verifies the ledger: loads all ledger rows for the touched action IDs and calls `lakebase_health.ledger.verify`; asserts True. Asserts the engine and app SPs have no Postgres write privileges (same query as the engine's criterion 6).

- [ ] **Step 4: Run acceptance**

Run: `TEST_SP_SECRET=$(cat /tmp/test_sp_secret) engine/.venv/bin/python scripts/fix_acceptance.py --profile my-profile --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 --test-sp-id $(cat /tmp/test_sp_app) --test-sp-secret-env TEST_SP_SECRET`
Expected: `ALL FIX-LOOP ACCEPTANCE CHECKS PASSED`.

- [ ] **Step 5: Commit**

```bash
git add scripts/fix_acceptance.py
git commit -m "test(fixer): end-to-end fix-loop acceptance on healthcheck

Co-authored-by: Isaac <no-reply@databricks.com>"
```
