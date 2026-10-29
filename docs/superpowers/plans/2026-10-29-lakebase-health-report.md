# Lakebase Health Report (Tabbed Project Page) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the project page into an 11-tab health report for one branch (score and grade over 12 areas, an ordered runbook with SQL, inventory panels and 24 h system charts) backed by six new checks and a per-run inventory snapshot, and let any viewer download it as one self-contained HTML file with no query text.

**Architecture:** The analysis job gains a read-only inventory collector (`pg_inventory.py`, through the existing Postgres executor) that writes seven `inventory_*` snapshot tables per active, enrolled endpoint, plus six checks (`suspend_blockers`, `synced_index_review` on Postgres; `synced_full_refresh`, `wal_burst`, `getpage_latency_spike`, `autovacuum_saturation` on telemetry and the snapshots), a shared synced-table helper, a stats-reset guard and a monitoring-tool query filter. Every check YAML gains `area`; a generated `app/server/report/checks.json` carries the areas to the app. The app server builds the report from one batched set of `report_*.sql` queries (pure functions for score, runbook and strip), the same React components in `app/client/src/report/` render the tabs in the browser and, through `renderToStaticMarkup`, the download, with `theme/tokens.css` and `report.css` inlined and charts as inline SVG.

**Tech Stack:** Python 3.10+ (`lakebase_health`, psycopg 3, databricks-sdk, Spark SQL on serverless), pytest, sqlglot (already a dependency after the LLM merge; used here only in tests to parse Databricks SQL); AppKit 0.76.1 (`@databricks/appkit`, `@databricks/appkit-ui`: `Tabs`, `TabsList`, `TabsTrigger`, `TabsContent`, `Select`, `Button`), react-router 7.18 for routing, React 19 `react-dom/server`, TypeScript, Vitest (happy-dom for views); Databricks Asset Bundles.

**Spec:** `docs/superpowers/specs/2026-10-28-lakebase-health-report-design.md` (builds on the engine, app and fix-loop specs of 2026-10-27/28). Executors read the spec and this plan together.

**Delivery:** branch `feature/health-report`, cut from `main` **after** `feature/llm-rewrites` has merged into `main` (the spec's §9 says "from `feature/fix-loop`"; that branch and the LLM rewrites are both in `main` by then). The executor creates the worktree (superpowers:using-git-worktrees); all paths are relative to it. No line numbers are given: every edit to an existing file quotes the exact text it replaces, as that text reads after the LLM merge and after the earlier tasks of this plan. Edits to files the LLM branch also changed are marked **[LLM-merge]**: if the quoted text is not found, read the file, find the same anchor in its merged form, and keep both changes.

**Proven:** every code block below was built and tested in a throwaway clone of `main` with `feature/llm-rewrites` merged (`/tmp`, never the real repo or a workspace): engine 560 passed, app 235 passed (6 skipped live tests), `tsc -b` clean for client and server, `tsdown` server bundle and `vite build` succeed, and the bundled server rendered an export under plain `node`. The SQL against the warehouse is exercised only by the live query test (Task 12) and the acceptance task (Task 18).

## Global Constraints

- One branch per report, default branch preselected; project-level checks (HA, backup window, branch protection, password roles) count in resilience and configuration & security.
- 12 areas, in this order: cache & memory; storage latency; write path; connections & sessions; locks & contention; vacuum & XID; indexes; tables & bloat; queries; configuration & security; scale to zero; resilience.
- Area score: 5 with no open finding; worst open finding info → 4, warning → 3, critical → 1. Not assessed when none of its checks ran for the branch; left out of the total. Score = sum / (5 × assessed areas); grade A ≥ 85 %, B ≥ 70 %, C ≥ 55 %, D ≥ 40 %, F below; header shows "assessed N of 12".
- Runbook: one item per open finding, ordered by severity, then whether a verified fix action exists, then impact (`measure` relative to its threshold); priority critical → HIGH, warning → MEDIUM, info → LOW; SQL that would quote user query text shows the query ID instead.
- Tabs and URL: `/projects/:id/branches/:branch/<tab>`; Health (default), Overview, Tables & Indexes, Queries, Waits & Locks, Vacuum & Bloat, Scale to zero, System, Settings, Runbook, Fixes. Every tab starts with a row of KPI tiles, then cards; each card has a one-line plain-language explanation.
- Export: self-contained HTML, inline CSS (reuse `app/client/src/theme/tokens.css`) and server-rendered inline SVG, no `<script>`, no external URLs, print stylesheet; **never query text, whatever the exporter's PII membership**; file name `lakebase-health-<project>-<branch>-<UTC yyyymmddThhmmZ>.html`; one `audit_events` row per export (user, project, branch, time).
- Access: project and branch visible to the user, otherwise 404. The app SP stays SELECT-only on engine tables except `MODIFY` on `approvals` and `audit_events`.
- Postgres: the engine is read-only (`pg_monitor`, `default_transaction_read_only`); it skips suspended computes and re-checks state before each connection; it never runs `CREATE EXTENSION` or `GRANT`.
- Inventory tables keyed by `(project_id, branch_id, endpoint_id, collected_at)`; a new snapshot per run; rows older than 7 days deleted by the run; `inventory_statements` has **no query text** and excludes platform roles (`databricks_writer_%`, `cloud_admin`, the engine and fixer roles).
- Stats-reset guard: `unused_index`, `synced_index_review`, `seq_scan_heavy`, `dead_tuples`, `seq_scan_read_ratio` do not evaluate when `pg_stat_database.stats_reset` or postmaster start is less than 24 h ago; the area shows "counters too young (reset N h ago)".
- Synced tables: every finding on a storage partition names the synced table; no DROP action for an index on a synced partition.
- No new npm dependencies (the Databricks Apps npm proxy lacks some packages); the chart is vendored (`LineChart.tsx`).
- Test project `healthcheck` (uid `11111111-2222-4333-8444-555555555555`), profile `my-profile`, catalog `my_catalog`, warehouse `0123456789abcdef`. Every new SQL file runs on `my_catalog.lakebase_health` in < 5 s; report data loads in < 5 s with a warm warehouse.
- Commit messages end with a blank line and `Co-authored-by: Isaac <no-reply@databricks.com>` (the `-m … -m "Co-authored-by: …"` form below does that); never `--no-verify`.

## Decisions recorded in this plan

- **`neon_*` functions: read when present and permitted, recorded either way.** The collector probes the `neon` extension (`pg_extension`, `has_function_privilege`, `has_table_privilege`) and reads `neon_get_lfc_stats()`, `approximate_working_set_size_seconds(900)`, `backpressure_throttling_time()`, `backpressure_lsns()` (lag bytes against `pg_current_wal_lsn()`) and the getpage rows of `neon_perf_counters`, each in its own statement; every source writes `source:<name>` = 1, or 0 with the reason (`not available: no SELECT on neon_perf_counters`, `not available: extension neon not installed`, or the error type). On healthcheck today (checked live by the controller): `neon` 1.25 in `public`; the engine role can execute the four functions but has no SELECT on `neon_perf_counters` / `neon_lfc_stats`. The README gains an optional `GRANT SELECT ON neon_perf_counters, neon_lfc_stats` that turns on the endpoint-level getpage p99 chart.
- **Telemetry fallback, checked during planning.** `DESCRIBE my_catalog.lakebase_health.raw_compute_counters` (2026-10-29) has no WAL bytes, no full-page images and no getpage histogram (columns: back-pressure seconds, host CPU/disk/network, `lfc_hits/misses/evictions/writes`, `replica_lfc_redo_evictions`; 30 s samples). So: `wal_burst` and `synced_full_refresh` read WAL and full-page images from `pg_stat_statements_counters.wal_bytes / wal_fpi` (30–60 s samples, per statement, so the burst names its statement); `getpage_latency_spike`, the Overview tile and the System chart read per-execution getpage wait from `plan_history` (`neon_getpage_wait_us / neon_getpage_count`, 5-minute buckets). The spec's 15-minute `pg_stat_wal` + `neon_perf_counters` fallback would be too coarse for a 5-minute rule and is not readable today; the endpoint-level histogram is collected anyway when granted and drawn as an extra p99 line.
- **One more snapshot table, `inventory_endpoint`** (metric, dims JSON, value, note): autovacuum workers busy/max (needed across 3 runs by `autovacuum_saturation`), oldest transaction, uptime, server version, idle clients per role/application, the `neon` values and every `source:*` availability row. The spec lists six inventory tables; this seventh holds the per-endpoint scalars they lack.
- **`audit_events` does not exist yet.** The engine creates it (`ddl.py`), the grants script gives the app SP `MODIFY` on it only.
- **Synced storage** = a relation in `__db_system` whose `pg_partition_root()` is another relation (the synced table), or a table owned by a pipeline writer role (`databricks_writer_<n>`). One SQL fragment (`synced.py`, the `{synced_tables}` token) is used by the index checks, `synced_index_review`, the collector, and a once-per-database lookup the runner uses to put `synced_table` in the evidence of any Postgres finding on synced storage.
- **"Do not evaluate"** = outcome `skipped:stats_young` with error `counters too young (reset N h ago)`: findings of those checks are neither raised nor resolved (frozen), and their area is not assessed when no other check of it ran. One young database skips the endpoint (partial results would resolve the other databases' findings). For the telemetry check `seq_scan_read_ratio` the guard is "telemetry covers at least 20 of the 24 hours" in its SQL.
- **`suspend_blockers` "no suspend in 24 h"** = Postgres started more than 24 h ago (a resume starts a new postmaster). The suspend timeout comes from the inventory through a `{suspend_timeout_s}` placeholder the runner fills per endpoint; scale to zero off → `skipped:scale_to_zero_off`.
- **Monitoring tools' queries** (user review, 2026-10-29): `exclusions.yaml` gains `tool_apps` (`pg_sage`, `lakebase-health-notebook`) and `tool_tags` (`pg_sage`, `lakebase-health`, matched only as a leading `/* tag */` comment). Query-scoped checks drop a tool's statements by (branch, role, query id), so a customer running the same statement under another role keeps it. The engine, fixer and notebook lead every statement with `/* lakebase-health */`: pg_stat_statements computes the query id from the parse tree, where comments do not exist, so the tag never splits or merges entries; it only appears in the stored text. Masked text (`<REDACTED>`) never matches: nothing is dropped by accident when the engine cannot read text.
- **Report scope inside the branch:** inventory panels and charts describe the branch's read-write endpoint; replicas appear in the header and the replica-lag chart.
- **The report JSON never carries query text** (findings are selected without `query_text`; evidence goes through `maskRows(…, false)`); the Queries tab gets text, for those allowed, from the existing `/api/projects/:id/queries` lookup. `strip()` still runs before every export.
- **Exported tabs:** Health, Overview, Tables & Indexes, Queries (statement tables, IDs only), Vacuum & Bloat, Scale to zero, System, Settings, Runbook, in tab order. Waits & Locks (its timeline is interactive) and Fixes are app-only.
- **Components shared by browser and server** live in `app/client/src/report/`, import only `react` and each other, and use `rp-*` classes from `report.css`; `tsconfig.server.json` gains `"jsx": "react-jsx"` so the server typechecks and bundles them (`tsdown` emits them under `dist/client/src/report/`).

## Review Focus

1. **A compute that suspends while the run or the page reads it.** Expect no connection to a suspended compute, the previous snapshot kept whole (never a partial one), and the page saying it shows the last snapshot. Tests: Task 5 `test_a_compute_that_suspends_part_way_keeps_its_previous_snapshot`, Task 12 `explains a branch that is not enrolled and a suspended compute`.
2. **A PII member (or the project owner) downloads the report.** They see text in the app, but the file must have none: not in findings, evidence or SQL. Tests: Task 12 `never carries statement text in evidence, even for a PII member`, Task 14 `never contains query text, even when a PII member exports it`.
3. **Counters reset by a scale-to-zero resume.** Expect the counter checks to skip (no `unused_index` flood, no findings resolved), the area "not assessed" with the reset age, and a banner. Tests: Task 2 `test_one_young_database_skips_the_endpoint`, Task 11 `shows young counters as the reason an area is not assessed`, Task 12 `warns about young counters, stale engine data and missing telemetry`.
4. **A customer query that looks like a tool's.** A comment later in the text, a string literal containing `/* pg_sage */`, an app named `pg_sage_dashboard`, or masked text must not be excluded. Tests: Task 10 `test_customer_queries_do_not_match`, `test_tool_applications_and_tags_as_sql_predicates`.
5. **One panel's query fails (or the warehouse is slow for one table).** Expect that card to show an error box in the app, "Section unavailable" in the file, and everything else to render. Tests: Task 12 `keeps rendering when one section fails`, Task 14 `marks a failed section unavailable instead of dropping it`.

---

## File Structure

```
engine/src/lakebase_health/
  checkspec.py              # modify: AREAS, area, min_stats_hours, render_text, {synced_tables}
  synced.py                 # create: synced storage SQL, per-database map, evidence annotation, DROP guard
  runner.py                 # modify: stats-young skip, synced annotation, {suspend_timeout_s}
  actions.py                # modify: no DROP INDEX action on synced storage
  exclusions.py             # modify: platform_role_pg, tool apps and tags
  executors/delta.py        # modify: lakebase_health.* rewrite, tool-query anti-join
  executors/postgres.py     # modify: QUERY_TAG on every engine statement
  fixer/pg.py               # modify: TaggedConn [LLM-merge]
  pg_inventory.py           # create: the inventory collector (incl. neon_* and pg_stat_io)
  ddl.py                    # modify: 7 inventory tables, audit_events [LLM-merge]
  jobs/run.py               # modify: collect inventory before the checks [LLM-merge]
  checks/**.yaml            # modify: area on all 64; min_stats_hours; synced exclusions
  checks/postgres/{synced_index_review,suspend_blockers}.yaml                                  # create
  checks/telemetry/{autovacuum_saturation,synced_full_refresh,wal_burst,getpage_latency_spike}.yaml  # create
  checks/exclusions.yaml    # modify: tool_apps, tool_tags
engine/tests/unit/test_{areas,synced,suspend_blockers,pg_inventory,report_checks,tool_queries}.py   # create
engine/tests/unit/test_{runner,catalog,dedupe,run,fixer_grants}.py                              # modify
scripts/gen_check_areas.py, scripts/regen_checks.sh, scripts/report_acceptance.py               # create
scripts/build_notebook.py, scripts/fixer_grants.sh [LLM-merge]                                  # modify
notebooks/lakebase_health_check.py, docs/CHECKS.md, README.md [LLM-merge]                       # regenerate / modify
app/server/report/{catalog,score,runbook,strip,build,export}.ts, checks.json (generated)        # create
app/server/queries/report_*.sql (14 files), app/server/writes/insert_audit.sql                  # create
app/server/lib/{handlers.ts [LLM-merge], validate.ts}, app/server/routes.ts [LLM-merge], server.ts [LLM-merge]
app/server/tests/report_{score,runbook,build,handlers,export}.test.ts                          # create
app/server/tests/readonly.test.ts, tests/live/queries.live.test.ts                              # modify
app/tsconfig.server.json                                                                        # modify: jsx
app/client/src/report/{types.ts,format.ts,ui.tsx,LineChart.tsx,tabs.tsx,Document.tsx,report.css}  # create
app/client/src/views/Report.tsx (+ Report.test.tsx)                                             # create
app/client/src/{App.tsx, lib/nav.ts, views/Actions.tsx}                                         # modify
docs/FOLLOW_UPS.md, docs/COMPARISON.md                                                          # modify
```

Generated files (`notebooks/lakebase_health_check.py`, `docs/CHECKS.md`, the README checks summary, `app/server/report/checks.json`) are never edited by hand: `scripts/regen_checks.sh` rewrites all of them, and tests fail when any is stale. After the LLM merge they may differ from what a task produces only by the regeneration itself.

---

# Phase 1: Engine


### Task 1: Check areas and the app's check catalog

Every check names one of the 12 report areas; a generator writes `app/server/report/checks.json` (area, source, title, thresholds, direction per check) so the app can score without reading the engine; one script regenerates every derived file.

**Files:**
- Modify: `engine/src/lakebase_health/checkspec.py`
- Modify (by a one-off script): every `engine/src/lakebase_health/checks/**/*.yaml` except `exclusions.yaml` (64 files; `checks/telemetry/plan_flip.yaml` is **[LLM-merge]**, the script only inserts a line after `category:`)
- Create: `scripts/gen_check_areas.py`, `scripts/regen_checks.sh`, `app/server/report/checks.json` (generated)
- Test: `engine/tests/unit/test_areas.py`

**Interfaces:**
- Consumes: `load_checks(directory, api_functions)`, `parse_check(data, stem, api_functions)`, `META_SPECS` (runner.py).
- Produces: `checkspec.AREAS: tuple[str, ...]` (the 12 ids, in report order: `cache_memory`, `storage_latency`, `write_path`, `connections`, `locks`, `vacuum_xid`, `indexes`, `tables_bloat`, `queries`, `config_security`, `scale_to_zero`, `resilience`); `CheckSpec.area: str | None` (None only for the meta checks); `scripts/gen_check_areas.py` with `build() -> str` and `--check`; `app/server/report/checks.json` = `{check_id: {"area", "source", "title", "thresholds", "direction"}}`; `scripts/regen_checks.sh` (notebook, `docs/CHECKS.md` + README summary, `checks.json`).

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_areas.py` (new file, complete):

```python
"""Every shipped check names one of the 12 report areas, and the app's copy of the mapping is current."""
import importlib.util
from pathlib import Path

import pytest

from lakebase_health.checkspec import AREAS, CheckSpecError, load_checks, parse_check
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.runner import META_SPECS

REPO = Path(__file__).resolve().parents[3]
BASE = {"id": "x", "title": "X", "category": "c", "source": "delta", "explain": "e", "sql": "SELECT 1",
        "thresholds": {"warning": 0}}


def test_there_are_twelve_areas():
    assert len(AREAS) == 12 and len(set(AREAS)) == 12


def test_area_is_parsed_and_validated():
    assert parse_check({**BASE, "area": "indexes"}, stem="x").area == "indexes"
    assert parse_check(dict(BASE), stem="x").area is None
    with pytest.raises(CheckSpecError, match="area must be one of"):
        parse_check({**BASE, "area": "everything"}, stem="x")


def test_every_shipped_check_has_an_area():
    missing = [s.id for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS)) if s.area not in AREAS]
    assert missing == []


def test_project_level_checks_score_in_resilience_and_security():
    specs = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
    assert {specs[i].area for i in ("primary_without_ha", "backup_restore_window", "default_branch_unprotected")} \
        == {"resilience"}
    assert specs["password_roles"].area == "config_security"


def test_meta_checks_have_no_area():
    assert all(s.area is None for s in META_SPECS.values())


def test_app_check_areas_file_is_current():
    spec = importlib.util.spec_from_file_location("gen_check_areas", REPO / "scripts" / "gen_check_areas.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert (REPO / "app" / "server" / "report" / "checks.json").read_text() == mod.build(), \
        "run python scripts/gen_check_areas.py"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_areas.py -v`
Expected: FAIL with `ImportError: cannot import name 'AREAS' from 'lakebase_health.checkspec'`.

- [ ] **Step 3: Add `area` to the check spec**

`engine/src/lakebase_health/checkspec.py`:

Replace (1 of 4):

```python
FIX_KINDS = {"ddl", "setting", "rewrite", "hint", "manual"}
ACTIONABLE_KINDS = {"ddl", "setting"}
RISKS = {"safe", "moderate", "high"}
REPLICA_CHECKS = {"intersect"}
PRECEDENCE_KEYS = ("object", "database", "endpoint")
GUARD_RE = re.compile(r"^\s*([a-z_]+)\s*(>=|<=|>|<|==)\s*(-?[0-9.]+)\s*$")
```

with:

```python
FIX_KINDS = {"ddl", "setting", "rewrite", "hint", "manual"}
ACTIONABLE_KINDS = {"ddl", "setting"}
RISKS = {"safe", "moderate", "high"}
# The health report's 12 scorecard areas, in report order (spec 2026-10-28 health report, section 5)
AREAS = ("cache_memory", "storage_latency", "write_path", "connections", "locks", "vacuum_xid", "indexes",
         "tables_bloat", "queries", "config_security", "scale_to_zero", "resilience")
REPLICA_CHECKS = {"intersect"}
PRECEDENCE_KEYS = ("object", "database", "endpoint")
GUARD_RE = re.compile(r"^\s*([a-z_]+)\s*(>=|<=|>|<|==)\s*(-?[0-9.]+)\s*$")
```

Replace (2 of 4):

```python
    superseded_by: SupersededBy | None = None  # not reported while one of these checks has an open finding
    query_scoped: bool = False  # reads per-query telemetry: platform roles are filtered out, evidence.queryid is a str
    fix: FixSpec | None = None

    def tables(self) -> set:
        return set(re.findall(r"system\.lakebase\.([a-z_]+)", self.sql or ""))
```

with:

```python
    superseded_by: SupersededBy | None = None  # not reported while one of these checks has an open finding
    query_scoped: bool = False  # reads per-query telemetry: platform roles are filtered out, evidence.queryid is a str
    fix: FixSpec | None = None
    area: str | None = None  # health report scorecard area (AREAS); None only for the engine's meta checks

    def tables(self) -> set:
        return set(re.findall(r"system\.lakebase\.([a-z_]+)", self.sql or ""))
```

Replace (3 of 4):

```python
    query_scoped = data.get("query_scoped", False)
    if query_scoped not in (True, False) or (query_scoped and source != "delta"):
        errors.append("query_scoped must be true or false, and true only on a delta check")
    superseded_by = _parse_superseded_by(data.get("superseded_by"), data.get("id"), errors)
    fix = _parse_fix(data.get("fix"), errors)
    if errors:
```

with:

```python
    query_scoped = data.get("query_scoped", False)
    if query_scoped not in (True, False) or (query_scoped and source != "delta"):
        errors.append("query_scoped must be true or false, and true only on a delta check")
    area = data.get("area")
    if area is not None and area not in AREAS:
        errors.append(f"area must be one of {list(AREAS)}")
    superseded_by = _parse_superseded_by(data.get("superseded_by"), data.get("id"), errors)
    fix = _parse_fix(data.get("fix"), errors)
    if errors:
```

Replace (4 of 4):

```python
                     schedule=schedule, guards=guards, requires_extension=data.get("requires_extension"),
                     replica_check=replica_check, per_database=per_database,
                     query_scoped=query_scoped, title_template=data.get("title_template"),
                     superseded_by=superseded_by, fix=fix)


def load_checks(directory: Path, api_functions: frozenset) -> list:
```

with:

```python
                     schedule=schedule, guards=guards, requires_extension=data.get("requires_extension"),
                     replica_check=replica_check, per_database=per_database,
                     query_scoped=query_scoped, title_template=data.get("title_template"),
                     superseded_by=superseded_by, fix=fix, area=area)


def load_checks(directory: Path, api_functions: frozenset) -> list:
```

- [ ] **Step 4: Give every check its area**

Run this once from the repository root (it is not committed). It inserts `area: <id>` after the `category:` line and stops on a check it does not know:

```python
"""One-off: add `area:` after the `category:` line of every check YAML (run once from the repo root)."""
import re
from pathlib import Path

AREA = {
    "cache_memory": ["buffer_hit_rate", "cpu_saturation", "cu_sizing", "lfc_hit_rate", "working_set_vs_cache"],
    "storage_latency": ["storage_wait"],
    "write_path": ["write_backpressure", "commit_quorum_wait"],
    "connections": ["connection_utilization", "connection_drops", "idle_in_transaction", "idle_in_tx_time",
                    "long_transaction"],
    "locks": ["blocked_session", "deadlocks", "hot_row_lock", "lock_blocking_history", "lock_wait_share",
              "gin_cleanup_lock_wait"],
    "vacuum_xid": ["autovacuum_blocker", "autovacuum_disabled", "autovacuum_scale_factor", "dead_tuples",
                   "xid_age_table", "xid_runway", "xmin_horizon_holder", "prepared_transaction",
                   "replication_slot_retention", "stale_statistics"],
    "indexes": ["duplicate_index", "gin_pending_list", "invalid_index", "missing_fk_index", "oversized_search_index",
                "redundant_index", "unused_index", "seq_scan_heavy", "seq_scan_read_ratio"],
    "tables_bloat": ["index_bloat", "table_bloat", "table_without_pk", "sequence_exhaustion"],
    "queries": ["top_db_time_query", "query_regression", "plan_flip", "row_misestimate", "sort_limit_waste",
                "disk_spill", "statement_timeouts", "rollback_ratio", "postgres_errors"],
    "config_security": ["config_drift", "guardrail_settings", "privileged_roles", "public_schema_create",
                        "non_ssl_sessions", "auth_failures", "password_roles"],
    "resilience": ["backup_restore_window", "default_branch_unprotected", "primary_without_ha", "replica_lag",
                   "storage_utilization", "storage_runway"],
}
BY_ID = {c: a for a, ids in AREA.items() for c in ids}
done = 0
for path in sorted(Path("engine/src/lakebase_health/checks").rglob("*.yaml")):
    if path.name == "exclusions.yaml":
        continue
    text = path.read_text()
    if re.search(r"^area:", text, re.M):
        continue
    area = BY_ID[path.stem]  # KeyError = a check this mapping does not know: add it above
    text, n = re.subn(r"^(category: .*)$", rf"\1\narea: {area}", text, count=1, flags=re.M)
    assert n == 1, path
    path.write_text(text)
    done += 1
print(f"added area to {done} checks")
```

Run: save the block as `/tmp/add_areas.py`, then from the repository root `python3 /tmp/add_areas.py`
Expected: `added area to 64 checks`. Spot-check: `head -4 engine/src/lakebase_health/checks/telemetry/plan_flip.yaml` shows `area: queries` under `category: performance`.

- [ ] **Step 5: Generator for the app's catalog, and one script for every derived file**

`scripts/gen_check_areas.py` (new file, complete):

```python
#!/usr/bin/env python3
"""Generate app/server/report/checks.json (check id -> area, source, title, thresholds) from the check YAMLs.

The app's report builder scores areas from this file; the engine's findings carry no area (the spec keeps it on
the check). Usage (needs PyYAML, for example the engine venv):
    python scripts/gen_check_areas.py           # rewrite the file
    python scripts/gen_check_areas.py --check   # exit 1 if it is out of date
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))

from lakebase_health.checkspec import load_checks  # noqa: E402
from lakebase_health.executors.api import API_CHECKS  # noqa: E402
from lakebase_health.jobs.run import CHECKS_DIR  # noqa: E402

OUT = ROOT / "app" / "server" / "report" / "checks.json"


def build() -> str:
    specs = sorted(load_checks(CHECKS_DIR, frozenset(API_CHECKS)), key=lambda s: s.id)
    data = {s.id: {"area": s.area, "source": s.source, "title": s.title, "thresholds": s.thresholds,
                   "direction": s.direction} for s in specs}
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    text = build()
    if a.check:
        if not OUT.exists() or OUT.read_text() != text:
            print(f"{OUT} is out of date: run python scripts/gen_check_areas.py", file=sys.stderr)
            sys.exit(1)
        return
    OUT.write_text(text)
    print(f"wrote {OUT}: {len(json.loads(text))} checks")


if __name__ == "__main__":
    main()
```

`scripts/regen_checks.sh` (new file, complete; `chmod +x scripts/regen_checks.sh`). It regenerates the notebook with the service roles and wheel path the committed notebook records, so its test stays green:

```bash
#!/bin/bash
# Regenerate every file derived from the check YAMLs (run from the repo root with the engine venv's python).
set -euo pipefail
PY=${PY:-engine/.venv/bin/python}
NB=notebooks/lakebase_health_check.py
ROLES=$($PY -c "import ast,re,sys; t=open('$NB').read(); print(','.join(ast.literal_eval(re.search(r'^SERVICE_ROLES = (.*)$', t, re.M).group(1))))")
WHEEL=$($PY -c "import ast,re; t=open('$NB').read(); print(ast.literal_eval(re.search(r'^ENGINE_WHEEL = (.*)$', t, re.M).group(1)) or '')")
$PY scripts/build_notebook.py --service-roles "$ROLES" --engine-wheel "$WHEEL"
$PY scripts/gen_checks_doc.py
$PY scripts/gen_check_areas.py
```

Run: `mkdir -p app/server/report && PY=engine/.venv/bin/python bash scripts/regen_checks.sh`
Expected: `wrote …/notebooks/lakebase_health_check.py: 64 checks (31 delta, 29 postgres, 4 api)`, `64 checks: wrote docs/CHECKS.md and the README summary`, `wrote …/app/server/report/checks.json: 64 checks`.

- [ ] **Step 6: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass (the new `test_areas.py` included).

- [ ] **Step 7: Commit**

```bash
git add engine/src/lakebase_health/checkspec.py engine/src/lakebase_health/checks engine/tests/unit/test_areas.py \
  scripts/gen_check_areas.py scripts/regen_checks.sh app/server/report/checks.json notebooks docs/CHECKS.md README.md
git commit -m "feat(engine): report area on every check; app check catalog and one regen script" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 2: Stats-reset guard

Counter-based Postgres checks declare `min_stats_hours: 24`; the runner skips them on an endpoint whose statistics (or postmaster) are younger, as `skipped:stats_young` with the note the report shows. `seq_scan_read_ratio` (telemetry) requires 20 of the 24 hours covered.

**Files:**
- Modify: `engine/src/lakebase_health/checkspec.py`, `engine/src/lakebase_health/runner.py`
- Modify: `engine/src/lakebase_health/checks/postgres/{unused_index,seq_scan_heavy,dead_tuples}.yaml`, `engine/src/lakebase_health/checks/telemetry/seq_scan_read_ratio.yaml`
- Test: `engine/tests/unit/test_runner.py`, `engine/tests/unit/test_catalog.py` (append)

**Interfaces:**
- Consumes: `PgSession.stats_window_hours()` (existing: hours since `pg_stat_database.stats_reset`, else postmaster start), the runner's per-database `ctx = {"stats_window_hours": ...}`.
- Produces: `CheckSpec.min_stats_hours: float | None` (YAML `min_stats_hours`, positive number, postgres checks only); `runner.STATS_YOUNG = "skipped:stats_young"`; the outcome's `error` = `"counters too young (reset N h ago)"` or `"counters too young (reset time unknown)"`. The app's scoring (Task 11) reads both from `check_results`.

- [ ] **Step 1: Write the failing tests**

Append to `engine/tests/unit/test_runner.py`:

Replace:

```python
    pg = PerEndpointPg({"e": [{"object": "i", "measure": 1}]}, suspended=("r1",))
    [o] = runner(pg=pg).run([s], [ep(), ep(endpoint_id="r1", endpoint_type=RO)], NOW, {}, set())
    assert pg.opened == ["e"] and o.results[0].evidence["replicas_unverified"] == ["r1"]
```

with:

```python
    pg = PerEndpointPg({"e": [{"object": "i", "measure": 1}]}, suspended=("r1",))
    [o] = runner(pg=pg).run([s], [ep(), ep(endpoint_id="r1", endpoint_type=RO)], NOW, {}, set())
    assert pg.opened == ["e"] and o.results[0].evidence["replicas_unverified"] == ["r1"]


def test_counter_checks_do_not_run_on_young_statistics():
    young = spec("u", min_stats_hours=24, thresholds={"info": 0})
    plain = spec("p", thresholds={"info": 0})
    sess = FakeSession([{"object": "s.i", "measure": 5.0}], window=3.4)
    out = {o.check_id: o for o in runner(pg=FakePg(sess)).run([young, plain], [ep()], NOW, {}, set())}
    assert out["u"].status == "skipped:stats_young" and out["u"].results == []
    assert out["u"].error == "counters too young (reset 3 h ago)"
    assert out["p"].status == "ok" and len(out["p"].results) == 1


def test_counter_checks_run_once_statistics_are_old_enough_and_unknown_age_skips():
    s = spec("u", min_stats_hours=24, thresholds={"info": 0})
    [o] = runner(pg=FakePg(FakeSession([{"object": "s.i", "measure": 5.0}], window=30.0))).run([s], [ep()], NOW, {}, set())
    assert o.status == "ok" and len(o.results) == 1
    [o] = runner(pg=FakePg(FakeSession([{"object": "s.i", "measure": 5.0}], window=None))).run([s], [ep()], NOW, {}, set())
    assert o.status == "skipped:stats_young" and o.error == "counters too young (reset time unknown)"


def test_one_young_database_skips_the_endpoint():
    class TwoDbs(FakeSession):
        def __init__(self):
            super().__init__([{"object": "s.i", "measure": 5.0}])
            self.windows = iter([48.0, 2.0])

        def stats_window_hours(self):
            return next(self.windows)

    s = spec("u", min_stats_hours=24, thresholds={"info": 0})
    [o] = runner(pg=FakePg(TwoDbs())).run([s], [ep(databases=["a", "b"])], NOW, {}, set())
    assert o.status == "skipped:stats_young" and o.results == []


def test_min_stats_hours_is_validated():
    import pytest
    from lakebase_health.checkspec import CheckSpecError
    for bad in (0, -1, "24", True):
        with pytest.raises(CheckSpecError, match="min_stats_hours"):
            spec("u", min_stats_hours=bad)
    with pytest.raises(CheckSpecError, match="min_stats_hours"):
        spec("d", "delta", min_stats_hours=24)
```

Append to `engine/tests/unit/test_catalog.py`:

Replace:

```python
    # Session history does not sample GIN cleanup (Lock/page) waits; the per-interval counters do.
    sql = specs()["gin_cleanup_lock_wait"].sql
    assert "system.lakebase.wait_event_counters" in sql and "active_session_history" not in sql
```

with:

```python
    # Session history does not sample GIN cleanup (Lock/page) waits; the per-interval counters do.
    sql = specs()["gin_cleanup_lock_wait"].sql
    assert "system.lakebase.wait_event_counters" in sql and "active_session_history" not in sql


def test_counter_based_checks_wait_for_24_hours_of_statistics():
    s = specs()
    for i in ("unused_index", "seq_scan_heavy", "dead_tuples"):
        assert s[i].min_stats_hours == 24, i
    assert "INTERVAL 20 HOURS" in s["seq_scan_read_ratio"].sql
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_runner.py tests/unit/test_catalog.py -v -k "stats or young or min_stats"`
Expected: FAIL (`CheckSpec` has no `min_stats_hours`; the young check returns status `ok`).

- [ ] **Step 3: Parse `min_stats_hours`**

`engine/src/lakebase_health/checkspec.py`:

Replace (1 of 3):

```python
    query_scoped: bool = False  # reads per-query telemetry: platform roles are filtered out, evidence.queryid is a str
    fix: FixSpec | None = None
    area: str | None = None  # health report scorecard area (AREAS); None only for the engine's meta checks

    def tables(self) -> set:
        return set(re.findall(r"system\.lakebase\.([a-z_]+)", self.sql or ""))
```

with:

```python
    query_scoped: bool = False  # reads per-query telemetry: platform roles are filtered out, evidence.queryid is a str
    fix: FixSpec | None = None
    area: str | None = None  # health report scorecard area (AREAS); None only for the engine's meta checks
    min_stats_hours: float | None = None  # counter-based postgres check: not run until stats are this old (h)

    def tables(self) -> set:
        return set(re.findall(r"system\.lakebase\.([a-z_]+)", self.sql or ""))
```

Replace (2 of 3):

```python
    query_scoped = data.get("query_scoped", False)
    if query_scoped not in (True, False) or (query_scoped and source != "delta"):
        errors.append("query_scoped must be true or false, and true only on a delta check")
    area = data.get("area")
    if area is not None and area not in AREAS:
        errors.append(f"area must be one of {list(AREAS)}")
```

with:

```python
    query_scoped = data.get("query_scoped", False)
    if query_scoped not in (True, False) or (query_scoped and source != "delta"):
        errors.append("query_scoped must be true or false, and true only on a delta check")
    min_stats_hours = data.get("min_stats_hours")
    if min_stats_hours is not None and (source != "postgres" or isinstance(min_stats_hours, bool)
                                        or not isinstance(min_stats_hours, (int, float)) or min_stats_hours <= 0):
        errors.append("min_stats_hours must be a positive number, on a postgres check")
    area = data.get("area")
    if area is not None and area not in AREAS:
        errors.append(f"area must be one of {list(AREAS)}")
```

Replace (3 of 3):

```python
                     schedule=schedule, guards=guards, requires_extension=data.get("requires_extension"),
                     replica_check=replica_check, per_database=per_database,
                     query_scoped=query_scoped, title_template=data.get("title_template"),
                     superseded_by=superseded_by, fix=fix, area=area)


def load_checks(directory: Path, api_functions: frozenset) -> list:
```

with:

```python
                     schedule=schedule, guards=guards, requires_extension=data.get("requires_extension"),
                     replica_check=replica_check, per_database=per_database,
                     query_scoped=query_scoped, title_template=data.get("title_template"),
                     superseded_by=superseded_by, fix=fix, area=area,
                     min_stats_hours=float(min_stats_hours) if min_stats_hours is not None else None)


def load_checks(directory: Path, api_functions: frozenset) -> list:
```

- [ ] **Step 4: Skip young counters in the runner**

`engine/src/lakebase_health/runner.py`:

Replace (1 of 2):

```python


INACTIVE = "skipped:compute_inactive"


class _Suspended(Exception):
```

with:

```python


INACTIVE = "skipped:compute_inactive"
STATS_YOUNG = "skipped:stats_young"


def _stats_young(spec, ctx) -> str | None:
    """Why a counter-based check must not run yet (spec min_stats_hours), or None when its counters are old enough."""
    if not spec.min_stats_hours:
        return None
    h = ctx.get("stats_window_hours")
    if h is None:
        return "counters too young (reset time unknown)"
    return f"counters too young (reset {h:.0f} h ago)" if h < spec.min_stats_hours else None


class _Suspended(Exception):
```

Replace (2 of 2):

```python
                        if not self._due(spec, now, last_ok):
                            o.status = "skipped:not_scheduled"
                            continue
                        if spec.requires_extension and not session.has_extension(spec.requires_extension):
                            o.status = "skipped:extension_missing"
                            missing.append({"object": f"{spec.requires_extension}", "measure": 1.0,
```

with:

```python
                        if not self._due(spec, now, last_ok):
                            o.status = "skipped:not_scheduled"
                            continue
                        young = _stats_young(spec, ctx)
                        if young:  # one young database skips the endpoint: partial results would resolve the rest
                            o.status, o.error, o.results = STATS_YOUNG, young, []
                            continue
                        if spec.requires_extension and not session.has_extension(spec.requires_extension):
                            o.status = "skipped:extension_missing"
                            missing.append({"object": f"{spec.requires_extension}", "measure": 1.0,
```

- [ ] **Step 5: Mark the counter-based checks**

`engine/src/lakebase_health/checks/postgres/unused_index.yaml`:

Replace (1 of 2):

```yaml
category: indexing
area: indexes
source: postgres
replica_check: intersect
guards: ["stats_window_hours >= 24"]
superseded_by:   # same index, same DROP; the duplicate/redundant finding also names the index that stays
```

with:

```yaml
category: indexing
area: indexes
source: postgres
min_stats_hours: 24   # counter-based: not run until stats_reset / postmaster start is 24 h old
replica_check: intersect
guards: ["stats_window_hours >= 24"]
superseded_by:   # same index, same DROP; the duplicate/redundant finding also names the index that stays
```

Replace (2 of 2):

```yaml
  and on every read replica of the branch (usage counters are per compute, so an index can serve only replica
  reads). It costs writes and storage. Confidence is low when a replica could not be read or its counters were
  reset recently; only trust this when the statistics window covers a full business cycle.
sql: |
  SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS object,
         quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS index_ident,
```

with:

```yaml
  and on every read replica of the branch (usage counters are per compute, so an index can serve only replica
  reads). It costs writes and storage. Confidence is low when a replica could not be read or its counters were
  reset recently; only trust this when the statistics window covers a full business cycle.
  Not run until the statistics are 24 h old (stats reset or compute start).
sql: |
  SELECT quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS object,
         quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) AS index_ident,
```

`engine/src/lakebase_health/checks/postgres/seq_scan_heavy.yaml`:

Replace:

```yaml
category: indexing
area: indexes
source: postgres
guards: ["stats_window_hours >= 24"]
thresholds: {warning: 10000, critical: 1000000}
explain: >
  Average rows read per sequential scan on a table with more than 10k live rows. Find the queries scanning it
  (top_db_time_query) and index their filter and join columns; verify the index on a branch.
sql: |
  SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object, seq_scan, idx_scan, n_live_tup,
         seq_tup_read, (seq_tup_read::float8 / NULLIF(seq_scan, 0)) AS measure,
```

with:

```yaml
category: indexing
area: indexes
source: postgres
min_stats_hours: 24   # counter-based: not run until stats_reset / postmaster start is 24 h old
guards: ["stats_window_hours >= 24"]
thresholds: {warning: 10000, critical: 1000000}
explain: >
  Average rows read per sequential scan on a table with more than 10k live rows. Find the queries scanning it
  (top_db_time_query) and index their filter and join columns; verify the index on a branch.
  Not run until the statistics are 24 h old (stats reset or compute start).
sql: |
  SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object, seq_scan, idx_scan, n_live_tup,
         seq_tup_read, (seq_tup_read::float8 / NULLIF(seq_scan, 0)) AS measure,
```

`engine/src/lakebase_health/checks/postgres/dead_tuples.yaml`:

Replace:

```yaml
category: maintenance
area: vacuum_xid
source: postgres
thresholds: {warning: 10, critical: 20}
explain: >
  Dead tuples as a share of all tuples on tables with more than 10k rows. Vacuum is falling behind or blocked;
  check autovacuum_blocker first, then tune autovacuum per table.
sql: |
  SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
         quote_ident(schemaname) || '.' || quote_ident(relname) AS table_ident,
```

with:

```yaml
category: maintenance
area: vacuum_xid
source: postgres
min_stats_hours: 24   # counter-based: not run until stats_reset / postmaster start is 24 h old
thresholds: {warning: 10, critical: 20}
explain: >
  Dead tuples as a share of all tuples on tables with more than 10k rows. Vacuum is falling behind or blocked;
  check autovacuum_blocker first, then tune autovacuum per table.
  Not run until the statistics are 24 h old (stats reset or compute start).
sql: |
  SELECT quote_ident(schemaname) || '.' || quote_ident(relname) AS object,
         quote_ident(schemaname) || '.' || quote_ident(relname) AS table_ident,
```

`engine/src/lakebase_health/checks/telemetry/seq_scan_read_ratio.yaml`:

Replace (1 of 2):

```yaml
thresholds: {warning: 10, critical: 100}
explain: >
  rows_returned (rows read, dominated by sequential scans) divided by rows_fetched (rows from index scans) per
  database over 24 h. A high ratio means missing indexes; see seq_scan_heavy and top_db_time_query.
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object, datname,
         sum(rows_returned) / NULLIF(sum(rows_fetched), 0) AS measure,
```

with:

```yaml
thresholds: {warning: 10, critical: 100}
explain: >
  rows_returned (rows read, dominated by sequential scans) divided by rows_fetched (rows from index scans) per
  database over 24 h. A high ratio means missing indexes; see seq_scan_heavy and top_db_time_query. Not reported
  for a database whose telemetry covers less than 20 of the 24 hours (a new or restarted compute).
sql: |
  SELECT project_id, branch_id, endpoint_id, endpoint_id || '/' || datname AS object, datname,
         sum(rows_returned) / NULLIF(sum(rows_fetched), 0) AS measure,
```

Replace (2 of 2):

```yaml
  FROM system.lakebase.database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, datname
  HAVING sum(rows_returned) > 1000000
```

with:

```yaml
  FROM system.lakebase.database_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY project_id, branch_id, endpoint_id, datname
  HAVING sum(rows_returned) > 1000000
     AND min(ts) <= current_timestamp() - INTERVAL 20 HOURS  -- counters young: telemetry covers < 20 of the 24 h
```

The existing `guards: ["stats_window_hours >= 24"]` on `unused_index` and `seq_scan_heavy` stay: the replica intersection still uses them to lower confidence when a replica's counters are young.

- [ ] **Step 6: Regenerate and run the tests**

Run: `PY=engine/.venv/bin/python bash scripts/regen_checks.sh && cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass (the notebook and `docs/CHECKS.md` carry the new explain text).

- [ ] **Step 7: Commit**

```bash
git add engine notebooks docs/CHECKS.md README.md app/server/report/checks.json
git commit -m "feat(engine): stats-reset guard: counter checks wait for 24 h of statistics" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 3: Synced-table helper and `synced_index_review`

One definition of "synced storage" (`synced.py`), spliced into check SQL as `{synced_tables}`. The index-drop checks skip synced storage, `synced_index_review` lists those indexes as info with no fix action, the runner names the synced table in the evidence of every Postgres finding on synced storage, and the action builder never proposes a DROP INDEX there.

**Files:**
- Create: `engine/src/lakebase_health/synced.py`, `engine/src/lakebase_health/checks/postgres/synced_index_review.yaml`
- Modify: `engine/src/lakebase_health/checkspec.py`, `engine/src/lakebase_health/runner.py`, `engine/src/lakebase_health/actions.py`
- Modify: `engine/src/lakebase_health/checks/postgres/{unused_index,duplicate_index,redundant_index}.yaml`
- Test: `engine/tests/unit/test_synced.py` (create), `engine/tests/unit/test_dedupe.py`, `engine/tests/unit/test_catalog.py`

**Interfaces:**
- Consumes: `render_sql(spec, excl)`, `Runner._run_endpoint`, `plan_actions(findings, existing, specs, now, gone)`.
- Produces: `synced.SYNCED_SCHEMA = "__db_system"`, `synced.SYNCED_TABLES_SQL` (subquery with `relid`, `synced_table`), `synced.SYNCED_MAP_SQL`, `synced_map(session) -> dict[str, str]` (identifier → synced table, `{}` on error), `synced_table_of(row, synced) -> str | None`, `annotate(rows, synced) -> list[dict]` (adds `synced_table`), `drops_synced_index(evidence: dict, sql: str | None) -> bool`; the `{synced_tables}` token in any check SQL; check `synced_index_review` (postgres, `area: indexes`, info, manual fix, evidence `synced_table`, `reason` (`unused`/`duplicate`), `duplicate_of`, `parent_index`); `plan_actions` expiry reason `synced_partition`. The collector (Task 5) uses `{synced_tables}` too.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_synced.py` (new file, complete):

```python
import json
from datetime import datetime, timezone

from lakebase_health.actions import plan_actions
from lakebase_health.checkspec import load_checks, parse_check, render_sql
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.models import Finding
from lakebase_health.synced import (SYNCED_MAP_SQL, SYNCED_TABLES_SQL, annotate, drops_synced_index, synced_map,
                                    synced_table_of)
from tests.unit.test_executors import EXCL, ep
from tests.unit.test_runner import FakePg, runner, spec

NOW = datetime(2026, 10, 29, tzinfo=timezone.utc)
SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}


class Session:
    def __init__(self, synced_rows, rows):
        self.synced_rows, self.rows, self.queries = synced_rows, rows, []

    def query(self, sql):
        self.queries.append(sql)
        return self.synced_rows if sql == SYNCED_MAP_SQL else self.rows

    def has_extension(self, name):
        return True

    def stats_window_hours(self):
        return 48.0


def test_synced_storage_is_a_db_system_partition_or_a_writer_owned_table():
    assert "pg_partition_root(c.oid)" in SYNCED_TABLES_SQL and "'__db_system'" in SYNCED_TABLES_SQL
    assert "databricks_writer_[0-9]+" in SYNCED_TABLES_SQL
    assert SYNCED_TABLES_SQL in SYNCED_MAP_SQL and "pg_index" in SYNCED_MAP_SQL


def test_the_synced_tables_token_is_rendered():
    s = parse_check({"id": "x", "title": "X", "category": "c", "source": "postgres", "explain": "e",
                     "thresholds": {"info": 0}, "sql": "SELECT 1 FROM {synced_tables} st"}, stem="x")
    assert render_sql(s, EXCL) == f"SELECT 1 FROM {SYNCED_TABLES_SQL} st"


def test_index_drop_checks_skip_synced_storage_and_the_review_check_covers_it():
    for i in ("unused_index", "duplicate_index", "redundant_index"):
        sql = render_sql(SPECS[i], EXCL)
        assert SYNCED_TABLES_SQL in sql and "st.relid IS NULL" in sql, i
    review = SPECS["synced_index_review"]
    assert review.thresholds == {"info": 0.0} and review.fix.kind == "manual" and not review.fix.actionable
    assert review.min_stats_hours == 24 and "parent_index" in review.sql and "pg_inherits" in review.sql


def test_synced_map_and_annotation():
    s = Session([{"ident": "__db_system.orders_p1", "synced_table": "public.orders"},
                 {"ident": "__db_system.orders_p1_idx", "synced_table": "public.orders"},
                 {"ident": None, "synced_table": "x"}], [])
    m = synced_map(s)
    assert m == {"__db_system.orders_p1": "public.orders", "__db_system.orders_p1_idx": "public.orders"}
    assert synced_table_of({"index_ident": "__db_system.orders_p1_idx"}, m) == "public.orders"
    rows = annotate([{"object": "__db_system.orders_p1"}, {"object": "public.other"}], m)
    assert rows == [{"object": "__db_system.orders_p1", "synced_table": "public.orders"}, {"object": "public.other"}]


def test_a_catalog_read_failure_maps_nothing():
    class Broken:
        def query(self, sql):
            raise RuntimeError("permission denied")
    assert synced_map(Broken()) == {}


def test_every_postgres_finding_on_synced_storage_names_the_synced_table():
    s = Session([{"ident": "__db_system.orders_p1", "synced_table": "public.orders"}],
                [{"object": "__db_system.orders_p1", "measure": 30.0}, {"object": "public.t", "measure": 30.0}])
    [o] = runner(pg=FakePg(s)).run([spec("dead", thresholds={"warning": 10})], [ep()], NOW, {}, set())
    by_obj = {r.object: r.evidence for r in o.results}
    assert by_obj["db:__db_system.orders_p1"]["synced_table"] == "public.orders"
    assert "synced_table" not in by_obj["db:public.t"]


def _finding(evidence, sql="DROP INDEX CONCURRENTLY __db_system.orders_p1_idx;"):
    return Finding("f1", "unused_index", "T", "indexing", "p", "b", "e", "db:__db_system.orders_p1_idx", "new",
                   "warning", "high", 1.0, NOW, NOW, NOW, 1, 0, json.dumps(evidence), None, "e", sql,
                   "CREATE INDEX CONCURRENTLY orders_p1_idx ON __db_system.orders_p1 USING btree (x);")


def test_no_drop_action_for_an_index_on_synced_storage():
    actions, events = plan_actions([_finding({"synced_table": "public.orders"})], {}, SPECS, NOW)
    assert actions == [] and events == []
    actions, _ = plan_actions([_finding({})], {}, SPECS, NOW)
    assert len(actions) == 1 and actions[0].sql.startswith("DROP INDEX")


def test_drops_synced_index():
    assert drops_synced_index({"synced_table": "public.orders"}, "  drop index concurrently x;")
    assert not drops_synced_index({"synced_table": "public.orders"}, "VACUUM (ANALYZE) x;")
    assert not drops_synced_index({}, "DROP INDEX x;") and not drops_synced_index(None, "DROP INDEX x;")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_synced.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'lakebase_health.synced'`.

- [ ] **Step 3: The helper**

`engine/src/lakebase_health/synced.py` (new file, complete):

```python
"""Synced tables (Unity Catalog tables synced into Lakebase).

Lakebase keeps a synced table's rows in storage partitions under the `__db_system` schema; the partition root
(`pg_partition_root()`) is the table the user sees. A table owned by a pipeline writer role (`databricks_writer_<n>`)
is a synced table too (an unpartitioned one is its own storage). Indexes on synced storage are managed by the sync:
Postgres refuses to drop a partition's index, and a refresh rebuilds them.
"""

SYNCED_SCHEMA = "__db_system"
WRITER_ROLE_RE = "^databricks_writer_[0-9]+$"

# relid -> synced table, for every relation that stores a synced table (a partition, or the table itself).
# Spliced into check SQL by checkspec.render_sql as the {synced_tables} token.
SYNCED_TABLES_SQL = (
    "(SELECT c.oid AS relid, quote_ident(rn.nspname) || '.' || quote_ident(r.relname) AS synced_table "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_class r ON r.oid = coalesce(pg_partition_root(c.oid), c.oid) "
    "JOIN pg_namespace rn ON rn.oid = r.relnamespace "
    "WHERE c.relkind IN ('r', 'p') "
    f"AND ((n.nspname = '{SYNCED_SCHEMA}' AND r.oid <> c.oid) OR pg_get_userbyid(r.relowner) ~ '{WRITER_ROLE_RE}'))"
)

# Every table and index identifier (quote_ident schema.name, the form check objects use) -> its synced table.
SYNCED_MAP_SQL = f"""
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, st.synced_table
FROM {SYNCED_TABLES_SQL} st JOIN pg_class c ON c.oid = st.relid JOIN pg_namespace n ON n.oid = c.relnamespace
UNION ALL
SELECT quote_ident(n.nspname) || '.' || quote_ident(ic.relname), st.synced_table
FROM {SYNCED_TABLES_SQL} st JOIN pg_index i ON i.indrelid = st.relid
JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_namespace n ON n.oid = ic.relnamespace
"""

# Result columns that may hold a table or index identifier, besides `object`
_IDENT_KEYS = ("object", "table_ident", "index_ident", "table_name")


def synced_map(session) -> dict:
    """{identifier: synced table} for one database; {} when it cannot be read (the checks still run)."""
    try:
        return {r["ident"]: r["synced_table"] for r in session.query(SYNCED_MAP_SQL)
                if r.get("ident") and r.get("synced_table")}
    except Exception:
        return {}


def synced_table_of(row: dict, synced: dict) -> str | None:
    for k in _IDENT_KEYS:
        v = row.get(k)
        if isinstance(v, str) and v in synced:
            return synced[v]
    return None


def annotate(rows: list, synced: dict) -> list:
    """Name the synced table on every result row about a synced table's storage (evidence.synced_table)."""
    if not synced:
        return rows
    out = []
    for row in rows:
        name = row.get("synced_table") or synced_table_of(row, synced)
        out.append({**row, "synced_table": name} if name else row)
    return out


def drops_synced_index(evidence, sql: str | None) -> bool:
    """A DROP INDEX on synced storage: never proposed as an action (Postgres refuses it; the sync rebuilds it)."""
    return bool(isinstance(evidence, dict) and evidence.get("synced_table") and sql
                and sql.lstrip().upper().startswith("DROP INDEX"))
```

- [ ] **Step 4: The `{synced_tables}` token**

`engine/src/lakebase_health/checkspec.py`:

Replace (1 of 2):

```python
import yaml

from .exclusions import KNOWN_TOKENS, Exclusions

SOURCES = {"delta", "postgres", "api"}
SCHEDULES = {"every_run", "daily"}
```

with:

```python
import yaml

from .exclusions import KNOWN_TOKENS, Exclusions
from .synced import SYNCED_TABLES_SQL

SOURCES = {"delta", "postgres", "api"}
SCHEDULES = {"every_run", "daily"}
```

Replace (2 of 2):

```python
    out = spec.sql or ""
    for name, value in excl.tokens().items():
        out = out.replace("{" + name + "}", value)
    return out


_CREATE_INDEX = re.compile(r"^(\s*CREATE\s+(?:UNIQUE\s+)?INDEX)\s+(?!CONCURRENTLY\b)", re.I)
```

with:

```python
    out = spec.sql or ""
    for name, value in excl.tokens().items():
        out = out.replace("{" + name + "}", value)
    return out.replace("{synced_tables}", SYNCED_TABLES_SQL)


_CREATE_INDEX = re.compile(r"^(\s*CREATE\s+(?:UNIQUE\s+)?INDEX)\s+(?!CONCURRENTLY\b)", re.I)
```

- [ ] **Step 5: Name the synced table in Postgres evidence**

`engine/src/lakebase_health/runner.py` (one catalog lookup per database session; a failed lookup maps nothing):

Replace (1 of 3):

```python
from .fingerprint import fingerprint
from .models import FLEET, CheckOutcome, CheckResult
from .severity import classify, guards_pass

DAILY_INTERVAL = timedelta(hours=23)
_LOCATION = ("project_id", "branch_id", "endpoint_id", "object", "query_text")
```

with:

```python
from .fingerprint import fingerprint
from .models import FLEET, CheckOutcome, CheckResult
from .severity import classify, guards_pass
from .synced import annotate, synced_map

DAILY_INTERVAL = timedelta(hours=23)
_LOCATION = ("project_id", "branch_id", "endpoint_id", "object", "query_text")
```

Replace (2 of 3):

```python
            try:
                with self.postgres.session(ep, db) as session:
                    ctx = {"stats_window_hours": session.stats_window_hours()}
                    for spec in specs:
                        o = acc[spec.id]
                        if o.status != "ok":
```

with:

```python
            try:
                with self.postgres.session(ep, db) as session:
                    ctx = {"stats_window_hours": session.stats_window_hours()}
                    synced = synced_map(session)
                    for spec in specs:
                        o = acc[spec.id]
                        if o.status != "ok":
```

Replace (3 of 3):

```python
                                            "extension": spec.requires_extension, "needed_by": spec.id, "db": db})
                            continue
                        try:
                            rows = session.query(render_sql(spec, self.excl))
                            prefix = f"{db}:" if spec.per_database else ""
                            o.results.extend(self._results(spec, rows, loc, prefix, ctx))
                            ran_once.add(spec.id)
```

with:

```python
                                            "extension": spec.requires_extension, "needed_by": spec.id, "db": db})
                            continue
                        try:
                            rows = annotate(session.query(render_sql(spec, self.excl)), synced)
                            prefix = f"{db}:" if spec.per_database else ""
                            o.results.extend(self._results(spec, rows, loc, prefix, ctx))
                            ran_once.add(spec.id)
```

- [ ] **Step 6: No DROP INDEX action on synced storage**

`engine/src/lakebase_health/actions.py`:

Replace (1 of 3):

```python
import hashlib
from datetime import timedelta

from .fixer.allowlist import runnable
from .models import Action

ACTION_TTL = timedelta(days=7)
FIXER_STATES = {"verifying", "verified", "verify_failed", "applying", "applied", "apply_failed", "regressed",
```

with:

```python
import hashlib
import json
from datetime import timedelta

from .fixer.allowlist import runnable
from .models import Action
from .synced import drops_synced_index

ACTION_TTL = timedelta(days=7)
FIXER_STATES = {"verifying", "verified", "verify_failed", "applying", "applied", "apply_failed", "regressed",
```

Replace (2 of 3):

```python
    return "L2"


def _action_id(fingerprint: str, sql: str) -> str:
    return hashlib.sha256(f"{fingerprint}\x1f{sql}".encode()).hexdigest()[:32]
```

with:

```python
    return "L2"


def _evidence(f) -> dict:
    try:
        e = json.loads(f.evidence) if isinstance(f.evidence, str) and f.evidence else f.evidence
    except ValueError:
        return {}
    return e if isinstance(e, dict) else {}


def _action_id(fingerprint: str, sql: str) -> str:
    return hashlib.sha256(f"{fingerprint}\x1f{sql}".encode()).hexdigest()[:32]
```

Replace (3 of 3):

```python
        fix = spec.fix if spec else None
        wanted, reason = None, "branch_gone" if f.fingerprint in gone else "finding_resolved"
        if f.status in ("new", "open") and fix is not None and fix.actionable and f.candidate_sql:
            if runnable_fix(f.candidate_sql, f.rollback_sql):
                wanted = _action_id(f.fingerprint, f.candidate_sql)
            else:
                reason = "not_runnable"  # outside the fixer allowlist: the app shows the SQL to apply by hand
```

with:

```python
        fix = spec.fix if spec else None
        wanted, reason = None, "branch_gone" if f.fingerprint in gone else "finding_resolved"
        if f.status in ("new", "open") and fix is not None and fix.actionable and f.candidate_sql:
            if drops_synced_index(_evidence(f), f.candidate_sql):
                reason = "synced_partition"  # the sync owns these indexes; the finding keeps its SQL as advice
            elif runnable_fix(f.candidate_sql, f.rollback_sql):
                wanted = _action_id(f.fingerprint, f.candidate_sql)
            else:
                reason = "not_runnable"  # outside the fixer allowlist: the app shows the SQL to apply by hand
```

- [ ] **Step 7: The index-drop checks skip synced storage**

`engine/src/lakebase_health/checks/postgres/unused_index.yaml`:

Replace:

```yaml
         s.relname AS table_name, s.idx_scan, pg_relation_size(s.indexrelid)::float8 AS measure,
         pg_get_indexdef(s.indexrelid) AS index_def
  FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
  WHERE s.idx_scan = 0 AND NOT i.indisunique AND NOT i.indisprimary AND i.indisvalid
    AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid)
    AND s.schemaname NOT IN {excluded_schemas}
fix:
```

with:

```yaml
         s.relname AS table_name, s.idx_scan, pg_relation_size(s.indexrelid)::float8 AS measure,
         pg_get_indexdef(s.indexrelid) AS index_def
  FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
  LEFT JOIN {synced_tables} st ON st.relid = s.relid  -- synced storage: synced_index_review instead
  WHERE st.relid IS NULL AND s.idx_scan = 0 AND NOT i.indisunique AND NOT i.indisprimary AND i.indisvalid
    AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid)
    AND s.schemaname NOT IN {excluded_schemas}
fix:
```

`engine/src/lakebase_health/checks/postgres/duplicate_index.yaml`:

Replace:

```yaml
           coalesce(i.indexprs::text, '') || '|' || coalesce(i.indpred::text, '') AS sig,
           quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, n.nspname
    FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname NOT IN {excluded_schemas}),
  r AS (SELECT d.*, row_number() OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS rn,
               first_value(ident) OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS kept
        FROM d)
```

with:

```yaml
           coalesce(i.indexprs::text, '') || '|' || coalesce(i.indpred::text, '') AS sig,
           quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, n.nspname
    FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN {synced_tables} st ON st.relid = i.indrelid  -- synced storage: synced_index_review instead
    WHERE n.nspname NOT IN {excluded_schemas} AND st.relid IS NULL),
  r AS (SELECT d.*, row_number() OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS rn,
               first_value(ident) OVER (PARTITION BY sig ORDER BY indisprimary DESC, indisunique DESC, ident) AS kept
        FROM d)
```

`engine/src/lakebase_health/checks/postgres/redundant_index.yaml`:

Replace:

```yaml
  JOIN pg_index b ON a.indrelid = b.indrelid AND a.indexrelid <> b.indexrelid
  JOIN pg_class ca ON ca.oid = a.indexrelid JOIN pg_namespace na ON na.oid = ca.relnamespace
  JOIN pg_class cb ON cb.oid = b.indexrelid JOIN pg_namespace nb ON nb.oid = cb.relnamespace
  WHERE ca.relam = cb.relam AND NOT a.indisunique AND NOT a.indisprimary AND b.indisvalid
    AND a.indpred IS NULL AND b.indpred IS NULL AND a.indexprs IS NULL AND b.indexprs IS NULL
    AND b.indkey::text LIKE a.indkey::text || ' %'
    AND na.nspname NOT IN {excluded_schemas}
```

with:

```yaml
  JOIN pg_index b ON a.indrelid = b.indrelid AND a.indexrelid <> b.indexrelid
  JOIN pg_class ca ON ca.oid = a.indexrelid JOIN pg_namespace na ON na.oid = ca.relnamespace
  JOIN pg_class cb ON cb.oid = b.indexrelid JOIN pg_namespace nb ON nb.oid = cb.relnamespace
  LEFT JOIN {synced_tables} st ON st.relid = a.indrelid  -- the sync owns indexes on synced storage
  WHERE st.relid IS NULL AND ca.relam = cb.relam AND NOT a.indisunique AND NOT a.indisprimary AND b.indisvalid
    AND a.indpred IS NULL AND b.indpred IS NULL AND a.indexprs IS NULL AND b.indexprs IS NULL
    AND b.indkey::text LIKE a.indkey::text || ' %'
    AND na.nspname NOT IN {excluded_schemas}
```

- [ ] **Step 8: The review check**

`engine/src/lakebase_health/checks/postgres/synced_index_review.yaml` (new file, complete). Unique, primary-key and constraint indexes are never listed; `parent_index` falls back to the index itself when the synced table is not partitioned:

```yaml
id: synced_index_review
title: "Index on a synced table to review"
category: indexing
area: indexes
source: postgres
min_stats_hours: 24   # counter-based: not run until stats_reset / postmaster start is 24 h old
thresholds: {info: 0}
explain: >
  An index on a synced table's storage that is never used (no scans since statistics reset) or duplicates another
  index on the same storage. The unused_index and duplicate_index checks skip synced tables because the sync owns
  their indexes: Postgres refuses to drop a partition's index and a full refresh rebuilds them. Confirm it stays
  unused across a full refresh cycle, then remove it from the synced table's definition (parent_index names the
  index to change). No fix action is proposed.
sql: |
  WITH st AS {synced_tables},
  ix AS (
    SELECT i.indexrelid, i.indrelid, st.synced_table,
           i.indrelid::text || '|' || i.indclass::text || '|' || i.indkey::text || '|' ||
           coalesce(i.indexprs::text, '') || '|' || coalesce(i.indpred::text, '') AS sig,
           quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident,
           i.indisunique OR i.indisprimary
             OR EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid) AS kept_always,
           coalesce(s.idx_scan, 0) AS idx_scan
    FROM pg_index i JOIN st ON st.relid = i.indrelid
    JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
    WHERE i.indisvalid AND n.nspname NOT IN {excluded_schemas}),
  r AS (SELECT ix.*, row_number() OVER (PARTITION BY sig ORDER BY kept_always DESC, ident) AS rn,
               first_value(ident) OVER (PARTITION BY sig ORDER BY kept_always DESC, ident) AS kept_index
        FROM ix)
  SELECT r.ident AS object, r.synced_table, r.idx_scan,
         CASE WHEN r.rn > 1 THEN 'duplicate' ELSE 'unused' END AS reason,
         CASE WHEN r.rn > 1 THEN r.kept_index END AS duplicate_of,
         coalesce((SELECT quote_ident(pn.nspname) || '.' || quote_ident(pc.relname)
                   FROM pg_inherits h JOIN pg_class pc ON pc.oid = h.inhparent
                   JOIN pg_namespace pn ON pn.oid = pc.relnamespace WHERE h.inhrelid = r.indexrelid), r.ident)
           AS parent_index,
         pg_relation_size(r.indexrelid)::float8 AS measure
  FROM r
  WHERE NOT r.kept_always AND (r.rn > 1 OR r.idx_scan = 0)
fix:
  kind: manual
  template: "Confirm {object} stays unused across a full refresh of {synced_table}, then remove {parent_index} from the synced table's definition; never drop it on the partition."
  risk: moderate
```

- [ ] **Step 9: Existing tests that count queries or list check ids**

`engine/tests/unit/test_dedupe.py` (the runner now runs one synced lookup per database session):

Replace (1 of 2):

```python
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.lifecycle import apply_run
from lakebase_health.models import CheckOutcome
from tests.unit.test_executors import ep
from tests.unit.test_runner import NOW, FakePg, FakeSession, runner, spec
```

with:

```python
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.lifecycle import apply_run
from lakebase_health.models import CheckOutcome
from lakebase_health.synced import SYNCED_MAP_SQL
from tests.unit.test_executors import ep
from tests.unit.test_runner import NOW, FakePg, FakeSession, runner, spec
```

Replace (2 of 2):

```python
        self.calls = 0

    def query(self, sql):
        self.calls += 1
        return super().query(sql)
```

with:

```python
        self.calls = 0

    def query(self, sql):
        if sql != SYNCED_MAP_SQL:  # the runner's synced-table lookup, once per database session
            self.calls += 1
        return super().query(sql)
```

`engine/tests/unit/test_catalog.py`:

Replace:

```python
    "xmin_horizon_holder", "long_transaction", "idle_in_transaction", "blocked_session", "prepared_transaction",
    "replication_slot_retention", "sequence_exhaustion", "stale_statistics", "public_schema_create",
    "privileged_roles", "table_without_pk", "non_ssl_sessions", "guardrail_settings", "config_drift",
    "autovacuum_blocker", "gin_pending_list", "oversized_search_index",
}
API = {"backup_restore_window", "default_branch_unprotected", "primary_without_ha", "password_roles"}
```

with:

```python
    "xmin_horizon_holder", "long_transaction", "idle_in_transaction", "blocked_session", "prepared_transaction",
    "replication_slot_retention", "sequence_exhaustion", "stale_statistics", "public_schema_create",
    "privileged_roles", "table_without_pk", "non_ssl_sessions", "guardrail_settings", "config_drift",
    "autovacuum_blocker", "gin_pending_list", "oversized_search_index", "synced_index_review",
}
API = {"backup_restore_window", "default_branch_unprotected", "primary_without_ha", "password_roles"}
```

- [ ] **Step 10: Regenerate and run the tests**

Run: `PY=engine/.venv/bin/python bash scripts/regen_checks.sh && cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass; the regen prints `65 checks (31 delta, 30 postgres, 4 api)`.

- [ ] **Step 11: Commit**

```bash
git add engine notebooks docs/CHECKS.md README.md app/server/report/checks.json
git commit -m "feat(engine): synced-table storage helper, synced_index_review, no DROP on synced indexes" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 4: `suspend_blockers` and per-endpoint values in check SQL

A Postgres check for idle client sessions that keep a scale-to-zero compute awake. "No suspend in 24 h" is "Postgres started more than 24 h ago"; the suspend timeout comes from the inventory through `{suspend_timeout_s}`, which the runner fills per endpoint (and the notebook from the endpoint it runs on).

**Files:**
- Create: `engine/src/lakebase_health/checks/postgres/suspend_blockers.yaml`
- Modify: `engine/src/lakebase_health/runner.py`, `scripts/build_notebook.py`
- Test: `engine/tests/unit/test_suspend_blockers.py` (create), `engine/tests/unit/test_dedupe.py`, `engine/tests/unit/test_catalog.py`

**Interfaces:**
- Consumes: `EndpointInfo.suspend_timeout_s` (inventory), the runner's per-database loop.
- Produces: `runner.SUSPEND_TOKEN = "{suspend_timeout_s}"`, `runner.endpoint_sql(sql, ep) -> str`, `runner.SCALE_TO_ZERO_OFF = "skipped:scale_to_zero_off"`; check `suspend_blockers` (postgres, `per_database: false`, `area: scale_to_zero`, warning, object `"<application> as <role>"`, evidence `role`, `application_name`, `idle_sessions`, `max_idle_s`, `suspend_timeout_s`, `hours_since_start`, never a pid).

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_suspend_blockers.py` (new file, complete):

```python
from lakebase_health.checkspec import load_checks, render_sql
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.runner import endpoint_sql
from tests.unit.test_executors import EXCL, ep
from tests.unit.test_runner import NOW, FakePg, runner

SPEC = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}["suspend_blockers"]
ROW = {"object": "worker as app_user", "role": "app_user", "application_name": "worker", "measure": 3.0,
       "idle_sessions": 3, "max_idle_s": 1800, "suspend_timeout_s": 300, "hours_since_start": 30}


class Session:
    def __init__(self, rows):
        self.rows, self.sql = rows, []

    def query(self, sql):
        self.sql.append(sql)
        return self.rows if "pg_stat_activity" in sql else []

    def has_extension(self, name):
        return True

    def stats_window_hours(self):
        return 48.0


def test_sql_groups_idle_clients_past_the_suspend_timeout_without_pids():
    sql = endpoint_sql(render_sql(SPEC, EXCL), ep(suspend_timeout_s=300))
    assert "make_interval(secs => 300)" in sql and "{suspend_timeout_s}" not in sql
    assert "pg_postmaster_start_time()" in sql and "interval '24 hours'" in sql
    assert "GROUP BY a.usename, a.application_name" in sql
    select = sql.split("FROM pg_stat_activity")[0]
    assert "pid" not in select.replace("pg_backend_pid", "")
    assert SPEC.area == "scale_to_zero" and SPEC.per_database is False


def test_raises_one_warning_per_role_and_application():
    s = Session([ROW])
    [o] = runner(pg=FakePg(s)).run([SPEC], [ep(suspend_timeout_s=300, databases=["a", "b"])], NOW, {}, set())
    assert o.status == "ok" and len(s.sql) == 3  # synced lookup per database opened, the check once
    [r] = o.results
    assert r.severity == "warning" and r.object == "worker as app_user" and r.evidence["idle_sessions"] == 3
    assert "300 s idle" in r.candidate_sql


def test_skipped_when_scale_to_zero_is_off():
    for off in (None, 0):
        [o] = runner(pg=FakePg(Session([ROW]))).run([SPEC], [ep(suspend_timeout_s=off)], NOW, {}, set())
        assert o.status == "skipped:scale_to_zero_off" and o.results == []
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_suspend_blockers.py -v`
Expected: FAIL with `KeyError: 'suspend_blockers'` (and `ImportError: cannot import name 'endpoint_sql'`).

- [ ] **Step 3: Fill the endpoint's suspend timeout in the runner**

`engine/src/lakebase_health/runner.py`:

Replace (1 of 2):

```python

INACTIVE = "skipped:compute_inactive"
STATS_YOUNG = "skipped:stats_young"


def _stats_young(spec, ctx) -> str | None:
```

with:

```python

INACTIVE = "skipped:compute_inactive"
STATS_YOUNG = "skipped:stats_young"
SCALE_TO_ZERO_OFF = "skipped:scale_to_zero_off"
SUSPEND_TOKEN = "{suspend_timeout_s}"  # filled in per endpoint from the inventory (suspend_blockers)


def endpoint_sql(sql: str, ep) -> str:
    return sql.replace(SUSPEND_TOKEN, str(int(ep.suspend_timeout_s or 0)))


def _stats_young(spec, ctx) -> str | None:
```

Replace (2 of 2):

```python
                        if young:  # one young database skips the endpoint: partial results would resolve the rest
                            o.status, o.error, o.results = STATS_YOUNG, young, []
                            continue
                        if spec.requires_extension and not session.has_extension(spec.requires_extension):
                            o.status = "skipped:extension_missing"
                            missing.append({"object": f"{spec.requires_extension}", "measure": 1.0,
                                            "extension": spec.requires_extension, "needed_by": spec.id, "db": db})
                            continue
                        try:
                            rows = annotate(session.query(render_sql(spec, self.excl)), synced)
                            prefix = f"{db}:" if spec.per_database else ""
                            o.results.extend(self._results(spec, rows, loc, prefix, ctx))
                            ran_once.add(spec.id)
```

with:

```python
                        if young:  # one young database skips the endpoint: partial results would resolve the rest
                            o.status, o.error, o.results = STATS_YOUNG, young, []
                            continue
                        if SUSPEND_TOKEN in (spec.sql or "") and not (ep.suspend_timeout_s or 0) > 0:
                            o.status = SCALE_TO_ZERO_OFF
                            continue
                        if spec.requires_extension and not session.has_extension(spec.requires_extension):
                            o.status = "skipped:extension_missing"
                            missing.append({"object": f"{spec.requires_extension}", "measure": 1.0,
                                            "extension": spec.requires_extension, "needed_by": spec.id, "db": db})
                            continue
                        try:
                            rows = annotate(session.query(endpoint_sql(render_sql(spec, self.excl), ep)), synced)
                            prefix = f"{db}:" if spec.per_database else ""
                            o.results.extend(self._results(spec, rows, loc, prefix, ctx))
                            ran_once.add(spec.id)
```

- [ ] **Step 4: The check**

`engine/src/lakebase_health/checks/postgres/suspend_blockers.yaml` (new file, complete). Engine, fixer and platform sessions are left out; synced-table writers and the Data API role stay in (their idle connections block a suspend too):

```yaml
id: suspend_blockers
title: "Idle clients keep the compute from scaling to zero"
category: connectivity
area: scale_to_zero
source: postgres
per_database: false   # pg_stat_activity is instance-wide
thresholds: {warning: 0}
explain: >
  Scale to zero is on, the compute has not suspended for 24 h (Postgres started more than 24 h ago) and client
  sessions have been idle for longer than the suspend timeout: open idle connections keep a compute awake. One
  result per role and application with session counts, never process ids. Set the pool's minimum idle connections
  to 0 or close idle connections in the client, and stop agents that poll the database.
sql: |
  SELECT coalesce(nullif(a.application_name, ''), '(no application_name)') || ' as ' || a.usename AS object,
         a.usename AS role, nullif(a.application_name, '') AS application_name,
         count(*)::float8 AS measure, count(*) AS idle_sessions,
         round(max(extract(epoch FROM now() - a.state_change)))::bigint AS max_idle_s,
         {suspend_timeout_s} AS suspend_timeout_s,
         round(extract(epoch FROM now() - pg_postmaster_start_time()) / 3600.0)::bigint AS hours_since_start
  FROM pg_stat_activity a
  WHERE a.backend_type = 'client backend' AND a.state = 'idle' AND a.pid <> pg_backend_pid()
    AND now() - a.state_change > make_interval(secs => {suspend_timeout_s})
    AND now() - pg_postmaster_start_time() > interval '24 hours'
    AND a.usename NOT IN {excluded_users} AND a.usename NOT IN ('databricks_control_plane', 'databricks_monitor')
    AND coalesce(a.application_name, '') NOT IN ('lakebase-health', 'lakebase-health-fixer')
  GROUP BY a.usename, a.application_name
fix: {kind: manual, template: "Set the minimum idle connections of the pool behind {object} to 0 (or close idle connections) and stop agents that poll the database; the compute can then suspend after {suspend_timeout_s} s idle.", risk: safe}
```

- [ ] **Step 5: The notebook fills the placeholder from its endpoint**

`scripts/build_notebook.py` (inside the `run_pg` template):

Replace:

```python
def run_pg(sql, check_id, measure, thresholds, direction, per_database=True, requires_extension=None, guards=()):
    """A Postgres check, read-only, on the chosen endpoint; in every database when per_database."""
    ep = _pg_endpoint()
    host = w.postgres.get_endpoint(name=ep.name).status.hosts.host
    branch = ep.name.split("/endpoints/")[0]
    dbs = [d.status.postgres_database for d in w.postgres.list_databases(parent=branch) if d.status]
    dbs = dbs if per_database else dbs[:1]
```

with:

```python
def run_pg(sql, check_id, measure, thresholds, direction, per_database=True, requires_extension=None, guards=()):
    """A Postgres check, read-only, on the chosen endpoint; in every database when per_database."""
    ep = _pg_endpoint()
    status = w.postgres.get_endpoint(name=ep.name).status
    host = status.hosts.host
    if "{suspend_timeout_s}" in sql:  # the engine fills in the endpoint's suspend timeout (suspend_blockers)
        t = getattr(status, "suspend_timeout_duration", None)
        secs = int(t.total_seconds()) if hasattr(t, "total_seconds") else int(float(str(t or "0").rstrip("s")))
        if secs <= 0:
            print(f"{ep.name}: skipped, scale to zero is off")
            return
        sql = sql.replace("{suspend_timeout_s}", str(secs))
    branch = ep.name.split("/endpoints/")[0]
    dbs = [d.status.postgres_database for d in w.postgres.list_databases(parent=branch) if d.status]
    dbs = dbs if per_database else dbs[:1]
```

- [ ] **Step 6: Existing tests that list check ids**

`engine/tests/unit/test_dedupe.py` (`suspend_blockers` reads `pg_stat_activity` once per endpoint):

Replace:

```python

CHECKS_DIR = Path(__file__).resolve().parents[2] / "src" / "lakebase_health" / "checks"
INSTANCE_WIDE = {"privileged_roles", "config_drift", "long_transaction", "idle_in_transaction", "blocked_session",
                 "prepared_transaction", "replication_slot_retention", "xmin_horizon_holder", "non_ssl_sessions"}


def packaged():
```

with:

```python

CHECKS_DIR = Path(__file__).resolve().parents[2] / "src" / "lakebase_health" / "checks"
INSTANCE_WIDE = {"privileged_roles", "config_drift", "long_transaction", "idle_in_transaction", "blocked_session",
                 "prepared_transaction", "replication_slot_retention", "xmin_horizon_holder", "non_ssl_sessions",
                 "suspend_blockers"}


def packaged():
```

`engine/tests/unit/test_catalog.py`:

Replace:

```python
    "replication_slot_retention", "sequence_exhaustion", "stale_statistics", "public_schema_create",
    "privileged_roles", "table_without_pk", "non_ssl_sessions", "guardrail_settings", "config_drift",
    "autovacuum_blocker", "gin_pending_list", "oversized_search_index", "synced_index_review",
}
API = {"backup_restore_window", "default_branch_unprotected", "primary_without_ha", "password_roles"}
```

with:

```python
    "replication_slot_retention", "sequence_exhaustion", "stale_statistics", "public_schema_create",
    "privileged_roles", "table_without_pk", "non_ssl_sessions", "guardrail_settings", "config_drift",
    "autovacuum_blocker", "gin_pending_list", "oversized_search_index", "synced_index_review",
    "suspend_blockers",
}
API = {"backup_restore_window", "default_branch_unprotected", "primary_without_ha", "password_roles"}
```

- [ ] **Step 7: Regenerate and run the tests**

Run: `PY=engine/.venv/bin/python bash scripts/regen_checks.sh && cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass; `66 checks (31 delta, 31 postgres, 4 api)`.

- [ ] **Step 8: Commit**

```bash
git add engine scripts/build_notebook.py notebooks docs/CHECKS.md README.md app/server/report/checks.json
git commit -m "feat(engine): suspend_blockers check; per-endpoint suspend timeout in check SQL" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 5: Inventory tables and the read-only collector

Seven snapshot tables and `pg_inventory.py`, which reads one active, enrolled endpoint through the existing read-only executor: per database the 25 largest tables, their indexes and per-table activity counters (deltas against the previous snapshot); once per endpoint the key settings with how to change them, the top statements by time and by I/O (no text), `pg_stat_io`, and endpoint metrics including the `neon_*` sources with their availability.

**Files:**
- Create: `engine/src/lakebase_health/pg_inventory.py`
- Modify: `engine/src/lakebase_health/ddl.py` **[LLM-merge]**, `engine/src/lakebase_health/checkspec.py`, `engine/src/lakebase_health/exclusions.py`
- Test: `engine/tests/unit/test_pg_inventory.py` (create)

**Interfaces:**
- Consumes: `PostgresExecutor.still_active(ep)`, `PostgresExecutor.session(ep, db)` → `PgSession.query(sql) -> list[dict]`, `PgSession.has_extension(name)`; `Exclusions.query_users`, `query_user_regex`; `synced.SYNCED_TABLES_SQL` via the `{synced_tables}` token; `Sql.insert_rows`, `Sql.execute(stmt, args)`, `Sql.query`.
- Produces: `checkspec.render_text(text, excl) -> str` (tokens for any engine SQL; `render_sql` now calls it); `Exclusions.platform_role_pg(col) -> str` (Postgres `~` form of `platform_role_sql`); `pg_inventory.INVENTORY_TABLES` (7 names), `RETENTION = timedelta(days=7)`, `Suspended`, `how_to_change(name, context) -> "session" | "role or database" | "support-managed"`, `lsn_bytes(lsn) -> int | None`, `neon_metrics(session) -> list[dict]`, `activity_deltas(rows, prev, had_previous, now) -> list[dict]`, `collect_endpoint(pg, ep, excl, prev, had_previous, now) -> dict[str, list[dict]]`, `previous_activity(sql, cfg) -> dict`, `collect_inventory(sql, cfg, pg, excl, inventory, run_id, now, errors) -> {"endpoints": int, "skipped": int}`; tables `inventory_tables`, `inventory_indexes`, `inventory_settings`, `inventory_statements`, `inventory_io`, `inventory_activity`, `inventory_endpoint` (all starting `run_id, collected_at, project_id, branch_id, endpoint_id`) and `audit_events (event_id, ts, actor, action, project_id, branch_id, detail)`. `inventory_endpoint.metric` values the app and checks read: `autovacuum_workers_busy`, `autovacuum_max_workers`, `oldest_xact_s`, `uptime_s`, `client_sessions`, `max_connections`, `server_version_num`, `server_version` (text in `note`), `idle_clients` (dims `role`, `application`, `max_idle_s`), `neon.lfc.<key>`, `neon.working_set_15m_bytes`, `neon.backpressure_throttling_us`, `neon.lag_bytes.<column>`, `neon.perf.<metric>` (dims `le`), and `source:<name>` (1 or 0, reason in `note`) for `neon`, the five neon sources, `pg_stat_io`, `pg_stat_statements`, `pg_stat_activity`.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_pg_inventory.py` (new file, complete):

```python
import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.ddl import TABLE_DDL
from lakebase_health.exclusions import Exclusions
from lakebase_health import pg_inventory as inv
from tests.fakes import FakeSql
from tests.unit.test_executors import ep

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
CFG = EngineConfig(catalog="cat")
EXCL = Exclusions(("s",), ("r",), ("cloud_admin",), ("pg_catalog",), "^(pg_)", "^neon_",
                  query_users=("cloud_admin", "sp-engine"), query_user_regex="^databricks_writer_[0-9]+$")
TABLE = {"schema_name": "hc_test", "table_name": "orders", "synced_table": None, "total_bytes": 81920,
         "n_live_tup": 1000, "n_dead_tup": 10}
ACTIVITY = {"schema_name": "hc_test", "table_name": "orders", "synced_table": None, "n_live_tup": 1000,
            "n_tup_ins": 1500, "n_tup_upd": 10, "n_tup_del": 0, "seq_scan": 5, "idx_scan": 40}
PROBE_ALL = {"version": "1.25", "lfc_stats": True, "working_set": True, "throttling": True, "lsns": True,
             "perf_counters": True}


class Session:
    """Answers each inventory query by a marker in its text; `fail` maps a marker to an exception."""

    def __init__(self, probe=PROBE_ALL, fail=(), statements=True, io=True):
        self.probe, self.fail, self.statements, self.io, self.sql = probe, dict(fail), statements, io, []

    def has_extension(self, name):
        return self.statements if name == "pg_stat_statements" else False

    def query(self, sql):
        self.sql.append(sql)
        for marker, exc in self.fail.items():
            if marker in sql:
                raise exc
        if "pg_total_relation_size(s.relid) AS total_bytes" in sql:
            return [dict(TABLE)]
        if "am.amname AS method" in sql:
            return [{"schema_name": "hc_test", "table_name": "orders", "index_name": "orders_pkey", "method": "btree",
                     "index_bytes": 16384, "idx_scan": 40, "is_valid": True, "is_primary": True, "is_unique": True,
                     "synced_table": None, "parent_index": None}]
        if "s.n_tup_ins, s.n_tup_upd" in sql:
            return [dict(ACTIVITY)]
        if "FROM pg_settings" in sql:
            return [{"name": "work_mem", "setting": "4096", "unit": "kB", "source": "default", "context": "user"},
                    {"name": "track_io_timing", "setting": "off", "unit": None, "source": "default",
                     "context": "superuser"},
                    {"name": "max_wal_size", "setting": "1024", "unit": "MB", "source": "configuration file",
                     "context": "sighup"}]
        if "pg_stat_statements s" in sql:
            return [{"ranking": "time", "rank": 1, "queryid": "42", "role": "app", "datname": "db", "calls": 10,
                     "mean_ms": 2.0, "total_ms": 20.0, "shared_blks_hit": 1, "shared_blks_read": 2,
                     "temp_blks_written": 0}]
        if "to_regclass('pg_catalog.pg_stat_io')" in sql:
            return [{"present": self.io}]
        if "FROM pg_stat_io" in sql:
            return [{"backend_type": "client backend", "object": "relation", "context": "normal", "reads": 5,
                     "writes": 0, "extends": 0, "hits": 100, "evictions": 0, "reuses": None, "fsyncs": 0,
                     "read_time": None, "write_time": None, "stats_reset": NOW}]
        if "autovacuum_workers_busy" in sql:
            return [{"autovacuum_workers_busy": 3.0, "autovacuum_max_workers": 3.0, "oldest_xact_s": 12.0,
                     "uptime_s": 90000.0, "client_sessions": 4.0, "max_connections": 100.0,
                     "server_version_num": 170005.0, "server_version": "17.5"}]
        if "AS max_idle_s" in sql:
            return [{"role": "app", "application": "worker", "sessions": 2.0, "max_idle_s": 900.0}]
        if "FROM pg_extension e JOIN pg_namespace n" in sql:
            return [dict(self.probe)] if self.probe else []
        if "neon_get_lfc_stats()" in sql:
            return [{"lfc_key": "file_cache_hits", "lfc_value": 900}, {"lfc_key": "file_cache_misses", "lfc_value": 100}]
        if "approximate_working_set_size_seconds(900)" in sql:
            return [{"pages": 1000.0, "block_size": 8192.0}]
        if "backpressure_throttling_time()" in sql:
            return [{"us": 0.0}]
        if "pg_current_wal_lsn()" in sql:
            return [{"lsn": "0/2000"}]
        if "FROM backpressure_lsns()" in sql:
            return [{"received_lsn": "0/1000", "disk_consistent_lsn": "0/800", "remote_consistent_lsn": "0/400"}]
        if "FROM neon_perf_counters" in sql:
            return [{"metric": "getpage_wait_seconds_bucket", "bucket_le": 0.001, "value": 10.0},
                    {"metric": "getpage_wait_seconds_count", "bucket_le": None, "value": 12.0}]
        return []


class Pg:
    def __init__(self, session, suspended_after=None):
        self.s, self.suspended_after, self.opened = session, suspended_after, []

    def still_active(self, e):
        return self.suspended_after is None or len(self.opened) < self.suspended_after

    @contextmanager
    def session(self, e, db):
        self.opened.append(db)
        yield self.s


def metrics(rows):
    return {r["metric"]: r for r in rows}


def test_every_inventory_table_is_created_with_the_snapshot_key():
    for t in inv.INVENTORY_TABLES:
        assert TABLE_DDL[t].startswith("run_id STRING, collected_at TIMESTAMP, project_id STRING, branch_id STRING, "
                                       "endpoint_id STRING"), t
    assert "audit_events" in TABLE_DDL


def test_collects_every_table_and_never_reads_query_text():
    s = Session()
    out = inv.collect_endpoint(Pg(s), ep(databases=["db"]), EXCL, {}, False, NOW)
    assert out["inventory_tables"][0]["datname"] == "db" and out["inventory_indexes"][0]["method"] == "btree"
    assert out["inventory_statements"][0]["queryid"] == "42" and out["inventory_io"][0]["hits"] == 100
    joined = "\n".join(s.sql)
    assert not re.search(r"\bquery(_text)?\b", joined)  # queryid only, never the statement text
    stmt = next(q for q in s.sql if "pg_stat_statements s" in q)
    assert "coalesce(r.rolname, '') IN ('cloud_admin', 'sp-engine')" in stmt
    assert "coalesce(r.rolname, '') ~ '^databricks_writer_[0-9]+$'" in stmt


def test_settings_say_how_each_one_can_be_changed():
    out = inv.collect_endpoint(Pg(Session()), ep(), EXCL, {}, False, NOW)
    how = {r["name"]: r["how_to_change"] for r in out["inventory_settings"]}
    assert how == {"work_mem": "session", "track_io_timing": "support-managed", "max_wal_size": "support-managed"}
    assert inv.how_to_change("statement_timeout", "user") == "session"
    assert inv.how_to_change("log_connections", "superuser-backend") == "role or database"
    sql = inv.SETTINGS_SQL
    assert "source NOT IN ('default', 'override')" in sql and "{excluded_settings_regex}" in sql


def test_instance_queries_run_once_and_tables_per_database():
    s = Session()
    out = inv.collect_endpoint(Pg(s), ep(databases=["a", "b"]), EXCL, {}, False, NOW)
    assert [r["datname"] for r in out["inventory_tables"]] == ["a", "b"]
    assert sum("FROM pg_settings" in q for q in s.sql) == 1 and sum("pg_stat_statements s" in q for q in s.sql) == 1


def test_neon_functions_are_read_when_present_and_readable():
    m = metrics(inv.collect_endpoint(Pg(Session()), ep(), EXCL, {}, False, NOW)["inventory_endpoint"])
    assert m["source:neon"]["value"] == 1 and m["source:neon"]["note"] == "neon 1.25"
    assert m["neon.lfc.file_cache_hits"]["value"] == 900 and m["neon.working_set_15m_bytes"]["value"] == 8192000
    assert m["neon.lag_bytes.received_lsn"]["value"] == 0x1000 and m["source:neon_perf_counters"]["value"] == 1
    assert m["neon.perf.getpage_wait_seconds_bucket"]["dims"] == '{"le": 0.001}'
    assert m["autovacuum_workers_busy"]["value"] == 3 and m["server_version"]["note"] == "17.5"
    assert m["idle_clients"]["dims"] == '{"application": "worker", "max_idle_s": 900.0, "role": "app"}'


def test_neon_sources_without_privilege_are_recorded_not_available():
    probe = {**PROBE_ALL, "perf_counters": False}
    m = metrics(inv.collect_endpoint(Pg(Session(probe=probe)), ep(), EXCL, {}, False, NOW)["inventory_endpoint"])
    assert m["source:neon_perf_counters"] == {"metric": "source:neon_perf_counters", "value": 0.0, "dims": None,
                                              "note": "not available: no SELECT on neon_perf_counters"}
    assert "neon.perf.getpage_wait_seconds_bucket" not in m and m["source:neon_get_lfc_stats"]["value"] == 1


def test_no_neon_extension_and_failing_reads_fall_back_quietly():
    m = metrics(inv.neon_metrics(Session(probe=None)))
    assert m["source:neon"]["note"] == "not available: extension neon not installed"
    assert all(m[f"source:{n}"]["value"] == 0 for n, _, _ in inv.NEON_SOURCES)
    m = metrics(inv.neon_metrics(Session(fail={"pg_current_wal_lsn()": RuntimeError("recovery is in progress")})))
    assert m["source:backpressure_lsns"]["note"] == "not available: RuntimeError"
    assert m["source:backpressure_throttling_time"]["value"] == 1


def test_missing_pg_stat_io_and_pg_stat_statements_are_recorded():
    out = inv.collect_endpoint(Pg(Session(statements=False, io=False)), ep(), EXCL, {}, False, NOW)
    m = metrics(out["inventory_endpoint"])
    assert out["inventory_io"] == [] and out["inventory_statements"] == []
    assert m["source:pg_stat_io"]["note"] == "not available: Postgres older than 16"
    assert m["source:pg_stat_statements"]["value"] == 0


def test_activity_deltas_since_the_previous_snapshot():
    at = NOW - timedelta(minutes=15)
    prev = {None: at, ("db", "hc_test", "orders"): {"n_tup_ins": 500, "n_tup_upd": 10, "n_tup_del": 0,
                                                    "seq_scan": 5, "idx_scan": 30, "collected_at": at}}
    [r] = inv.activity_deltas([{**ACTIVITY, "datname": "db"}], prev, True, NOW)
    assert (r["ins_delta"], r["idx_delta"], r["window_s"]) == (1000, 10, 900.0)
    reset = {**prev, ("db", "hc_test", "orders"): {**prev[("db", "hc_test", "orders")], "n_tup_ins": 5000}}
    [r] = inv.activity_deltas([{**ACTIVITY, "datname": "db"}], reset, True, NOW)
    assert r["ins_delta"] is None and r["window_s"] is None


def test_a_new_synced_partition_counts_from_zero_but_not_on_the_first_snapshot():
    part = {**ACTIVITY, "datname": "db", "table_name": "orders_p2", "synced_table": "public.orders"}
    [r] = inv.activity_deltas([part], {None: NOW - timedelta(minutes=15)}, True, NOW)
    assert r["ins_delta"] == 1500 and r["window_s"] == 900.0
    [r] = inv.activity_deltas([part], {}, False, NOW)
    assert r["ins_delta"] is None
    [r] = inv.activity_deltas([{**part, "synced_table": None}], {None: NOW}, True, NOW)
    assert r["ins_delta"] is None


def test_lsn_bytes():
    assert inv.lsn_bytes("16/B374D848") == (0x16 << 32) + 0xB374D848
    assert inv.lsn_bytes(None) is None and inv.lsn_bytes("zz/1") is None


def test_collect_inventory_writes_snapshots_skips_and_cleans_up():
    sql = FakeSql()
    endpoints = [ep(), ep(endpoint_id="e2", current_state="IDLE"), ep(endpoint_id="e3", enrolled=False),
                 ep(endpoint_id="e4", opted_out=True)]
    errors = []
    counts = inv.collect_inventory(sql, CFG, Pg(Session()), EXCL, endpoints, "run1", NOW, errors)
    assert counts == {"endpoints": 1, "skipped": 3} and errors == []
    [row] = sql.tables["cat.lakebase_health.inventory_tables"]
    assert row["run_id"] == "run1" and row["collected_at"] == NOW and row["endpoint_id"] == "e"
    assert row["project_id"] == "p" and row["branch_id"] == "b"
    deletes = sql.ran(r"DELETE FROM cat\.lakebase_health\.inventory_")
    assert len(deletes) == len(inv.INVENTORY_TABLES)
    assert all(a == {"cutoff": NOW - timedelta(days=7)} for s, a in sql.statements if s.startswith("DELETE"))


def test_a_compute_that_suspends_part_way_keeps_its_previous_snapshot():
    sql = FakeSql()
    pg = Pg(Session(), suspended_after=1)
    counts = inv.collect_inventory(sql, CFG, pg, EXCL, [ep(databases=["a", "b"])], "run1", NOW, [])
    assert counts == {"endpoints": 0, "skipped": 1} and pg.opened == ["a"]
    assert "cat.lakebase_health.inventory_tables" not in sql.tables


def test_an_endpoint_error_is_reported_and_the_others_still_collect():
    sql, errors = FakeSql(), []
    class Flaky(Pg):
        @contextmanager
        def session(self, e, db):
            if e.endpoint_id == "bad":
                raise RuntimeError("connection refused")
            yield self.s
    counts = inv.collect_inventory(sql, CFG, Flaky(Session()), EXCL, [ep(endpoint_id="bad"), ep()], "r", NOW, errors)
    assert counts["endpoints"] == 1 and errors == ["pg inventory bad: RuntimeError: connection refused"]


def test_previous_activity_reads_each_endpoints_last_snapshot_as_utc():
    naive = datetime(2026, 10, 29, 11, 45)
    sql = FakeSql(responses=[(r"QUALIFY collected_at = max\(collected_at\)",
                              [{"endpoint_id": "e", "datname": "db", "schema_name": "s", "table_name": "t",
                                "n_tup_ins": 1, "collected_at": naive}])])
    prev = inv.previous_activity(sql, CFG)
    assert prev["e"][None] == naive.replace(tzinfo=timezone.utc)
    assert prev["e"][("db", "s", "t")]["n_tup_ins"] == 1
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_pg_inventory.py -v`
Expected: FAIL with `ImportError: cannot import name 'pg_inventory' from 'lakebase_health'`.

- [ ] **Step 3: Token rendering for any engine SQL, and the Postgres form of the platform-role filter**

`engine/src/lakebase_health/checkspec.py`:

Replace:

```python
    return specs


def render_sql(spec: CheckSpec, excl: Exclusions) -> str:
    out = spec.sql or ""
    for name, value in excl.tokens().items():
        out = out.replace("{" + name + "}", value)
    return out.replace("{synced_tables}", SYNCED_TABLES_SQL)


_CREATE_INDEX = re.compile(r"^(\s*CREATE\s+(?:UNIQUE\s+)?INDEX)\s+(?!CONCURRENTLY\b)", re.I)
```

with:

```python
    return specs


def render_text(text: str, excl: Exclusions) -> str:
    """Fill the {excluded_*} and {synced_tables} tokens of any engine SQL (checks and inventory queries)."""
    out = text
    for name, value in excl.tokens().items():
        out = out.replace("{" + name + "}", value)
    return out.replace("{synced_tables}", SYNCED_TABLES_SQL)


def render_sql(spec: CheckSpec, excl: Exclusions) -> str:
    return render_text(spec.sql or "", excl)


_CREATE_INDEX = re.compile(r"^(\s*CREATE\s+(?:UNIQUE\s+)?INDEX)\s+(?!CONCURRENTLY\b)", re.I)
```

`engine/src/lakebase_health/exclusions.py`:

Replace:

```python
            parts.append(f"coalesce({col}, '') RLIKE {_lit(self.query_user_regex)}")
        return "(" + " OR ".join(parts) + ")" if parts else "(1 = 0)"

    def platform_app_sql(self, col: str) -> str:
        """SQL predicate, true when `col` is an application name the engine or fixer connects with."""
        return f"(coalesce({col}, '') IN {_sql_list(self.query_apps)})" if self.query_apps else "(1 = 0)"
```

with:

```python
            parts.append(f"coalesce({col}, '') RLIKE {_lit(self.query_user_regex)}")
        return "(" + " OR ".join(parts) + ")" if parts else "(1 = 0)"

    def platform_role_pg(self, col: str) -> str:
        """The platform_role_sql predicate in Postgres syntax (for queries run on the endpoint itself)."""
        parts = []
        if self.query_users:
            parts.append(f"coalesce({col}, '') IN {_sql_list(self.query_users)}")
        if self.query_user_regex:
            parts.append(f"coalesce({col}, '') ~ {_lit(self.query_user_regex)}")
        return "(" + " OR ".join(parts) + ")" if parts else "(1 = 0)"

    def platform_app_sql(self, col: str) -> str:
        """SQL predicate, true when `col` is an application name the engine or fixer connects with."""
        return f"(coalesce({col}, '') IN {_sql_list(self.query_apps)})" if self.query_apps else "(1 = 0)"
```

- [ ] **Step 4: The tables** **[LLM-merge]**

`engine/src/lakebase_health/ddl.py` (the LLM branch added `drafts`, `llm_calls` and `llm_status` after `metrics_daily`; the new entries go between `metrics_daily` and those, and `_INV` above `TABLE_DDL`):

Replace (1 of 2):

```python
TABLE_DDL = {
    "runs": "run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP, status STRING, step_seconds STRING, "
            "counts STRING, errors STRING",
```

with:

```python
_INV = "run_id STRING, collected_at TIMESTAMP, project_id STRING, branch_id STRING, endpoint_id STRING, "

TABLE_DDL = {
    "runs": "run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP, status STRING, step_seconds STRING, "
            "counts STRING, errors STRING",
```

Replace (2 of 2):

```python
    "action_runs": "run_id STRING, approval_id STRING, action_id STRING, kind STRING, started_at TIMESTAMP, "
                   "ended_at TIMESTAMP, outcome STRING, report STRING, branch STRING, error STRING",
    "metrics_daily": "day DATE, project_id STRING, endpoint_id STRING, metric STRING, value DOUBLE",
    # LLM query rewrites: one row per attempt, one per model call (hashes and token counts only), endpoint set-up
    "drafts": "draft_id STRING, finding_fingerprint STRING, attempt INT, action_id STRING, approval_id STRING, "
              "endpoint STRING, prompt_version STRING, context_hash STRING, rationale STRING, "
```

with:

```python
    "action_runs": "run_id STRING, approval_id STRING, action_id STRING, kind STRING, started_at TIMESTAMP, "
                   "ended_at TIMESTAMP, outcome STRING, report STRING, branch STRING, error STRING",
    "metrics_daily": "day DATE, project_id STRING, endpoint_id STRING, metric STRING, value DOUBLE",
    # Health report inventory: one snapshot per endpoint per analysis run (pg_inventory.py), kept 7 days
    "inventory_tables": _INV + "datname STRING, schema_name STRING, table_name STRING, synced_table STRING, "
                        "total_bytes BIGINT, data_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT, "
                        "n_live_tup BIGINT, n_dead_tup BIGINT, seq_scan BIGINT, idx_scan BIGINT, "
                        "heap_blks_read BIGINT, heap_blks_hit BIGINT, last_vacuum TIMESTAMP, "
                        "last_autovacuum TIMESTAMP, last_analyze TIMESTAMP, last_autoanalyze TIMESTAMP, "
                        "vacuum_count BIGINT, autovacuum_count BIGINT, analyze_count BIGINT, "
                        "autoanalyze_count BIGINT, vacuum_threshold DOUBLE, xid_age BIGINT, "
                        "autovacuum_overrides STRING",
    "inventory_indexes": _INV + "datname STRING, schema_name STRING, table_name STRING, index_name STRING, "
                         "method STRING, index_bytes BIGINT, idx_scan BIGINT, is_valid BOOLEAN, is_primary BOOLEAN, "
                         "is_unique BOOLEAN, synced_table STRING, parent_index STRING",
    "inventory_settings": _INV + "name STRING, setting STRING, unit STRING, source STRING, context STRING, "
                          "how_to_change STRING",
    "inventory_statements": _INV + "ranking STRING, rank INT, queryid STRING, role STRING, datname STRING, "
                            "calls BIGINT, mean_ms DOUBLE, total_ms DOUBLE, shared_blks_hit BIGINT, "
                            "shared_blks_read BIGINT, temp_blks_written BIGINT",
    "inventory_io": _INV + "backend_type STRING, object STRING, context STRING, reads BIGINT, writes BIGINT, "
                    "extends BIGINT, hits BIGINT, evictions BIGINT, reuses BIGINT, fsyncs BIGINT, read_time DOUBLE, "
                    "write_time DOUBLE, stats_reset TIMESTAMP",
    "inventory_activity": _INV + "datname STRING, schema_name STRING, table_name STRING, synced_table STRING, "
                          "n_live_tup BIGINT, n_tup_ins BIGINT, n_tup_upd BIGINT, n_tup_del BIGINT, seq_scan BIGINT, "
                          "idx_scan BIGINT, ins_delta BIGINT, upd_delta BIGINT, del_delta BIGINT, seq_delta BIGINT, "
                          "idx_delta BIGINT, window_s DOUBLE",
    "inventory_endpoint": _INV + "metric STRING, dims STRING, value DOUBLE, note STRING",
    # The app's report exports (its only write besides approvals)
    "audit_events": "event_id STRING, ts TIMESTAMP, actor STRING, action STRING, project_id STRING, "
                    "branch_id STRING, detail STRING",
    # LLM query rewrites: one row per attempt, one per model call (hashes and token counts only), endpoint set-up
    "drafts": "draft_id STRING, finding_fingerprint STRING, attempt INT, action_id STRING, approval_id STRING, "
              "endpoint STRING, prompt_version STRING, context_hash STRING, rationale STRING, "
```

No column masks: none of these tables holds query text (`audit_events.detail` is the file name and the list of unavailable sections).

- [ ] **Step 5: The collector**

`engine/src/lakebase_health/pg_inventory.py` (new file, complete):

```python
"""Inventory snapshots for the health report: one read-only pass per active, enrolled endpoint per analysis run.

Everything goes through the read-only Postgres executor (pg_monitor role, default_transaction_read_only), which never
wakes a compute: the state is re-read right before each connection, and an endpoint that suspends part-way keeps its
previous snapshot (nothing partial is written). Query text is never read. Rows older than 7 days are deleted.

The `neon` extension's functions are read when the extension is installed and the engine role may execute or select
them; each source records whether it was available (metric `source:<name>`, 1 or 0, with the reason in `note`).
Without them the report falls back to the system.lakebase telemetry (compute_counters, compute_gauges) for LFC,
working set and back-pressure, and to plan_history for getpage latency. Nothing here creates an extension or grants.
"""
import json
from datetime import timedelta, timezone

from .checkspec import render_text

RETENTION = timedelta(days=7)
TOP_TABLES = 25
TOP_STATEMENTS = 20
INVENTORY_TABLES = ("inventory_tables", "inventory_indexes", "inventory_settings", "inventory_statements",
                    "inventory_io", "inventory_activity", "inventory_endpoint")

KEY_SETTINGS = (
    "shared_buffers", "work_mem", "maintenance_work_mem", "effective_cache_size", "random_page_cost",
    "max_connections", "max_wal_size", "checkpoint_timeout", "default_statistics_target", "statement_timeout",
    "lock_timeout", "idle_in_transaction_session_timeout", "autovacuum_max_workers", "neon.file_cache_size_limit",
    "track_io_timing", "log_min_duration_statement", "shared_preload_libraries",
)
# Settings a customer cannot change on Lakebase, whatever their context says (spec: support-managed)
SUPPORT_MANAGED = frozenset({"track_io_timing", "log_min_duration_statement", "shared_preload_libraries",
                             "neon.file_cache_size_limit", "shared_buffers", "max_connections"})

_TOP = f"""SELECT s.relid FROM pg_stat_user_tables s
  WHERE s.schemaname NOT IN {{excluded_schemas}} ORDER BY pg_total_relation_size(s.relid) DESC LIMIT {TOP_TABLES}"""

TABLES_SQL = f"""
SELECT s.schemaname AS schema_name, s.relname AS table_name, st.synced_table,
       pg_total_relation_size(s.relid) AS total_bytes, pg_relation_size(s.relid) AS data_bytes,
       pg_indexes_size(s.relid) AS index_bytes,
       CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_total_relation_size(c.reltoastrelid) END AS toast_bytes,
       s.n_live_tup, s.n_dead_tup, s.seq_scan, coalesce(s.idx_scan, 0) AS idx_scan,
       io.heap_blks_read, io.heap_blks_hit,
       s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze,
       s.vacuum_count, s.autovacuum_count, s.analyze_count, s.autoanalyze_count,
       (coalesce(substring(array_to_string(c.reloptions, ',') FROM 'autovacuum_vacuum_threshold=([0-9]+)')::float8,
                 current_setting('autovacuum_vacuum_threshold')::float8)
        + coalesce(substring(array_to_string(c.reloptions, ',') FROM 'autovacuum_vacuum_scale_factor=([0-9.]+)')::float8,
                   current_setting('autovacuum_vacuum_scale_factor')::float8) * greatest(c.reltuples, 0))
         AS vacuum_threshold,
       CASE WHEN c.relkind = 'r' THEN age(c.relfrozenxid) END AS xid_age,
       (SELECT string_agg(o, ', ' ORDER BY o) FROM unnest(c.reloptions) o WHERE o LIKE 'autovacuum%')
         AS autovacuum_overrides
FROM ({_TOP}) t JOIN pg_stat_user_tables s ON s.relid = t.relid JOIN pg_class c ON c.oid = s.relid
LEFT JOIN pg_statio_user_tables io ON io.relid = s.relid
LEFT JOIN {{synced_tables}} st ON st.relid = s.relid
ORDER BY total_bytes DESC
"""

INDEXES_SQL = f"""
SELECT n.nspname AS schema_name, tc.relname AS table_name, ic.relname AS index_name, am.amname AS method,
       pg_relation_size(i.indexrelid) AS index_bytes, coalesce(s.idx_scan, 0) AS idx_scan,
       i.indisvalid AS is_valid, i.indisprimary AS is_primary, i.indisunique AS is_unique, st.synced_table,
       (SELECT quote_ident(pn.nspname) || '.' || quote_ident(pc.relname)
        FROM pg_inherits h JOIN pg_class pc ON pc.oid = h.inhparent JOIN pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE h.inhrelid = i.indexrelid) AS parent_index
FROM ({_TOP}) t JOIN pg_index i ON i.indrelid = t.relid
JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace JOIN pg_am am ON am.oid = ic.relam
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
LEFT JOIN {{synced_tables}} st ON st.relid = i.indrelid
ORDER BY index_bytes DESC LIMIT 200
"""

# Cumulative per-table counters; the deltas against the previous snapshot are computed in Python. Synced storage
# always comes first so a refreshed synced table is never cut by the LIMIT.
ACTIVITY_SQL = """
SELECT s.schemaname AS schema_name, s.relname AS table_name, st.synced_table, s.n_live_tup,
       s.n_tup_ins, s.n_tup_upd, s.n_tup_del, s.seq_scan, coalesce(s.idx_scan, 0) AS idx_scan
FROM pg_stat_user_tables s LEFT JOIN {synced_tables} st ON st.relid = s.relid
WHERE s.schemaname NOT IN {excluded_schemas}
ORDER BY (st.relid IS NOT NULL) DESC, s.n_tup_ins + s.n_tup_upd + s.n_tup_del DESC LIMIT 200
"""

SETTINGS_SQL = """
SELECT name, setting, unit, source, context FROM pg_settings
WHERE name IN ({names}) OR (source NOT IN ('default', 'override') AND name !~ {excluded_settings_regex})
ORDER BY name LIMIT 300
"""

STATEMENTS_SQL = """
WITH s AS (
  SELECT s.queryid, r.rolname AS role, d.datname, sum(s.calls) AS calls, sum(s.total_exec_time) AS total_ms,
         sum(s.total_exec_time) / nullif(sum(s.calls), 0) AS mean_ms, sum(s.shared_blks_hit) AS shared_blks_hit,
         sum(s.shared_blks_read) AS shared_blks_read, sum(s.temp_blks_written) AS temp_blks_written
  FROM pg_stat_statements s JOIN pg_roles r ON r.oid = s.userid JOIN pg_database d ON d.oid = s.dbid
  WHERE s.queryid IS NOT NULL AND NOT {platform}
  GROUP BY s.queryid, r.rolname, d.datname),
ranked AS (
  SELECT s.*, row_number() OVER (ORDER BY total_ms DESC) AS time_rank,
         row_number() OVER (ORDER BY shared_blks_read DESC) AS io_rank FROM s)
SELECT 'time' AS ranking, time_rank AS rank, queryid::text AS queryid, role, datname, calls, mean_ms, total_ms,
       shared_blks_hit, shared_blks_read, temp_blks_written FROM ranked WHERE time_rank <= {n}
UNION ALL
SELECT 'io', io_rank, queryid::text, role, datname, calls, mean_ms, total_ms,
       shared_blks_hit, shared_blks_read, temp_blks_written FROM ranked WHERE io_rank <= {n}
"""

IO_SQL = """
SELECT backend_type, object, context, reads, writes, extends, hits, evictions, reuses, fsyncs,
       CASE WHEN current_setting('track_io_timing') = 'on' THEN read_time END AS read_time,
       CASE WHEN current_setting('track_io_timing') = 'on' THEN write_time END AS write_time, stats_reset
FROM pg_stat_io
WHERE coalesce(reads, 0) + coalesce(writes, 0) + coalesce(extends, 0) + coalesce(hits, 0)
      + coalesce(evictions, 0) + coalesce(fsyncs, 0) > 0
"""

ENDPOINT_SQL = """
SELECT (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker')::float8
         AS autovacuum_workers_busy,
       current_setting('autovacuum_max_workers')::float8 AS autovacuum_max_workers,
       (SELECT extract(epoch FROM now() - min(xact_start)) FROM pg_stat_activity
        WHERE xact_start IS NOT NULL AND backend_type = 'client backend' AND pid <> pg_backend_pid())::float8
         AS oldest_xact_s,
       extract(epoch FROM now() - pg_postmaster_start_time())::float8 AS uptime_s,
       (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')::float8 AS client_sessions,
       current_setting('max_connections')::float8 AS max_connections,
       current_setting('server_version_num')::float8 AS server_version_num,
       current_setting('server_version') AS server_version
"""

IDLE_SQL = """
SELECT usename AS role, coalesce(nullif(application_name, ''), '(no application_name)') AS application,
       count(*)::float8 AS sessions, round(max(extract(epoch FROM now() - state_change)))::float8 AS max_idle_s
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND state = 'idle' AND pid <> pg_backend_pid()
  AND usename NOT IN {excluded_users} AND coalesce(application_name, '') NOT IN ('lakebase-health', 'lakebase-health-fixer')
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 50
"""

NEON_PROBE_SQL = """
SELECT e.extversion AS version,
  coalesce(has_function_privilege(to_regprocedure(quote_ident(n.nspname) || '.neon_get_lfc_stats()'), 'EXECUTE'),
           false) AS lfc_stats,
  coalesce(has_function_privilege(
           to_regprocedure(quote_ident(n.nspname) || '.approximate_working_set_size_seconds(integer)'), 'EXECUTE'),
           false) AS working_set,
  coalesce(has_function_privilege(to_regprocedure(quote_ident(n.nspname) || '.backpressure_throttling_time()'),
           'EXECUTE'), false) AS throttling,
  coalesce(has_function_privilege(to_regprocedure(quote_ident(n.nspname) || '.backpressure_lsns()'), 'EXECUTE'),
           false) AS lsns,
  coalesce(has_table_privilege(to_regclass(quote_ident(n.nspname) || '.neon_perf_counters'), 'SELECT'), false)
    AS perf_counters
FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'neon'
"""
NEON_LFC_SQL = "SELECT lfc_key, lfc_value FROM neon_get_lfc_stats() AS t(lfc_key text, lfc_value bigint)"
NEON_WORKING_SET_SQL = ("SELECT approximate_working_set_size_seconds(900)::float8 AS pages, "
                        "current_setting('block_size')::float8 AS block_size")
NEON_THROTTLING_SQL = "SELECT backpressure_throttling_time()::float8 AS us"
NEON_LSNS_SQL = "SELECT * FROM backpressure_lsns()"
CURRENT_LSN_SQL = "SELECT pg_current_wal_lsn()::text AS lsn"
NEON_PERF_SQL = "SELECT * FROM neon_perf_counters WHERE metric LIKE 'getpage%'"
# The five neon sources and the probe column that says whether the engine role may read each
NEON_SOURCES = (("neon_get_lfc_stats", "lfc_stats", "EXECUTE"),
                ("approximate_working_set_size_seconds", "working_set", "EXECUTE"),
                ("backpressure_throttling_time", "throttling", "EXECUTE"),
                ("backpressure_lsns", "lsns", "EXECUTE"),
                ("neon_perf_counters", "perf_counters", "SELECT"))
HAS_PG_STAT_IO_SQL = "SELECT to_regclass('pg_catalog.pg_stat_io') IS NOT NULL AS present"


class Suspended(Exception):
    """The compute suspended part-way: the endpoint keeps its previous snapshot."""


def how_to_change(name: str, context: str | None) -> str:
    if name in SUPPORT_MANAGED or context in ("postmaster", "sighup", "internal"):
        return "support-managed"
    return "session" if context == "user" else "role or database"


def lsn_bytes(lsn) -> int | None:
    """'16/B374D848' -> bytes; None for anything else."""
    if not isinstance(lsn, str) or "/" not in lsn:
        return None
    hi, lo = lsn.split("/", 1)
    try:
        return (int(hi, 16) << 32) + int(lo, 16)
    except ValueError:
        return None


def _num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _metric(name, value, note=None, dims=None) -> dict:
    return {"metric": name, "value": _num(value), "note": note,
            "dims": json.dumps(dims, sort_keys=True, default=str) if dims else None}


def _try(session, sql):
    """(rows, None) or ([], 'ErrorType'); one failed read never stops the snapshot."""
    try:
        return session.query(sql), None
    except Exception as e:
        return [], type(e).__name__


def neon_metrics(session) -> list:
    """The neon extension's endpoint-level values, or `source:*` rows saying why each one is not available."""
    probe, err = _try(session, NEON_PROBE_SQL)
    if err or not probe:
        why = f"not available: {err}" if err else "not available: extension neon not installed"
        return [_metric("source:neon", 0, why)] + [_metric(f"source:{n}", 0, why) for n, _, _ in NEON_SOURCES]
    p = probe[0]
    out = [_metric("source:neon", 1, f"neon {p.get('version')}")]
    readers = {
        "neon_get_lfc_stats": lambda: [_metric(f"neon.lfc.{r['lfc_key']}", r["lfc_value"])
                                       for r in session.query(NEON_LFC_SQL) if r.get("lfc_key")],
        "approximate_working_set_size_seconds": lambda: [
            _metric("neon.working_set_15m_bytes", r["pages"] * r["block_size"])
            for r in session.query(NEON_WORKING_SET_SQL) if r.get("pages") is not None],
        "backpressure_throttling_time": lambda: [_metric("neon.backpressure_throttling_us", r["us"])
                                                 for r in session.query(NEON_THROTTLING_SQL)],
        "backpressure_lsns": lambda: _lsn_lags(session),
        "neon_perf_counters": lambda: [
            _metric(f"neon.perf.{r['metric']}", r.get("value"),
                    dims={"le": r["bucket_le"]} if r.get("bucket_le") is not None else None)
            for r in session.query(NEON_PERF_SQL) if r.get("metric")],
    }
    for name, col, privilege in NEON_SOURCES:
        if not p.get(col):
            out.append(_metric(f"source:{name}", 0, f"not available: no {privilege} on {name}"))
            continue
        try:
            rows = readers[name]()
        except Exception as e:
            out.append(_metric(f"source:{name}", 0, f"not available: {type(e).__name__}"))
            continue
        out.append(_metric(f"source:{name}", 1))
        out.extend(rows)
    return out


def _lsn_lags(session) -> list:
    current = lsn_bytes((session.query(CURRENT_LSN_SQL) or [{}])[0].get("lsn"))  # raises on a replica: not read
    out = []
    for r in session.query(NEON_LSNS_SQL)[:1]:
        for k, v in r.items():
            b = lsn_bytes(v)
            if b is not None and current is not None:
                out.append(_metric(f"neon.lag_bytes.{k}", max(current - b, 0)))
    return out


def endpoint_metrics(session, excl) -> list:
    out = []
    rows, err = _try(session, ENDPOINT_SQL)
    for r in rows[:1]:
        for k, v in r.items():
            out.append(_metric(k, None, v) if k == "server_version" else _metric(k, v))
    if err:
        out.append(_metric("source:pg_stat_activity", 0, f"not available: {err}"))
    idle, err = _try(session, render_text(IDLE_SQL, excl))
    out += [_metric("idle_clients", r["sessions"], dims={"role": r["role"], "application": r["application"],
                                                          "max_idle_s": r["max_idle_s"]}) for r in idle]
    return out + neon_metrics(session)


def settings_rows(session, excl) -> list:
    names = ", ".join(f"'{n}'" for n in KEY_SETTINGS)
    rows = session.query(render_text(SETTINGS_SQL.replace("{names}", names), excl))
    return [{**r, "how_to_change": how_to_change(r["name"], r.get("context"))} for r in rows]


def statements_rows(session, excl) -> list:
    return session.query(STATEMENTS_SQL.format(platform=excl.platform_role_pg("r.rolname"), n=TOP_STATEMENTS))


def io_rows(session) -> tuple:
    present, err = _try(session, HAS_PG_STAT_IO_SQL)
    if err or not present or not present[0].get("present"):
        return [], _metric("source:pg_stat_io", 0, f"not available: {err or 'Postgres older than 16'}")
    return session.query(IO_SQL), _metric("source:pg_stat_io", 1)


def activity_deltas(rows: list, prev: dict, had_previous: bool, now) -> list:
    """Cumulative counters -> deltas since the previous snapshot of the same endpoint. A counter that went backwards
    (stats reset) gives no delta. A synced storage table missing from a previous snapshot (a new partition after a
    full refresh) counts from zero."""
    out = []
    for r in rows:
        p = prev.get((r.get("datname"), r["schema_name"], r["table_name"]))
        if p is None and had_previous and r.get("synced_table"):
            p = {"n_tup_ins": 0, "n_tup_upd": 0, "n_tup_del": 0, "seq_scan": 0, "idx_scan": 0,
                 "collected_at": prev.get(None)}
        d = {"ins_delta": None, "upd_delta": None, "del_delta": None, "seq_delta": None, "idx_delta": None,
             "window_s": None}
        if p is not None:
            pairs = [("ins_delta", "n_tup_ins"), ("upd_delta", "n_tup_upd"), ("del_delta", "n_tup_del"),
                     ("seq_delta", "seq_scan"), ("idx_delta", "idx_scan")]
            diffs = {k: (r.get(c) or 0) - (p.get(c) or 0) for k, c in pairs}
            if all(v >= 0 for v in diffs.values()):
                d.update(diffs)
                at = p.get("collected_at")
                d["window_s"] = (now - at).total_seconds() if at is not None else None
        out.append({**r, **d})
    return out


def collect_endpoint(pg, ep, excl, prev: dict, had_previous: bool, now) -> dict:
    """{inventory table short name: rows} for one endpoint. Raises Suspended if the compute stops part-way."""
    out = {t: [] for t in INVENTORY_TABLES}
    instance_done, statements_done = False, False
    for db in ep.databases:
        if not pg.still_active(ep):  # re-read right before connecting: never wake a compute
            raise Suspended(ep.endpoint_id)
        with pg.session(ep, db) as s:
            out["inventory_tables"] += [{**r, "datname": db} for r in s.query(render_text(TABLES_SQL, excl))]
            out["inventory_indexes"] += [{**r, "datname": db} for r in s.query(render_text(INDEXES_SQL, excl))]
            activity = [{**r, "datname": db} for r in s.query(render_text(ACTIVITY_SQL, excl))]
            out["inventory_activity"] += activity_deltas(activity, prev, had_previous, now)
            if not statements_done and s.has_extension("pg_stat_statements"):
                out["inventory_statements"] = statements_rows(s, excl)
                statements_done = True
            if not instance_done:
                out["inventory_settings"] = settings_rows(s, excl)
                out["inventory_io"], io_source = io_rows(s)
                out["inventory_endpoint"] = endpoint_metrics(s, excl) + [io_source]
                instance_done = True
    if instance_done and not statements_done:
        out["inventory_endpoint"].append(_metric("source:pg_stat_statements", 0,
                                                 "not available: extension pg_stat_statements not installed"))
    return out


def previous_activity(sql, cfg) -> dict:
    """endpoint id -> {(datname, schema, table): counters, None: collected_at} from each endpoint's last snapshot."""
    rows = sql.query(f"SELECT * FROM {cfg.table('inventory_activity')} "
                     "QUALIFY collected_at = max(collected_at) OVER (PARTITION BY endpoint_id)")
    out = {}
    for r in rows:
        at = r.get("collected_at")  # Spark returns naive UTC timestamps
        at = at.replace(tzinfo=timezone.utc) if at is not None and at.tzinfo is None else at
        r = {**r, "collected_at": at}
        by = out.setdefault(r["endpoint_id"], {None: at})
        by[(r.get("datname"), r.get("schema_name"), r.get("table_name"))] = r
    return out


def collect_inventory(sql, cfg, pg, excl, inventory, run_id, now, errors: list) -> dict:
    """Snapshot every active, enrolled, not opted-out endpoint; delete snapshots older than 7 days."""
    counts = {"endpoints": 0, "skipped": 0}
    try:
        prev_all = previous_activity(sql, cfg)
    except Exception as e:
        errors.append(f"pg inventory: previous snapshot: {type(e).__name__}: {e}"[:500])
        prev_all = {}
    for ep in inventory:
        if ep.opted_out or not ep.enrolled or not ep.active:
            counts["skipped"] += 1
            continue
        prev = prev_all.get(ep.endpoint_id, {})
        try:
            snapshot = collect_endpoint(pg, ep, excl, prev, bool(prev), now)
        except Suspended:
            counts["skipped"] += 1
            continue
        except Exception as e:
            errors.append(f"pg inventory {ep.endpoint_id}: {type(e).__name__}: {e}"[:500])
            continue
        key = {"run_id": run_id, "collected_at": now, "project_id": ep.project_id, "branch_id": ep.branch_id,
               "endpoint_id": ep.endpoint_id}
        for table, rows in snapshot.items():
            sql.insert_rows(cfg.table(table), [{**key, **r} for r in rows])
        counts["endpoints"] += 1
    for table in INVENTORY_TABLES:
        try:
            sql.execute(f"DELETE FROM {cfg.table(table)} WHERE collected_at < :cutoff", {"cutoff": now - RETENTION})
        except Exception as e:
            errors.append(f"pg inventory cleanup {table}: {type(e).__name__}: {e}"[:500])
    return counts
```

- [ ] **Step 6: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add engine/src/lakebase_health/pg_inventory.py engine/src/lakebase_health/ddl.py \
  engine/src/lakebase_health/checkspec.py engine/src/lakebase_health/exclusions.py engine/tests/unit/test_pg_inventory.py
git commit -m "feat(engine): read-only inventory collector, inventory tables and audit_events" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 6: Inventory in the analysis run

`run_engine` takes the snapshot after the endpoint inventory and before the checks (`autovacuum_saturation` and `synced_full_refresh` read this run's rows); a failure is recorded and the checks still run. The README gets the enrolment note for the optional `neon` grant.

**Files:**
- Modify: `engine/src/lakebase_health/jobs/run.py` **[LLM-merge]**, `README.md` **[LLM-merge]**
- Test: `engine/tests/unit/test_run.py` **[LLM-merge]** (append)

**Interfaces:**
- Consumes: `collect_inventory(sql, cfg, pg, excl, inventory, run_id, now, errors)` (Task 5); one `PostgresExecutor` now shared by the collector and the `Runner`.
- Produces: `runs.step_seconds` gains `pg_inventory`; `runs.counts` gains `inventory_endpoints`; error lines `pg inventory …` in `runs.errors`.

- [ ] **Step 1: Write the failing tests** **[LLM-merge]**

`engine/tests/unit/test_run.py`, imports (the LLM branch appended tests to this file but left the imports alone):

Replace (1 of 2):

```python
from datetime import datetime, timezone
from types import SimpleNamespace as NS

from lakebase_health.config import EngineConfig
from lakebase_health.jobs.run import run_engine
from tests.fakes import FakeSql
```

with:

```python
import json
from datetime import datetime, timezone
from types import SimpleNamespace as NS

from lakebase_health.config import EngineConfig
from lakebase_health.jobs import run as run_mod
from lakebase_health.jobs.run import run_engine
from tests.fakes import FakeSql
```

Replace (2 of 2):

```python
    run = run_engine(FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])]), fake_w(),
                     EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
    assert run["status"] == "ok_with_errors" and "rewrite postcheck: RuntimeError" in run["errors"]
```

with:

```python
    run = run_engine(FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])]), fake_w(),
                     EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
    assert run["status"] == "ok_with_errors" and "rewrite postcheck: RuntimeError" in run["errors"]


def test_the_inventory_snapshot_is_taken_before_the_checks(tmp_path, monkeypatch):
    write_check(tmp_path)
    order = []
    monkeypatch.setattr("lakebase_health.jobs.run.collect_inventory",
                        lambda *a, **k: order.append("inventory") or {"endpoints": 2, "skipped": 0})
    real_run = run_mod.Runner.run
    monkeypatch.setattr(run_mod.Runner, "run", lambda self, *a: order.append("checks") or real_run(self, *a))
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert order == ["inventory", "checks"]
    assert json.loads(run["counts"])["inventory_endpoints"] == 2 and "pg_inventory" in json.loads(run["step_seconds"])


def test_an_inventory_failure_is_reported_and_the_checks_still_run(tmp_path, monkeypatch):
    write_check(tmp_path)
    def boom(*a, **k):
        raise RuntimeError("warehouse gone")
    monkeypatch.setattr("lakebase_health.jobs.run.collect_inventory", boom)
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}]),
                             (r"raw_compute_gauges$", [{"project_id": "p", "branch_id": "b", "endpoint_id": "e",
                                                        "object": "o", "measure": 1.0}])])
    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW,
                     checks_dir=tmp_path)
    assert run["status"] == "ok_with_errors" and "pg inventory: RuntimeError: warehouse gone" in run["errors"]
    assert sql.tables["cat.lakebase_health.findings"][0]["check_id"] == "x"


def test_old_inventory_snapshots_are_deleted_each_run(tmp_path):
    write_check(tmp_path)
    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
    run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
    assert len(sql.ran(r"DELETE FROM cat\.lakebase_health\.inventory_\w+ WHERE collected_at < :cutoff")) == 7
```

The appended tests go at the end of the file, after whatever the LLM branch added last.

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_run.py -v -k inventory`
Expected: FAIL with `AttributeError: <module 'lakebase_health.jobs.run'> does not have the attribute 'collect_inventory'`.

- [ ] **Step 3: Collect before the checks** **[LLM-merge]**

`engine/src/lakebase_health/jobs/run.py` (the LLM branch changed the `postcheck` import line and the post-check block; these hunks touch neither):

Replace (1 of 2):

```python
from ..executors.postgres import PostgresExecutor
from ..ingest import ingest_all
from ..inventory import build_inventory
from ..fixer.postcheck import run_postcheck, run_rewrite_postcheck
from ..record import load_last_ok, persist, persist_regressions, resolve_rolled_back
from ..runner import Runner
```

with:

```python
from ..executors.postgres import PostgresExecutor
from ..ingest import ingest_all
from ..inventory import build_inventory
from ..pg_inventory import collect_inventory
from ..fixer.postcheck import run_postcheck, run_rewrite_postcheck
from ..record import load_last_ok, persist, persist_regressions, resolve_rolled_back
from ..runner import Runner
```

Replace (2 of 2):

```python

    t = time.monotonic()
    excl = load_exclusions(checks_dir / "exclusions.yaml").with_query_users(service_roles(cfg, me))
    specs = load_checks(checks_dir, frozenset(API_CHECKS))
    runner = Runner(DeltaExecutor(sql, cfg, excl), PostgresExecutor(w, me, connect), ApiExecutor(excl), excl)
    outcomes = runner.run(specs, inventory, now, load_last_ok(sql, cfg), stale)
    errors += [f"check {o.check_id} {o.scope}: {o.error}" for o in outcomes if o.status in ("error", "timeout")]
    steps["checks"] = time.monotonic() - t

    t = time.monotonic()
    counts = persist(sql, cfg, run["run_id"], now, outcomes, {s.id: s for s in specs}, inventory, branches)
    steps["record"] = time.monotonic() - t

    t = time.monotonic()
```

with:

```python

    t = time.monotonic()
    excl = load_exclusions(checks_dir / "exclusions.yaml").with_query_users(service_roles(cfg, me))
    pg = PostgresExecutor(w, me, connect)
    try:  # before the checks: autovacuum_saturation and synced_full_refresh read this run's snapshot
        inventory_counts = collect_inventory(sql, cfg, pg, excl, inventory, run["run_id"], now, errors)
    except Exception as e:
        inventory_counts = None
        errors.append(f"pg inventory: {type(e).__name__}: {e}")
    steps["pg_inventory"] = time.monotonic() - t

    t = time.monotonic()
    specs = load_checks(checks_dir, frozenset(API_CHECKS))
    runner = Runner(DeltaExecutor(sql, cfg, excl), pg, ApiExecutor(excl), excl)
    outcomes = runner.run(specs, inventory, now, load_last_ok(sql, cfg), stale)
    errors += [f"check {o.check_id} {o.scope}: {o.error}" for o in outcomes if o.status in ("error", "timeout")]
    steps["checks"] = time.monotonic() - t

    t = time.monotonic()
    counts = persist(sql, cfg, run["run_id"], now, outcomes, {s.id: s for s in specs}, inventory, branches)
    counts["inventory_endpoints"] = inventory_counts["endpoints"] if inventory_counts else 0
    steps["record"] = time.monotonic() - t

    t = time.monotonic()
```

- [ ] **Step 4: The enrolment note** **[LLM-merge]**

`README.md`, section "6. Enroll a project for Postgres checks". Replace:

```markdown
The app's **Postgres checks** column turns **on** once the role exists, at the next analysis run. The engine only
connects to computes that are already running.
```

with:

````markdown
The app's **Postgres checks** column turns **on** once the role exists, at the next analysis run. The engine only
connects to computes that are already running.

With the role, each analysis run also takes the health report's inventory snapshot (largest tables, indexes,
settings, top statements by query ID, `pg_stat_io`, autovacuum workers, idle clients). When the `neon` extension is
installed, `pg_monitor` can already run `neon_get_lfc_stats()`, `approximate_working_set_size_seconds()`,
`backpressure_throttling_time()` and `backpressure_lsns()`, and the engine reads them. Optional: the System tab's
endpoint-level getpage p99 (all page reads, not only sampled queries) needs `neon_perf_counters`, which `pg_monitor`
cannot read. Grant it, connected as the branch owner:

```sql
GRANT SELECT ON neon_perf_counters, neon_lfc_stats TO "<engine-sp-application-id>";
```

Without it the System tab says "not available: no SELECT on neon_perf_counters" and uses sampled queries. The engine
never runs `CREATE EXTENSION` or `GRANT` itself.
````

- [ ] **Step 5: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add engine/src/lakebase_health/jobs/run.py engine/tests/unit/test_run.py README.md
git commit -m "feat(engine): inventory snapshot in every analysis run, before the checks" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 7: Engine tables in telemetry SQL, and `autovacuum_saturation`

Telemetry checks may read the engine's own tables as `lakebase_health.<name>` (rewritten to the configured catalog and schema, like `system.lakebase.*`). The first one: all autovacuum workers busy in the last 3 snapshots while a table has stayed over its vacuum threshold for more than an hour.

**Files:**
- Modify: `engine/src/lakebase_health/executors/delta.py`
- Create: `engine/src/lakebase_health/checks/telemetry/autovacuum_saturation.yaml`
- Test: `engine/tests/unit/test_report_checks.py` (create), `engine/tests/unit/test_catalog.py`

**Interfaces:**
- Consumes: `inventory_endpoint` metrics `autovacuum_workers_busy`, `autovacuum_max_workers`; `inventory_tables.n_dead_tup`, `vacuum_threshold` (Task 5).
- Produces: `rewrite_tables` maps `lakebase_health.<name>` (not preceded by `.`, a word character or a backtick) to `cfg.table(name)`; check `autovacuum_saturation` (delta, `area: vacuum_xid`, warning when hours over threshold > 1, object `<db>:<schema>.<table>`, evidence `datname`, `table_ident`, `hours_over`, `n_dead_tup`, `vacuum_threshold`, `busy_workers`, `max_workers`; fix `ALTER TABLE … SET (autovacuum_vacuum_cost_limit = 2000)`, safe, rollback `RESET`; the fixer allowlist already accepts `autovacuum_*` table settings).

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_report_checks.py` (new file; Tasks 8 and 9 extend it). The SQL is parsed with sqlglot's Databricks dialect; its results are checked on the warehouse in Task 18:

```python
"""The report's telemetry checks: rendered SQL parses as Databricks SQL and reads the right tables.

Their semantics are exercised on the warehouse by the acceptance task (scripts/report_acceptance.py)."""
import pytest
import sqlglot

from lakebase_health.checkspec import load_checks
from lakebase_health.config import EngineConfig
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.executors.delta import rewrite_tables
from lakebase_health.jobs.run import CHECKS_DIR

SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
CFG = EngineConfig(catalog="cat", schema="lbh")
NEW_DELTA = ["autovacuum_saturation"]


def rendered(check_id):
    return rewrite_tables(SPECS[check_id].sql, CFG)


def test_engine_tables_are_rewritten_to_the_configured_schema():
    out = rewrite_tables("SELECT * FROM lakebase_health.inventory_endpoint e JOIN system.lakebase.plan_history p "
                         "JOIN lakebase_health.findings f", CFG)
    assert "cat.lbh.inventory_endpoint e" in out and "cat.lbh.raw_plan_history p" in out and "cat.lbh.findings f" in out
    assert rewrite_tables("SELECT * FROM cat.lakebase_health.findings", CFG) == "SELECT * FROM cat.lakebase_health.findings"


@pytest.mark.parametrize("check_id", NEW_DELTA)
def test_new_telemetry_check_sql_parses(check_id):
    sql = rendered(check_id)
    assert "lakebase_health." not in sql.replace("cat.lbh.", "")
    sqlglot.parse_one(sql, read="databricks")


def test_autovacuum_saturation_needs_three_busy_snapshots_and_an_hour_over_threshold():
    s = SPECS["autovacuum_saturation"]
    assert s.area == "vacuum_xid" and s.thresholds == {"warning": 1.0}
    assert "HAVING count(*) = 3" in s.sql and "busy >= max_workers" in s.sql
    assert "n_dead_tup > vacuum_threshold" in s.sql and "last_under" in s.sql
    assert s.fix.actionable and "autovacuum_vacuum_cost_limit" in s.fix.template
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_report_checks.py -v`
Expected: FAIL with `KeyError: 'autovacuum_saturation'` and `cat.lbh.inventory_endpoint` missing from the rewrite.

- [ ] **Step 3: Rewrite the engine's own tables**

`engine/src/lakebase_health/executors/delta.py`:

Replace (1 of 2):

```python


def rewrite_tables(sql_text: str, cfg, excl=None) -> str:
    """Point system.lakebase.* at the ingested raw tables. With `excl`, query telemetry drops platform roles."""
    def table(m):
        name, raw = m.group(1), cfg.raw(m.group(1))
        if excl is None:
```

with:

```python


def rewrite_tables(sql_text: str, cfg, excl=None) -> str:
    """Point system.lakebase.* at the ingested raw tables and lakebase_health.* at the engine's schema. With `excl`,
    query telemetry drops platform roles."""
    def table(m):
        name, raw = m.group(1), cfg.raw(m.group(1))
        if excl is None:
```

Replace (2 of 2):

```python
                    f"FROM {cfg.raw('plan_history')} WHERE ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS "
                    f"AND {excl.platform_role_sql('usename')}) x ON t.branch_id = x.branch_id AND t.userid = x.userid)")
        return raw
    return re.sub(r"\bsystem\.lakebase\.([a-z_]+)", table, sql_text)


class DeltaExecutor:
```

with:

```python
                    f"FROM {cfg.raw('plan_history')} WHERE ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS "
                    f"AND {excl.platform_role_sql('usename')}) x ON t.branch_id = x.branch_id AND t.userid = x.userid)")
        return raw
    out = re.sub(r"\bsystem\.lakebase\.([a-z_]+)", table, sql_text)
    # The engine's own tables (inventory snapshots, findings), written as lakebase_health.<name> in check SQL
    return re.sub(r"(?<![\w.`])lakebase_health\.([a-z_]+)\b", lambda m: cfg.table(m.group(1)), out)


class DeltaExecutor:
```

- [ ] **Step 4: The check**

`engine/src/lakebase_health/checks/telemetry/autovacuum_saturation.yaml` (new file, complete):

```yaml
id: autovacuum_saturation
title: "Every autovacuum worker busy while a table waits"
category: maintenance
area: vacuum_xid
source: delta
thresholds: {warning: 1}
explain: >
  All autovacuum workers were busy in the last 3 inventory snapshots (one per analysis run) while this table has
  stayed over its vacuum threshold (dead tuples > threshold + scale factor x rows) for more than an hour (measure =
  hours). Vacuum cannot keep up. Let the workers go faster on this table (per-table autovacuum_vacuum_cost_limit,
  the fix below), vacuum it by hand in a quiet period, or reduce dead tuples at the source (fewer UPDATEs of
  indexed columns, batched deletes). autovacuum_max_workers is support-managed.
sql: |
  WITH snap AS (
    SELECT project_id, branch_id, endpoint_id, collected_at,
           max(CASE WHEN metric = 'autovacuum_workers_busy' THEN value END) AS busy,
           max(CASE WHEN metric = 'autovacuum_max_workers' THEN value END) AS max_workers
    FROM lakebase_health.inventory_endpoint
    WHERE collected_at >= current_timestamp() - INTERVAL 3 HOURS
      AND metric IN ('autovacuum_workers_busy', 'autovacuum_max_workers')
    GROUP BY project_id, branch_id, endpoint_id, collected_at),
  last3 AS (
    SELECT * FROM snap QUALIFY row_number() OVER (PARTITION BY endpoint_id ORDER BY collected_at DESC) <= 3),
  saturated AS (
    SELECT project_id, branch_id, endpoint_id, max(busy) AS busy_workers, max(max_workers) AS max_workers,
           max(collected_at) AS latest
    FROM last3 GROUP BY project_id, branch_id, endpoint_id
    HAVING count(*) = 3 AND min(CASE WHEN busy >= max_workers AND max_workers > 0 THEN 1 ELSE 0 END) = 1),
  tbl AS (
    SELECT endpoint_id, datname, schema_name, table_name, collected_at, n_dead_tup, vacuum_threshold,
           n_dead_tup > vacuum_threshold AS over_threshold
    FROM lakebase_health.inventory_tables WHERE collected_at >= current_timestamp() - INTERVAL 3 HOURS),
  runs AS (
    SELECT endpoint_id, datname, schema_name, table_name, max(collected_at) AS last_at,
           max(CASE WHEN NOT over_threshold THEN collected_at END) AS last_under,
           max_by(over_threshold, collected_at) AS over_now, max_by(n_dead_tup, collected_at) AS n_dead_tup,
           max_by(vacuum_threshold, collected_at) AS vacuum_threshold
    FROM tbl GROUP BY endpoint_id, datname, schema_name, table_name),
  waiting AS (
    SELECT r.endpoint_id, r.datname, r.schema_name, r.table_name, r.last_at, r.n_dead_tup, r.vacuum_threshold,
           (unix_timestamp(r.last_at) - unix_timestamp(min(t.collected_at))) / 3600.0 AS hours_over
    FROM runs r JOIN tbl t ON t.endpoint_id = r.endpoint_id AND t.datname = r.datname
      AND t.schema_name = r.schema_name AND t.table_name = r.table_name
    WHERE r.over_now AND (r.last_under IS NULL OR t.collected_at > r.last_under)
    GROUP BY r.endpoint_id, r.datname, r.schema_name, r.table_name, r.last_at, r.n_dead_tup, r.vacuum_threshold)
  SELECT s.project_id, s.branch_id, s.endpoint_id,
         w.datname || ':' || w.schema_name || '.' || w.table_name AS object, w.datname,
         '"' || replace(w.schema_name, '"', '""') || '"."' || replace(w.table_name, '"', '""') || '"' AS table_ident,
         w.hours_over AS measure, round(w.hours_over, 2) AS hours_over, w.n_dead_tup,
         round(w.vacuum_threshold) AS vacuum_threshold, s.busy_workers, s.max_workers
  FROM saturated s JOIN waiting w ON w.endpoint_id = s.endpoint_id AND w.last_at = s.latest
fix:
  kind: ddl
  template: "ALTER TABLE {table_ident} SET (autovacuum_vacuum_cost_limit = 2000);"
  rollback: "ALTER TABLE {table_ident} RESET (autovacuum_vacuum_cost_limit);"
  risk: safe
  reversible: true
```

`engine/tests/unit/test_catalog.py`:

Replace:

```python
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
    "commit_quorum_wait", "hot_row_lock", "statement_timeouts", "gin_cleanup_lock_wait", "connection_drops",
}
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
```

with:

```python
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
    "commit_quorum_wait", "hot_row_lock", "statement_timeouts", "gin_cleanup_lock_wait", "connection_drops",
    "autovacuum_saturation",
}
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
```

- [ ] **Step 5: Regenerate and run the tests**

Run: `PY=engine/.venv/bin/python bash scripts/regen_checks.sh && cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass; `67 checks (32 delta, 31 postgres, 4 api)`.

- [ ] **Step 6: Commit**

```bash
git add engine notebooks docs/CHECKS.md README.md app/server/report/checks.json
git commit -m "feat(engine): telemetry checks can read engine tables; autovacuum_saturation" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 8: `synced_full_refresh`

An analysis window that inserted at least half of a synced table's rows (from `inventory_activity`) is a full refresh: information, or a warning when it overlapped a getpage latency spike on the endpoint. Evidence carries the WAL bytes and full-page images written meanwhile (from `pg_stat_statements_counters`, see the decision on the telemetry fallback).

**Files:**
- Create: `engine/src/lakebase_health/checks/telemetry/synced_full_refresh.yaml`
- Test: `engine/tests/unit/test_report_checks.py`, `engine/tests/unit/test_catalog.py`

**Interfaces:**
- Consumes: `inventory_activity` (`synced_table`, `ins_delta`, `n_live_tup`, `window_s`, `collected_at`; Task 5), `system.lakebase.plan_history`, `system.lakebase.pg_stat_statements_counters`.
- Produces: check `synced_full_refresh` (delta, `area: write_path`, measure 1 → info, 2 → warning, object `<db>:<synced table>`, evidence `synced_table`, `insert_pct`, `rows_inserted`, `seen_at`, `getpage_spike_buckets`, `wal_bytes`, `wal_fpi`). The spike rule is the same as Task 9's `getpage_latency_spike` (5-minute p99 > 5× the median of the 24 h p99s and > 20 ms, at least 5 executions); it is inlined here because both run in the same pass and neither can read the other's result.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_report_checks.py`:

Replace (1 of 2):

```python

SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
CFG = EngineConfig(catalog="cat", schema="lbh")
NEW_DELTA = ["autovacuum_saturation"]


def rendered(check_id):
```

with:

```python

SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
CFG = EngineConfig(catalog="cat", schema="lbh")
NEW_DELTA = ["autovacuum_saturation", "synced_full_refresh"]


def rendered(check_id):
```

Replace (2 of 2):

```python
    assert "HAVING count(*) = 3" in s.sql and "busy >= max_workers" in s.sql
    assert "n_dead_tup > vacuum_threshold" in s.sql and "last_under" in s.sql
    assert s.fix.actionable and "autovacuum_vacuum_cost_limit" in s.fix.template
```

with:

```python
    assert "HAVING count(*) = 3" in s.sql and "busy >= max_workers" in s.sql
    assert "n_dead_tup > vacuum_threshold" in s.sql and "last_under" in s.sql
    assert s.fix.actionable and "autovacuum_vacuum_cost_limit" in s.fix.template


def test_synced_full_refresh_is_info_or_warning_when_it_overlaps_a_getpage_spike():
    s = SPECS["synced_full_refresh"]
    assert s.area == "write_path" and s.thresholds == {"info": 0.0, "warning": 1.0}
    assert "synced_table IS NOT NULL" in s.sql and "ins_delta >= 0.5 * greatest(n_live_tup, 1)" in s.sql
    assert "p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20" in s.sql and "THEN 2.0 ELSE 1.0" in s.sql
    assert "wal_bytes" in s.sql and "wal_fpi" in s.sql and s.fix.kind == "manual"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_report_checks.py -v`
Expected: FAIL with `KeyError: 'synced_full_refresh'`.

- [ ] **Step 3: The check**

`engine/src/lakebase_health/checks/telemetry/synced_full_refresh.yaml` (new file, complete). A finding stays open for the 24 h its refresh window is in view, then resolves:

```yaml
id: synced_full_refresh
title: "Synced table fully refreshed"
category: throughput
area: write_path
source: delta
thresholds: {info: 0, warning: 1}
explain: >
  In the last 24 h one analysis window inserted at least half of a synced table's rows (inventory_activity
  deltas), the sign of a full refresh. A full refresh rewrites every row and index, floods WAL and evicts the
  cache; it is a warning when it overlapped a getpage latency spike on the same endpoint (measure 2), information
  otherwise (measure 1). Prefer triggered or continuous sync, drop unused secondary indexes from the synced
  table's definition first, and schedule full refreshes off-peak.
sql: |
  WITH refresh AS (
    SELECT project_id, branch_id, endpoint_id, datname, synced_table, collected_at, window_s, ins_delta,
           100.0 * ins_delta / greatest(n_live_tup, 1) AS insert_pct
    FROM lakebase_health.inventory_activity
    WHERE collected_at >= current_timestamp() - INTERVAL 24 HOURS AND synced_table IS NOT NULL
      AND window_s IS NOT NULL AND ins_delta > 0 AND ins_delta >= 0.5 * greatest(n_live_tup, 1)),
  pages AS (
    SELECT endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300) AS bucket, count(*) AS execs,
           percentile_approx(neon_getpage_wait_us / 1000.0 / neon_getpage_count, 0.99) AS p99_ms
    FROM system.lakebase.plan_history
    WHERE ts >= current_timestamp() - INTERVAL 48 HOURS AND neon_getpage_count > 0
    GROUP BY endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300)),
  base AS (SELECT endpoint_id, percentile_approx(p99_ms, 0.5) AS baseline_ms FROM pages GROUP BY endpoint_id),
  spikes AS (
    SELECT p.endpoint_id, p.bucket FROM pages p JOIN base b ON b.endpoint_id = p.endpoint_id
    WHERE p.execs >= 5 AND p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20),
  wal AS (
    SELECT endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300) AS bucket,
           sum(wal_bytes) AS wal_bytes, sum(wal_fpi) AS wal_fpi
    FROM system.lakebase.pg_stat_statements_counters WHERE ts >= current_timestamp() - INTERVAL 25 HOURS
    GROUP BY endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300)),
  windows AS (
    SELECT r.*, timestamp_seconds(unix_timestamp(r.collected_at) - r.window_s) AS window_start FROM refresh r),
  agg AS (
    SELECT w.project_id, w.branch_id, w.endpoint_id, w.datname, w.synced_table,
           max(CASE WHEN s.bucket IS NOT NULL THEN 1 ELSE 0 END) AS overlapped,
           round(max(w.insert_pct), 1) AS insert_pct, max(w.ins_delta) AS rows_inserted,
           min(w.window_start) AS first_start, max(w.collected_at) AS seen_at,
           count(DISTINCT s.bucket) AS getpage_spike_buckets
    FROM windows w
    LEFT JOIN spikes s ON s.endpoint_id = w.endpoint_id AND s.bucket >= w.window_start AND s.bucket <= w.collected_at
    GROUP BY w.project_id, w.branch_id, w.endpoint_id, w.datname, w.synced_table)
  SELECT a.project_id, a.branch_id, a.endpoint_id, a.datname || ':' || a.synced_table AS object, a.datname,
         a.synced_table, CASE WHEN a.overlapped = 1 THEN 2.0 ELSE 1.0 END AS measure, a.insert_pct,
         a.rows_inserted, a.seen_at, a.getpage_spike_buckets, sum(x.wal_bytes) AS wal_bytes, sum(x.wal_fpi) AS wal_fpi
  FROM agg a
  LEFT JOIN wal x ON x.endpoint_id = a.endpoint_id AND x.bucket >= a.first_start AND x.bucket <= a.seen_at
  GROUP BY a.project_id, a.branch_id, a.endpoint_id, a.datname, a.synced_table, a.overlapped, a.insert_pct,
           a.rows_inserted, a.seen_at, a.getpage_spike_buckets
fix: {kind: manual, template: "Switch {synced_table} to triggered or continuous sync, drop unused secondary indexes from its definition before the next refresh, and schedule full refreshes off-peak.", risk: moderate}
```

`engine/tests/unit/test_catalog.py`:

Replace:

```python
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
    "commit_quorum_wait", "hot_row_lock", "statement_timeouts", "gin_cleanup_lock_wait", "connection_drops",
    "autovacuum_saturation",
}
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
```

with:

```python
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
    "commit_quorum_wait", "hot_row_lock", "statement_timeouts", "gin_cleanup_lock_wait", "connection_drops",
    "autovacuum_saturation", "synced_full_refresh",
}
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
```

- [ ] **Step 4: Regenerate and run the tests**

Run: `PY=engine/.venv/bin/python bash scripts/regen_checks.sh && cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass; `68 checks (33 delta, 31 postgres, 4 api)`.

- [ ] **Step 5: Commit**

```bash
git add engine notebooks docs/CHECKS.md README.md app/server/report/checks.json
git commit -m "feat(engine): synced_full_refresh check" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 9: `wal_burst` and `getpage_latency_spike`

Two 5-minute-resolution telemetry checks. `compute_counters` has neither WAL nor a getpage histogram (checked 2026-10-29), so WAL comes from `pg_stat_statements_counters` (and names the statement that wrote most of the worst burst) and getpage latency from `plan_history`.

**Files:**
- Create: `engine/src/lakebase_health/checks/telemetry/wal_burst.yaml`, `engine/src/lakebase_health/checks/telemetry/getpage_latency_spike.yaml`
- Test: `engine/tests/unit/test_report_checks.py`, `engine/tests/unit/test_catalog.py`

**Interfaces:**
- Consumes: `system.lakebase.pg_stat_statements_counters` (`wal_bytes`, `wal_fpi`, `queryid`, 30–60 s samples), `system.lakebase.plan_history` (`neon_getpage_wait_us`, `neon_getpage_count`).
- Produces: check `wal_burst` (delta, `area: write_path`, measure = bursts in 24 h, info > 0, warning > 3; evidence `max_burst_bytes`, `max_burst_fpi`, `median_bytes`, `worst_at`, `top_wal_queryid`); check `getpage_latency_spike` (delta, `area: storage_latency`, measure = worst 5-minute p99 ms, warning > 20; evidence `spike_windows`, `baseline_ms`, `worst_at`, `worst_ratio`, `source = 'plan_history'`). Neither is query-scoped: synced-table writers' WAL and reads are part of the picture.

- [ ] **Step 1: Write the failing tests**

`engine/tests/unit/test_report_checks.py`:

Replace (1 of 2):

```python

SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
CFG = EngineConfig(catalog="cat", schema="lbh")
NEW_DELTA = ["autovacuum_saturation", "synced_full_refresh"]


def rendered(check_id):
```

with:

```python

SPECS = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
CFG = EngineConfig(catalog="cat", schema="lbh")
NEW_DELTA = ["autovacuum_saturation", "synced_full_refresh", "wal_burst", "getpage_latency_spike"]


def rendered(check_id):
```

Replace (2 of 2):

```python
    assert "synced_table IS NOT NULL" in s.sql and "ins_delta >= 0.5 * greatest(n_live_tup, 1)" in s.sql
    assert "p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20" in s.sql and "THEN 2.0 ELSE 1.0" in s.sql
    assert "wal_bytes" in s.sql and "wal_fpi" in s.sql and s.fix.kind == "manual"
```

with:

```python
    assert "synced_table IS NOT NULL" in s.sql and "ins_delta >= 0.5 * greatest(n_live_tup, 1)" in s.sql
    assert "p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20" in s.sql and "THEN 2.0 ELSE 1.0" in s.sql
    assert "wal_bytes" in s.sql and "wal_fpi" in s.sql and s.fix.kind == "manual"


def test_wal_burst_counts_bursts_over_ten_times_the_median_and_a_gib():
    s = SPECS["wal_burst"]
    assert s.area == "write_path" and s.thresholds == {"info": 0.0, "warning": 3.0}
    assert "b.wal_bytes > 10 * m.median_bytes AND b.wal_bytes > 1073741824" in s.sql
    assert "system.lakebase.pg_stat_statements_counters" in s.sql and "top_wal_queryid" in s.sql
    assert "query AS" not in s.sql and "max(query)" not in s.sql  # names the statement by id only


def test_getpage_latency_spike_compares_five_minute_p99_with_the_24h_baseline():
    s = SPECS["getpage_latency_spike"]
    assert s.area == "storage_latency" and s.thresholds == {"warning": 20.0}
    assert "neon_getpage_wait_us / 1000.0 / neon_getpage_count" in s.sql and "p.execs >= 5" in s.sql
    assert "p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20" in s.sql and "query_text" not in s.sql
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_report_checks.py -v`
Expected: FAIL with `KeyError: 'wal_burst'`.

- [ ] **Step 3: The checks**

`engine/src/lakebase_health/checks/telemetry/wal_burst.yaml` (new file, complete):

```yaml
id: wal_burst
title: "Bursts of WAL far above the usual rate"
category: throughput
area: write_path
source: delta
thresholds: {info: 0, warning: 3}
explain: >
  5-minute windows in the last 24 h whose WAL volume was more than 10 times the endpoint's median 5-minute volume
  and more than 1 GiB (measure = number of bursts; a warning above 3 a day). WAL comes from the statements that
  wrote it (pg_stat_statements_counters wal_bytes and full-page images), so top_wal_queryid names the COPY, bulk
  load or index build behind the worst burst. Bursts throttle writes and slow replicas: batch bulk loads, build
  indexes off-peak and avoid rewriting whole tables.
sql: |
  WITH b AS (
    SELECT project_id, branch_id, endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300) AS bucket,
           sum(wal_bytes) AS wal_bytes, sum(wal_fpi) AS wal_fpi
    FROM system.lakebase.pg_stat_statements_counters WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY project_id, branch_id, endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300)),
  m AS (SELECT endpoint_id, percentile_approx(wal_bytes, 0.5) AS median_bytes FROM b GROUP BY endpoint_id),
  bursts AS (
    SELECT b.*, m.median_bytes FROM b JOIN m ON m.endpoint_id = b.endpoint_id
    WHERE b.wal_bytes > 10 * m.median_bytes AND b.wal_bytes > 1073741824),
  top AS (
    SELECT s.endpoint_id, timestamp_seconds(floor(unix_timestamp(s.ts) / 300) * 300) AS bucket,
           max_by(CAST(s.queryid AS STRING), s.wal_bytes) AS top_queryid
    FROM system.lakebase.pg_stat_statements_counters s
    JOIN bursts x ON x.endpoint_id = s.endpoint_id
      AND x.bucket = timestamp_seconds(floor(unix_timestamp(s.ts) / 300) * 300)
    WHERE s.ts >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY s.endpoint_id, timestamp_seconds(floor(unix_timestamp(s.ts) / 300) * 300))
  SELECT x.project_id, x.branch_id, x.endpoint_id, x.endpoint_id AS object, CAST(count(*) AS DOUBLE) AS measure,
         max(x.wal_bytes) AS max_burst_bytes, max_by(x.wal_fpi, x.wal_bytes) AS max_burst_fpi,
         max(x.median_bytes) AS median_bytes, max_by(x.bucket, x.wal_bytes) AS worst_at,
         max_by(t.top_queryid, x.wal_bytes) AS top_wal_queryid
  FROM bursts x LEFT JOIN top t ON t.endpoint_id = x.endpoint_id AND t.bucket = x.bucket
  GROUP BY x.project_id, x.branch_id, x.endpoint_id
fix: {kind: manual, template: "Find statement {top_wal_queryid} (the largest WAL writer at the worst burst): batch it, run it off-peak, or build indexes after the load.", risk: moderate}
```

`engine/src/lakebase_health/checks/telemetry/getpage_latency_spike.yaml` (new file, complete):

```yaml
id: getpage_latency_spike
title: "Storage read latency spiked"
category: performance
area: storage_latency
source: delta
thresholds: {warning: 20}
explain: >
  5-minute windows in the last 24 h whose p99 getpage wait (time per page read from the pageserver after a local
  cache miss, from plan_history: neon_getpage_wait_us / neon_getpage_count per execution, at least 5 executions)
  was more than 5 times the endpoint's 24 h baseline (the median of the 5-minute p99s) and above 20 ms (measure =
  the worst p99 in ms). Cache misses stall queries: raise min CU when the working set is near the cache size, and
  serve heavy reads from a read replica while synced tables refresh.
sql: |
  WITH pages AS (
    SELECT project_id, branch_id, endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300) AS bucket,
           count(*) AS execs, percentile_approx(neon_getpage_wait_us / 1000.0 / neon_getpage_count, 0.99) AS p99_ms
    FROM system.lakebase.plan_history
    WHERE ts >= current_timestamp() - INTERVAL 24 HOURS AND neon_getpage_count > 0
    GROUP BY project_id, branch_id, endpoint_id, timestamp_seconds(floor(unix_timestamp(ts) / 300) * 300)),
  base AS (SELECT endpoint_id, percentile_approx(p99_ms, 0.5) AS baseline_ms FROM pages GROUP BY endpoint_id),
  spikes AS (
    SELECT p.*, b.baseline_ms FROM pages p JOIN base b ON b.endpoint_id = p.endpoint_id
    WHERE p.execs >= 5 AND p.p99_ms > 5 * b.baseline_ms AND p.p99_ms > 20)
  SELECT project_id, branch_id, endpoint_id, endpoint_id AS object, max(p99_ms) AS measure,
         count(*) AS spike_windows, round(max(baseline_ms), 2) AS baseline_ms, max_by(bucket, p99_ms) AS worst_at,
         round(max(p99_ms / NULLIF(baseline_ms, 0)), 1) AS worst_ratio, 'plan_history' AS source
  FROM spikes GROUP BY project_id, branch_id, endpoint_id
fix: {kind: manual, template: "Raise min CU on {object} if the working set is near the cache size; move heavy reads to a read replica during synced-table refreshes.", risk: moderate}
```

`engine/tests/unit/test_catalog.py`:

Replace:

```python
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
    "commit_quorum_wait", "hot_row_lock", "statement_timeouts", "gin_cleanup_lock_wait", "connection_drops",
    "autovacuum_saturation", "synced_full_refresh",
}
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
```

with:

```python
    "disk_spill", "row_misestimate", "sort_limit_waste", "storage_wait", "lock_wait_share",
    "lock_blocking_history", "postgres_errors", "xid_runway", "replica_lag", "auth_failures",
    "commit_quorum_wait", "hot_row_lock", "statement_timeouts", "gin_cleanup_lock_wait", "connection_drops",
    "autovacuum_saturation", "synced_full_refresh", "wal_burst", "getpage_latency_spike",
}
POSTGRES = {
    "unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "seq_scan_heavy",
```

- [ ] **Step 4: Regenerate and run the tests**

Run: `PY=engine/.venv/bin/python bash scripts/regen_checks.sh && cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass; `70 checks (35 delta, 31 postgres, 4 api)`.

- [ ] **Step 5: Commit**

```bash
git add engine notebooks docs/CHECKS.md README.md app/server/report/checks.json
git commit -m "feat(engine): wal_burst and getpage_latency_spike checks" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 10: Monitoring tools' queries never count; the `/* lakebase-health */` tag

Query checks flagged pg_sage's queries and our own notebook or psql diagnostics as critical: they run under a person's login, so the platform-role filter misses them. The shared exclusions gain a known-tool list matched on `application_name` and on a **leading** tag comment; the engine, fixer and notebook tag every statement; the notebook connects with its own application name.

pg_stat_statements grouping is unaffected: the query id is computed from the parse tree, which has no comments, so a tagged and an untagged run of a statement by the same role share one entry (the stored text is whichever came first), and the engine's and fixer's statements run under their own roles anyway. Matching keys on (branch, role, query id): a customer running the same statement under another role is kept.

**Files:**
- Modify: `engine/src/lakebase_health/checks/exclusions.yaml`, `engine/src/lakebase_health/exclusions.py`, `engine/src/lakebase_health/executors/delta.py`, `engine/src/lakebase_health/executors/postgres.py`, `engine/src/lakebase_health/fixer/pg.py` **[LLM-merge]**, `engine/src/lakebase_health/pg_inventory.py`, `scripts/build_notebook.py`, `README.md` **[LLM-merge]**
- Test: `engine/tests/unit/test_tool_queries.py` (create), `engine/tests/unit/test_dedupe.py`

**Interfaces:**
- Consumes: `rewrite_tables(sql, cfg, excl)` for query-scoped checks; `PgSession.query`; `FixerPg.session`; `statements_rows` (Task 5).
- Produces: `Exclusions.tool_apps`, `Exclusions.tool_tags` (YAML `tool_apps: [pg_sage, lakebase-health-notebook]`, `tool_tags: [pg_sage, lakebase-health]`), `tag_regex(tags) -> str`, `Exclusions.tool_app_sql(col)` (engine, fixer and tool applications), `Exclusions.tool_text_sql(col)` (Spark `RLIKE`, backslashes doubled), `Exclusions.tool_text_pg(col)` (Postgres `~`); `executors.postgres.QUERY_TAG = "/* lakebase-health */ "`, `tagged(sql) -> str`; `fixer.pg.TaggedConn` (what `FixerPg.session` now yields: `execute(sql, params=None)`, `close()`); the notebook's `application_name = "lakebase-health-notebook"` and `QUERY_TAG`.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_tool_queries.py` (new file, complete):

```python
"""Monitoring tools' own queries (pg_sage, the engine, fixer and notebook) never count in the query checks, and
customer queries are never dropped by accident."""
import re
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace as NS

import pytest

from lakebase_health.checkspec import parse_check
from lakebase_health.config import EngineConfig
from lakebase_health.exclusions import load_exclusions, tag_regex
from lakebase_health.executors.delta import DeltaExecutor
from lakebase_health.executors.postgres import QUERY_TAG, PostgresExecutor, tagged
from lakebase_health.fixer.pg import FixerPg
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.pg_inventory import STATEMENTS_SQL, statements_rows
from tests.fakes import FakeSql
from tests.unit.test_executors import FakeConn, ep

EXCL = load_exclusions(CHECKS_DIR / "exclusions.yaml")
RX = tag_regex(EXCL.tool_tags)


@pytest.mark.parametrize("q", ["/* pg_sage */ SELECT * FROM pg_stat_activity", "  /*lakebase-health*/ SELECT 1",
                               "/* lakebase-health check=unused_index */ SELECT 1", "\n/* pg_sage v2 */\nSELECT $1"])
def test_tool_tagged_queries_match(q):
    assert re.search(RX, q)


@pytest.mark.parametrize("q", ["SELECT * FROM orders", "SELECT 1 /* pg_sage */", "SELECT '/* pg_sage */' AS x",
                               "/* pg_sage_fork */ SELECT 1", "/* lakebase-healthy */ SELECT 1",
                               "-- Who blocks whom\nSELECT 1", "/* app=checkout */ SELECT 1", "<REDACTED>", ""])
def test_customer_queries_do_not_match(q):
    assert not re.search(RX, q)


def _sqlite(pred, **cols):
    db = sqlite3.connect(":memory:")
    db.create_function("REGEXP", 2, lambda rx, v: v is not None and re.search(rx, v) is not None)
    db.execute(f"CREATE TABLE r AS SELECT {', '.join(f'? AS {c}' for c in cols)}", list(cols.values()))
    # Spark unescapes a doubled backslash in a string literal; sqlite does not
    sql = pred.replace(" RLIKE ", " REGEXP ").replace("\\\\", "\\")
    return bool(db.execute(f"SELECT CASE WHEN {sql} THEN 1 ELSE 0 END FROM r").fetchone()[0])


def test_tool_applications_and_tags_as_sql_predicates():
    apps = EXCL.tool_app_sql("application_name")
    for app in ("pg_sage", "lakebase-health-notebook", "lakebase-health", "lakebase-health-fixer"):
        assert _sqlite(apps, application_name=app), app
    for app in ("psql", "checkout-service", "pg_sage_dashboard", None):
        assert not _sqlite(apps, application_name=app), app
    text = EXCL.tool_text_sql("query_text")
    assert "\\\\s" in text  # doubled for Spark's string literal
    assert _sqlite(text, query_text="/* pg_sage */ SELECT 1")
    assert not _sqlite(text, query_text="SELECT 1 /* pg_sage */") and not _sqlite(text, query_text=None)
    assert EXCL.tool_text_pg("s.query").startswith("(coalesce(s.query, '') ~ '^\\s*/\\*")


def test_query_scoped_checks_drop_tool_queries_by_role_and_query_id():
    s = parse_check({"id": "q", "title": "Q", "category": "c", "source": "delta", "explain": "e", "query_scoped": True,
                     "thresholds": {"warning": 0}, "sql": "SELECT 1 FROM system.lakebase.plan_history p "
                     "JOIN system.lakebase.pg_stat_statements_counters c JOIN system.lakebase.active_session_history a"},
                    stem="q")
    fake = FakeSql()
    DeltaExecutor(fake, EngineConfig(catalog="cat"), EXCL).run(s)
    out = fake.statements[-1][0]
    assert out.count("'pg_sage'") >= 3  # plan_history filter + both tool joins
    assert out.count("t.queryid = tq.queryid") == 2 and out.count("t.userid = tq.userid") == 2
    assert "coalesce(query, '') RLIKE" in out and "coalesce(query_text, '') RLIKE" in out


def test_inventory_statements_skip_tagged_text_inside_postgres():
    class S:
        def query(self, sql):
            self.sql = sql
            return []
    s = S()
    statements_rows(s, EXCL)
    assert "NOT (coalesce(s.query, '') ~ '^\\s*/\\*\\s*(pg_sage|lakebase-health)" in s.sql
    assert "{tool}" in STATEMENTS_SQL and not re.search(r"SELECT[^;]*\bs\.query\b[^;]*FROM pg_stat_statements", s.sql.split("WHERE")[0])


def test_engine_and_fixer_statements_lead_with_the_tag():
    assert tagged("SELECT 1") == "/* lakebase-health */ SELECT 1" and tagged(QUERY_TAG + "SELECT 1") == QUERY_TAG + "SELECT 1"
    conn = FakeConn({})
    w = NS(postgres=NS(generate_database_credential=lambda endpoint: NS(token="t")))
    with PostgresExecutor(w, "svc", connect=lambda **k: conn).session(ep(), "db") as s:
        s.query("SELECT 1 AS x")
    assert conn.executed[-1] == "/* lakebase-health */ SELECT 1 AS x"

    class Conn:
        def __init__(self):
            self.ran = []

        def execute(self, sql, params=None):
            self.ran.append((sql, params))

        def close(self):
            pass
    c = Conn()
    with FixerPg(w, "fixer", connect=lambda **k: c).session("projects/p/branches/b/endpoints/e", "h", "db", "analyze") as fx:
        fx.execute("ANALYZE public.t")
        fx.execute("SELECT 1 WHERE x = %s", ("y",))
    assert c.ran[-2:] == [("/* lakebase-health */ ANALYZE public.t", None), ("/* lakebase-health */ SELECT 1 WHERE x = %s", ("y",))]


def test_exclusions_reject_a_tag_that_would_break_the_regex():
    with pytest.raises(ValueError, match="tool tag"):
        tag_regex(["pg.sage"])
```

`engine/tests/unit/test_dedupe.py` (the notebook stays out of the engine-application predicate; it is a tool, covered above):

Replace:

```python
    pred = PKG_EXCL.platform_app_sql("application_name")
    assert _sqlite_true(pred, application_name="lakebase-health")
    assert _sqlite_true(pred, application_name="lakebase-health-fixer")
    for app in ("psql", "lakebase-health-notebook", "whats-new-studio", None):
        assert not _sqlite_true(pred, application_name=app)
```

with:

```python
    pred = PKG_EXCL.platform_app_sql("application_name")
    assert _sqlite_true(pred, application_name="lakebase-health")
    assert _sqlite_true(pred, application_name="lakebase-health-fixer")
    for app in ("psql", "lakebase-health-notebook", "whats-new-studio", None):  # the notebook is a tool: tool_app_sql
        assert not _sqlite_true(pred, application_name=app)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_tool_queries.py -v`
Expected: FAIL with `ImportError: cannot import name 'tag_regex' from 'lakebase_health.exclusions'`.

- [ ] **Step 3: The tool lists**

`engine/src/lakebase_health/checks/exclusions.yaml`:

Replace:

```yaml
query_users: [cloud_admin, databricks_control_plane, databricks_monitor]
query_user_regex: '^databricks_writer_[0-9]+$'
query_apps: [lakebase-health, lakebase-health-fixer]   # application_name the engine and fixer connect with
```

with:

```yaml
query_users: [cloud_admin, databricks_control_plane, databricks_monitor]
query_user_regex: '^databricks_writer_[0-9]+$'
query_apps: [lakebase-health, lakebase-health-fixer]   # application_name the engine and fixer connect with
# Monitoring tools' own queries often run under a person's login, so the role filters above miss them. Query-scoped
# checks also drop queries whose application_name is a known tool, or whose text starts with a tag comment
# /* <tag> */ (the engine, fixer and notebook prefix every statement with /* lakebase-health */). A tag anywhere
# else in the text (a later comment, a string literal) does not count.
tool_apps: [pg_sage, lakebase-health-notebook]
tool_tags: [pg_sage, lakebase-health]
```

`engine/src/lakebase_health/exclusions.py`:

Replace (1 of 5):

```python
from dataclasses import dataclass, replace
from pathlib import Path
```

with:

```python
import re
from dataclasses import dataclass, replace
from pathlib import Path
```

Replace (2 of 5):

```python
    return "(" + ", ".join(_lit(i) for i in items) + ")"


@dataclass(frozen=True)
class Exclusions:
    slots: tuple
```

with:

```python
    return "(" + ", ".join(_lit(i) for i in items) + ")"


_TAG = re.compile(r"^[a-z0-9_-]+$")


def tag_regex(tags) -> str:
    """A leading /* <tag> */ comment (whitespace allowed, the tag a whole word); Java and Postgres regex alike."""
    for t in tags:
        if not _TAG.match(t):
            raise ValueError(f"tool tag must be lowercase letters, digits, _ or -: {t!r}")
    return r"^\s*/\*\s*(" + "|".join(tags) + r")([^a-z0-9_-][^*]*)?\*/"


@dataclass(frozen=True)
class Exclusions:
    slots: tuple
```

Replace (3 of 5):

```python
    query_users: tuple = ()
    query_user_regex: str | None = None
    query_apps: tuple = ()

    def with_query_users(self, users) -> "Exclusions":
        extra = tuple(u for u in users if u and u not in self.query_users)
```

with:

```python
    query_users: tuple = ()
    query_user_regex: str | None = None
    query_apps: tuple = ()
    tool_apps: tuple = ()  # monitoring tools' application_name (exclusions.yaml tool_apps)
    tool_tags: tuple = ()  # leading /* <tag> */ comments that mark a tool's query (exclusions.yaml tool_tags)

    def with_query_users(self, users) -> "Exclusions":
        extra = tuple(u for u in users if u and u not in self.query_users)
```

Replace (4 of 5):

```python
        """SQL predicate, true when `col` is an application name the engine or fixer connects with."""
        return f"(coalesce({col}, '') IN {_sql_list(self.query_apps)})" if self.query_apps else "(1 = 0)"

    def tokens(self) -> dict:
        return {
            "excluded_slots": _sql_list(self.slots),
```

with:

```python
        """SQL predicate, true when `col` is an application name the engine or fixer connects with."""
        return f"(coalesce({col}, '') IN {_sql_list(self.query_apps)})" if self.query_apps else "(1 = 0)"

    def tool_app_sql(self, col: str) -> str:
        """SQL predicate, true when `col` is the engine's, the fixer's or a known monitoring tool's application."""
        apps = tuple(dict.fromkeys(self.query_apps + self.tool_apps))
        return f"(coalesce({col}, '') IN {_sql_list(apps)})" if apps else "(1 = 0)"

    def tool_text_sql(self, col: str) -> str:
        """Spark SQL predicate, true when the query text in `col` starts with a tool's tag comment. Spark string
        literals unescape backslashes, so the regex's are doubled."""
        if not self.tool_tags:
            return "(1 = 0)"
        return f"(coalesce({col}, '') RLIKE {_lit(tag_regex(self.tool_tags).replace(chr(92), chr(92) * 2))})"

    def tool_text_pg(self, col: str) -> str:
        """The tool_text_sql predicate in Postgres syntax."""
        return f"(coalesce({col}, '') ~ {_lit(tag_regex(self.tool_tags))})" if self.tool_tags else "(1 = 0)"

    def tokens(self) -> dict:
        return {
            "excluded_slots": _sql_list(self.slots),
```

Replace (5 of 5):

```python
    return Exclusions(slots=tuple(d["slots"]), roles=tuple(d["roles"]), users=tuple(d["users"]),
                      schemas=tuple(d["schemas"]), role_regex=d["role_regex"], settings_regex=d["settings_regex"],
                      query_users=tuple(d.get("query_users") or ()), query_user_regex=d.get("query_user_regex"),
                      query_apps=tuple(d.get("query_apps") or ()))
```

with:

```python
    return Exclusions(slots=tuple(d["slots"]), roles=tuple(d["roles"]), users=tuple(d["users"]),
                      schemas=tuple(d["schemas"]), role_regex=d["role_regex"], settings_regex=d["settings_regex"],
                      query_users=tuple(d.get("query_users") or ()), query_user_regex=d.get("query_user_regex"),
                      query_apps=tuple(d.get("query_apps") or ()), tool_apps=tuple(d.get("tool_apps") or ()),
                      tool_tags=tuple(d.get("tool_tags") or ()))
```

- [ ] **Step 4: Query-scoped checks drop tool statements**

`engine/src/lakebase_health/executors/delta.py`:

Replace (1 of 2):

```python
_BY_USERID = ("pg_stat_statements_counters", "active_session_history")


def rewrite_tables(sql_text: str, cfg, excl=None) -> str:
    """Point system.lakebase.* at the ingested raw tables and lakebase_health.* at the engine's schema. With `excl`,
    query telemetry drops platform roles."""
```

with:

```python
_BY_USERID = ("pg_stat_statements_counters", "active_session_history")


def _tool_join(cfg, excl) -> str:
    """Drop a monitoring tool's statements: the (branch, role, query id) of every plan_history row from a tool's
    application or with a tool's leading tag comment, and every pg_stat_statements entry whose text has the tag.
    Keyed on the role too, so a customer running the same statement keeps it."""
    if not (excl.tool_tags or excl.tool_apps or excl.query_apps):
        return ""
    since = f"ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS"
    return (f" LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid, queryid FROM {cfg.raw('plan_history')} "
            f"WHERE {since} AND ({excl.tool_app_sql('application_name')} OR {excl.tool_text_sql('query_text')}) "
            f"UNION SELECT DISTINCT branch_id, userid, queryid FROM {cfg.raw('pg_stat_statements_counters')} "
            f"WHERE {since} AND {excl.tool_text_sql('query')}) tq "
            "ON t.branch_id = tq.branch_id AND t.userid = tq.userid AND t.queryid = tq.queryid")


def rewrite_tables(sql_text: str, cfg, excl=None) -> str:
    """Point system.lakebase.* at the ingested raw tables and lakebase_health.* at the engine's schema. With `excl`,
    query telemetry drops platform roles."""
```

Replace (2 of 2):

```python
            return raw
        if name == "plan_history":
            return (f"(SELECT * FROM {raw} WHERE NOT ({excl.platform_role_sql('usename')} "
                    f"OR {excl.platform_app_sql('application_name')}))")
        if name in _BY_USERID:
            return (f"(SELECT t.* FROM {raw} t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid "
                    f"FROM {cfg.raw('plan_history')} WHERE ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS "
                    f"AND {excl.platform_role_sql('usename')}) x ON t.branch_id = x.branch_id AND t.userid = x.userid)")
        return raw
    out = re.sub(r"\bsystem\.lakebase\.([a-z_]+)", table, sql_text)
    # The engine's own tables (inventory snapshots, findings), written as lakebase_health.<name> in check SQL
```

with:

```python
            return raw
        if name == "plan_history":
            return (f"(SELECT * FROM {raw} WHERE NOT ({excl.platform_role_sql('usename')} "
                    f"OR {excl.tool_app_sql('application_name')} OR {excl.tool_text_sql('query_text')}))")
        if name in _BY_USERID:
            return (f"(SELECT t.* FROM {raw} t LEFT ANTI JOIN (SELECT DISTINCT branch_id, userid "
                    f"FROM {cfg.raw('plan_history')} WHERE ts >= current_timestamp() - INTERVAL {_ROLE_MAP_DAYS} DAYS "
                    f"AND {excl.platform_role_sql('usename')}) x ON t.branch_id = x.branch_id AND t.userid = x.userid"
                    f"{_tool_join(cfg, excl)})")
        return raw
    out = re.sub(r"\bsystem\.lakebase\.([a-z_]+)", table, sql_text)
    # The engine's own tables (inventory snapshots, findings), written as lakebase_health.<name> in check SQL
```

`engine/src/lakebase_health/pg_inventory.py` (the text is matched inside Postgres and never returned):

Replace (1 of 2):

```python
         sum(s.total_exec_time) / nullif(sum(s.calls), 0) AS mean_ms, sum(s.shared_blks_hit) AS shared_blks_hit,
         sum(s.shared_blks_read) AS shared_blks_read, sum(s.temp_blks_written) AS temp_blks_written
  FROM pg_stat_statements s JOIN pg_roles r ON r.oid = s.userid JOIN pg_database d ON d.oid = s.dbid
  WHERE s.queryid IS NOT NULL AND NOT {platform}
  GROUP BY s.queryid, r.rolname, d.datname),
ranked AS (
  SELECT s.*, row_number() OVER (ORDER BY total_ms DESC) AS time_rank,
```

with:

```python
         sum(s.total_exec_time) / nullif(sum(s.calls), 0) AS mean_ms, sum(s.shared_blks_hit) AS shared_blks_hit,
         sum(s.shared_blks_read) AS shared_blks_read, sum(s.temp_blks_written) AS temp_blks_written
  FROM pg_stat_statements s JOIN pg_roles r ON r.oid = s.userid JOIN pg_database d ON d.oid = s.dbid
  WHERE s.queryid IS NOT NULL AND NOT {platform} AND NOT {tool}
  GROUP BY s.queryid, r.rolname, d.datname),
ranked AS (
  SELECT s.*, row_number() OVER (ORDER BY total_ms DESC) AS time_rank,
```

Replace (2 of 2):

```python


def statements_rows(session, excl) -> list:
    return session.query(STATEMENTS_SQL.format(platform=excl.platform_role_pg("r.rolname"), n=TOP_STATEMENTS))


def io_rows(session) -> tuple:
```

with:

```python


def statements_rows(session, excl) -> list:
    # The text is matched inside Postgres for the tool tag and never returned
    return session.query(STATEMENTS_SQL.format(platform=excl.platform_role_pg("r.rolname"),
                                               tool=excl.tool_text_pg("s.query"), n=TOP_STATEMENTS))


def io_rows(session) -> tuple:
```

- [ ] **Step 5: Tag every statement the engine, fixer and notebook send**

`engine/src/lakebase_health/executors/postgres.py` (the session setup statements stay untagged; they run under the engine role, which the role filter already drops):

Replace:

```python
)


class PgSession:
    def __init__(self, conn):
        self.conn = conn

    def query(self, sql: str) -> list:
        cur = self.conn.execute(sql)
        if cur.description is None:
            return []
        cols = [d.name for d in cur.description]
```

with:

```python
)


# Leads every statement the engine, fixer and notebook send, so query checks can drop them whoever runs them
# (exclusions.yaml tool_tags). pg_stat_statements computes the query id from the parse tree, which has no comments:
# the tag never splits or merges entries; it only shows in the stored text.
QUERY_TAG = "/* lakebase-health */ "


def tagged(sql: str) -> str:
    return sql if sql.lstrip().startswith("/* lakebase-health") else QUERY_TAG + sql


class PgSession:
    def __init__(self, conn):
        self.conn = conn

    def query(self, sql: str) -> list:
        cur = self.conn.execute(tagged(sql))
        if cur.description is None:
            return []
        cols = [d.name for d in cur.description]
```

`engine/src/lakebase_health/fixer/pg.py` **[LLM-merge]** (the LLM branch added the `READ_ONLY` kinds to `FIXER_SETUP`; these hunks are at the import, before `class FixerPg` and in `session`). The fixer's allowlist and SQL hashes see the stored SQL, never the tag, which is added at execution:

Replace (1 of 3):

```python
import re
from contextlib import contextmanager

from ..executors.postgres import SEARCH_PATH_SQL
from .allowlist import IDENT, parse

LONG = {"create_index", "drop_index"}
```

with:

```python
import re
from contextlib import contextmanager

from ..executors.postgres import SEARCH_PATH_SQL, tagged
from .allowlist import IDENT, parse

LONG = {"create_index", "drop_index"}
```

Replace (2 of 3):

```python
    sqlstate = CREDENTIAL_DENIED


class FixerPg:
    def __init__(self, w, user: str, connect=None):
        self.w, self.user = w, user
```

with:

```python
    sqlstate = CREDENTIAL_DENIED


class TaggedConn:
    """The fixer's connection: every statement leads with the /* lakebase-health */ tag (executors.postgres)."""

    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        return self.conn.execute(tagged(sql)) if params is None else self.conn.execute(tagged(sql), params)

    def close(self):
        self.conn.close()


class FixerPg:
    def __init__(self, w, user: str, connect=None):
        self.w, self.user = w, user
```

Replace (3 of 3):

```python
        try:
            for stmt in FIXER_SETUP(kind):
                conn.execute(stmt)
            yield conn
        finally:
            conn.close()
```

with:

```python
        try:
            for stmt in FIXER_SETUP(kind):
                conn.execute(stmt)
            yield TaggedConn(conn)
        finally:
            conn.close()
```

`scripts/build_notebook.py`:

Replace (1 of 5):

```python
from lakebase_health.exclusions import load_exclusions  # noqa: E402
from lakebase_health.executors.api import API_CHECKS  # noqa: E402
from lakebase_health.executors.delta import DeltaExecutor  # noqa: E402
from lakebase_health.executors.postgres import SESSION_SETUP, STATS_WINDOW_SQL  # noqa: E402
from lakebase_health.fixer.allowlist import NotAllowed, parse  # noqa: E402
from lakebase_health.jobs.run import CHECKS_DIR, service_roles  # noqa: E402
```

with:

```python
from lakebase_health.exclusions import load_exclusions  # noqa: E402
from lakebase_health.executors.api import API_CHECKS  # noqa: E402
from lakebase_health.executors.delta import DeltaExecutor  # noqa: E402
from lakebase_health.executors.postgres import QUERY_TAG, SESSION_SETUP, STATS_WINDOW_SQL  # noqa: E402
from lakebase_health.fixer.allowlist import NotAllowed, parse  # noqa: E402
from lakebase_health.jobs.run import CHECKS_DIR, service_roles  # noqa: E402
```

Replace (2 of 5):

```python
EXCLUDED_ROLES = __EXCLUDED_ROLES__
SESSION_SETUP = __SESSION_SETUP__
STATS_WINDOW_SQL = __STATS_WINDOW_SQL__

w = WorkspaceClient()
USER = w.current_user.me().user_name
```

with:

```python
EXCLUDED_ROLES = __EXCLUDED_ROLES__
SESSION_SETUP = __SESSION_SETUP__
STATS_WINDOW_SQL = __STATS_WINDOW_SQL__
QUERY_TAG = __QUERY_TAG__

w = WorkspaceClient()
USER = w.current_user.me().user_name
```

Replace (3 of 5):

```python
    frames = []
    for db in dbs:
        token = w.postgres.generate_database_credential(endpoint=ep.name).token
        # application_name as the engine's, so the query checks ignore this traffic like the engine's own
        with psycopg.connect(host=host, user=USER, password=token, dbname=db, sslmode="require", autocommit=True,
                             application_name="lakebase-health", connect_timeout=10, prepare_threshold=None) as c:
            for stmt in SESSION_SETUP:  # search_path, default_transaction_read_only = on, statement/lock timeouts
                c.execute(stmt)
            if requires_extension and not c.execute(
```

with:

```python
    frames = []
    for db in dbs:
        token = w.postgres.generate_database_credential(endpoint=ep.name).token
        # The notebook's own application_name and the /* lakebase-health */ tag: the query checks ignore it
        with psycopg.connect(host=host, user=USER, password=token, dbname=db, sslmode="require", autocommit=True,
                             application_name="lakebase-health-notebook", connect_timeout=10,
                             prepare_threshold=None) as c:
            for stmt in SESSION_SETUP:  # search_path, default_transaction_read_only = on, statement/lock timeouts
                c.execute(stmt)
            if requires_extension and not c.execute(
```

Replace (4 of 5):

```python
            if guards:
                print(f"{db}: stats window {c.execute(STATS_WINDOW_SQL).fetchone()[0]} h (confidence is low unless "
                      f"{', '.join(guards)})")
            cur = c.execute(sql)
            pdf = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        if per_database and "object" in pdf.columns:
            pdf["object"] = db + ":" + pdf["object"].astype(str)  # the engine's object naming
```

with:

```python
            if guards:
                print(f"{db}: stats window {c.execute(STATS_WINDOW_SQL).fetchone()[0]} h (confidence is low unless "
                      f"{', '.join(guards)})")
            cur = c.execute(QUERY_TAG + sql)
            pdf = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        if per_database and "object" in pdf.columns:
            pdf["object"] = db + ":" + pdf["object"].astype(str)  # the engine's object naming
```

Replace (5 of 5):

```python
    setup = (SETUP_PY.replace("__SERVICE_ROLES__", repr(tuple(roles)))
             .replace("__EXCLUDED_ROLES__", repr(tuple(excl.roles)))
             .replace("__SESSION_SETUP__", repr(tuple(SESSION_SETUP)))
             .replace("__STATS_WINDOW_SQL__", repr(STATS_WINDOW_SQL)))
    cells = [md_cell(header), INSTALL_PY.replace("__WHEEL__", repr(engine_wheel)), setup]
    for src, title in SOURCE_ORDER:
        group = [s for s in specs if s.source == src]
```

with:

```python
    setup = (SETUP_PY.replace("__SERVICE_ROLES__", repr(tuple(roles)))
             .replace("__EXCLUDED_ROLES__", repr(tuple(excl.roles)))
             .replace("__SESSION_SETUP__", repr(tuple(SESSION_SETUP)))
             .replace("__STATS_WINDOW_SQL__", repr(STATS_WINDOW_SQL))
             .replace("__QUERY_TAG__", repr(QUERY_TAG)))
    cells = [md_cell(header), INSTALL_PY.replace("__WHEEL__", repr(engine_wheel)), setup]
    for src, title in SOURCE_ORDER:
        group = [s for s in specs if s.source == src]
```

- [ ] **Step 6: Document it** **[LLM-merge]**

`README.md`, section "What is excluded". Replace:

```markdown
- Query checks also drop the telemetry of synced-table writers (`databricks_writer_<n>`), platform agents, and the
  engine and fixer SPs and their application names (`lakebase-health`, `lakebase-health-fixer`).
```

with:

```markdown
- Query checks also drop the telemetry of synced-table writers (`databricks_writer_<n>`), platform agents, and the
  engine and fixer SPs and their application names (`lakebase-health`, `lakebase-health-fixer`).
- Monitoring tools' own queries, whoever runs them: application names `pg_sage` and `lakebase-health-notebook`, and
  any statement whose text **starts** with `/* pg_sage */` or `/* lakebase-health */`. The engine, fixer and
  notebook tag every statement that way. For ad-hoc diagnostics in psql, start the query with
  `/* lakebase-health */` or connect with `PGAPPNAME=lakebase-health-notebook scripts/pg.sh`. A tag elsewhere in the
  text does not count, and a customer running the same statement under another role keeps it.
```

- [ ] **Step 7: Regenerate and run the tests**

Run: `PY=engine/.venv/bin/python bash scripts/regen_checks.sh && cd engine && .venv/bin/python -m pytest tests/unit -q`
Expected: all pass (the notebook's query-scoped SQL now carries the tool joins).

- [ ] **Step 8: Commit**

```bash
git add engine scripts/build_notebook.py notebooks docs/CHECKS.md README.md app/server/report/checks.json
git commit -m "feat(engine): drop monitoring tools' queries; tag engine, fixer and notebook statements" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

# Phase 2: App


### Task 11: Report types, scoring, runbook and strip (app server, pure)

The report's shared types, the 12 areas with their one-line explanations, the score and grade, the "where to focus" note, the ordered runbook and `strip()` for exports. Pure functions over rows; no I/O.

**Files:**
- Create: `app/client/src/report/types.ts` (shared with the server: pure types)
- Create: `app/server/report/catalog.ts`, `app/server/report/score.ts`, `app/server/report/runbook.ts`, `app/server/report/strip.ts`
- Test: `app/server/tests/report_score.test.ts`, `app/server/tests/report_runbook.test.ts`

**Interfaces:**
- Consumes: `app/server/report/checks.json` (Task 1); `maskRows(rows, pii)` and `TEXT_FIELDS` (`server/lib/mask.ts`).
- Produces: types `Report`, `AreaId`, `AreaResult`, `Score`, `RunbookItem`, `ReportFinding`, `Section<T>`, `Series`, `SeriesPoint`, `IoData`, `IoRow`, `Overview`, `EndpointMetrics`, `Branch`, `Endpoint`, `Row` (`client/src/report/types.ts`); `CATALOG: Catalog`, `CheckInfo`, `AREAS: {id, label, blurb}[]`, `areaLabel(id)`, `areaOf(checkId)` (catalog.ts); `CheckStatus = {check_id, endpoint_id, status, error}`, `Coverage = {primaryEndpoints, telemetry, enrolled}`, `checkRan(checkId, statuses, cov, catalog?) -> {ran, note}`, `scoreAreas(findings, statuses, cov, catalog?) -> AreaResult[]`, `score(areas) -> Score`, `grade(pct)`, `focus(areas) -> string` (score.ts); `ActionRow = {action_id, fingerprint, state}`, `impact(f, catalog?)`, `evidenceNumbers(evidence)`, `firstSentence(s)`, `safeSql(f)`, `runbook(findings, actions, catalog?) -> RunbookItem[]`, `runbookCounts(items)` (runbook.ts); `strip(report) -> Report` (strip.ts).

- [ ] **Step 1: Write the failing tests**

`app/server/tests/report_score.test.ts` (new file, complete):

```ts
import { describe, expect, it } from "vitest";
import type { ReportFinding } from "../../client/src/report/types";
import { AREAS, CATALOG, type Catalog } from "../report/catalog";
import { checkRan, focus, grade, score, scoreAreas, type CheckStatus, type Coverage } from "../report/score";

const CAT: Catalog = {
  lfc_hit_rate: { area: "cache_memory", source: "delta", title: "LFC", thresholds: { warning: 99, critical: 95 }, direction: "below" },
  unused_index: { area: "indexes", source: "postgres", title: "Unused", thresholds: { info: 0 }, direction: "above" },
  dead_tuples: { area: "vacuum_xid", source: "postgres", title: "Dead", thresholds: { warning: 10, critical: 20 }, direction: "above" },
  primary_without_ha: { area: "resilience", source: "api", title: "HA", thresholds: { warning: 0 }, direction: "above" },
};
const COV: Coverage = { primaryEndpoints: ["ep-a"], telemetry: true, enrolled: true };
const OK: CheckStatus[] = [
  { check_id: "lfc_hit_rate", endpoint_id: null, status: "ok", error: null },
  { check_id: "unused_index", endpoint_id: "ep-a", status: "ok", error: null },
  { check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok", error: null },
  { check_id: "primary_without_ha", endpoint_id: null, status: "ok", error: null },
];

const f = (check_id: string, severity: ReportFinding["severity"], extra: Partial<ReportFinding> = {}): ReportFinding => ({
  fingerprint: `${check_id}-${severity}`, check_id, title: `${check_id} title`, category: "c", area: null, endpoint_id: "ep-a",
  object: "o", status: "open", severity, confidence: "high", measure: 1, first_seen: null, last_seen: null, evidence: "{}",
  explain: "Why it matters. More.", candidate_sql: null, queryid: null, ...extra,
});
const byId = (areas: ReturnType<typeof scoreAreas>) => Object.fromEntries(areas.map((a) => [a.id, a]));

describe("report scoring", () => {
  it("has the 12 areas in report order and every catalog check maps to one", () => {
    expect(AREAS.map((a) => a.id)).toEqual(["cache_memory", "storage_latency", "write_path", "connections", "locks", "vacuum_xid",
      "indexes", "tables_bloat", "queries", "config_security", "scale_to_zero", "resilience"]);
    expect(Object.keys(CATALOG).length).toBeGreaterThan(60);
    expect(Object.values(CATALOG).every((c) => c.area === null || AREAS.some((a) => a.id === c.area))).toBe(true);
  });

  it("scores 5 with no open finding and 4 / 3 / 1 by the worst open severity", () => {
    const a = byId(scoreAreas([f("lfc_hit_rate", "info"), f("dead_tuples", "warning"), f("dead_tuples", "critical"),
      f("unused_index", "warning", { status: "resolved" })], OK, COV, CAT));
    expect([a.cache_memory.score, a.cache_memory.state]).toEqual([4, "warn"]);
    expect([a.vacuum_xid.score, a.vacuum_xid.state, a.vacuum_xid.worst?.severity]).toEqual([1, "fail", "critical"]);
    expect([a.indexes.score, a.indexes.state, a.indexes.open]).toEqual([5, "pass", 0]);
    expect([a.resilience.score, a.resilience.state]).toEqual([5, "pass"]);
  });

  it("leaves an area none of whose checks ran out of the total", () => {
    const a = byId(scoreAreas([], OK, COV, CAT));
    expect([a.queries.state, a.queries.score, a.queries.note]).toEqual(["not_assessed", null, "no checks ran"]);
    expect(score(Object.values(a))).toEqual({ pct: 100, grade: "A", assessed: 4, total: 12 });
  });

  it("does not assess Postgres areas on a branch that is not enrolled, and says why", () => {
    const st = OK.filter((s) => s.endpoint_id === null);
    const a = byId(scoreAreas([f("dead_tuples", "critical")], st, { ...COV, enrolled: false }, CAT));
    expect(a.vacuum_xid.state).toBe("not_assessed");
    expect(a.vacuum_xid.note).toBe("Postgres checks are not enabled on this branch");
    expect(a.cache_memory.state).toBe("pass");
  });

  it("shows young counters as the reason an area is not assessed", () => {
    const st = OK.map((s) => (s.check_id === "unused_index" ? { ...s, status: "skipped:stats_young", error: "counters too young (reset 3 h ago)" } : s));
    const a = byId(scoreAreas([], st, COV, CAT));
    expect([a.indexes.state, a.indexes.note]).toEqual(["not_assessed", "counters too young (reset 3 h ago)"]);
  });

  it("does not assess telemetry areas without telemetry or when the source is stale", () => {
    expect(checkRan("lfc_hit_rate", OK, { ...COV, telemetry: false }, CAT)).toEqual({ ran: false, note: "no telemetry for this branch in the last 24 h" });
    const stale = OK.map((s) => (s.check_id === "lfc_hit_rate" ? { ...s, status: "ok_stale_source" } : s));
    expect(checkRan("lfc_hit_rate", stale, COV, CAT)).toEqual({ ran: false, note: "telemetry is stale" });
    expect(checkRan("dead_tuples", OK, { ...COV, primaryEndpoints: ["ep-other"] }, CAT).ran).toBe(false);
  });

  it("grades on the percentage boundaries", () => {
    expect([85, 84, 70, 69, 55, 54, 40, 39].map(grade)).toEqual(["A", "B", "B", "C", "C", "D", "D", "F"]);
    const areas = scoreAreas([f("dead_tuples", "critical")], OK, COV, CAT);
    expect(score(areas)).toEqual({ pct: 80, grade: "B", assessed: 4, total: 12 }); // (5 + 5 + 1 + 5) / 20
    expect(score(scoreAreas([], [], { ...COV, telemetry: false, enrolled: false }, CAT))).toEqual({ pct: null, grade: null, assessed: 0, total: 12 });
  });

  it("points the focus note at failing areas first", () => {
    expect(focus(scoreAreas([f("dead_tuples", "critical")], OK, COV, CAT))).toMatch(/^Start with vacuum & xid/);
    expect(focus(scoreAreas([f("lfc_hit_rate", "warning")], OK, COV, CAT))).toMatch(/^Next: cache & memory/);
    expect(focus(scoreAreas([], OK, COV, CAT))).toBe("No open findings in the assessed areas.");
  });
});
```

`app/server/tests/report_runbook.test.ts` (new file, complete):

```ts
import { describe, expect, it } from "vitest";
import type { Report, ReportFinding } from "../../client/src/report/types";
import type { Catalog } from "../report/catalog";
import { evidenceNumbers, firstSentence, impact, runbook, runbookCounts, safeSql } from "../report/runbook";
import { strip } from "../report/strip";

const CAT: Catalog = {
  dead_tuples: { area: "vacuum_xid", source: "postgres", title: "Dead", thresholds: { warning: 10, critical: 20 }, direction: "above" },
  lfc_hit_rate: { area: "cache_memory", source: "delta", title: "LFC", thresholds: { warning: 99, critical: 95 }, direction: "below" },
  guardrail_settings: { area: "config_security", source: "postgres", title: "G", thresholds: { info: 0, warning: 1 }, direction: "above" },
};
const f = (fp: string, check_id: string, severity: ReportFinding["severity"], measure: number, extra: Partial<ReportFinding> = {}): ReportFinding => ({
  fingerprint: fp, check_id, title: `T ${fp}`, category: "c", area: null, endpoint_id: "ep-a", object: `db:public.${fp}`,
  status: "open", severity, confidence: "high", measure, first_seen: null, last_seen: null,
  evidence: JSON.stringify({ n_dead_tup: 12000, statement: "SELECT secret", table_ident: "public.t" }),
  explain: "Dead tuples pile up. Vacuum is behind.", candidate_sql: "VACUUM (ANALYZE) public.t;", queryid: null, ...extra,
});

describe("runbook", () => {
  it("orders by severity, then a verified fix, then impact", () => {
    const items = runbook([
      f("a", "dead_tuples", "warning", 11),
      f("b", "dead_tuples", "warning", 19),
      f("c", "dead_tuples", "critical", 21),
      f("d", "dead_tuples", "warning", 12),
      f("e", "lfc_hit_rate", "info", 99.5, { status: "resolved" }),
    ], [{ action_id: "x1", fingerprint: "a", state: "verified" }, { action_id: "x2", fingerprint: "d", state: "proposed" }], CAT);
    expect(items.map((i) => i.fingerprint)).toEqual(["c", "a", "d", "b"]);
    expect(items.map((i) => i.priority)).toEqual(["HIGH", "MEDIUM", "MEDIUM", "MEDIUM"]);
    expect(items[1]).toMatchObject({ actionId: "x1", actionState: "verified", areaLabel: "Vacuum & XID", why: "Dead tuples pile up." });
    expect(runbookCounts(items)).toEqual({ HIGH: 1, MEDIUM: 3, LOW: 0 });
  });

  it("measures impact against the threshold in the check's direction", () => {
    expect(impact(f("a", "dead_tuples", "warning", 15), CAT)).toBe(0.5);
    expect(impact(f("a", "lfc_hit_rate", "critical", 90), CAT)).toBeCloseTo(5 / 95);
    expect(impact(f("a", "unknown", "warning", 15), CAT)).toBe(0);
  });

  it("shows evidence numbers, never text", () => {
    expect(evidenceNumbers(JSON.stringify({ n_dead_tup: 12000, ratio: 0.12345, query: "SELECT 1", statement: "x" }))).toEqual([
      "n dead tup: 12,000", "ratio: 0.12"]);
    expect(evidenceNumbers("not json")).toEqual([]);
  });

  it("gives SQL, or a support-managed note, or the guidance note", () => {
    const [sql] = runbook([f("a", "dead_tuples", "warning", 11)], [], CAT);
    expect([sql.sql, sql.note]).toEqual(["VACUUM (ANALYZE) public.t;", null]);
    const [sm] = runbook([f("s", "guardrail_settings", "info", 1, { candidate_sql: null, evidence: JSON.stringify({ name: "track_io_timing" }) })], [], CAT);
    expect(sm.sql).toBeNull();
    expect(sm.note).toMatch(/^support-managed: track_io_timing/);
    const [none] = runbook([f("n", "dead_tuples", "warning", 11, { candidate_sql: null })], [], CAT);
    expect(none.note).toBe("No SQL fix: follow the guidance on the finding page");
  });

  it("never quotes query text in SQL", () => {
    expect(safeSql(f("q", "dead_tuples", "warning", 11, { candidate_sql: "EXPLAIN SELECT * FROM t WHERE ssn = 1", query_text: "SELECT * FROM t WHERE ssn = 1", queryid: "42" })))
      .toBe("EXPLAIN query 42");
    expect(firstSentence("One. Two.")).toBe("One.");
  });
});

describe("strip", () => {
  it("removes query text and statement text in evidence, and query groups", () => {
    const report = { findings: [f("a", "dead_tuples", "warning", 11, { query_text: "SELECT secret" })], queryGroups: [{ x: 1 }] } as unknown as Report;
    const out = strip(report);
    expect(out.findings[0].query_text).toBeNull();
    expect(JSON.parse(out.findings[0].evidence ?? "{}")).toEqual({ n_dead_tup: 12000, statement: "[masked]", table_ident: "public.t" });
    expect(out.queryGroups).toEqual([]);
    expect(JSON.stringify(out)).not.toContain("SELECT secret");
  });
});
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd app && npx vitest run server/tests/report_score.test.ts server/tests/report_runbook.test.ts`
Expected: FAIL with `Failed to resolve import "../report/catalog"`.

- [ ] **Step 3: The shared types**

`app/client/src/report/types.ts` (new file, complete):

```ts
/**
 * The health report's data, built by the server (server/report/build.ts) and rendered by the components in this
 * folder, both in the app and in the downloaded HTML. Pure types: the server imports this file too.
 */
export type AreaId =
  | "cache_memory"
  | "storage_latency"
  | "write_path"
  | "connections"
  | "locks"
  | "vacuum_xid"
  | "indexes"
  | "tables_bloat"
  | "queries"
  | "config_security"
  | "scale_to_zero"
  | "resilience";

export type Severity = "info" | "warning" | "critical";
export type AreaState = "pass" | "warn" | "fail" | "not_assessed";

export type AreaResult = {
  id: AreaId;
  label: string;
  /** One line: what the area covers. */
  blurb: string;
  state: AreaState;
  /** 5 with no open finding, 4 info, 3 warning, 1 critical; null when not assessed. */
  score: number | null;
  open: number;
  worst: { fingerprint: string; title: string; object: string; severity: Severity } | null;
  /** Why it is not assessed (e.g. "counters too young (reset 3 h ago)"). */
  note: string | null;
};

export type Score = { pct: number | null; grade: "A" | "B" | "C" | "D" | "F" | null; assessed: number; total: number };

export type Priority = "HIGH" | "MEDIUM" | "LOW";
export type RunbookItem = {
  priority: Priority;
  severity: Severity;
  fingerprint: string;
  checkId: string;
  area: AreaId | null;
  areaLabel: string;
  title: string;
  object: string;
  why: string;
  evidence: string[];
  sql: string | null;
  /** Shown instead of SQL when there is none. */
  note: string | null;
  actionId: string | null;
  actionState: string | null;
};

export type ReportFinding = {
  fingerprint: string;
  check_id: string;
  title: string;
  category: string;
  area: AreaId | null;
  endpoint_id: string | null;
  object: string;
  status: string;
  severity: Severity;
  confidence: string | null;
  measure: number | null;
  first_seen: string | null;
  last_seen: string | null;
  /** Evidence JSON, with statement text removed. */
  evidence: string | null;
  explain: string | null;
  candidate_sql: string | null;
  queryid: string | null;
  query_text?: string | null;
};

export type Endpoint = {
  id: string;
  type: "primary" | "replica";
  state: string;
  minCu: number | null;
  maxCu: number | null;
  suspendTimeoutS: number | null;
  enrolled: boolean;
};

export type Branch = { id: string; name: string; isDefault: boolean };

export type Row = Record<string, string | number | boolean | null>;

/** One panel's data: rows, or an error, with the snapshot time and whether it is stale. */
export type Section<T = Row[]> = {
  status: "ok" | "error";
  data: T;
  error?: string;
  asOf?: string | null;
  stale?: boolean;
};

export type SeriesPoint = { t: string; v: number | null };
export type Series = Record<string, SeriesPoint[]>;

export type IoRow = {
  backend_type: string;
  object: string;
  context: string;
  reads: number | null;
  writes: number | null;
  extends: number | null;
  hits: number | null;
  evictions: number | null;
  reuses: number | null;
  fsyncs: number | null;
  read_time: number | null;
  write_time: number | null;
};
export type IoData = { rows: IoRow[]; delta: boolean; timing: boolean; windowS: number | null; statsReset: string | null };

export type Overview = {
  lfcHitPct: number | null;
  workingSetBytes: number | null;
  lfcSizeBytes: number | null;
  connections: number | null;
  maxConnections: number | null;
  databaseBytes: number | null;
  oldestXactS: number | null;
  getpageP99Ms: number | null;
  tempSpillBytes: number | null;
  commitsPerS: number | null;
  telemetryAt: string | null;
};

export type EndpointMetrics = {
  /** Latest value per metric (autovacuum_workers_busy, neon.lfc.file_cache_hits, ...). */
  latest: Record<string, number | null>;
  /** source:<name> -> available, with the reason when not. */
  sources: Record<string, { available: boolean; note: string | null }>;
  idleClients: { role: string; application: string; sessions: number; maxIdleS: number | null }[];
  /** Endpoint-level getpage p99 (ms) per snapshot pair, from neon_perf_counters; [] when not readable. */
  getpageP99: SeriesPoint[];
  serverVersion: string | null;
};

export type ReportSections = {
  overview: Section<Overview>;
  databases: Section;
  tables: Section;
  indexes: Section;
  statements: Section;
  settings: Section;
  io: Section<IoData>;
  endpoint: Section<EndpointMetrics>;
  series: Section<Series>;
  waits: Section;
};

export type Report = {
  project: { id: string; name: string };
  branch: Branch;
  branches: Branch[];
  endpoints: Endpoint[];
  /** The read-write endpoint the inventory panels and charts describe. */
  primary: string | null;
  pgVersion: string | null;
  dataAsOf: string | null;
  generatedAt: string;
  banners: string[];
  score: Score;
  areas: AreaResult[];
  focus: string;
  findings: ReportFinding[];
  runbook: RunbookItem[];
  sections: ReportSections;
  /** In the app only: query findings grouped one row per query (server/lib/group.ts). Empty in exports. */
  queryGroups: unknown[];
};
```

- [ ] **Step 4: Catalog, score, runbook, strip**

`app/server/report/catalog.ts` (new file, complete):

```ts
import type { AreaId, Severity } from "../../client/src/report/types";
import checks from "./checks.json";

/** One check as the engine defines it (scripts/gen_check_areas.py writes checks.json from the YAMLs). */
export type CheckInfo = {
  area: AreaId | null;
  source: "delta" | "postgres" | "api";
  title: string;
  thresholds: Partial<Record<Severity, number>>;
  direction: "above" | "below";
};
export type Catalog = Record<string, CheckInfo>;

export const CATALOG = checks as unknown as Catalog;

/** The 12 scorecard areas in report order, with the one-line explanation each card shows. */
export const AREAS: { id: AreaId; label: string; blurb: string }[] = [
  { id: "cache_memory", label: "Cache & memory", blurb: "Local cache hit rate, working set against the cache, CPU and CU sizing." },
  { id: "storage_latency", label: "Storage latency", blurb: "Time queries wait on the pageserver after a cache miss." },
  { id: "write_path", label: "Write path", blurb: "Write throttling, WAL bursts, commit waits and synced-table refreshes." },
  { id: "connections", label: "Connections & sessions", blurb: "Connection use, drops and sessions left idle in a transaction." },
  { id: "locks", label: "Locks & contention", blurb: "Blocked sessions, deadlocks, hot rows and lock wait share." },
  { id: "vacuum_xid", label: "Vacuum & XID", blurb: "Autovacuum health, dead tuples, transaction ID age and what holds it back." },
  { id: "indexes", label: "Indexes", blurb: "Unused, duplicate, redundant, invalid and missing indexes; sequential scans." },
  { id: "tables_bloat", label: "Tables & bloat", blurb: "Table and index bloat, tables without a primary key, sequence runway." },
  { id: "queries", label: "Queries", blurb: "The slowest and regressed queries, plan flips, spills and errors." },
  { id: "config_security", label: "Configuration & security", blurb: "Guardrail settings, drift, privileged and password roles, TLS." },
  { id: "scale_to_zero", label: "Scale to zero", blurb: "Whether idle clients keep the compute from suspending." },
  { id: "resilience", label: "Resilience", blurb: "High availability, restore window, branch protection, storage runway, replica lag." },
];

export const areaLabel = (id: AreaId | null | undefined) => AREAS.find((a) => a.id === id)?.label ?? "Engine";
export const areaOf = (checkId: string, catalog: Catalog = CATALOG): AreaId | null => catalog[checkId]?.area ?? null;
```

`app/server/report/score.ts` (new file, complete). A Postgres check ran for the branch when its status row for the branch's read-write endpoint is `ok`; a telemetry check when its fleet row is `ok` and the branch had telemetry in the last 24 h; an API check when its fleet row is `ok`:

```ts
import type { AreaId, AreaResult, AreaState, ReportFinding, Score, Severity } from "../../client/src/report/types";
import { AREAS, CATALOG, type Catalog } from "./catalog";

/** The latest check_results status row of one check: fleet-wide (endpoint_id null) or for one endpoint. */
export type CheckStatus = { check_id: string; endpoint_id: string | null; status: string; error: string | null };

/** What decides whether a check ran for this branch. */
export type Coverage = {
  /** The branch's read-write endpoint(s): Postgres checks run there. */
  primaryEndpoints: string[];
  /** Some telemetry arrived for the branch in the last 24 h. */
  telemetry: boolean;
  /** The branch's primary is enrolled for Postgres checks. */
  enrolled: boolean;
};

const RANK: Record<Severity, number> = { info: 0, warning: 1, critical: 2 };
const AREA_SCORE: Record<Severity, number> = { info: 4, warning: 3, critical: 1 };

/** Whether a check ran for the branch, and why not. */
export function checkRan(checkId: string, statuses: CheckStatus[], cov: Coverage, catalog: Catalog = CATALOG) {
  const info = catalog[checkId];
  if (!info) return { ran: false, note: null as string | null };
  if (info.source === "postgres") {
    const mine = statuses.filter((s) => s.check_id === checkId && s.endpoint_id !== null && cov.primaryEndpoints.includes(s.endpoint_id));
    if (mine.some((s) => s.status === "ok")) return { ran: true, note: null };
    const young = mine.find((s) => s.status === "skipped:stats_young");
    if (young) return { ran: false, note: young.error ?? "counters too young" };
    return { ran: false, note: cov.enrolled ? "Postgres checks did not run (compute suspended or unreachable)" : "Postgres checks are not enabled on this branch" };
  }
  const fleet = statuses.find((s) => s.check_id === checkId && s.endpoint_id === null);
  const ok = fleet?.status === "ok";
  if (info.source === "api") return { ran: ok, note: ok ? null : "API checks did not run" };
  if (!cov.telemetry) return { ran: false, note: "no telemetry for this branch in the last 24 h" };
  if (ok) return { ran: true, note: null };
  return { ran: false, note: fleet?.status === "ok_stale_source" ? "telemetry is stale" : "telemetry checks did not run" };
}

const stateOf = (score: number | null): AreaState =>
  score === null ? "not_assessed" : score === 5 ? "pass" : score >= 3 ? "warn" : "fail";

/** One result per area: the worst open finding decides the score; an area none of whose checks ran is not assessed. */
export function scoreAreas(findings: ReportFinding[], statuses: CheckStatus[], cov: Coverage, catalog: Catalog = CATALOG): AreaResult[] {
  const open = findings.filter((f) => f.status === "new" || f.status === "open");
  return AREAS.map(({ id, label, blurb }) => {
    const checks = Object.keys(catalog).filter((c) => catalog[c].area === id);
    const runs = checks.map((c) => checkRan(c, statuses, cov, catalog));
    const assessed = runs.some((r) => r.ran);
    const mine = open.filter((f) => catalog[f.check_id]?.area === id);
    const worst = mine.reduce<ReportFinding | null>(
      (w, f) => (w === null || RANK[f.severity] > RANK[w.severity] || (RANK[f.severity] === RANK[w.severity] && (f.measure ?? 0) > (w.measure ?? 0)) ? f : w),
      null,
    );
    const score = assessed ? (worst ? AREA_SCORE[worst.severity] : 5) : null;
    const notes = [...new Set(runs.map((r) => r.note).filter((n): n is string => !!n))];
    const young = notes.find((n) => n.startsWith("counters too young"));
    return {
      id,
      label,
      blurb,
      state: stateOf(score),
      score,
      open: mine.length,
      worst: worst ? { fingerprint: worst.fingerprint, title: worst.title, object: worst.object, severity: worst.severity } : null,
      note: assessed ? (young ?? null) : (young ?? notes[0] ?? "no checks ran"),
    };
  });
}

export function grade(pct: number): Score["grade"] {
  if (pct >= 85) return "A";
  if (pct >= 70) return "B";
  if (pct >= 55) return "C";
  if (pct >= 40) return "D";
  return "F";
}

/** Score = sum / (5 x assessed areas), as a whole percentage; no grade when nothing was assessed. */
export function score(areas: AreaResult[]): Score {
  const assessed = areas.filter((a) => a.score !== null);
  if (assessed.length === 0) return { pct: null, grade: null, assessed: 0, total: areas.length };
  const pct = Math.round((100 * assessed.reduce((n, a) => n + (a.score ?? 0), 0)) / (5 * assessed.length));
  return { pct, grade: grade(pct), assessed: assessed.length, total: areas.length };
}

/** The short "where to focus" note under the scorecard. */
export function focus(areas: AreaResult[]): string {
  const fail = areas.filter((a) => a.state === "fail");
  const warn = areas.filter((a) => a.state === "warn").sort((x, y) => (x.score ?? 5) - (y.score ?? 5));
  const names = (xs: AreaResult[]) => xs.slice(0, 3).map((a) => a.label.toLowerCase()).join(", ");
  if (fail.length) return `Start with ${names(fail)}: ${fail.length === 1 ? "it has" : "they have"} a critical finding. Then work down the runbook.`;
  if (warn.some((a) => a.score === 3)) return `Next: ${names(warn.filter((a) => a.score === 3))}. Warnings there are worth a fix this week.`;
  if (warn.length) return `Only informational findings, in ${names(warn)}. Review them when convenient.`;
  if (areas.every((a) => a.state === "not_assessed")) return "Nothing was assessed yet: see the banners above.";
  return "No open findings in the assessed areas.";
}

export const AREA_IDS: AreaId[] = AREAS.map((a) => a.id);
```

`app/server/report/runbook.ts` (new file, complete):

```ts
import type { Priority, ReportFinding, RunbookItem, Severity } from "../../client/src/report/types";
import { TEXT_FIELDS } from "../lib/mask";
import { CATALOG, areaLabel, type Catalog } from "./catalog";

/** An open fix action of a finding (proposed_actions joined to its finding). */
export type ActionRow = { action_id: string; fingerprint: string; state: string };

const PRIORITY: Record<Severity, Priority> = { critical: "HIGH", warning: "MEDIUM", info: "LOW" };
const SEV_RANK: Record<Severity, number> = { critical: 2, warning: 1, info: 0 };
/** A fix verified on a branch beats one only proposed, which beats none. */
const FIX_RANK: Record<string, number> = { verified: 2, proposed: 1, verifying: 1, verify_failed: 0, applying: 1 };
/** Settings only Databricks support can change on Lakebase. */
const SUPPORT_MANAGED = new Set(["track_io_timing", "log_min_duration_statement", "shared_preload_libraries",
  "autovacuum_max_workers", "max_connections", "shared_buffers"]);

/** How far past its threshold a finding is, comparable across checks: (measure - threshold) / max(|threshold|, 1). */
export function impact(f: ReportFinding, catalog: Catalog = CATALOG): number {
  const info = catalog[f.check_id];
  const t = info?.thresholds[f.severity];
  if (f.measure === null || t === undefined) return 0;
  const d = info.direction === "below" ? t - f.measure : f.measure - t;
  return d / Math.max(Math.abs(t), 1);
}

function parse(evidence: string | null): Record<string, unknown> {
  try {
    const e: unknown = JSON.parse(evidence ?? "{}");
    return typeof e === "object" && e !== null && !Array.isArray(e) ? (e as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

const fmt = (v: number) => (Number.isInteger(v) ? v.toLocaleString("en-US") : v.toLocaleString("en-US", { maximumFractionDigits: 2 }));

/** Up to 4 "key: value" numbers from the evidence (never text). */
export function evidenceNumbers(evidence: string | null): string[] {
  return Object.entries(parse(evidence))
    .filter(([k, v]) => typeof v === "number" && Number.isFinite(v) && !TEXT_FIELDS.has(k))
    .slice(0, 4)
    .map(([k, v]) => `${k.replace(/_/g, " ")}: ${fmt(v as number)}`);
}

/** The explain text's first sentence. */
export const firstSentence = (s: string | null) => {
  const t = (s ?? "").trim();
  const m = /^(.+?[.!?])(\s|$)/s.exec(t);
  return (m ? m[1] : t).replace(/\s+/g, " ");
};

/** SQL to show for a finding: never the user's query text (that becomes "query <id>"). */
export function safeSql(f: ReportFinding): string | null {
  const sql = f.candidate_sql;
  if (!sql) return null;
  const text = f.query_text;
  if (text && text.length > 8 && sql.includes(text)) return sql.split(text).join(`query ${f.queryid ?? "?"}`);
  return sql;
}

/** One item per open finding: severity, then a verified fix, then impact. */
export function runbook(findings: ReportFinding[], actions: ActionRow[], catalog: Catalog = CATALOG): RunbookItem[] {
  const best = new Map<string, ActionRow>();
  for (const a of actions) {
    const cur = best.get(a.fingerprint);
    if (!cur || (FIX_RANK[a.state] ?? 0) > (FIX_RANK[cur.state] ?? 0)) best.set(a.fingerprint, a);
  }
  const open = findings.filter((f) => f.status === "new" || f.status === "open");
  const fixRank = (f: ReportFinding) => {
    const a = best.get(f.fingerprint);
    return a ? (FIX_RANK[a.state] ?? 0) + 1 : 0;
  };
  return [...open]
    .sort((x, y) => SEV_RANK[y.severity] - SEV_RANK[x.severity] || fixRank(y) - fixRank(x) || impact(y, catalog) - impact(x, catalog) || x.title.localeCompare(y.title))
    .map((f) => {
      const a = best.get(f.fingerprint) ?? null;
      const sql = safeSql(f);
      const setting = parse(f.evidence).name;
      const supportManaged = typeof setting === "string" && SUPPORT_MANAGED.has(setting);
      return {
        priority: PRIORITY[f.severity],
        severity: f.severity,
        fingerprint: f.fingerprint,
        checkId: f.check_id,
        area: catalog[f.check_id]?.area ?? null,
        areaLabel: areaLabel(catalog[f.check_id]?.area),
        title: f.title,
        object: f.object,
        why: firstSentence(f.explain),
        evidence: evidenceNumbers(f.evidence),
        sql: supportManaged ? null : sql,
        note: supportManaged
          ? `support-managed: ${setting} cannot be changed from a customer session; open a support request`
          : sql
            ? null
            : "No SQL fix: follow the guidance on the finding page",
        actionId: a?.action_id ?? null,
        actionState: a?.state ?? null,
      };
    });
}

export function runbookCounts(items: RunbookItem[]): Record<Priority, number> {
  const out: Record<Priority, number> = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const i of items) out[i.priority] += 1;
  return out;
}
```

`app/server/report/strip.ts` (new file, complete):

```ts
import type { Report } from "../../client/src/report/types";
import { maskRows } from "../lib/mask";

/**
 * The report as exported: no query text, statement text in evidence or session samples, whatever the exporter's
 * PII membership. Query-grouped rows (which only carry text through the in-app label lookup) are dropped.
 */
export function strip(report: Report): Report {
  const findings = maskRows(report.findings.map((f) => ({ ...f, query_text: null })), false);
  return { ...report, findings, queryGroups: [] };
}
```

- [ ] **Step 5: Run the tests and typecheck**

Run: `cd app && npx vitest run server/tests/report_score.test.ts server/tests/report_runbook.test.ts && npx tsc -b tsconfig.server.json`
Expected: 14 tests pass; no type errors.

- [ ] **Step 6: Commit**

```bash
git add app/client/src/report/types.ts app/server/report/catalog.ts app/server/report/score.ts \
  app/server/report/runbook.ts app/server/report/strip.ts app/server/tests/report_score.test.ts \
  app/server/tests/report_runbook.test.ts
git commit -m "feat(app): report scoring, runbook ordering and export strip" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 12: Report data: the queries and the builder

Fourteen read-only queries, each bounded by the caller's project allow-list and `(project_id, branch_id)` (panels also by the branch's read-write `endpoint_id`), and `buildReport()`, which runs them in parallel, gives each panel its own success or error, scores the findings, orders the runbook and writes the banners.

**Files:**
- Create: `app/server/queries/report_{branches,findings,actions,checks,overview,databases,tables,indexes,statements,settings,io,endpoint,series,waits}.sql`
- Create: `app/server/report/build.ts`
- Test: `app/server/tests/report_build.test.ts` (create), `app/server/tests/live/queries.live.test.ts` (modify)

**Interfaces:**
- Consumes: `createRunner(...).run(name, projectIds, params)` (`server/lib/sql.ts`: binds `:project_ids` and STRING params; values come back as strings); `Access` (`server/lib/access.ts`); `QUERY_CHECKS`, `groupFindings`, `page` (`server/lib/group.ts`); `maskRows`; Task 11's `scoreAreas`, `score`, `focus`, `runbook`; the `inventory_*` tables (Task 5); `proposed_actions.origin` (added by the LLM branch).
- Produces: `ANALYSIS_MINUTES = 15`, `ReportNotFound`, `buildReport({runner, access, projectId, branch, now, scheduleMinutes, catalog?}) -> Promise<Report>` (`branch` = a branch uid or `"default"`), and the shapers `overviewOf`, `seriesOf`, `ioOf`, `histogramP99`, `endpointOf`, `branchesOf`. Banner texts the page and file show: stale engine, findings unavailable, no read-write compute, not enrolled, counters too young, no telemetry, compute not running, inventory older than two analysis runs.

- [ ] **Step 1: Write the failing test**

`app/server/tests/report_build.test.ts` (new file, complete):

```ts
import { describe, expect, it } from "vitest";
import type { Access } from "../lib/access";
import { ReportNotFound, buildReport, endpointOf, histogramP99, ioOf, seriesOf } from "../report/build";

const HC = "11111111-2222-4333-8444-555555555555";
const NOW = new Date("2026-10-29T12:00:00Z");
const A: Access = { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: true };
const BRANCHES = [
  { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", min_cu: "1", max_cu: "4", suspend_timeout_s: "300", enrolled: "true" },
  { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-r", endpoint_type: "ENDPOINT_TYPE_READ_ONLY", current_state: "ACTIVE", min_cu: "1", max_cu: "1", suspend_timeout_s: "300", enrolled: "true" },
  { branch_id: "br-dev", branch_name: "dev", is_default_branch: "false", endpoint_id: "ep-d", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "IDLE", min_cu: "0.5", max_cu: "1", suspend_timeout_s: "300", enrolled: "false" },
];
const FINDING = {
  fingerprint: "f1", check_id: "dead_tuples", title: "High share of dead tuples", category: "maintenance", endpoint_id: "ep-a",
  object: "db:hc_test.orders", status: "open", severity: "warning", measure: "15", evidence: JSON.stringify({ n_dead_tup: 5000, statement: "SELECT ssn FROM t" }),
  explain: "Dead tuples pile up. More.", candidate_sql: "VACUUM (ANALYZE) hc_test.orders;", queryid: null,
};

function runner(rows: Record<string, Record<string, unknown>[]>, fail: string[] = []) {
  const calls: { name: string; params: Record<string, unknown> }[] = [];
  return {
    calls,
    async run(name: string, _ids: string[], params: Record<string, string | Date> = {}) {
      calls.push({ name, params });
      if (fail.includes(name)) throw new Error(`${name} exploded`);
      return rows[name] ?? [];
    },
  };
}
const fresh = {
  report_overview: [{ telemetry_at: "2026-10-29T11:50:00Z", lfc_hit_pct: "99.5" }],
  meta: [{ last_success_at: "2026-10-29T11:50:00Z" }],
  report_tables: [{ collected_at: "2026-10-29T11:50:00Z", schema_name: "hc_test", table_name: "orders" }],
};
const okStatus = [{ check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok", error: null }];

describe("buildReport", () => {
  it("resolves the default branch, scopes every query to it and scores the findings", async () => {
    const r = runner({ report_branches: BRANCHES, report_findings: [FINDING], report_checks: okStatus, ...fresh });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "default", now: NOW, scheduleMinutes: 60 });
    expect(rep.branch).toEqual({ id: "br-prod", name: "production", isDefault: true });
    expect(rep.primary).toBe("ep-a");
    expect(rep.endpoints.map((e) => e.type)).toEqual(["primary", "replica"]);
    expect(r.calls.find((c) => c.name === "report_tables")?.params).toEqual({ project_id: HC, branch_id: "br-prod", endpoint_id: "ep-a" });
    expect(r.calls.find((c) => c.name === "report_findings")?.params).toEqual({ project_id: HC, branch_id: "br-prod" });
    const vac = rep.areas.find((a) => a.id === "vacuum_xid");
    expect([vac?.state, vac?.score, vac?.worst?.fingerprint]).toEqual(["warn", 3, "f1"]);
    expect(rep.score).toEqual({ pct: 60, grade: "C", assessed: 1, total: 12 });
    expect(rep.runbook[0]).toMatchObject({ fingerprint: "f1", priority: "MEDIUM", sql: "VACUUM (ANALYZE) hc_test.orders;" });
    expect(rep.banners).toEqual([]);
  });

  it("returns 404 (ReportNotFound) for a branch outside the project", async () => {
    const r = runner({ report_branches: BRANCHES });
    await expect(buildReport({ runner: r, access: A, projectId: HC, branch: "br-other", now: NOW, scheduleMinutes: 60 })).rejects.toBeInstanceOf(ReportNotFound);
    await expect(buildReport({ runner: runner({}), access: A, projectId: HC, branch: "default", now: NOW, scheduleMinutes: 60 })).rejects.toBeInstanceOf(ReportNotFound);
  });

  it("never carries statement text in evidence, even for a PII member", async () => {
    const r = runner({ report_branches: BRANCHES, report_findings: [FINDING], report_checks: okStatus, ...fresh });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(JSON.stringify(rep)).not.toContain("SELECT ssn");
  });

  it("keeps rendering when one section fails, and marks that section", async () => {
    const r = runner({ report_branches: BRANCHES, report_findings: [FINDING], report_checks: okStatus, ...fresh }, ["report_tables"]);
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(rep.sections.tables).toEqual({ status: "error", data: [], error: "This panel could not be loaded." });
    expect(rep.sections.settings.status).toBe("ok");
    expect(rep.runbook).toHaveLength(1);
  });

  it("explains a branch that is not enrolled and a suspended compute, without inventory queries failing", async () => {
    const r = runner({ report_branches: BRANCHES, ...fresh });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-dev", now: NOW, scheduleMinutes: 60 });
    expect(rep.banners.some((b) => b.startsWith("Postgres checks are not enabled on this branch"))).toBe(true);
    expect(rep.banners.some((b) => b.startsWith("The compute is idle"))).toBe(true);
  });

  it("warns about young counters, stale engine data and missing telemetry", async () => {
    const r = runner({
      report_branches: BRANCHES,
      report_checks: [{ check_id: "unused_index", endpoint_id: "ep-a", status: "skipped:stats_young", error: "counters too young (reset 3 h ago)" }],
      meta: [{ last_success_at: "2026-10-29T08:00:00Z" }],
    });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(rep.banners).toEqual(expect.arrayContaining([
      expect.stringMatching(/^The health engine has not completed a run/),
      expect.stringMatching(/reset 3 h ago/),
      expect.stringMatching(/^No telemetry for this branch/),
    ]));
    expect(rep.areas.find((a) => a.id === "indexes")?.note).toBe("counters too young (reset 3 h ago)");
    expect(rep.score.assessed).toBe(0);
  });

  it("marks inventory older than two analysis runs as stale", async () => {
    const r = runner({ report_branches: BRANCHES, ...fresh, report_tables: [{ collected_at: "2026-10-29T10:00:00Z", table_name: "t" }] });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect([rep.sections.tables.asOf, rep.sections.tables.stale]).toEqual(["2026-10-29T10:00:00.000Z", true]);
    expect(rep.banners.some((b) => b.startsWith("The inventory is older than two analysis runs"))).toBe(true);
  });
});

describe("section shapers", () => {
  it("pivots the long-format series", () => {
    expect(seriesOf([{ bucket: "2026-10-29T11:15:00.000Z", metric: "wal_bytes", value: "10" }, { bucket: "2026-10-29T11:00:00.000Z", metric: "wal_bytes", value: "5" }]))
      .toEqual({ wal_bytes: [{ t: "2026-10-29T11:00:00.000Z", v: 5 }, { t: "2026-10-29T11:15:00.000Z", v: 10 }] });
  });

  it("shows pg_stat_io as the change since the previous snapshot, or totals after a reset", () => {
    const row = (at: string, reads: string, reset = "2026-10-28T00:00:00Z") => ({
      collected_at: at, backend_type: "autovacuum worker", object: "relation", context: "bulkread", reads, writes: "0",
      extends: "0", hits: "10", evictions: "1", reuses: "0", fsyncs: "0", read_time: null, write_time: null, stats_reset: reset,
    });
    const d = ioOf([row("2026-10-29T12:00:00Z", "150"), row("2026-10-29T11:45:00Z", "100")]);
    expect([d.delta, d.windowS, d.rows[0].reads, d.rows[0].hits, d.timing]).toEqual([true, 900, 50, 0, false]);
    const reset = ioOf([row("2026-10-29T12:00:00Z", "20", "2026-10-29T11:50:00Z"), row("2026-10-29T11:45:00Z", "100")]);
    expect([reset.delta, reset.rows[0].reads]).toEqual([false, 20]);
  });

  it("computes p99 from histogram bucket deltas", () => {
    const prev = new Map([[0.001, 0], [0.01, 0], [0.1, 0], [Infinity, 0]]);
    const cur = new Map([[0.001, 50], [0.01, 98], [0.1, 100], [Infinity, 100]]);
    expect(histogramP99(prev, cur)).toBe(100);
    expect(histogramP99(cur, prev)).toBeNull();
  });

  it("reads endpoint metrics: latest values, sources, idle clients, getpage p99", () => {
    const m = endpointOf([
      { collected_at: "2026-10-29T11:45:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.01}', value: "10" },
      { collected_at: "2026-10-29T11:45:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.1}', value: "10" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.01}', value: "110" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.1}', value: "110" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "autovacuum_workers_busy", dims: null, value: "2" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "source:neon_perf_counters", dims: null, value: "0", note: "not available: no SELECT on neon_perf_counters" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "idle_clients", dims: '{"role": "app", "application": "worker", "max_idle_s": 900}', value: "3" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "server_version", dims: null, value: null, note: "17.5" },
    ]);
    expect(m.latest).toEqual({ autovacuum_workers_busy: 2 });
    expect(m.sources.neon_perf_counters).toEqual({ available: false, note: "not available: no SELECT on neon_perf_counters" });
    expect(m.idleClients).toEqual([{ role: "app", application: "worker", sessions: 3, maxIdleS: 900 }]);
    expect(m.getpageP99).toEqual([{ t: "2026-10-29T12:00:00.000Z", v: 10 }]);
    expect(m.serverVersion).toBe("17.5");
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd app && npx vitest run server/tests/report_build.test.ts`
Expected: FAIL with `Failed to resolve import "../report/build"`.

- [ ] **Step 3: The queries**

Each file is complete. The inventory panels read the latest snapshot of the read-write endpoint (`QUALIFY collected_at = max(collected_at) OVER ()`); the series are 15-minute buckets over 24 h in long format (`bucket, metric, value`).

`app/server/queries/report_branches.sql`:

```sql
SELECT branch_id, branch_name, is_default_branch, endpoint_id, endpoint_type, current_state, min_cu, max_cu,
       suspend_timeout_s, enrolled, captured_at
FROM {catalog}.lakebase_health.inventory
WHERE run_id = (SELECT max_by(run_id, captured_at) FROM {catalog}.lakebase_health.inventory)
  AND project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
ORDER BY is_default_branch DESC, branch_name, endpoint_type DESC, endpoint_id
```

`app/server/queries/report_findings.sql`:

```sql
SELECT fingerprint, check_id, title, category, branch_id, endpoint_id, object, status, severity, confidence, measure,
       first_seen, last_seen, occurrences, evidence, explain, candidate_sql,
       get_json_object(evidence, '$.queryid') AS queryid,
       to_json(transform(from_json(get_json_object(evidence, '$.top_queries'), 'array<struct<queryid:string>>'), x -> x.queryid)) AS top_queryids,
       coalesce(CAST(get_json_object(evidence, '$.total_ms') AS DOUBLE), CAST(get_json_object(evidence, '$.misestimated_ms') AS DOUBLE)) AS db_ms
FROM {catalog}.lakebase_health.findings
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND (branch_id = :branch_id OR branch_id IS NULL) AND status IN ('new', 'open')
ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, last_seen DESC
LIMIT 2000
```

`app/server/queries/report_actions.sql`:

```sql
SELECT a.action_id, a.fingerprint, a.state
FROM {catalog}.lakebase_health.proposed_actions a
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
WHERE f.project_id = :project_id AND array_contains(split(:project_ids, ','), f.project_id)
  AND (f.branch_id = :branch_id OR f.branch_id IS NULL) AND f.status IN ('new', 'open')
  AND coalesce(a.origin, 'check') = 'check'
  AND a.state IN ('proposed', 'verifying', 'verified', 'verify_failed', 'applying', 'apply_failed', 'blocked')
```

`app/server/queries/report_checks.sql`:

```sql
SELECT check_id, endpoint_id, status, error, ts
FROM {catalog}.lakebase_health.check_results
WHERE fingerprint IS NULL AND ts >= current_timestamp() - INTERVAL 2 DAYS AND status <> 'skipped:not_scheduled'
  AND (project_id IS NULL OR (project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)))
QUALIFY row_number() OVER (PARTITION BY check_id, coalesce(endpoint_id, '*') ORDER BY ts DESC) = 1
```

`app/server/queries/report_overview.sql`:

```sql
WITH cc AS (
  SELECT * FROM {catalog}.lakebase_health.raw_compute_counters
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
), cg AS (
  SELECT * FROM {catalog}.lakebase_health.raw_compute_gauges
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
), dg AS (
  SELECT * FROM {catalog}.lakebase_health.raw_database_gauges
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
), dc AS (
  SELECT * FROM {catalog}.lakebase_health.raw_database_counters
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
), ph AS (
  SELECT neon_getpage_wait_us, neon_getpage_count FROM {catalog}.lakebase_health.raw_plan_history
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS AND neon_getpage_count > 0
), ps AS (
  SELECT temp_blks_written FROM {catalog}.lakebase_health.raw_pg_stat_statements_counters
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
), ie AS (
  SELECT metric, value, collected_at FROM {catalog}.lakebase_health.inventory_endpoint
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 24 HOURS
    AND metric = 'oldest_xact_s'
)
SELECT
  (SELECT 100.0 * sum(lfc_hits) / nullif(sum(lfc_hits) + sum(lfc_misses), 0) FROM cc) AS lfc_hit_pct,
  (SELECT max_by(lfc_working_set_size_60m, ts) FROM cg) AS working_set_bytes,
  (SELECT max_by(lfc_size_limit, ts) FROM cg) AS lfc_size_bytes,
  (SELECT max_by(max_connections, ts) FROM cg) AS max_connections,
  (SELECT max(ts) FROM cg) AS telemetry_at,
  (SELECT sum(numbackends) FROM dg WHERE ts = (SELECT max(ts) FROM dg)) AS connections,
  (SELECT sum(size) FROM dg WHERE ts = (SELECT max(ts) FROM dg)) AS database_bytes,
  (SELECT max_by(value, collected_at) FROM ie) AS oldest_xact_s,
  (SELECT percentile_approx(neon_getpage_wait_us / 1000.0 / neon_getpage_count, 0.99) FROM ph) AS getpage_p99_ms,
  (SELECT sum(temp_blks_written) * 8192 FROM ps) AS temp_spill_bytes,
  (SELECT sum(xact_commit) / 86400.0 FROM dc) AS commits_per_s
```

`app/server/queries/report_databases.sql`:

```sql
SELECT datname, max_by(size, ts) AS size_bytes, max_by(numbackends, ts) AS backends,
       max_by(oldest_frozen_xid_age, ts) AS xid_age, max(ts) AS at
FROM {catalog}.lakebase_health.raw_database_gauges
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY datname
ORDER BY size_bytes DESC
```

`app/server/queries/report_tables.sql`:

```sql
SELECT * FROM {catalog}.lakebase_health.inventory_tables
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 7 DAYS
QUALIFY collected_at = max(collected_at) OVER ()
ORDER BY total_bytes DESC
LIMIT 500
```

`app/server/queries/report_indexes.sql`:

```sql
SELECT * FROM {catalog}.lakebase_health.inventory_indexes
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 7 DAYS
QUALIFY collected_at = max(collected_at) OVER ()
ORDER BY index_bytes DESC
LIMIT 500
```

`app/server/queries/report_statements.sql`:

```sql
SELECT * FROM {catalog}.lakebase_health.inventory_statements
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 7 DAYS
QUALIFY collected_at = max(collected_at) OVER ()
ORDER BY ranking, rank
LIMIT 500
```

`app/server/queries/report_settings.sql`:

```sql
SELECT * FROM {catalog}.lakebase_health.inventory_settings
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 7 DAYS
QUALIFY collected_at = max(collected_at) OVER ()
ORDER BY name
LIMIT 500
```

`app/server/queries/report_io.sql`:

```sql
SELECT * FROM {catalog}.lakebase_health.inventory_io
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 1 DAY
QUALIFY dense_rank() OVER (ORDER BY collected_at DESC) <= 2
ORDER BY collected_at DESC, backend_type, object, context
```

`app/server/queries/report_endpoint.sql`:

```sql
SELECT collected_at, metric, dims, value, note FROM {catalog}.lakebase_health.inventory_endpoint
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 24 HOURS
  AND (metric NOT LIKE 'neon.perf.%' OR metric LIKE 'neon.perf.getpage_wait_seconds_bucket')
ORDER BY collected_at, metric
```

`app/server/queries/report_series.sql`:

```sql
WITH win AS (SELECT current_timestamp() - INTERVAL 24 HOURS AS since)
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900) AS bucket, 'lfc_hit_pct' AS metric,
       100.0 * sum(lfc_hits) / nullif(sum(lfc_hits) + sum(lfc_misses), 0) AS value
FROM {catalog}.lakebase_health.raw_compute_counters, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= win.since
GROUP BY 1
UNION ALL
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900), 'working_set_bytes', CAST(max(lfc_working_set_size_60m) AS DOUBLE)
FROM {catalog}.lakebase_health.raw_compute_gauges, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= win.since
GROUP BY 1
UNION ALL
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900), 'lfc_size_bytes', CAST(max(lfc_size_limit) AS DOUBLE)
FROM {catalog}.lakebase_health.raw_compute_gauges, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= win.since
GROUP BY 1
UNION ALL
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900), 'replica_lag_bytes', CAST(max(replication_lag_bytes) AS DOUBLE)
FROM {catalog}.lakebase_health.raw_compute_gauges, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND branch_id = :branch_id AND endpoint_id <> :endpoint_id AND ts >= win.since
GROUP BY 1
UNION ALL
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900), 'getpage_avg_ms',
       sum(neon_getpage_wait_us) / 1000.0 / nullif(sum(neon_getpage_count), 0)
FROM {catalog}.lakebase_health.raw_plan_history, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= win.since AND neon_getpage_count > 0
GROUP BY 1
UNION ALL
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900), 'getpage_p99_ms',
       percentile_approx(neon_getpage_wait_us / 1000.0 / neon_getpage_count, 0.99)
FROM {catalog}.lakebase_health.raw_plan_history, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= win.since AND neon_getpage_count > 0
GROUP BY 1
UNION ALL
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900), 'wal_bytes', CAST(sum(wal_bytes) AS DOUBLE)
FROM {catalog}.lakebase_health.raw_pg_stat_statements_counters, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= win.since
GROUP BY 1
UNION ALL
SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900), 'wal_fpi', CAST(sum(wal_fpi) AS DOUBLE)
FROM {catalog}.lakebase_health.raw_pg_stat_statements_counters, win
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= win.since
GROUP BY 1
ORDER BY bucket, metric
```

`app/server/queries/report_waits.sql`:

```sql
WITH s AS (
  SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900) AS bucket, ts,
         coalesce(wait_event_type, 'CPU') AS wait_class
  FROM {catalog}.lakebase_health.raw_active_session_history
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
), n AS (SELECT bucket, count(DISTINCT ts) AS samples FROM s GROUP BY bucket)
SELECT s.bucket, s.wait_class, count(*) / max(n.samples) AS avg_active
FROM s JOIN n ON n.bucket = s.bucket
GROUP BY s.bucket, s.wait_class
ORDER BY s.bucket, s.wait_class
```

`server/tests/readonly.test.ts` already requires every query to start with `SELECT`/`WITH`, carry `:project_ids` and filter open findings exactly as `status IN ('new', 'open')`; the files above satisfy it.

- [ ] **Step 4: The builder**

`app/server/report/build.ts` (new file, complete):

```ts
import type {
  Branch,
  Endpoint,
  EndpointMetrics,
  IoData,
  IoRow,
  Overview,
  Report,
  ReportFinding,
  Row,
  Section,
  Series,
  SeriesPoint,
} from "../../client/src/report/types";
import type { Access } from "../lib/access";
import { QUERY_CHECKS, groupFindings, page } from "../lib/group";
import { maskRows } from "../lib/mask";
import type { Row as SqlRow } from "../lib/sql";
import { CATALOG, type Catalog } from "./catalog";
import { runbook, type ActionRow } from "./runbook";
import { focus, score, scoreAreas, type CheckStatus } from "./score";

export type Runner = { run(name: string, ids: string[], params?: Record<string, string | Date>): Promise<SqlRow[]> };

/** The analysis job's schedule (resources/analysis_job.yml): inventory older than two intervals is stale. */
export const ANALYSIS_MINUTES = 15;

export class ReportNotFound extends Error {}

const num = (v: unknown): number | null => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const str = (v: unknown): string | null => (typeof v === "string" ? v : typeof v === "number" ? String(v) : null);
const truthy = (v: unknown) => v === true || v === "true";
const cell = (v: unknown): Row[string] =>
  v === null || v === undefined ? null : typeof v === "number" || typeof v === "boolean" || typeof v === "string" ? v : JSON.stringify(v);
const rowOf = (r: SqlRow): Row => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, cell(v)]));
const iso = (v: unknown) => {
  const s = str(v);
  if (!s) return null;
  const d = new Date(s.includes("T") || s.endsWith("Z") ? s : `${s.replace(" ", "T")}Z`);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
};

async function section<T>(work: () => Promise<SqlRow[]>, shape: (rows: SqlRow[]) => T, empty: T, now: Date, at?: string): Promise<Section<T>> {
  try {
    const rows = await work();
    const asOf = at ? iso(rows[0]?.[at]) : undefined;
    const stale = asOf ? now.getTime() - new Date(asOf).getTime() > 2 * ANALYSIS_MINUTES * 60_000 : undefined;
    return { status: "ok", data: shape(rows), ...(at ? { asOf: asOf ?? null, stale } : {}) };
  } catch (e) {
    console.error("lakebase-health report section failed", e);
    return { status: "error", data: empty, error: "This panel could not be loaded." };
  }
}

export function overviewOf(rows: SqlRow[]): Overview {
  const r = rows[0] ?? {};
  return {
    lfcHitPct: num(r.lfc_hit_pct),
    workingSetBytes: num(r.working_set_bytes),
    lfcSizeBytes: num(r.lfc_size_bytes),
    connections: num(r.connections),
    maxConnections: num(r.max_connections),
    databaseBytes: num(r.database_bytes),
    oldestXactS: num(r.oldest_xact_s),
    getpageP99Ms: num(r.getpage_p99_ms),
    tempSpillBytes: num(r.temp_spill_bytes),
    commitsPerS: num(r.commits_per_s),
    telemetryAt: iso(r.telemetry_at),
  };
}

export function seriesOf(rows: SqlRow[]): Series {
  const out: Series = {};
  for (const r of rows) {
    const m = str(r.metric);
    const t = iso(r.bucket);
    if (!m || !t) continue;
    (out[m] ??= []).push({ t, v: num(r.value) });
  }
  for (const pts of Object.values(out)) pts.sort((a, b) => a.t.localeCompare(b.t));
  return out;
}

const IO_COUNTERS = ["reads", "writes", "extends", "hits", "evictions", "reuses", "fsyncs", "read_time", "write_time"] as const;
const ioKey = (r: SqlRow) => [r.backend_type, r.object, r.context].map((v) => str(v) ?? "").join("|");

/** pg_stat_io: the change since the previous snapshot when both exist and nothing was reset, else the totals. */
export function ioOf(rows: SqlRow[]): IoData {
  const times = [...new Set(rows.map((r) => iso(r.collected_at)).filter((t): t is string => !!t))].sort().reverse();
  const at = (t: string | undefined) => rows.filter((r) => t !== undefined && iso(r.collected_at) === t);
  const latest = at(times[0]);
  const prev = new Map(at(times[1]).map((r) => [ioKey(r), r]));
  const toRow = (r: SqlRow): IoRow => ({
    backend_type: str(r.backend_type) ?? "", object: str(r.object) ?? "", context: str(r.context) ?? "",
    ...Object.fromEntries(IO_COUNTERS.map((c) => [c, num(r[c])])),
  }) as IoRow;
  const timing = latest.some((r) => num(r.read_time) !== null || num(r.write_time) !== null);
  const sameReset = latest.every((r) => {
    const p = prev.get(ioKey(r));
    return p !== undefined && iso(p.stats_reset) === iso(r.stats_reset) && IO_COUNTERS.every((c) => (num(r[c]) ?? 0) >= (num(p[c]) ?? 0));
  });
  const delta = times.length > 1 && latest.length > 0 && sameReset;
  const out = latest.map((r) => {
    const cur = toRow(r);
    if (!delta) return cur;
    const p = toRow(prev.get(ioKey(r)) as SqlRow);
    return { ...cur, ...Object.fromEntries(IO_COUNTERS.map((c) => [c, cur[c] === null ? null : (cur[c] ?? 0) - (p[c] ?? 0)])) } as IoRow;
  });
  return {
    rows: out.sort((x, y) => (y.reads ?? 0) + (y.hits ?? 0) - ((x.reads ?? 0) + (x.hits ?? 0))),
    delta,
    timing,
    windowS: delta ? (new Date(times[0]).getTime() - new Date(times[1]).getTime()) / 1000 : null,
    statsReset: iso(latest[0]?.stats_reset),
  };
}

/** p99 (ms) from the change in a cumulative histogram between two snapshots ({le: count}). */
export function histogramP99(prev: Map<number, number>, cur: Map<number, number>): number | null {
  const les = [...cur.keys()].sort((a, b) => a - b);
  if (les.length === 0) return null;
  const d = les.map((le) => ({ le, n: (cur.get(le) ?? 0) - (prev.get(le) ?? 0) }));
  if (d.some((x) => x.n < 0)) return null; // reset between the snapshots
  const total = d[d.length - 1].n;
  if (total <= 0) return null;
  const hit = d.find((x) => x.n >= 0.99 * total);
  return hit && Number.isFinite(hit.le) ? hit.le * 1000 : null;
}

export function endpointOf(rows: SqlRow[]): EndpointMetrics {
  const times = [...new Set(rows.map((r) => iso(r.collected_at)).filter((t): t is string => !!t))].sort();
  const last = times[times.length - 1];
  const latestRows = rows.filter((r) => iso(r.collected_at) === last);
  const latest: Record<string, number | null> = {};
  const sources: EndpointMetrics["sources"] = {};
  const idleClients: EndpointMetrics["idleClients"] = [];
  let serverVersion: string | null = null;
  for (const r of latestRows) {
    const m = String(r.metric);
    if (m.startsWith("source:")) sources[m.slice(7)] = { available: num(r.value) === 1, note: str(r.note) };
    else if (m === "idle_clients") {
      const d = JSON.parse(str(r.dims) ?? "{}") as Record<string, unknown>;
      idleClients.push({ role: str(d.role) ?? "", application: str(d.application) ?? "", sessions: num(r.value) ?? 0, maxIdleS: num(d.max_idle_s) });
    } else if (m === "server_version") serverVersion = str(r.note);
    else if (!m.startsWith("neon.perf.")) latest[m] = num(r.value);
  }
  const hist = new Map<string, Map<number, number>>();
  for (const r of rows) {
    if (r.metric !== "neon.perf.getpage_wait_seconds_bucket") continue;
    const t = iso(r.collected_at);
    const le = num((JSON.parse(str(r.dims) ?? "{}") as { le?: unknown }).le);
    if (!t || le === null) continue;
    (hist.get(t) ?? hist.set(t, new Map()).get(t)!).set(le, num(r.value) ?? 0);
  }
  const ht = [...hist.keys()].sort();
  const getpageP99: SeriesPoint[] = ht.slice(1).map((t, i) => ({ t, v: histogramP99(hist.get(ht[i])!, hist.get(t)!) }));
  return { latest, sources, idleClients: idleClients.sort((a, b) => b.sessions - a.sessions), getpageP99, serverVersion };
}

function findingOf(r: SqlRow, catalog: Catalog): ReportFinding {
  return {
    fingerprint: String(r.fingerprint), check_id: String(r.check_id), title: String(r.title ?? r.check_id),
    category: str(r.category) ?? "", area: catalog[String(r.check_id)]?.area ?? null, endpoint_id: str(r.endpoint_id),
    object: str(r.object) ?? "", status: String(r.status), severity: String(r.severity) as ReportFinding["severity"],
    confidence: str(r.confidence), measure: num(r.measure), first_seen: iso(r.first_seen), last_seen: iso(r.last_seen),
    evidence: str(r.evidence), explain: str(r.explain), candidate_sql: str(r.candidate_sql), queryid: str(r.queryid),
    query_text: null,
  };
}

export function branchesOf(rows: SqlRow[]): { branches: Branch[]; endpoints: Map<string, Endpoint[]> } {
  const branches = new Map<string, Branch>();
  const endpoints = new Map<string, Endpoint[]>();
  for (const r of rows) {
    const id = String(r.branch_id);
    if (!branches.has(id)) branches.set(id, { id, name: str(r.branch_name) ?? id, isDefault: truthy(r.is_default_branch) });
    (endpoints.get(id) ?? endpoints.set(id, []).get(id)!).push({
      id: String(r.endpoint_id), type: String(r.endpoint_type).endsWith("READ_WRITE") ? "primary" : "replica",
      state: str(r.current_state) ?? "", minCu: num(r.min_cu), maxCu: num(r.max_cu),
      suspendTimeoutS: num(r.suspend_timeout_s), enrolled: truthy(r.enrolled),
    });
  }
  return { branches: [...branches.values()], endpoints };
}

const hoursAgo = (note: string | null) => /reset (\d+) h ago/.exec(note ?? "")?.[1];

/**
 * The live report for one branch (rawBranch: a branch id, or "default"). Throws ReportNotFound when the branch is
 * not one of the project's. Each panel fails on its own: an error section, never a failed page.
 */
export async function buildReport(opts: {
  runner: Runner;
  access: Access;
  projectId: string;
  branch: string;
  now: Date;
  scheduleMinutes: number;
  catalog?: Catalog;
}): Promise<Report> {
  const { runner, access: a, projectId, now } = opts;
  const catalog = opts.catalog ?? CATALOG;
  const ids = a.projectIds;
  const inv = branchesOf(await runner.run("report_branches", ids, { project_id: projectId }));
  const branch = opts.branch === "default"
    ? inv.branches.find((b) => b.isDefault) ?? inv.branches[0]
    : inv.branches.find((b) => b.id === opts.branch);
  if (!branch) throw new ReportNotFound("not found");
  const endpoints = inv.endpoints.get(branch.id) ?? [];
  const primary = endpoints.find((e) => e.type === "primary") ?? null;
  const base = { project_id: projectId, branch_id: branch.id };
  const ep = { ...base, endpoint_id: primary?.id ?? "-" };
  const run = (name: string, p: Record<string, string> = ep) => () => runner.run(name, ids, p);
  const rows = (rs: SqlRow[]) => rs.map(rowOf);
  const none = <T>(empty: T): Promise<Section<T>> => Promise.resolve({ status: "ok", data: empty, asOf: null });
  const emptyOverview = overviewOf([]);
  const emptyEndpoint = endpointOf([]);
  const emptyIo: IoData = { rows: [], delta: false, timing: false, windowS: null, statsReset: null };

  const [findingsS, actionsS, checksS, metaS, overview, databases, tables, indexes, statements, settings, io, endpoint, series, waits] =
    await Promise.all([
      section(run("report_findings", base), (r) => r, [] as SqlRow[], now),
      section(run("report_actions", base), (r) => r, [] as SqlRow[], now),
      section(run("report_checks", base), (r) => r, [] as SqlRow[], now),
      section(() => runner.run("meta", ids), (r) => r, [] as SqlRow[], now),
      primary ? section(run("report_overview"), overviewOf, emptyOverview, now) : none(emptyOverview),
      primary ? section(run("report_databases"), rows, [], now) : none<Row[]>([]),
      primary ? section(run("report_tables"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_indexes"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_statements"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_settings"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_io"), ioOf, emptyIo, now, "collected_at") : none(emptyIo),
      primary ? section(run("report_endpoint"), endpointOf, emptyEndpoint, now) : none(emptyEndpoint),
      primary ? section(run("report_series"), seriesOf, {} as Series, now) : none<Series>({}),
      primary ? section(run("report_waits"), rows, [], now) : none<Row[]>([]),
    ]);

  const rawFindings = maskRows(findingsS.data, false);
  const findings = rawFindings.map((r) => findingOf(r, catalog));
  const statuses: CheckStatus[] = checksS.data.map((r) => ({
    check_id: String(r.check_id), endpoint_id: str(r.endpoint_id), status: String(r.status), error: str(r.error),
  }));
  const telemetryAt = overview.data.telemetryAt;
  const telemetry = !!telemetryAt && now.getTime() - new Date(telemetryAt).getTime() < 24 * 3600_000;
  const enrolled = !!primary?.enrolled;
  const areas = scoreAreas(findings, statuses, { primaryEndpoints: primary ? [primary.id] : [], telemetry, enrolled }, catalog);

  const banners: string[] = [];
  const lastRun = iso(metaS.data[0]?.last_success_at);
  if (!lastRun || now.getTime() - new Date(lastRun).getTime() > 2 * opts.scheduleMinutes * 60_000)
    banners.push("The health engine has not completed a run in over two schedule intervals; this report may be out of date.");
  if (findingsS.status === "error" || checksS.status === "error")
    banners.push("Findings could not be loaded: the scorecard and runbook are incomplete.");
  if (!primary) banners.push("This branch has no read-write compute: inventory panels and charts are empty.");
  else if (!enrolled)
    banners.push("Postgres checks are not enabled on this branch, so its Postgres areas are not assessed and the inventory is empty. Enable them with GRANT pg_monitor to the engine's role (README: Enroll a project for Postgres checks).");
  const young = statuses.find((s) => s.status === "skipped:stats_young" && s.endpoint_id === primary?.id);
  if (young) banners.push(`Counters too young: Postgres statistics were reset ${hoursAgo(young.error) ?? "less than 24"} h ago, so counter-based checks wait until they are 24 h old.`);
  if (primary && !telemetry) banners.push("No telemetry for this branch in the last 24 h: telemetry areas are not assessed.");
  if (primary && primary.state !== "ACTIVE")
    banners.push(`The compute is ${primary.state.toLowerCase() || "not running"}: panels show the last snapshot taken while it ran; nothing here wakes it.`);
  else if (enrolled && tables.status === "ok" && (tables.stale || !tables.asOf))
    banners.push("The inventory is older than two analysis runs: panels show their age; the score uses findings only.");

  const queryRows = rawFindings.filter((r) => QUERY_CHECKS.has(String(r.check_id)));
  return {
    project: { id: projectId, name: a.projectNames[projectId] ?? projectId },
    branch,
    branches: inv.branches,
    endpoints,
    primary: primary?.id ?? null,
    pgVersion: endpoint.data.serverVersion,
    dataAsOf: lastRun,
    generatedAt: now.toISOString(),
    banners,
    score: score(areas),
    areas,
    focus: focus(areas),
    findings,
    runbook: runbook(findings, actionsS.data.map((r) => ({ action_id: String(r.action_id), fingerprint: String(r.fingerprint), state: String(r.state) })) as ActionRow[], catalog),
    sections: { overview, databases, tables, indexes, statements, settings, io, endpoint, series, waits },
    queryGroups: page(groupFindings(queryRows), true).rows,
  };
}
```

- [ ] **Step 5: The live query test**

`app/server/tests/live/queries.live.test.ts` (opt-in, `npm run test:live`; run in Task 18):

Replace:

```ts
    expect(Object.keys(grouped[0])).toEqual(expect.arrayContaining(["queryid", "top_queryids", "db_ms"]));
  }, 120_000);

  it("returns nothing for a project outside the allow-list", async () => {
    const rows = await runner.run("project_findings", ["00000000-0000-0000-0000-000000000000"], { project_id: HC });
    expect(rows).toEqual([]);
```

with:

```ts
    expect(Object.keys(grouped[0])).toEqual(expect.arrayContaining(["queryid", "top_queryids", "db_ms"]));
  }, 120_000);

  it("report queries run on healthcheck's default branch within 5 s each", async () => {
    const { rows: br } = await timed("report_branches", { project_id: HC });
    const prod = br.find((r) => r.is_default_branch === true || r.is_default_branch === "true");
    expect(prod).toBeDefined();
    const branch = String(prod?.branch_id);
    const ep = String(br.find((r) => r.branch_id === branch && String(r.endpoint_type).endsWith("READ_WRITE"))?.endpoint_id);
    for (const name of ["report_findings", "report_actions", "report_checks"]) {
      const { ms } = await timed(name, { project_id: HC, branch_id: branch });
      expect(ms, name).toBeLessThan(5000);
    }
    const panels = ["report_overview", "report_databases", "report_tables", "report_indexes", "report_statements",
      "report_settings", "report_io", "report_endpoint", "report_series", "report_waits"];
    for (const name of panels) {
      const { rows, ms } = await timed(name, { project_id: HC, branch_id: branch, endpoint_id: ep });
      expect(ms, name).toBeLessThan(5000);
      if (name === "report_overview") expect(rows).toHaveLength(1);
    }
  }, 300_000);

  it("returns nothing for a project outside the allow-list", async () => {
    const rows = await runner.run("project_findings", ["00000000-0000-0000-0000-000000000000"], { project_id: HC });
    expect(rows).toEqual([]);
```

- [ ] **Step 6: Run the tests and typecheck**

Run: `cd app && npx vitest run server && npx tsc -b tsconfig.server.json`
Expected: all server tests pass (live tests skipped); no type errors.

- [ ] **Step 7: Commit**

```bash
git add app/server/queries/report_*.sql app/server/report/build.ts app/server/tests/report_build.test.ts \
  app/server/tests/live/queries.live.test.ts
git commit -m "feat(app): report queries and builder with per-panel errors and banners" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 13: The report route and its access rule

`GET /api/report/:id/:branch` returns the report JSON: 400 for a malformed project or branch, 404 for a project the caller cannot see (no query runs) or a branch that is not one of the project's.

**Files:**
- Modify: `app/server/lib/validate.ts`, `app/server/lib/handlers.ts` **[LLM-merge]**, `app/server/routes.ts` **[LLM-merge]**
- Test: `app/server/tests/report_handlers.test.ts` (create)

**Interfaces:**
- Consumes: `buildReport`, `ReportNotFound` (Task 12); `who(ctx)`, `HttpError`, `Ctx` in `handlers.ts`.
- Produces: `parseBranchRef(s) -> string | null` (`default` or `br-…`); `reportScope(ctx, rawId, rawBranch) -> {a, id, report}` inside `createHandlers` (Task 15 reuses it); handler `report(ctx, rawId, rawBranch) -> Promise<Report>`; route `GET /api/report/:id/:branch`.

- [ ] **Step 1: Write the failing test**

`app/server/tests/report_handlers.test.ts` (new file; Task 15 appends the export tests):

```ts
import { describe, expect, it } from "vitest";
import type { Access } from "../lib/access";
import { createHandlers } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const OTHER = "11111111-2222-3333-4444-555555555555";
const ctx = { token: "t", user: "alice", now: new Date("2026-10-29T12:00:00Z") };
const BRANCH = { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a",
  endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", enrolled: "true" };

function deps(rows: Record<string, Record<string, unknown>[]> = {}) {
  const calls: string[] = [];
  return {
    calls,
    access: {
      async resolve() {
        const a: Access = { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: false };
        return a;
      },
      refresh() {},
    },
    runner: {
      async run(name: string) {
        calls.push(name);
        return rows[name] ?? [];
      },
    },
    scheduleMinutes: 60,
  };
}

describe("report handler", () => {
  it("returns the report of a visible branch", async () => {
    const out = await createHandlers(deps({ report_branches: [BRANCH] })).report(ctx, HC, "br-prod");
    expect(out.project).toEqual({ id: HC, name: "healthcheck" });
    expect(out.branch.id).toBe("br-prod");
    expect(out.areas).toHaveLength(12);
  });

  it("returns 404 for a hidden project without querying, and for a branch the project does not have", async () => {
    const d = deps({ report_branches: [BRANCH] });
    await expect(createHandlers(d).report(ctx, OTHER, "br-prod")).rejects.toMatchObject({ status: 404 });
    expect(d.calls).toEqual([]);
    await expect(createHandlers(d).report(ctx, HC, "br-elsewhere")).rejects.toMatchObject({ status: 404 });
  });

  it("rejects malformed ids with 400", async () => {
    const h = createHandlers(deps());
    await expect(h.report(ctx, "nope", "default")).rejects.toMatchObject({ status: 400 });
    await expect(h.report(ctx, HC, "main; DROP")).rejects.toMatchObject({ status: 400 });
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd app && npx vitest run server/tests/report_handlers.test.ts`
Expected: FAIL with `TypeError: createHandlers(...).report is not a function`.

- [ ] **Step 3: Validate the branch reference**

`app/server/lib/validate.ts`:

Replace:

```ts
export const parseEndpointId = match(ENDPOINT);
export const parseProjectId = match(UUID);
export const parseActionId = match(FINGERPRINT);

function parseDate(s: unknown): Date | null | undefined {
  if (s === undefined || s === "") return undefined;
```

with:

```ts
export const parseEndpointId = match(ENDPOINT);
export const parseProjectId = match(UUID);
export const parseActionId = match(FINGERPRINT);
/** A branch uid, or "default" for the project's default branch (the report URL). */
export const parseBranchRef = match(/^(default|br-[a-z0-9-]{1,61})$/);

function parseDate(s: unknown): Date | null | undefined {
  if (s === undefined || s === "") return undefined;
```

- [ ] **Step 4: The handler** **[LLM-merge]**

`app/server/lib/handlers.ts`. The LLM branch added `drafts` imports, the `llm` dependency, `draftView`/`visibleFinding` before `return {`, and the `drafts`/`requestDraft` handlers before `refresh`; the anchors below are the merged text:

Replace (1 of 3):

```ts
import { maskRows } from "./mask";
import { parseQueryPairs, queryLabel, skeleton } from "./querytext";
import type { Row } from "./sql";
import { parseActionId, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "./validate";

export type Ctx = { token?: string; user?: string; now: Date };
type Deps = {
```

with:

```ts
import { maskRows } from "./mask";
import { parseQueryPairs, queryLabel, skeleton } from "./querytext";
import type { Row } from "./sql";
import { ReportNotFound, buildReport } from "../report/build";
import { parseActionId, parseBranchRef, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "./validate";

export type Ctx = { token?: string; user?: string; now: Date };
type Deps = {
```

Replace (2 of 3):

```ts
    return { a, fp, finding };
  }

  return {
    async meta(ctx: Ctx) {
      const a = await who(ctx);
```

with:

```ts
    return { a, fp, finding };
  }

  // A report is visible when its project is one of the caller's; the branch must be one of that project's.
  async function reportScope(ctx: Ctx, rawId: string, rawBranch: string) {
    const a = await who(ctx);
    const id = parseProjectId(rawId);
    if (!id) throw new HttpError(400, "invalid project id");
    const branch = parseBranchRef(rawBranch);
    if (!branch) throw new HttpError(400, "invalid branch");
    if (!a.projectIds.includes(id)) throw new HttpError(404, "not found");
    try {
      return { a, id, report: await buildReport({ runner, access: a, projectId: id, branch, now: ctx.now, scheduleMinutes }) };
    } catch (e) {
      if (e instanceof ReportNotFound) throw new HttpError(404, "not found");
      throw e;
    }
  }

  return {
    async meta(ctx: Ctx) {
      const a = await who(ctx);
```

Replace (3 of 3):

```ts
      return started === false ? { ...out, message: SCHEDULED } : out;
    },

    refresh(ctx: Ctx) {
      if (ctx.user) access.refresh(ctx.user);
    },
```

with:

```ts
      return started === false ? { ...out, message: SCHEDULED } : out;
    },

    /** The health report of one branch ("default" = the project's default branch), for the tabbed project page. */
    async report(ctx: Ctx, rawId: string, rawBranch: string) {
      return (await reportScope(ctx, rawId, rawBranch)).report;
    },

    refresh(ctx: Ctx) {
      if (ctx.user) access.refresh(ctx.user);
    },
```

- [ ] **Step 5: The route** **[LLM-merge]**

`app/server/routes.ts` (the LLM branch added the `/api/findings/:fp/drafts` routes further down; this hunk sits above them):

Replace:

```ts
  app.get("/api/projects/:id/queries", (req, res) =>
    send(res, () => h.queries(ctxOf(req), param(req.params.id), req.query.ids)),
  );
  app.get("/api/actions/:id", (req, res) => send(res, () => h.action(ctxOf(req), param(req.params.id))));
  // AppKit's server plugin already parses JSON bodies.
  app.post("/api/actions/:id/requests", (req, res) =>
```

with:

```ts
  app.get("/api/projects/:id/queries", (req, res) =>
    send(res, () => h.queries(ctxOf(req), param(req.params.id), req.query.ids)),
  );
  app.get("/api/report/:id/:branch", (req, res) =>
    send(res, () => h.report(ctxOf(req), param(req.params.id), param(req.params.branch))),
  );
  app.get("/api/actions/:id", (req, res) => send(res, () => h.action(ctxOf(req), param(req.params.id))));
  // AppKit's server plugin already parses JSON bodies.
  app.post("/api/actions/:id/requests", (req, res) =>
```

- [ ] **Step 6: Run the tests and typecheck**

Run: `cd app && npx vitest run server && npx tsc -b tsconfig.server.json`
Expected: all pass; no type errors.

- [ ] **Step 7: Commit**

```bash
git add app/server/lib/validate.ts app/server/lib/handlers.ts app/server/routes.ts app/server/tests/report_handlers.test.ts
git commit -m "feat(app): report route, scoped to visible projects and their branches" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 14: Report components and the static HTML render

The tab bodies, header and document as plain React (no AppKit, no router, `rp-*` classes), a vendored SVG line chart, `report.css` on the theme tokens, and `renderExport()`: `strip()`, then `renderToStaticMarkup` of `ReportDocument` with `tokens.css` + `report.css` inlined. The same components render the page in Task 16.

**Files:**
- Create: `app/client/src/report/format.ts`, `app/client/src/report/ui.tsx`, `app/client/src/report/LineChart.tsx`, `app/client/src/report/tabs.tsx`, `app/client/src/report/Document.tsx`, `app/client/src/report/report.css`
- Create: `app/server/report/export.ts`
- Modify: `app/tsconfig.server.json`
- Test: `app/server/tests/report_export.test.ts` (create)

**Interfaces:**
- Consumes: the `Report` type and its sections (Tasks 11 and 12), `strip` (Task 11), `client/src/theme/tokens.css` (plain CSS, no URLs).
- Produces: `TABS` (11 `{id, label, exported}` in tab order), `TabId`, `isTab(v)`, `TAB_BODY` (tab id → body component for the 9 exported tabs), `HealthTab`, `OverviewTab`, `TablesTab`, `QueriesTab`, `QueriesTiles`, `StatementsCards`, `WaitsCards`, `waitSummary(rows)`, `VacuumTab`, `ScaleToZeroTab`, `SystemTab`, `SettingsTab`, `RunbookTab` (all `({report, mode})`, `mode: "app" | "export"`; tabs.tsx); `ReportHeader({report, mode, actions?})`, `ReportDocument({report, css})` (Document.tsx); `Tiles`, `Card` (title, one-line `explain`, optional `section` for the as-of badge and the error box), `Table`, `Badge`, `Code`, `Note`, `Mode`, `Tone` (ui.tsx); `LineChart({lines, format, title})` (LineChart.tsx); `exportCss(root?)`, `exportFilename(report, now)`, `renderExport(report, styles?)` (export.ts).

- [ ] **Step 1: Write the failing test**

`app/server/tests/report_export.test.ts` (new file, complete):

```ts
import { describe, expect, it } from "vitest";
import { TABS } from "../../client/src/report/tabs";
import type { Access } from "../lib/access";
import { buildReport } from "../report/build";
import { exportCss, exportFilename, renderExport } from "../report/export";

const HC = "11111111-2222-4333-8444-555555555555";
const NOW = new Date("2026-10-29T12:07:00Z");
const PII: Access = { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: true, admin: true, owned: [HC] };
const SECRET = "SELECT ssn FROM hc_test.customers WHERE email = 'a@b.c'";
const ROWS: Record<string, Record<string, unknown>[]> = {
  report_branches: [{ branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", min_cu: "1", max_cu: "4", suspend_timeout_s: "300", enrolled: "true" }],
  report_findings: [
    { fingerprint: "f1", check_id: "top_db_time_query", title: "Query uses the most DB time", category: "performance", endpoint_id: "ep-a", object: "42", status: "open", severity: "warning", measure: "50", evidence: JSON.stringify({ queryid: "42", query_text: SECRET, total_ms: 5000 }), explain: "A query dominates. More.", candidate_sql: null, queryid: "42", query_text: SECRET },
    { fingerprint: "f2", check_id: "dead_tuples", title: "High share of dead tuples", category: "maintenance", endpoint_id: "ep-a", object: "db:hc_test.orders", status: "open", severity: "critical", measure: "25", evidence: JSON.stringify({ statement: SECRET, n_dead_tup: 900 }), explain: "Dead tuples. More.", candidate_sql: "VACUUM (ANALYZE) hc_test.orders;" },
  ],
  report_checks: [{ check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok" }, { check_id: "top_db_time_query", endpoint_id: null, status: "ok" }],
  report_overview: [{ telemetry_at: "2026-10-29T12:00:00Z", lfc_hit_pct: "98.7", working_set_bytes: "1000", lfc_size_bytes: "2000" }],
  report_series: [{ bucket: "2026-10-29T11:00:00Z", metric: "lfc_hit_pct", value: "99" }, { bucket: "2026-10-29T11:15:00Z", metric: "lfc_hit_pct", value: "97" }],
  report_statements: [{ collected_at: "2026-10-29T12:00:00Z", ranking: "time", rank: "1", queryid: "42", role: "app", datname: "db", calls: "10", mean_ms: "5", total_ms: "50" }],
  meta: [{ last_success_at: "2026-10-29T12:00:00Z" }],
};
const runner = (fail: string[] = []) => ({
  async run(name: string) {
    if (fail.includes(name)) throw new Error("boom");
    return ROWS[name] ?? [];
  },
});
const report = (fail: string[] = []) => buildReport({ runner: runner(fail), access: PII, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });

describe("HTML export", () => {
  it("is one self-contained document: inline CSS, no scripts, no external URLs", async () => {
    const html = renderExport(await report());
    expect(html.startsWith("<!DOCTYPE html><html")).toBe(true);
    expect(html).toContain("<style>");
    expect(html).toContain("--lbh-sev-critical-fg");
    expect(html).not.toMatch(/<script/i);
    expect(html).not.toMatch(/https?:\/\//i);
    expect(html).not.toMatch(/\bsrc=|url\(|<link|@import/i);
    expect(html).toContain("<svg");
  });

  it("never contains query text, even when a PII member exports it", async () => {
    const html = renderExport(await report());
    expect(html).not.toContain("ssn");
    expect(html).not.toContain("a@b.c");
    expect(html).toContain("42"); // the query id stays
  });

  it("renders the exported tabs in tab order with tiles and explained cards", async () => {
    const html = renderExport(await report());
    const labels = TABS.filter((t) => t.exported).map((t) => `<h2 class="rp-tab-title">${t.label.replace("&", "&amp;")}</h2>`);
    const at = labels.map((l) => html.indexOf(l));
    expect(at.every((i) => i > 0)).toBe(true);
    expect([...at].sort((a, b) => a - b)).toEqual(at);
    expect(html).not.toContain(">Fixes</h2>");
    expect(html.split('class="rp-tiles"').length - 1).toBe(labels.length);
    expect(html.split('class="rp-card"').length).toBe(html.split('class="rp-explain"').length);
    expect(html).toContain("HIGH");
    expect(html).toContain("VACUUM (ANALYZE) hc_test.orders;");
  });

  it("marks a failed section unavailable instead of dropping it", async () => {
    const html = renderExport(await report(["report_tables"]));
    expect(html).toContain("Section unavailable");
    expect(html).toContain("Largest tables");
  });

  it("names the file after project, branch and UTC minute", async () => {
    expect(exportFilename(await report(), NOW)).toBe("lakebase-health-healthcheck-production-20261029T1207Z.html");
  });

  it("ships CSS without imports or URLs", () => {
    expect(exportCss()).not.toMatch(/@import|url\(/);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd app && npx vitest run server/tests/report_export.test.ts`
Expected: FAIL with `Failed to resolve import "../../client/src/report/tabs"`.

- [ ] **Step 3: Formatting and primitives**

`app/client/src/report/format.ts` (new file, complete):

```ts
/** Formatting shared by the report page and its HTML export (no locale or time-zone surprises in exports). */
export const num = (v: unknown): number | null => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

export const DASH = "—";

export function fmtNum(v: unknown, digits = 0): string {
  const n = num(v);
  return n === null ? DASH : n.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: 0 });
}

export function fmtPct(v: unknown, digits = 1): string {
  const n = num(v);
  return n === null ? DASH : `${n.toFixed(digits)} %`;
}

const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];
export function fmtBytes(v: unknown): string {
  let n = num(v);
  if (n === null) return DASH;
  let i = 0;
  while (Math.abs(n) >= 1024 && i < UNITS.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 || Math.abs(n) >= 100 ? 0 : 1)} ${UNITS[i]}`;
}

export function fmtDuration(seconds: unknown): string {
  const s = num(seconds);
  if (s === null) return DASH;
  if (s < 60) return `${Math.round(s)} s`;
  if (s < 3600) return `${Math.round(s / 60)} min`;
  if (s < 86400) return `${(s / 3600).toFixed(1)} h`;
  return `${(s / 86400).toFixed(1)} d`;
}

export function fmtMs(v: unknown): string {
  const n = num(v);
  if (n === null) return DASH;
  return n < 10 ? `${n.toFixed(2)} ms` : n < 1000 ? `${Math.round(n)} ms` : `${(n / 1000).toFixed(1)} s`;
}

/** UTC, minute precision: "2026-10-29 12:00 UTC". */
export function fmtUtc(v: unknown): string {
  if (typeof v !== "string" && typeof v !== "number") return DASH;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return DASH;
  return `${d.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}

export const text = (v: unknown): string =>
  typeof v === "string" ? v : typeof v === "number" || typeof v === "boolean" ? String(v) : DASH;
```

`app/client/src/report/ui.tsx` (new file, complete):

```tsx
import type { ReactNode } from "react";
import type { Row, Section } from "./types";
import { fmtUtc, text } from "./format";

/**
 * Report primitives. Plain elements and rp-* classes (report.css) only, no AppKit or router imports: the server
 * renders these same components to static HTML for the download.
 */
export type Mode = "app" | "export";
export type Tone = "critical" | "warning" | "info" | "ok" | "neutral";

export function Badge({ tone, children }: { tone: Tone; children: ReactNode }) {
  return <span className={`rp-badge rp-tone-${tone}`}>{children}</span>;
}

export type TileSpec = { label: string; value: ReactNode; hint?: ReactNode; tone?: Tone };

/** The row of KPI tiles every tab starts with. */
export function Tiles({ tiles }: { tiles: TileSpec[] }) {
  return (
    <div className="rp-tiles">
      {tiles.map((t) => (
        <div key={t.label} className={`rp-tile${t.tone ? ` rp-accent-${t.tone}` : ""}`}>
          <div className="rp-tile-label">{t.label}</div>
          <div className="rp-tile-value">{t.value}</div>
          {t.hint ? <div className="rp-tile-hint">{t.hint}</div> : null}
        </div>
      ))}
    </div>
  );
}

/** A card: a title, a one-line plain-language explanation, then the content (or why it is missing). */
export function Card({
  title,
  explain,
  section,
  mode,
  children,
}: {
  title: string;
  explain: string;
  section?: Section<unknown>;
  mode: Mode;
  children: ReactNode;
}) {
  const failed = section?.status === "error";
  return (
    <section className="rp-card">
      <header className="rp-card-head">
        <h3 className="rp-card-title">{title}</h3>
        {section?.asOf ? (
          <span className={`rp-asof${section.stale ? " rp-stale" : ""}`}>
            {section.stale ? "stale: " : "as of "}
            {fmtUtc(section.asOf)}
          </span>
        ) : null}
      </header>
      <p className="rp-explain">{explain}</p>
      {failed ? (
        <div className="rp-error" role="alert">
          {mode === "export" ? "Section unavailable: its data could not be loaded when this report was exported." : section?.error}
        </div>
      ) : (
        children
      )}
    </section>
  );
}

export type Column = {
  key: string;
  label: string;
  numeric?: boolean;
  format?: (v: Row[string], row: Row) => ReactNode;
};

export function Table({ columns, rows, empty, limit }: { columns: Column[]; rows: Row[]; empty: string; limit?: number }) {
  if (rows.length === 0) return <p className="rp-empty">{empty}</p>;
  const shown = limit ? rows.slice(0, limit) : rows;
  return (
    <div className="rp-table-wrap">
      <table className="rp-table">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} className={c.numeric ? "rp-num" : undefined}>
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((r, i) => (
            <tr key={i}>
              {columns.map((c) => (
                <td key={c.key} className={c.numeric ? "rp-num" : undefined}>
                  {c.format ? c.format(r[c.key], r) : text(r[c.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {limit && rows.length > limit ? <p className="rp-more">Showing {limit} of {rows.length}.</p> : null}
    </div>
  );
}

export function Code({ children }: { children: string }) {
  return <pre className="rp-code">{children}</pre>;
}

export function Note({ children }: { children: ReactNode }) {
  return <p className="rp-note">{children}</p>;
}
```

`app/client/src/report/LineChart.tsx` (new file, complete; vendored instead of an npm chart library, which the Apps npm proxy may not have, and because the export needs SVG rendered on the server):

```tsx
import type { SeriesPoint } from "./types";

/**
 * A small dependency-free SVG line chart, rendered identically in the app and (server-side) in the HTML export.
 * Gaps (null values) break the line. Colours are the theme's chart tokens.
 */
export type Line = { label: string; points: SeriesPoint[]; color: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 };

const W = 640;
const H = 180;
const PAD = { left: 56, right: 12, top: 10, bottom: 24 };

const hhmm = (t: string) => new Date(t).toISOString().slice(11, 16);

export function LineChart({ lines, format, title }: { lines: Line[]; format: (v: number) => string; title: string }) {
  const pts = lines.flatMap((l) => l.points.filter((p) => p.v !== null));
  if (pts.length === 0) return <p className="rp-empty">No data in the last 24 hours.</p>;
  const times = pts.map((p) => new Date(p.t).getTime());
  const t0 = Math.min(...times);
  const t1 = Math.max(...times);
  const vals = pts.map((p) => p.v as number);
  const lo = Math.min(0, ...vals);
  const hi = Math.max(...vals) === lo ? lo + 1 : Math.max(...vals);
  const x = (t: string) => PAD.left + ((new Date(t).getTime() - t0) / Math.max(t1 - t0, 1)) * (W - PAD.left - PAD.right);
  const y = (v: number) => PAD.top + (1 - (v - lo) / (hi - lo)) * (H - PAD.top - PAD.bottom);
  const segments = (points: SeriesPoint[]) => {
    const out: string[][] = [];
    let cur: string[] = [];
    for (const p of points) {
      if (p.v === null) {
        if (cur.length) out.push(cur);
        cur = [];
      } else cur.push(`${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`);
    }
    if (cur.length) out.push(cur);
    return out;
  };
  const grid = [lo, (lo + hi) / 2, hi];
  return (
    <figure className="rp-chart">
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={title} preserveAspectRatio="none">
        <title>{title}</title>
        {grid.map((g) => (
          <g key={g}>
            <line x1={PAD.left} x2={W - PAD.right} y1={y(g)} y2={y(g)} className="rp-grid" />
            <text x={PAD.left - 6} y={y(g) + 4} textAnchor="end" className="rp-axis">
              {format(g)}
            </text>
          </g>
        ))}
        <text x={PAD.left} y={H - 6} className="rp-axis">
          {hhmm(new Date(t0).toISOString())} UTC
        </text>
        <text x={W - PAD.right} y={H - 6} textAnchor="end" className="rp-axis">
          {hhmm(new Date(t1).toISOString())} UTC
        </text>
        {lines.map((l) =>
          segments(l.points).map((s, i) =>
            s.length === 1 ? (
              <circle key={`${l.label}-${i}`} cx={s[0].split(",")[0]} cy={s[0].split(",")[1]} r={2} className={`rp-line rp-c${l.color}`} />
            ) : (
              <polyline key={`${l.label}-${i}`} points={s.join(" ")} className={`rp-line rp-c${l.color}`} fill="none" />
            ),
          ),
        )}
      </svg>
      {lines.length > 1 ? (
        <figcaption className="rp-legend">
          {lines.map((l) => (
            <span key={l.label} className="rp-legend-item">
              <span className={`rp-swatch rp-c${l.color}`} aria-hidden />
              {l.label}
            </span>
          ))}
        </figcaption>
      ) : null}
    </figure>
  );
}
```

- [ ] **Step 4: The tabs, header and document**

`app/client/src/report/tabs.tsx` (new file, complete). Every body starts with `Tiles`, then `Card`s, each with its one-line explanation:

```tsx
import type { ReactNode } from "react";
import { LineChart, type Line } from "./LineChart";
import { DASH, fmtBytes, fmtDuration, fmtMs, fmtNum, fmtPct, fmtUtc, num, text } from "./format";
import type { AreaResult, Report, ReportFinding, Row, RunbookItem, SeriesPoint } from "./types";
import { Badge, Card, Code, Note, Table, Tiles, type Mode, type Tone } from "./ui";

/** The project page's tabs in order; `exported` ones make up the downloaded report, in the same order. */
export const TABS = [
  { id: "health", label: "Health", exported: true },
  { id: "overview", label: "Overview", exported: true },
  { id: "tables", label: "Tables & Indexes", exported: true },
  { id: "queries", label: "Queries", exported: true },
  { id: "waits", label: "Waits & Locks", exported: false },
  { id: "vacuum", label: "Vacuum & Bloat", exported: true },
  { id: "scale-to-zero", label: "Scale to zero", exported: true },
  { id: "system", label: "System", exported: true },
  { id: "settings", label: "Settings", exported: true },
  { id: "runbook", label: "Runbook", exported: true },
  { id: "fixes", label: "Fixes", exported: false },
] as const;
export type TabId = (typeof TABS)[number]["id"];
export const isTab = (v: unknown): v is TabId => TABS.some((t) => t.id === v);

type P = { report: Report; mode: Mode };

const SEV_TONE: Record<string, Tone> = { critical: "critical", warning: "warning", info: "info" };
const STATE_TONE: Record<AreaResult["state"], Tone> = { pass: "ok", warn: "warning", fail: "critical", not_assessed: "neutral" };
const STATE_LABEL: Record<AreaResult["state"], string> = { pass: "PASS", warn: "WARN", fail: "FAIL", not_assessed: "NOT ASSESSED" };
const PRIORITY_TONE: Record<string, Tone> = { HIGH: "critical", MEDIUM: "warning", LOW: "info" };

const of = (r: Report, checks: string[]) => r.findings.filter((f) => checks.includes(f.check_id));
const rowsOf = (fs: ReportFinding[]): Row[] =>
  fs.map((f) => ({ severity: f.severity, title: f.title, object: f.object, measure: f.measure, fingerprint: f.fingerprint }));

function findingLink(mode: Mode, fp: unknown, label: ReactNode) {
  return mode === "app" ? <a href={`/findings/${text(fp)}`}>{label}</a> : label;
}

function FindingTable({ findings, mode, empty }: { findings: ReportFinding[]; mode: Mode; empty: string }) {
  return (
    <Table
      empty={empty}
      rows={rowsOf(findings)}
      columns={[
        { key: "severity", label: "Severity", format: (v) => <Badge tone={SEV_TONE[text(v)] ?? "neutral"}>{text(v)}</Badge> },
        { key: "title", label: "Finding", format: (v, r) => findingLink(mode, r.fingerprint, text(v)) },
        { key: "object", label: "Object" },
        { key: "measure", label: "Measure", numeric: true, format: (v) => fmtNum(v, 2) },
      ]}
    />
  );
}

const ago = (s: number | null) => (s === null ? DASH : fmtDuration(s));

export function HealthTab({ report: r, mode }: P) {
  const critical = r.runbook.filter((i) => i.severity === "critical");
  const warnings = r.runbook.filter((i) => i.severity === "warning");
  return (
    <>
      <Tiles
        tiles={[
          { label: "Score", value: r.score.pct === null ? DASH : `${r.score.pct} %`, hint: `assessed ${r.score.assessed} of ${r.score.total} areas` },
          { label: "Grade", value: r.score.grade ?? DASH, tone: r.score.grade === "A" || r.score.grade === "B" ? "ok" : r.score.grade ? "warning" : undefined },
          { label: "Critical", value: critical.length, tone: critical.length ? "critical" : "ok" },
          { label: "Warnings", value: warnings.length, tone: warnings.length ? "warning" : "ok" },
        ]}
      />
      <Card mode={mode} title="Scorecard" explain="Each area scores 5 with no open finding, 4 with only information, 3 with a warning and 1 with a critical finding; areas whose checks did not run are left out of the score.">
        <div className="rp-scorecard">
          {r.areas.map((a) => (
            <div key={a.id} className={`rp-area rp-area-${a.state}`}>
              <div className="rp-area-head">
                <Badge tone={STATE_TONE[a.state]}>{STATE_LABEL[a.state]}</Badge>
                <span className="rp-area-label">{a.label}</span>
                <span className="rp-area-score">{a.score === null ? "" : `${a.score}/5`}</span>
              </div>
              <p className="rp-area-blurb">{a.blurb}</p>
              <p className="rp-area-detail">
                {a.worst
                  ? findingLink(mode, a.worst.fingerprint, `${a.worst.title}: ${a.worst.object}${a.open > 1 ? ` (+${a.open - 1} more)` : ""}`)
                  : a.note ?? "No open findings."}
              </p>
            </div>
          ))}
        </div>
      </Card>
      <Card mode={mode} title="Where to focus" explain="The first place to look, from the lowest-scoring areas.">
        <p className="rp-focus">{r.focus}</p>
      </Card>
      <Card mode={mode} title="Critical items and warnings" explain="Open findings that need action, most severe first; the Runbook tab has the SQL.">
        <FindingTable mode={mode} empty="No critical or warning findings." findings={r.findings.filter((f) => f.severity !== "info").sort((a, b) => (a.severity === b.severity ? 0 : a.severity === "critical" ? -1 : 1))} />
      </Card>
    </>
  );
}

export function OverviewTab({ report: r, mode }: P) {
  const o = r.sections.overview.data;
  const ws = o.workingSetBytes !== null && o.lfcSizeBytes ? (100 * o.workingSetBytes) / o.lfcSizeBytes : null;
  return (
    <>
      <Tiles
        tiles={[
          { label: "LFC hit (24 h)", value: fmtPct(o.lfcHitPct, 2), tone: o.lfcHitPct !== null && o.lfcHitPct < 99 ? "warning" : undefined, hint: "share of page reads served by the compute's local cache" },
          { label: "Working set vs LFC", value: fmtPct(ws, 0), hint: `${fmtBytes(o.workingSetBytes)} of ${fmtBytes(o.lfcSizeBytes)}`, tone: ws !== null && ws > 100 ? "warning" : undefined },
          { label: "Connections", value: `${fmtNum(o.connections)} / ${fmtNum(o.maxConnections)}`, hint: "open now vs max_connections" },
          { label: "Database size", value: fmtBytes(o.databaseBytes) },
          { label: "Oldest transaction", value: ago(o.oldestXactS), hint: "age of the oldest open client transaction" },
          { label: "Getpage p99 (24 h)", value: fmtMs(o.getpageP99Ms), hint: "per page read from storage, sampled queries" },
          { label: "Temp spill (24 h)", value: fmtBytes(o.tempSpillBytes), hint: "sorts and hashes that did not fit in work_mem" },
          { label: "Commits per second", value: fmtNum(o.commitsPerS, 1), hint: "24 h average" },
        ]}
      />
      <Card mode={mode} title="Databases" explain="Size and connections per database on the read-write compute, from the last telemetry sample." section={r.sections.databases}>
        <Table
          empty="No database telemetry in the last 24 hours."
          rows={r.sections.databases.data}
          columns={[
            { key: "datname", label: "Database" },
            { key: "size_bytes", label: "Size", numeric: true, format: fmtBytes },
            { key: "backends", label: "Connections", numeric: true, format: (v) => fmtNum(v) },
            { key: "xid_age", label: "Oldest XID age", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
    </>
  );
}

const INDEX_CHECKS = ["unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "synced_index_review"];

export function TablesTab({ report: r, mode }: P) {
  const t = r.sections.tables.data;
  const synced = t.filter((x) => x.synced_table);
  const idxFindings = of(r, INDEX_CHECKS);
  return (
    <>
      <Tiles
        tiles={[
          { label: "Largest tables", value: t.length, hint: "up to 25 per database" },
          { label: "Their total size", value: fmtBytes(t.reduce((n, x) => n + (num(x.total_bytes) ?? 0), 0)) },
          { label: "Synced table storage", value: synced.length },
          { label: "Index findings", value: idxFindings.length, tone: idxFindings.length ? "warning" : "ok" },
        ]}
      />
      <Card mode={mode} title="Largest tables" explain="Size (data, indexes, TOAST), live and dead rows, and how the table is read: many sequential scans on a big table usually mean a missing index." section={r.sections.tables}>
        <Table
          empty="No inventory yet: it is collected on the next analysis run of an enrolled, running compute."
          rows={t}
          columns={[
            { key: "table_name", label: "Table", format: (v, x) => `${text(x.datname)}:${text(x.schema_name)}.${text(v)}` },
            { key: "total_bytes", label: "Total", numeric: true, format: fmtBytes },
            { key: "data_bytes", label: "Data", numeric: true, format: fmtBytes },
            { key: "index_bytes", label: "Indexes", numeric: true, format: fmtBytes },
            { key: "toast_bytes", label: "TOAST", numeric: true, format: fmtBytes },
            { key: "n_live_tup", label: "Live rows", numeric: true, format: (v) => fmtNum(v) },
            { key: "n_dead_tup", label: "Dead rows", numeric: true, format: (v) => fmtNum(v) },
            { key: "seq_scan", label: "Seq scans", numeric: true, format: (v) => fmtNum(v) },
            { key: "idx_scan", label: "Index scans", numeric: true, format: (v) => fmtNum(v) },
            { key: "heap_blks_read", label: "Blocks read", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
      <Card mode={mode} title="Indexes" explain="Indexes on those tables by size; an index with no scans since statistics reset costs writes and storage." section={r.sections.indexes}>
        <Table
          empty="No indexes in the inventory."
          limit={50}
          rows={r.sections.indexes.data}
          columns={[
            { key: "index_name", label: "Index", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "table_name", label: "Table" },
            { key: "method", label: "Method" },
            { key: "index_bytes", label: "Size", numeric: true, format: fmtBytes },
            { key: "idx_scan", label: "Scans", numeric: true, format: (v) => fmtNum(v) },
            { key: "is_valid", label: "Valid", format: (v) => (v === false || v === "false" ? "no" : "yes") },
            { key: "synced_table", label: "Synced table", format: (v) => (v ? text(v) : "") },
          ]}
        />
      </Card>
      <Card mode={mode} title="Unused, duplicate and redundant indexes" explain="Index findings from the checks; indexes on synced tables are listed for review only, because the sync owns them.">
        <FindingTable mode={mode} findings={idxFindings} empty="No index findings." />
      </Card>
      <Card mode={mode} title="Synced tables" explain="Storage of tables synced from Unity Catalog: a full refresh rewrites every row and index." section={r.sections.tables}>
        <Table
          empty="No synced tables among the largest tables."
          rows={synced}
          columns={[
            { key: "synced_table", label: "Synced table" },
            { key: "table_name", label: "Storage", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "total_bytes", label: "Size", numeric: true, format: fmtBytes },
            { key: "n_live_tup", label: "Rows", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
    </>
  );
}

const STATEMENT_COLUMNS = [
  { key: "queryid", label: "Query ID" },
  { key: "role", label: "Role" },
  { key: "datname", label: "Database" },
  { key: "calls", label: "Calls", numeric: true, format: (v: Row[string]) => fmtNum(v) },
  { key: "mean_ms", label: "Mean", numeric: true, format: fmtMs },
  { key: "total_ms", label: "Total", numeric: true, format: fmtMs },
  { key: "shared_blks_hit", label: "Blocks hit", numeric: true, format: (v: Row[string]) => fmtNum(v) },
  { key: "shared_blks_read", label: "Blocks read", numeric: true, format: (v: Row[string]) => fmtNum(v) },
  { key: "temp_blks_written", label: "Temp blocks", numeric: true, format: (v: Row[string]) => fmtNum(v) },
];

/** Top statements by time and by I/O (query IDs only; the app adds text for those allowed to read it). */
export function StatementsCards({ report: r, mode }: P) {
  const s = r.sections.statements.data;
  return (
    <>
      <Card mode={mode} title="Top statements by time" explain="pg_stat_statements since its last reset: where the database spends its time. Platform and engine roles are left out." section={r.sections.statements}>
        <Table empty="pg_stat_statements has no data yet, or the extension is not installed." rows={s.filter((x) => x.ranking === "time")} columns={STATEMENT_COLUMNS} />
      </Card>
      <Card mode={mode} title="Top statements by I/O" explain="The statements that read the most blocks from outside shared buffers: candidates for an index or a smaller result." section={r.sections.statements}>
        <Table empty="No statements read from storage." rows={s.filter((x) => x.ranking === "io")} columns={STATEMENT_COLUMNS} />
      </Card>
    </>
  );
}

export function QueriesTiles({ report: r }: { report: Report }) {
  const s = r.sections.statements.data.filter((x) => x.ranking === "time");
  const q = r.findings.filter((f) => f.area === "queries");
  return (
    <Tiles
      tiles={[
        { label: "Query findings", value: q.length, tone: q.length ? "warning" : "ok" },
        { label: "Statements tracked", value: s.length, hint: "top 20 by total time" },
        { label: "Busiest statement", value: fmtMs(s[0]?.total_ms), hint: s[0] ? `query ${text(s[0].queryid)}` : undefined },
        { label: "Temp blocks written", value: fmtNum(s.reduce((n, x) => n + (num(x.temp_blks_written) ?? 0), 0)) },
      ]}
    />
  );
}

export function QueriesTab({ report, mode }: P) {
  return (
    <>
      <QueriesTiles report={report} />
      <StatementsCards report={report} mode={mode} />
    </>
  );
}

/** Average active sessions per wait class over 24 h, from the per-15-minute buckets. */
export function waitSummary(rows: Row[]): Row[] {
  const by = new Map<string, number>();
  const buckets = new Set(rows.map((r) => text(r.bucket)));
  for (const r of rows) by.set(text(r.wait_class), (by.get(text(r.wait_class)) ?? 0) + (num(r.avg_active) ?? 0));
  return [...by.entries()]
    .map(([wait_class, n]) => ({ wait_class, avg_active: n / Math.max(buckets.size, 1) }))
    .sort((a, b) => b.avg_active - a.avg_active);
}

const COLORS: Line["color"][] = [1, 2, 3, 4, 5, 6, 7, 8];
function waitLines(rows: Row[]): Line[] {
  const classes = waitSummary(rows).map((r) => text(r.wait_class)).slice(0, 8);
  return classes.map((c, i) => ({
    label: c,
    color: COLORS[i],
    points: rows.filter((r) => text(r.wait_class) === c).map((r) => ({ t: text(r.bucket), v: num(r.avg_active) })),
  }));
}

export function WaitsCards({ report: r, mode }: P) {
  const summary = waitSummary(r.sections.waits.data);
  return (
    <>
      <Tiles
        tiles={[
          { label: "Active sessions (24 h avg)", value: fmtNum(summary.reduce((n, x) => n + (num(x.avg_active) ?? 0), 0), 2) },
          { label: "Top wait class", value: summary[0] ? text(summary[0].wait_class) : DASH },
          { label: "Lock findings", value: r.findings.filter((f) => f.area === "locks").length },
          { label: "Connection findings", value: r.findings.filter((f) => f.area === "connections").length },
        ]}
      />
      <Card mode={mode} title="Wait events by class (24 h)" explain="Average sessions active in each wait class, from sampled session history; CPU means running, not waiting." section={r.sections.waits}>
        <LineChart title="Active sessions by wait class" lines={waitLines(r.sections.waits.data)} format={(v) => v.toFixed(1)} />
        <Table empty="No sampled sessions in the last 24 hours." rows={summary} columns={[{ key: "wait_class", label: "Wait class" }, { key: "avg_active", label: "Average active sessions", numeric: true, format: (v) => fmtNum(v, 2) }]} />
      </Card>
      <Card mode={mode} title="Lock and session findings" explain="Blocked sessions, deadlocks, hot rows and long or idle transactions found by the checks.">
        <FindingTable mode={mode} findings={r.findings.filter((f) => f.area === "locks" || f.area === "connections")} empty="No lock or session findings." />
      </Card>
    </>
  );
}

export function VacuumTab({ report: r, mode }: P) {
  const t = r.sections.tables.data;
  const e = r.sections.endpoint.data.latest;
  const over = t.filter((x) => (num(x.n_dead_tup) ?? 0) > (num(x.vacuum_threshold) ?? Infinity));
  const xid = [...t].sort((a, b) => (num(b.xid_age) ?? 0) - (num(a.xid_age) ?? 0));
  return (
    <>
      <Tiles
        tiles={[
          { label: "Autovacuum workers", value: `${fmtNum(e.autovacuum_workers_busy)} / ${fmtNum(e.autovacuum_max_workers)}`, hint: "busy now / autovacuum_max_workers", tone: e.autovacuum_workers_busy !== undefined && e.autovacuum_workers_busy === e.autovacuum_max_workers ? "warning" : undefined },
          { label: "Tables over vacuum threshold", value: over.length, tone: over.length ? "warning" : "ok" },
          { label: "Highest table XID age", value: fmtNum(xid[0]?.xid_age), hint: "wraparound protection starts at 200 million" },
          { label: "Dead rows (largest tables)", value: fmtNum(t.reduce((n, x) => n + (num(x.n_dead_tup) ?? 0), 0)) },
        ]}
      />
      <Card mode={mode} title="Vacuum and analyze" explain="When each large table was last vacuumed and analyzed, and whether dead rows are above the point where autovacuum should start." section={r.sections.tables}>
        <Table
          empty="No inventory yet."
          rows={t}
          columns={[
            { key: "table_name", label: "Table", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "n_dead_tup", label: "Dead rows", numeric: true, format: (v) => fmtNum(v) },
            { key: "vacuum_threshold", label: "Vacuum threshold", numeric: true, format: (v) => fmtNum(v) },
            { key: "last_autovacuum", label: "Last autovacuum", format: fmtUtc },
            { key: "last_autoanalyze", label: "Last autoanalyze", format: fmtUtc },
            { key: "autovacuum_count", label: "Autovacuums", numeric: true, format: (v) => fmtNum(v) },
            { key: "autoanalyze_count", label: "Autoanalyzes", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
      <Card mode={mode} title="Transaction ID age" explain="How close each table is to forced anti-wraparound vacuum; a high age with old vacuums means something holds vacuum back." section={r.sections.tables}>
        <Table
          empty="No inventory yet."
          limit={10}
          rows={xid}
          columns={[
            { key: "table_name", label: "Table", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "xid_age", label: "XID age", numeric: true, format: (v) => fmtNum(v) },
            { key: "last_vacuum", label: "Last manual vacuum", format: fmtUtc },
          ]}
        />
      </Card>
      <Card mode={mode} title="Bloat and vacuum findings" explain="Table and index bloat, dead tuples and what blocks autovacuum, from the checks.">
        <FindingTable mode={mode} findings={r.findings.filter((f) => f.area === "vacuum_xid" || f.area === "tables_bloat")} empty="No vacuum or bloat findings." />
      </Card>
    </>
  );
}

export function ScaleToZeroTab({ report: r, mode }: P) {
  const primary = r.endpoints.find((e) => e.id === r.primary);
  const idle = r.sections.endpoint.data.idleClients;
  const overrides = r.sections.tables.data.filter((x) => x.autovacuum_overrides);
  const blockers = of(r, ["suspend_blockers"]);
  return (
    <>
      <Tiles
        tiles={[
          { label: "Scale to zero", value: primary?.suspendTimeoutS ? "on" : "off", hint: primary?.suspendTimeoutS ? `suspends after ${fmtDuration(primary.suspendTimeoutS)} idle` : "the compute never suspends" },
          { label: "Idle client sessions", value: idle.reduce((n, c) => n + c.sessions, 0) },
          { label: "Suspend blockers", value: blockers.length, tone: blockers.length ? "warning" : "ok" },
          { label: "Up for", value: fmtDuration(r.sections.endpoint.data.latest.uptime_s ?? null), hint: "since Postgres last started" },
        ]}
      />
      <Card mode={mode} title="Suspend blockers" explain="Applications whose idle connections kept the compute awake for a day: set their pool's minimum idle connections to 0.">
        <FindingTable mode={mode} findings={blockers} empty="Nothing keeps the compute from suspending." />
      </Card>
      <Card mode={mode} title="Idle clients by application" explain="Client sessions open but idle at the last snapshot, per role and application; any of them prevents a suspend." section={r.sections.endpoint}>
        <Table
          empty="No idle client sessions."
          rows={idle.map((c) => ({ application: c.application, role: c.role, sessions: c.sessions, maxIdleS: c.maxIdleS }))}
          columns={[
            { key: "application", label: "Application" },
            { key: "role", label: "Role" },
            { key: "sessions", label: "Sessions", numeric: true },
            { key: "maxIdleS", label: "Longest idle", numeric: true, format: fmtDuration },
          ]}
        />
      </Card>
      <Card mode={mode} title="Autovacuum overrides" explain="Tables with their own autovacuum settings; aggressive settings can keep a compute busy." section={r.sections.tables}>
        <Table empty="No table overrides autovacuum settings." rows={overrides} columns={[{ key: "table_name", label: "Table", format: (v, x) => `${text(x.schema_name)}.${text(v)}` }, { key: "autovacuum_overrides", label: "Settings" }]} />
      </Card>
    </>
  );
}

const pts = (s: SeriesPoint[] | undefined) => s ?? [];
const last = (s: SeriesPoint[] | undefined) => [...pts(s)].reverse().find((p) => p.v !== null)?.v ?? null;

export function SystemTab({ report: r, mode }: P) {
  const s = r.sections.series.data;
  const e = r.sections.endpoint.data;
  const io = r.sections.io.data;
  const sum = (k: string) => pts(s[k]).reduce((n, p) => n + (p.v ?? 0), 0);
  const replicas = r.endpoints.some((x) => x.type === "replica");
  const perf = e.sources.neon_perf_counters;
  const sources = Object.entries(e.sources).map(([name, v]) => ({ name, available: v.available ? "yes" : "no", note: v.note ?? "" }));
  return (
    <>
      <Tiles
        tiles={[
          { label: "LFC hit (last 15 min)", value: fmtPct(last(s.lfc_hit_pct), 2) },
          { label: "Working set", value: fmtBytes(last(s.working_set_bytes)), hint: `cache ${fmtBytes(last(s.lfc_size_bytes))}` },
          { label: "Getpage p99 (last 15 min)", value: fmtMs(last(s.getpage_p99_ms)) },
          { label: "WAL (24 h)", value: fmtBytes(sum("wal_bytes")), hint: `${fmtNum(sum("wal_fpi"))} full-page images` },
        ]}
      />
      <Card mode={mode} title="LFC hit rate" explain="Share of page reads served by the compute's local cache per 15 minutes; below 99 % queries wait on storage." section={r.sections.series}>
        <LineChart title="LFC hit rate" lines={[{ label: "LFC hit %", color: 1, points: pts(s.lfc_hit_pct) }]} format={(v) => `${v.toFixed(1)} %`} />
      </Card>
      <Card mode={mode} title="Working set vs cache" explain="Pages touched in the last hour against the cache size: a working set above the cache means constant evictions." section={r.sections.series}>
        <LineChart title="Working set" lines={[{ label: "Working set (60 min)", color: 2, points: pts(s.working_set_bytes) }, { label: "Cache size", color: 3, points: pts(s.lfc_size_bytes) }]} format={fmtBytes} />
      </Card>
      <Card mode={mode} title="Getpage latency" explain="Time per page read from storage after a cache miss, average and p99 of sampled queries per 15 minutes." section={r.sections.series}>
        <LineChart title="Getpage latency" lines={[{ label: "average", color: 1, points: pts(s.getpage_avg_ms) }, { label: "p99", color: 2, points: pts(s.getpage_p99_ms) }, ...(e.getpageP99.length ? [{ label: "p99, all reads (neon_perf_counters)", color: 4 as const, points: e.getpageP99 }] : [])]} format={(v) => fmtMs(v)} />
        {perf && !perf.available ? <Note>Endpoint-level p99 over all reads: {perf.note ?? "not available"}. See the README to enable it.</Note> : null}
      </Card>
      <Card mode={mode} title="WAL and full-page images" explain="Write-ahead log written by statements per 15 minutes; spikes come from bulk loads, index builds and synced-table refreshes." section={r.sections.series}>
        <LineChart title="WAL bytes" lines={[{ label: "WAL", color: 6, points: pts(s.wal_bytes) }]} format={fmtBytes} />
        <LineChart title="Full-page images" lines={[{ label: "Full-page images", color: 5, points: pts(s.wal_fpi) }]} format={(v) => fmtNum(v)} />
      </Card>
      <Card mode={mode} title="Active sessions by wait class" explain="Average sessions active in each wait class per 15 minutes." section={r.sections.waits}>
        <LineChart title="Active sessions by wait class" lines={waitLines(r.sections.waits.data)} format={(v) => v.toFixed(1)} />
      </Card>
      {replicas ? (
        <Card mode={mode} title="Replica lag" explain="How far the read replicas are behind the primary, in bytes of WAL not yet replayed." section={r.sections.series}>
          <LineChart title="Replica lag" lines={[{ label: "lag", color: 7, points: pts(s.replica_lag_bytes) }]} format={fmtBytes} />
        </Card>
      ) : null}
      <Card mode={mode} title="I/O by backend" explain={`pg_stat_io ${io.delta ? `over the last ${fmtDuration(io.windowS)}` : "since the counters were reset"}: who reads and writes, by backend type, object and context. Counters reset on compute restart; high autovacuum or bulkread reads point at cache churn.`} section={r.sections.io}>
        <Table
          empty="pg_stat_io is not available (Postgres 16 or later), or nothing was read yet."
          rows={io.rows as unknown as Row[]}
          columns={[
            { key: "backend_type", label: "Backend" },
            { key: "object", label: "Object" },
            { key: "context", label: "Context" },
            { key: "reads", label: "Reads", numeric: true, format: (v) => fmtNum(v) },
            { key: "writes", label: "Writes", numeric: true, format: (v) => fmtNum(v) },
            { key: "hits", label: "Hits", numeric: true, format: (v) => fmtNum(v) },
            { key: "evictions", label: "Evictions", numeric: true, format: (v) => fmtNum(v) },
            { key: "extends", label: "Extends", numeric: true, format: (v) => fmtNum(v) },
            { key: "fsyncs", label: "Fsyncs", numeric: true, format: (v) => fmtNum(v) },
            { key: "read_time", label: "Read time", numeric: true, format: (v) => (io.timing ? fmtMs(v) : "timing off") },
            { key: "write_time", label: "Write time", numeric: true, format: (v) => (io.timing ? fmtMs(v) : "timing off") },
          ]}
        />
      </Card>
      <Card mode={mode} title="Endpoint sources" explain="Which Postgres-side sources the engine could read on this compute; without them the charts use telemetry only." section={r.sections.endpoint}>
        <Table empty="No snapshot from this compute yet." rows={sources} columns={[{ key: "name", label: "Source" }, { key: "available", label: "Read" }, { key: "note", label: "Note" }]} />
      </Card>
    </>
  );
}

export function SettingsTab({ report: r, mode }: P) {
  const s = r.sections.settings.data;
  return (
    <>
      <Tiles
        tiles={[
          { label: "Settings shown", value: s.length },
          { label: "Not at default", value: s.filter((x) => x.source !== "default").length },
          { label: "Changeable per session", value: s.filter((x) => x.how_to_change === "session").length },
          { label: "Support-managed", value: s.filter((x) => x.how_to_change === "support-managed").length },
        ]}
      />
      <Card mode={mode} title="Key parameters" explain="The settings that matter most for performance, plus every one changed from its default, with where it was set and how you can change it." section={r.sections.settings}>
        <Table
          empty="No settings snapshot yet."
          rows={s}
          columns={[
            { key: "name", label: "Setting" },
            { key: "setting", label: "Value", format: (v, x) => `${text(v)}${x.unit ? ` ${text(x.unit)}` : ""}` },
            { key: "source", label: "Source" },
            { key: "how_to_change", label: "How to change" },
          ]}
        />
      </Card>
    </>
  );
}

function RunbookEntry({ item, mode, n }: { item: RunbookItem; mode: Mode; n: number }) {
  return (
    <li className="rp-runbook-item">
      <div className="rp-runbook-head">
        <span className="rp-runbook-n">{n}.</span>
        <Badge tone={PRIORITY_TONE[item.priority]}>{item.priority}</Badge>
        <span className="rp-runbook-area">{item.areaLabel}</span>
        <span className="rp-runbook-title">{findingLink(mode, item.fingerprint, item.title)}</span>
      </div>
      <p className="rp-runbook-object">{item.object}</p>
      <p className="rp-runbook-why">{item.why}</p>
      {item.evidence.length ? <p className="rp-runbook-evidence">{item.evidence.join(" · ")}</p> : null}
      {item.sql ? <Code>{item.sql}</Code> : <Note>{item.note}</Note>}
      {mode === "app" && item.actionId ? (
        <p className="rp-runbook-fix">
          <a href={`/findings/${item.fingerprint}`}>Open the fix card</a> ({item.actionState?.replace(/_/g, " ")})
        </p>
      ) : null}
    </li>
  );
}

export function RunbookTab({ report: r, mode }: P) {
  const count = (p: string) => r.runbook.filter((i) => i.priority === p).length;
  const all = r.runbook.filter((i) => i.sql).map((i) => `-- ${i.priority}: ${i.title} (${i.object})\n${i.sql}`).join("\n\n");
  return (
    <>
      <Tiles
        tiles={[
          { label: "High", value: count("HIGH"), tone: count("HIGH") ? "critical" : "ok" },
          { label: "Medium", value: count("MEDIUM"), tone: count("MEDIUM") ? "warning" : "ok" },
          { label: "Low", value: count("LOW"), tone: "info" },
          { label: "With SQL", value: r.runbook.filter((i) => i.sql).length },
        ]}
      />
      <Card mode={mode} title="Actions in order" explain="One item per open finding: most severe first, then those with a fix verified on a branch, then by how far past its threshold.">
        {r.runbook.length ? (
          <ol className="rp-runbook">
            {r.runbook.map((i, n) => (
              <RunbookEntry key={i.fingerprint} item={i} mode={mode} n={n + 1} />
            ))}
          </ol>
        ) : (
          <p className="rp-empty">No open findings.</p>
        )}
      </Card>
      <Card mode={mode} title="All SQL" explain="Every item's SQL in runbook order, to review and run as one script; verify each change on a branch first.">
        {all ? <Code>{all}</Code> : <p className="rp-empty">No item has SQL.</p>}
      </Card>
    </>
  );
}

/** The tab bodies the export (and the page, for these tabs) renders. */
export const TAB_BODY: Partial<Record<TabId, (p: P) => ReactNode>> = {
  health: HealthTab,
  overview: OverviewTab,
  tables: TablesTab,
  queries: QueriesTab,
  vacuum: VacuumTab,
  "scale-to-zero": ScaleToZeroTab,
  system: SystemTab,
  settings: SettingsTab,
  runbook: RunbookTab,
};
```

`app/client/src/report/Document.tsx` (new file, complete):

```tsx
import type { ReactNode } from "react";
import { DASH, fmtUtc } from "./format";
import { TABS, TAB_BODY } from "./tabs";
import type { Report } from "./types";
import type { Mode } from "./ui";

/** Project, branch, computes, Postgres version, data time, score and the banners; above the tabs and on page 1. */
export function ReportHeader({ report: r, mode, actions }: { report: Report; mode: Mode; actions?: ReactNode }) {
  const eps = r.endpoints.map((e) => `${e.id} (${e.type}, ${e.state.toLowerCase() || "unknown"}${e.minCu !== null ? `, ${e.minCu}–${e.maxCu} CU` : ""})`);
  return (
    <header className="rp-header">
      <div className="rp-header-main">
        <div>
          <p className="rp-eyebrow">Lakebase health report</p>
          <h1 className="rp-title">
            {r.project.name} <span className="rp-title-branch">/ {r.branch.name}</span>
          </h1>
          <dl className="rp-facts">
            <dt>Computes</dt>
            <dd>{eps.length ? eps.join("; ") : DASH}</dd>
            <dt>Postgres</dt>
            <dd>{r.pgVersion ?? DASH}</dd>
            <dt>Data as of</dt>
            <dd>{fmtUtc(r.dataAsOf)} (last analysis run)</dd>
            {mode === "export" ? (
              <>
                <dt>Exported</dt>
                <dd>{fmtUtc(r.generatedAt)}</dd>
              </>
            ) : null}
          </dl>
        </div>
        <div className="rp-grade">
          <div className={`rp-grade-letter rp-grade-${(r.score.grade ?? "none").toLowerCase()}`}>{r.score.grade ?? DASH}</div>
          <div className="rp-grade-score">{r.score.pct === null ? "not assessed" : `${r.score.pct} %`}</div>
          <div className="rp-grade-assessed">
            assessed {r.score.assessed} of {r.score.total}
          </div>
          {actions}
        </div>
      </div>
      {r.banners.map((b) => (
        <p key={b} className="rp-banner" role="status">
          {b}
        </p>
      ))}
    </header>
  );
}

/** The whole downloadable report: one self-contained page, the exported tabs in tab order, inline CSS only. */
export function ReportDocument({ report, css }: { report: Report; css: string }) {
  const title = `Lakebase health: ${report.project.name} / ${report.branch.name}`;
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="color-scheme" content="light dark" />
        <title>{title}</title>
        <style dangerouslySetInnerHTML={{ __html: css }} />
      </head>
      <body className="rp-root rp-export">
        <ReportHeader report={report} mode="export" />
        <nav className="rp-toc" aria-label="Contents">
          {TABS.filter((t) => t.exported).map((t) => (
            <a key={t.id} href={`#${t.id}`}>
              {t.label}
            </a>
          ))}
        </nav>
        {TABS.filter((t) => t.exported).map((t) => {
          const Body = TAB_BODY[t.id];
          return Body ? (
            <section key={t.id} id={t.id} className="rp-tab">
              <h2 className="rp-tab-title">{t.label}</h2>
              <Body report={report} mode="export" />
            </section>
          ) : null;
        })}
        <footer className="rp-footer">
          Generated by Lakebase Health from the latest analysis run. Query text is never included in this file.
        </footer>
      </body>
    </html>
  );
}
```

`app/client/src/report/report.css` (new file, complete; only `--lbh-*` tokens, no `@import`, no `url()`; the print rules start each tab on a new page):

```css
/*
 * Health report styles: the tabbed project page and the downloaded HTML both use them (the server inlines this
 * file after theme/tokens.css). Plain CSS on the --lbh-* tokens only; no @import, no url(), no web fonts.
 */
.rp-root { font-family: var(--lbh-font-sans); color: var(--lbh-text); }
.rp-export { background: var(--lbh-page); margin: 0 auto; max-width: 1200px; padding: var(--lbh-space-6); line-height: 1.45; }
.rp-root a { color: var(--lbh-link); text-decoration: underline; text-decoration-color: var(--lbh-accent); text-underline-offset: 3px; }

.rp-header { background: var(--lbh-card); border: 1px solid var(--lbh-card-border); border-radius: var(--lbh-radius); padding: var(--lbh-space-4) var(--lbh-space-6); margin-bottom: var(--lbh-space-4); }
.rp-header-main { display: flex; flex-wrap: wrap; justify-content: space-between; gap: var(--lbh-space-4); }
.rp-eyebrow { margin: 0; font-size: var(--lbh-text-xs); text-transform: uppercase; letter-spacing: 0.06em; color: var(--lbh-text-muted); }
.rp-title { margin: var(--lbh-space-1) 0 var(--lbh-space-2); font-size: var(--lbh-text-2xl); color: var(--lbh-heading); font-weight: 600; }
.rp-title-branch { font-weight: 400; color: var(--lbh-text-muted); }
.rp-facts { display: grid; grid-template-columns: max-content 1fr; gap: 2px var(--lbh-space-3); margin: 0; font-size: var(--lbh-text-sm); }
.rp-facts dt { color: var(--lbh-text-muted); }
.rp-facts dd { margin: 0; font-family: var(--lbh-font-mono); font-size: 0.8125rem; }
.rp-grade { text-align: center; min-width: 9rem; }
.rp-grade-letter { font-size: 3rem; font-weight: 700; line-height: 1; color: var(--lbh-heading); }
.rp-grade-a, .rp-grade-b { color: var(--lbh-sev-ok-fg); }
.rp-grade-c, .rp-grade-d { color: var(--lbh-sev-warning-fg); }
.rp-grade-f { color: var(--lbh-sev-critical-fg); }
.rp-grade-score { font-size: var(--lbh-text-lg); font-weight: 600; }
.rp-grade-assessed { font-size: var(--lbh-text-xs); color: var(--lbh-text-muted); margin-bottom: var(--lbh-space-2); }
.rp-banner { margin: var(--lbh-space-3) 0 0; padding: var(--lbh-space-2) var(--lbh-space-3); border-radius: var(--lbh-radius); border: 1px solid var(--lbh-sev-warning-border); background: var(--lbh-sev-warning-bg); color: var(--lbh-sev-warning-fg); font-size: var(--lbh-text-sm); }

.rp-toc { display: flex; flex-wrap: wrap; gap: var(--lbh-space-4); margin: 0 0 var(--lbh-space-4); font-size: var(--lbh-text-sm); }
.rp-tab { margin-bottom: var(--lbh-space-8); }
.rp-tab-title { font-size: var(--lbh-text-xl); color: var(--lbh-heading); border-bottom: 2px solid var(--lbh-accent); padding-bottom: var(--lbh-space-1); }

.rp-tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr)); gap: var(--lbh-space-3); margin-bottom: var(--lbh-space-4); }
.rp-tile { position: relative; background: var(--lbh-card); border: 1px solid var(--lbh-card-border); border-radius: var(--lbh-radius); padding: var(--lbh-space-3) var(--lbh-space-4); overflow: hidden; }
.rp-tile::before { content: ""; position: absolute; inset: 0 auto 0 0; width: 4px; background: transparent; }
.rp-accent-critical::before { background: var(--lbh-sev-critical-fg); }
.rp-accent-warning::before { background: var(--lbh-sev-warning-fg); }
.rp-accent-info::before { background: var(--lbh-sev-info-fg); }
.rp-accent-ok::before { background: var(--lbh-sev-ok-fg); }
.rp-tile-label { font-size: var(--lbh-text-xs); text-transform: uppercase; letter-spacing: 0.04em; color: var(--lbh-text-muted); }
.rp-tile-value { font-size: var(--lbh-text-2xl); font-weight: 600; color: var(--lbh-heading); font-variant-numeric: tabular-nums; }
.rp-tile-hint { font-size: var(--lbh-text-xs); color: var(--lbh-text-muted); }

.rp-card { background: var(--lbh-card); border: 1px solid var(--lbh-card-border); border-radius: var(--lbh-radius); padding: var(--lbh-space-4) var(--lbh-space-4); margin-bottom: var(--lbh-space-4); break-inside: avoid; }
.rp-card-head { display: flex; justify-content: space-between; align-items: baseline; gap: var(--lbh-space-3); }
.rp-card-title { margin: 0; font-size: var(--lbh-text-base); font-weight: 600; color: var(--lbh-heading); }
.rp-explain { margin: var(--lbh-space-1) 0 var(--lbh-space-3); font-size: var(--lbh-text-sm); color: var(--lbh-text-muted); }
.rp-asof { font-size: var(--lbh-text-xs); color: var(--lbh-text-muted); white-space: nowrap; }
.rp-stale { color: var(--lbh-sev-warning-fg); font-weight: 600; }
.rp-error { padding: var(--lbh-space-3); border: 1px solid var(--lbh-sev-critical-border); background: var(--lbh-sev-critical-bg); color: var(--lbh-sev-critical-fg); border-radius: var(--lbh-radius); font-size: var(--lbh-text-sm); }
.rp-empty, .rp-note, .rp-more { font-size: var(--lbh-text-sm); color: var(--lbh-text-muted); margin: var(--lbh-space-2) 0; }
.rp-focus { margin: 0; }

.rp-table-wrap { overflow-x: auto; }
.rp-table { width: 100%; border-collapse: collapse; font-size: var(--lbh-text-sm); }
.rp-table th { text-align: left; font-size: var(--lbh-text-xs); font-weight: 500; color: var(--lbh-text-muted); background: var(--lbh-subtle); padding: var(--lbh-space-2) var(--lbh-space-3); }
.rp-table td { padding: var(--lbh-space-2) var(--lbh-space-3); border-top: 1px solid var(--lbh-card-border); vertical-align: top; }
.rp-num { text-align: right !important; font-family: var(--lbh-font-mono); font-variant-numeric: tabular-nums; white-space: nowrap; }

.rp-badge { display: inline-block; padding: 0 var(--lbh-space-2); border: 1px solid; border-radius: 999px; font-size: var(--lbh-text-xs); font-weight: 600; white-space: nowrap; }
.rp-tone-critical { color: var(--lbh-sev-critical-fg); background: var(--lbh-sev-critical-bg); border-color: var(--lbh-sev-critical-border); }
.rp-tone-warning { color: var(--lbh-sev-warning-fg); background: var(--lbh-sev-warning-bg); border-color: var(--lbh-sev-warning-border); }
.rp-tone-info { color: var(--lbh-sev-info-fg); background: var(--lbh-sev-info-bg); border-color: var(--lbh-sev-info-border); }
.rp-tone-ok { color: var(--lbh-sev-ok-fg); background: var(--lbh-sev-ok-bg); border-color: var(--lbh-sev-ok-border); }
.rp-tone-neutral { color: var(--lbh-sev-neutral-fg); background: var(--lbh-sev-neutral-bg); border-color: var(--lbh-sev-neutral-border); }

.rp-scorecard { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: var(--lbh-space-3); }
.rp-area { border: 1px solid var(--lbh-card-border); border-radius: var(--lbh-radius); padding: var(--lbh-space-3); }
.rp-area-fail { border-color: var(--lbh-sev-critical-border); }
.rp-area-warn { border-color: var(--lbh-sev-warning-border); }
.rp-area-head { display: flex; align-items: center; gap: var(--lbh-space-2); }
.rp-area-label { font-weight: 600; color: var(--lbh-heading); }
.rp-area-score { margin-left: auto; font-family: var(--lbh-font-mono); color: var(--lbh-text-muted); }
.rp-area-blurb { margin: var(--lbh-space-1) 0; font-size: var(--lbh-text-xs); color: var(--lbh-text-muted); }
.rp-area-detail { margin: 0; font-size: var(--lbh-text-sm); }

.rp-runbook { list-style: none; margin: 0; padding: 0; }
.rp-runbook-item { border-top: 1px solid var(--lbh-card-border); padding: var(--lbh-space-3) 0; break-inside: avoid; }
.rp-runbook-head { display: flex; flex-wrap: wrap; align-items: center; gap: var(--lbh-space-2); }
.rp-runbook-n { font-family: var(--lbh-font-mono); color: var(--lbh-text-muted); }
.rp-runbook-area { font-size: var(--lbh-text-xs); color: var(--lbh-text-muted); text-transform: uppercase; letter-spacing: 0.04em; }
.rp-runbook-title { font-weight: 600; }
.rp-runbook-object { margin: var(--lbh-space-1) 0 0; font-family: var(--lbh-font-mono); font-size: 0.8125rem; }
.rp-runbook-why, .rp-runbook-evidence, .rp-runbook-fix { margin: var(--lbh-space-1) 0; font-size: var(--lbh-text-sm); }
.rp-runbook-evidence { color: var(--lbh-text-muted); }
.rp-code { margin: var(--lbh-space-2) 0 0; padding: var(--lbh-space-2) var(--lbh-space-3); background: var(--lbh-code-bg); border: 1px solid var(--lbh-card-border); border-radius: var(--lbh-radius); font-family: var(--lbh-font-mono); font-size: 0.8125rem; white-space: pre-wrap; word-break: break-word; }

.rp-chart { margin: 0 0 var(--lbh-space-3); }
.rp-chart svg { width: 100%; height: 180px; display: block; }
.rp-grid { stroke: var(--lbh-chart-grid); stroke-width: 1; }
.rp-axis { fill: var(--lbh-chart-axis); font-size: 11px; font-family: var(--lbh-font-sans); }
.rp-line { stroke-width: 2; vector-effect: non-scaling-stroke; }
.rp-c1 { stroke: var(--lbh-chart-1); fill: var(--lbh-chart-1); } .rp-c2 { stroke: var(--lbh-chart-2); fill: var(--lbh-chart-2); }
.rp-c3 { stroke: var(--lbh-chart-3); fill: var(--lbh-chart-3); } .rp-c4 { stroke: var(--lbh-chart-4); fill: var(--lbh-chart-4); }
.rp-c5 { stroke: var(--lbh-chart-5); fill: var(--lbh-chart-5); } .rp-c6 { stroke: var(--lbh-chart-6); fill: var(--lbh-chart-6); }
.rp-c7 { stroke: var(--lbh-chart-7); fill: var(--lbh-chart-7); } .rp-c8 { stroke: var(--lbh-chart-8); fill: var(--lbh-chart-8); }
polyline.rp-line { fill: none; }
.rp-legend { display: flex; flex-wrap: wrap; gap: var(--lbh-space-3); font-size: var(--lbh-text-xs); color: var(--lbh-text-muted); }
.rp-legend-item { display: inline-flex; align-items: center; gap: var(--lbh-space-1); }
.rp-swatch { display: inline-block; width: 10px; height: 10px; border-radius: 2px; background: currentColor; }
.rp-swatch.rp-c1 { background: var(--lbh-chart-1); } .rp-swatch.rp-c2 { background: var(--lbh-chart-2); }
.rp-swatch.rp-c3 { background: var(--lbh-chart-3); } .rp-swatch.rp-c4 { background: var(--lbh-chart-4); }
.rp-swatch.rp-c5 { background: var(--lbh-chart-5); } .rp-swatch.rp-c6 { background: var(--lbh-chart-6); }
.rp-swatch.rp-c7 { background: var(--lbh-chart-7); } .rp-swatch.rp-c8 { background: var(--lbh-chart-8); }
.rp-footer { margin-top: var(--lbh-space-8); font-size: var(--lbh-text-xs); color: var(--lbh-text-muted); }

@media (max-width: 720px) { .rp-scorecard { grid-template-columns: 1fr; } }
@media print {
  .rp-export { max-width: none; padding: 0; background: #fff; }
  .rp-toc { display: none; }
  .rp-tab { break-before: page; }
  .rp-card, .rp-tile, .rp-header { border-color: #ccc; }
  .rp-root a { text-decoration: none; }
}
```

- [ ] **Step 5: The render, and JSX for the server's typecheck**

`app/server/report/export.ts` (new file, complete):

```ts
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ReportDocument } from "../../client/src/report/Document";
import type { Report } from "../../client/src/report/types";
import { strip } from "./strip";

/** The theme tokens and the report styles, inlined into every export (read once; the app ships its sources). */
export function exportCss(root = process.cwd()): string {
  const css = ["client/src/theme/tokens.css", "client/src/report/report.css"]
    .map((f) => readFileSync(join(root, f), "utf8").replace(/\/\*[\s\S]*?\*\//g, ""))
    .join("\n");
  if (/@import|url\(|<\/style/i.test(css)) throw new Error("report CSS must not import, load URLs or close the style tag");
  return css;
}

let cached: string | undefined;
const css = () => (cached ??= exportCss());

const slug = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60) || "x";

/** lakebase-health-<project>-<branch>-<UTC yyyymmddThhmmZ>.html */
export function exportFilename(report: Report, now: Date): string {
  const t = now.toISOString().replace(/[-:]/g, "").slice(0, 13);
  return `lakebase-health-${slug(report.project.name)}-${slug(report.branch.name)}-${t}Z.html`;
}

/** The downloadable report: query text stripped, one self-contained HTML document with inline CSS and SVG. */
export function renderExport(report: Report, styles: string = css()): string {
  return `<!DOCTYPE html>${renderToStaticMarkup(createElement(ReportDocument, { report: strip(report), css: styles }))}`;
}
```

`app/tsconfig.server.json`:

Replace:

```jsonc
    "target": "ES2020",
    "lib": ["ES2020"],
    "noEmit": true,
    "rootDir": "./"
  },
  "include": ["server/**/*", "shared/**/*", "config/**/*"],
  "exclude": ["node_modules", "dist", "client"]
```

with:

```jsonc
    "target": "ES2020",
    "lib": ["ES2020"],
    "noEmit": true,
    "rootDir": "./",
    // The report components in client/src/report are rendered to static HTML on the server (the download)
    "jsx": "react-jsx"
  },
  "include": ["server/**/*", "shared/**/*", "config/**/*"],
  "exclude": ["node_modules", "dist", "client"]
```

- [ ] **Step 6: Run the tests, typecheck and bundle**

Run: `cd app && npx vitest run server && npx tsc -b tsconfig.server.json tsconfig.client.json`
Expected: all pass; no type errors.

The bundle is checked in Task 15, once a route imports the render.

- [ ] **Step 7: Commit**

```bash
git add app/client/src/report app/server/report/export.ts app/tsconfig.server.json app/server/tests/report_export.test.ts
git commit -m "feat(app): report tabs as shared components; self-contained HTML render" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 15: "Download report": the export endpoint and its audit row

`GET /api/report/:id/:branch.html` builds the same report, renders the file, writes one `audit_events` row as the app service principal, and sends it as an attachment with a CSP that allows nothing but inline styles. No audit row, no file.

**Files:**
- Create: `app/server/writes/insert_audit.sql`
- Modify: `app/server/lib/handlers.ts` **[LLM-merge]**, `app/server/routes.ts` **[LLM-merge]**, `app/server/server.ts` **[LLM-merge]**
- Test: `app/server/tests/report_handlers.test.ts` (append), `app/server/tests/readonly.test.ts` (modify)

**Interfaces:**
- Consumes: `reportScope` (Task 13), `renderExport`, `exportFilename` (Task 14), `createWriter(file, catalog, exec)` (`server/lib/sql.ts`).
- Produces: `type Audit = { insert(row: Record<string, string>): Promise<void>; newId(): string }` and `Deps.audit?: Audit` (absent → the download is 404); handler `reportExport(ctx, rawId, rawBranch) -> Promise<{filename, html}>`; audit row `{event_id, actor, action: "report_export", project_id, branch_id, detail: '{"filename": …, "unavailable": [section, …]}'}`.

- [ ] **Step 1: Write the failing tests**

`app/server/tests/report_handlers.test.ts`, append:

Replace:

```ts
    await expect(h.report(ctx, HC, "main; DROP")).rejects.toMatchObject({ status: 400 });
  });
});
```

with:

```ts
    await expect(h.report(ctx, HC, "main; DROP")).rejects.toMatchObject({ status: 400 });
  });
});

describe("report export handler", () => {
  const audited = () => {
    const rows: Record<string, string>[] = [];
    return { rows, insert: async (r: Record<string, string>) => void rows.push(r), newId: () => "e1" };
  };

  it("writes an audit row and returns the file", async () => {
    const audit = audited();
    const out = await createHandlers({ ...deps({ report_branches: [BRANCH] }), audit }).reportExport(ctx, HC, "default");
    expect(out.filename).toBe("lakebase-health-healthcheck-production-20261029T1200Z.html");
    expect(out.html).toMatch(/^<!DOCTYPE html>/);
    expect(audit.rows).toEqual([{ event_id: "e1", actor: "alice", action: "report_export", project_id: HC, branch_id: "br-prod",
      detail: JSON.stringify({ filename: out.filename, unavailable: [] }) }]);
  });

  it("returns 404 for a hidden project or branch and writes no audit row", async () => {
    const audit = audited();
    const h = createHandlers({ ...deps({ report_branches: [BRANCH] }), audit });
    await expect(h.reportExport(ctx, OTHER, "default")).rejects.toMatchObject({ status: 404 });
    await expect(h.reportExport(ctx, HC, "br-elsewhere")).rejects.toMatchObject({ status: 404 });
    expect(audit.rows).toEqual([]);
  });

  it("fails when the audit row cannot be written", async () => {
    const audit = { insert: async () => { throw new Error("no MODIFY"); }, newId: () => "e1" };
    await expect(createHandlers({ ...deps({ report_branches: [BRANCH] }), audit }).reportExport(ctx, HC, "default")).rejects.toThrow("no MODIFY");
  });
});
```

`app/server/tests/readonly.test.ts`:

Replace:

```ts
});

describe("app writes", () => {
  it("server/writes holds only the approval insert", () => {
    const files = readdirSync(join(root, "server", "writes"));
    expect(files).toEqual(["insert_approval.sql"]);
    const sql = body(join(root, "server", "writes", "insert_approval.sql"));
    expect(sql).toMatch(/^INSERT INTO \{catalog\}\.lakebase_health\.approvals\s*\(/);
    expect(sql.split(";").filter((s) => s.trim()).length).toBe(1);
```

with:

```ts
});

describe("app writes", () => {
  it("server/writes holds only the approval and audit inserts", () => {
    const files = readdirSync(join(root, "server", "writes")).sort();
    expect(files).toEqual(["insert_approval.sql", "insert_audit.sql"]);
    const audit = body(join(root, "server", "writes", "insert_audit.sql"));
    expect(audit).toMatch(/^INSERT INTO \{catalog\}\.lakebase_health\.audit_events\s*\(/);
    expect(audit).not.toMatch(/\b(UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT|SELECT)\b/i);
    const sql = body(join(root, "server", "writes", "insert_approval.sql"));
    expect(sql).toMatch(/^INSERT INTO \{catalog\}\.lakebase_health\.approvals\s*\(/);
    expect(sql.split(";").filter((s) => s.trim()).length).toBe(1);
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd app && npx vitest run server/tests/report_handlers.test.ts server/tests/readonly.test.ts`
Expected: FAIL (`reportExport is not a function`; `server/writes` lists only `insert_approval.sql`).

- [ ] **Step 3: The insert**

`app/server/writes/insert_audit.sql` (new file, complete):

```sql
INSERT INTO {catalog}.lakebase_health.audit_events (event_id, ts, actor, action, project_id, branch_id, detail)
VALUES (:event_id, current_timestamp(), :actor, :action, :project_id, :branch_id, :detail)
```

- [ ] **Step 4: The handler** **[LLM-merge]**

`app/server/lib/handlers.ts` (the `createHandlers` signature below is the merged one, with `llm`):

Replace (1 of 5):

```ts
import { parseQueryPairs, queryLabel, skeleton } from "./querytext";
import type { Row } from "./sql";
import { ReportNotFound, buildReport } from "../report/build";
import { parseActionId, parseBranchRef, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "./validate";

export type Ctx = { token?: string; user?: string; now: Date };
```

with:

```ts
import { parseQueryPairs, queryLabel, skeleton } from "./querytext";
import type { Row } from "./sql";
import { ReportNotFound, buildReport } from "../report/build";
import { exportFilename, renderExport } from "../report/export";
import { parseActionId, parseBranchRef, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "./validate";

export type Ctx = { token?: string; user?: string; now: Date };
```

Replace (2 of 5):

```ts
  runner: { run(name: string, ids: string[], params?: Record<string, string | Date>): Promise<Row[]> };
  scheduleMinutes: number;
  approvals?: Approvals;
  /** LLM query rewrites: LLM_ENABLED and LLM_ENDPOINT. Absent means not set up. */
  llm?: { enabled: boolean; endpoint: string };
};
```

with:

```ts
  runner: { run(name: string, ids: string[], params?: Record<string, string | Date>): Promise<Row[]> };
  scheduleMinutes: number;
  approvals?: Approvals;
  /** The report export's audit row (server/writes/insert_audit.sql); absent means exports are off. */
  audit?: Audit;
  /** LLM query rewrites: LLM_ENABLED and LLM_ENDPOINT. Absent means not set up. */
  llm?: { enabled: boolean; endpoint: string };
};
```

Replace (3 of 5):

```ts
  newId(): string;
};

export class HttpError extends Error {
  constructor(
    public status: number,
```

with:

```ts
  newId(): string;
};

/** One audit_events row per report download. */
export type Audit = { insert(row: Record<string, string>): Promise<void>; newId(): string };

export class HttpError extends Error {
  constructor(
    public status: number,
```

Replace (4 of 5):

```ts
const REWRITE = "query_rewrite";
const ADVISORY = "a query rewrite is advisory: change it in your application code";

export function createHandlers({ access, runner, scheduleMinutes, approvals, llm }: Deps) {
  async function who(ctx: Ctx): Promise<Access> {
    try {
      return await access.resolve(ctx.token, ctx.user);
```

with:

```ts
const REWRITE = "query_rewrite";
const ADVISORY = "a query rewrite is advisory: change it in your application code";

export function createHandlers({ access, runner, scheduleMinutes, approvals, llm, audit }: Deps) {
  async function who(ctx: Ctx): Promise<Access> {
    try {
      return await access.resolve(ctx.token, ctx.user);
```

Replace (5 of 5):

```ts
      return (await reportScope(ctx, rawId, rawBranch)).report;
    },

    refresh(ctx: Ctx) {
      if (ctx.user) access.refresh(ctx.user);
    },
```

with:

```ts
      return (await reportScope(ctx, rawId, rawBranch)).report;
    },

    /**
     * "Download report": the same data, query text stripped whatever the caller's PII membership, as one HTML file.
     * The audit row is written first; no row, no file.
     */
    async reportExport(ctx: Ctx, rawId: string, rawBranch: string) {
      if (!audit) throw new HttpError(404, "not found");
      const { a, id, report } = await reportScope(ctx, rawId, rawBranch);
      const filename = exportFilename(report, ctx.now);
      const html = renderExport(report);
      const unavailable = Object.entries(report.sections).filter(([, s]) => s.status === "error").map(([k]) => k);
      await audit.insert({
        event_id: audit.newId(),
        actor: a.userName ?? ctx.user ?? "",
        action: "report_export",
        project_id: id,
        branch_id: report.branch.id,
        detail: JSON.stringify({ filename, unavailable }),
      });
      return { filename, html };
    },

    refresh(ctx: Ctx) {
      if (ctx.user) access.refresh(ctx.user);
    },
```

- [ ] **Step 5: The route** **[LLM-merge]**

`app/server/routes.ts` (one route serves both: a branch ending in `.html` is the download):

Replace (1 of 2):

```ts
  now: new Date(),
});

function send(res: express.Response, work: () => Promise<unknown>, status = 200) {
  work()
    .then((body) => res.status(status).json(body))
    .catch((e: unknown) => {
      if (e instanceof HttpError) {
        res.status(e.status).json(e.code ? { error: e.message, code: e.code } : { error: e.message });
        return;
      }
      console.error("lakebase-health route failed", e);
      res.status(502).json({ error: "upstream error" });
    });
}

const param = (v: unknown) => (typeof v === "string" ? v : "");
```

with:

```ts
  now: new Date(),
});

function fail(res: express.Response, e: unknown) {
  if (e instanceof HttpError) {
    res.status(e.status).json(e.code ? { error: e.message, code: e.code } : { error: e.message });
    return;
  }
  console.error("lakebase-health route failed", e);
  res.status(502).json({ error: "upstream error" });
}

function send(res: express.Response, work: () => Promise<unknown>, status = 200) {
  work()
    .then((body) => res.status(status).json(body))
    .catch((e: unknown) => fail(res, e));
}

const param = (v: unknown) => (typeof v === "string" ? v : "");
```

Replace (2 of 2):

```ts
  app.get("/api/projects/:id/queries", (req, res) =>
    send(res, () => h.queries(ctxOf(req), param(req.params.id), req.query.ids)),
  );
  app.get("/api/report/:id/:branch", (req, res) =>
    send(res, () => h.report(ctxOf(req), param(req.params.id), param(req.params.branch))),
  );
  app.get("/api/actions/:id", (req, res) => send(res, () => h.action(ctxOf(req), param(req.params.id))));
  // AppKit's server plugin already parses JSON bodies.
  app.post("/api/actions/:id/requests", (req, res) =>
```

with:

```ts
  app.get("/api/projects/:id/queries", (req, res) =>
    send(res, () => h.queries(ctxOf(req), param(req.params.id), req.query.ids)),
  );
  app.get("/api/report/:id/:branch", (req, res) => {
    const branch = param(req.params.branch);
    if (!branch.endsWith(".html")) {
      send(res, () => h.report(ctxOf(req), param(req.params.id), branch));
      return;
    }
    h.reportExport(ctxOf(req), param(req.params.id), branch.slice(0, -".html".length))
      .then(({ filename, html }) => {
        res
          .status(200)
          .set({
            "Content-Type": "text/html; charset=utf-8",
            "Content-Disposition": `attachment; filename="${filename}"`,
            // the file needs nothing but its own inline styles
            "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'",
          })
          .send(html);
      })
      .catch((e: unknown) => fail(res, e));
  });
  app.get("/api/actions/:id", (req, res) => send(res, () => h.action(ctxOf(req), param(req.params.id))));
  // AppKit's server plugin already parses JSON bodies.
  app.post("/api/actions/:id/requests", (req, res) =>
```

- [ ] **Step 6: Wire the writer** **[LLM-merge]**

`app/server/server.ts` (the LLM branch added `llm: {...}` after `approvals,`; `audit` goes between them):

Replace (1 of 2):

```ts
    const runner = createRunner(loadQueries(join(process.cwd(), "server", "queries"), catalog), exec);
    const http = userHttp(host.startsWith("http") ? host : `https://${host}`);
    const insert = createWriter(join(process.cwd(), "server", "writes", "insert_approval.sql"), catalog, exec);
    const fixerJobId = Number(process.env.FIXER_JOB_ID ?? "");
    const approvals: Approvals = {
      // The approver check uses the caller's own token, GET-only: project owner, or able to read the project ACL.
```

with:

```ts
    const runner = createRunner(loadQueries(join(process.cwd(), "server", "queries"), catalog), exec);
    const http = userHttp(host.startsWith("http") ? host : `https://${host}`);
    const insert = createWriter(join(process.cwd(), "server", "writes", "insert_approval.sql"), catalog, exec);
    // Runs as the app service principal: MODIFY on audit_events (scripts/fixer_grants.sh)
    const insertAudit = createWriter(join(process.cwd(), "server", "writes", "insert_audit.sql"), catalog, exec);
    const fixerJobId = Number(process.env.FIXER_JOB_ID ?? "");
    const approvals: Approvals = {
      // The approver check uses the caller's own token, GET-only: project owner, or able to read the project ACL.
```

Replace (2 of 2):

```ts
      runner,
      scheduleMinutes: Number(process.env.SCHEDULE_MINUTES ?? "60"),
      approvals,
      llm: {
        enabled: process.env.LLM_ENABLED?.trim().toLowerCase() !== "false",
        endpoint: process.env.LLM_ENDPOINT ?? "databricks-claude-opus-5-5",
```

with:

```ts
      runner,
      scheduleMinutes: Number(process.env.SCHEDULE_MINUTES ?? "60"),
      approvals,
      audit: { insert: insertAudit, newId: () => randomUUID().replace(/-/g, "") },
      llm: {
        enabled: process.env.LLM_ENABLED?.trim().toLowerCase() !== "false",
        endpoint: process.env.LLM_ENDPOINT ?? "databricks-claude-opus-5-5",
```

- [ ] **Step 7: Run the tests, typecheck and bundle**

Run: `cd app && npx vitest run server && npx tsc -b tsconfig.server.json && npx tsdown -c tsdown.server.config.ts && ls dist/client/src/report && rm -rf dist`
Expected: all pass; the bundle lists `Document.js`, `LineChart.js`, `format.js`, `tabs.js`, `ui.js` (the report components are bundled with the server).

- [ ] **Step 8: Commit**

```bash
git add app/server/writes/insert_audit.sql app/server/lib/handlers.ts app/server/routes.ts app/server/server.ts \
  app/server/tests/report_handlers.test.ts app/server/tests/readonly.test.ts
git commit -m "feat(app): report download with an audit row, never query text" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 16: The tabbed project page

`/projects/:id` becomes the report: it loads `/api/report/:id/default` and redirects to `/projects/:id/branches/<default>/health`. A branch picker (AppKit `Select`) lists the project's branches; AppKit `Tabs` are controlled by the URL's `:tab` (react-router); the header carries **Download report**. Queries adds the existing one-row-per-query list with text labels (masked per user by the existing lookup); Waits & Locks links each compute's incident timeline; Fixes embeds the existing fixes view.

**Files:**
- Create: `app/client/src/views/Report.tsx`
- Modify: `app/client/src/App.tsx`, `app/client/src/lib/nav.ts`, `app/client/src/views/Actions.tsx`
- Test: `app/client/src/views/Report.test.tsx` (create)

**Interfaces:**
- Consumes: `TABS`, `TAB_BODY`, `isTab`, `QueriesTiles`, `StatementsCards`, `WaitsCards`, `ReportHeader` (Task 14); `useApi`, `useQueryLabels`, `queryRefs`, `FindingRows` (from `views/Project.tsx`), `ActionsView`; AppKit `Tabs`, `TabsList`, `TabsTrigger`, `TabsContent`, `Select`, `SelectTrigger`, `SelectValue`, `SelectContent`, `SelectItem`, `Button`.
- Produces: `ReportView({report, tab})`, default `ReportPage`; routes `/projects/:id`, `/projects/:id/branches/:branch`, `/projects/:id/branches/:branch/:tab`; `ActionsView({data, embedded?})`. `/projects/:id/actions` keeps working. `views/Project.tsx` stays: its `FindingRows` is reused, and its tests still cover `ProjectView`.

- [ ] **Step 1: Write the failing test**

`app/client/src/views/Report.test.tsx` (new file, complete; it builds its report with the server's `buildReport` over fake rows):

```tsx
// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { buildReport } from "../../../server/report/build";
import type { Report } from "../report/types";
import ReportPage, { ReportView } from "./Report";

const HC = "11111111-2222-4333-8444-555555555555";
const ROWS: Record<string, Record<string, unknown>[]> = {
  report_branches: [
    { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", min_cu: "1", max_cu: "4", suspend_timeout_s: "300", enrolled: "true" },
    { branch_id: "br-dev", branch_name: "dev", is_default_branch: "false", endpoint_id: "ep-d", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", enrolled: "true" },
  ],
  report_findings: [
    { fingerprint: "f1", check_id: "dead_tuples", title: "High share of dead tuples", category: "maintenance", endpoint_id: "ep-a", object: "db:hc_test.orders", status: "open", severity: "critical", measure: "25", evidence: "{}", explain: "Dead tuples. More.", candidate_sql: "VACUUM (ANALYZE) hc_test.orders;" },
    { fingerprint: "f2", check_id: "top_db_time_query", title: "Query uses the most DB time", category: "performance", endpoint_id: "ep-a", object: "42", status: "open", severity: "warning", measure: "50", evidence: '{"queryid":"42"}', queryid: "42", top_queryids: "[]", occurrences: "3" },
  ],
  report_checks: [{ check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok" }],
  report_overview: [{ telemetry_at: new Date().toISOString(), lfc_hit_pct: "98.5" }],
  meta: [{ last_success_at: new Date().toISOString() }],
};
let report: Report;
beforeAll(async () => {
  report = await buildReport({
    runner: { run: async (n: string) => ROWS[n] ?? [] },
    access: { userKey: "a", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: false },
    projectId: HC, branch: "default", now: new Date(), scheduleMinutes: 60,
  });
});

let calls: string[] = [];
beforeEach(() => {
  calls = [];
  vi.stubGlobal("fetch", async (url: string) => {
    calls.push(url);
    if (url.startsWith("/api/report/")) return new Response(JSON.stringify(report), { status: 200 });
    if (url.includes("/queries?ids=")) {
      return new Response(JSON.stringify({ queries: [{ endpoint_id: "ep-a", queryid: "42", text: "SELECT $1", label: "SELECT $1", kind: "skeleton", truncated: false }] }), { status: 200 });
    }
    if (url.endsWith("/actions")) return new Response(JSON.stringify({ project: { id: HC, name: "healthcheck" }, groups: [], canManage: false }), { status: 200 });
    return new Response("{}", { status: 404 });
  });
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function Where() {
  return <span data-testid="where">{useLocation().pathname}</span>;
}
const at = (path: string) =>
  render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/projects/:id" element={<ReportPage />} />
        <Route path="/projects/:id/branches/:branch/:tab" element={<><ReportPage /><Where /></>} />
      </Routes>
    </MemoryRouter>,
  );

describe("the project page is the tabbed report", () => {
  it("opens the default branch on the Health tab", async () => {
    at(`/projects/${HC}`);
    await waitFor(() => expect(screen.getByTestId("where").textContent).toBe(`/projects/${HC}/branches/br-prod/health`));
    expect(calls[0]).toBe(`/api/report/${HC}/default`);
    expect((await screen.findAllByRole("tab")).map((t) => t.textContent)).toEqual([
      "Health", "Overview", "Tables & Indexes", "Queries", "Waits & Locks", "Vacuum & Bloat", "Scale to zero", "System",
      "Settings", "Runbook", "Fixes"]);
    expect(screen.getByText("Scorecard")).toBeTruthy();
    expect(document.querySelectorAll(".rp-area")).toHaveLength(12);
  });

  it("has a header with score, assessed count and the download link", () => {
    render(<MemoryRouter><ReportView report={report} tab="health" /></MemoryRouter>);
    expect(screen.getByText(`assessed ${report.score.assessed} of 12`)).toBeTruthy();
    const link = screen.getByRole("link", { name: /Download report/ });
    expect(link.getAttribute("href")).toBe(`/api/report/${HC}/br-prod.html`);
    expect(link.hasAttribute("download")).toBe(true);
  });

  it("switches tabs through the URL", async () => {
    at(`/projects/${HC}/branches/br-prod/health`);
    const runbook = await screen.findByRole("tab", { name: "Runbook" });
    fireEvent.mouseDown(runbook);
    await waitFor(() => expect(screen.getByTestId("where").textContent).toBe(`/projects/${HC}/branches/br-prod/runbook`));
    expect(await screen.findByText("Actions in order")).toBeTruthy();
    expect(screen.getByText("VACUUM (ANALYZE) hc_test.orders;")).toBeTruthy();
  });

  it("starts every tab with KPI tiles and explains every card", async () => {
    for (const tab of ["health", "overview", "tables", "vacuum", "scale-to-zero", "system", "settings", "runbook"] as const) {
      const { unmount } = render(<MemoryRouter><ReportView report={report} tab={tab} /></MemoryRouter>);
      const panel = screen.getByRole("tabpanel");
      expect(panel.querySelector(".rp-tiles"), tab).toBeTruthy();
      expect(panel.querySelectorAll(".rp-card").length, tab).toBe(panel.querySelectorAll(".rp-card .rp-explain").length);
      unmount();
    }
  });

  it("lists query findings with labels fetched in one batch on the Queries tab", async () => {
    render(<MemoryRouter><ReportView report={report} tab="queries" /></MemoryRouter>);
    const card = screen.getByRole("heading", { name: "Queries with issues" }).closest("[data-slot=card]") as HTMLElement;
    await waitFor(() => expect(within(card).getByText("SELECT $1")).toBeTruthy());
    expect(calls.filter((c) => c.includes("/queries?ids="))).toHaveLength(1);
  });

  it("shows the fixes view without its own header on the Fixes tab", async () => {
    render(<MemoryRouter><ReportView report={report} tab="fixes" /></MemoryRouter>);
    expect(await screen.findByText("No proposed fixes")).toBeTruthy();
    expect(calls).toContain(`/api/projects/${HC}/actions`);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd app && npx vitest run client/src/views/Report.test.tsx`
Expected: FAIL with `Failed to resolve import "./Report"`.

- [ ] **Step 3: The page**

`app/client/src/views/Report.tsx` (new file, complete):

```tsx
import {
  Button,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@databricks/appkit-ui/react";
import { Download } from "lucide-react";
import { Navigate, useNavigate, useParams } from "react-router";
import { ApiMessage } from "../ApiMessage";
import { LoadingState, PageHeader, SectionCard, TextLink } from "../components/ui";
import { queryRefs, type GroupRow } from "../lib/findings";
import { useApi } from "../lib/useApi";
import { useQueryLabels } from "../lib/useQueryLabels";
import { ReportHeader } from "../report/Document";
import { QueriesTiles, StatementsCards, TABS, TAB_BODY, WaitsCards, isTab, type TabId } from "../report/tabs";
import type { Report } from "../report/types";
import "../report/report.css";
import { ActionsView, type ActionsData } from "./Actions";
import { FindingRows } from "./Project";

function QueriesBody({ report }: { report: Report }) {
  const rows = report.queryGroups as GroupRow[];
  const labels = useQueryLabels(report.project.id, queryRefs(rows));
  return (
    <>
      <QueriesTiles report={report} />
      <SectionCard
        title="Queries with issues"
        description="One row per query with a badge per issue; the text shows only to those allowed to read it, others see the query with its values removed."
        contentClassName="px-0"
      >
        {rows.length ? <FindingRows rows={rows} labels={labels} /> : <p className="rp-empty px-5">No query findings.</p>}
      </SectionCard>
      <StatementsCards report={report} mode="app" />
    </>
  );
}

function WaitsBody({ report }: { report: Report }) {
  return (
    <>
      <WaitsCards report={report} mode="app" />
      <SectionCard title="Incident timeline" description="Waits, lock blockers, commits and events on one time axis, up to 7 days, per compute.">
        <ul className="space-y-1">
          {report.endpoints.map((e) => (
            <li key={e.id}>
              <TextLink to={`/endpoints/${e.id}/timeline`}>Open the timeline of {e.id}</TextLink> ({e.type})
            </li>
          ))}
        </ul>
      </SectionCard>
    </>
  );
}

function FixesBody({ projectId }: { projectId: string }) {
  const r = useApi<ActionsData>(`/api/projects/${projectId}/actions`);
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <LoadingState label="Loading fixes" />;
  return <ActionsView data={r.data} embedded />;
}

export function ReportView({ report, tab }: { report: Report; tab: TabId }) {
  const navigate = useNavigate();
  const base = `/projects/${report.project.id}/branches`;
  const go = (branch: string, t: string) => void navigate(`${base}/${branch}/${t}`);
  const actions = (
    <div className="mt-2 flex flex-col items-center gap-2">
      <Button asChild size="sm">
        <a href={`/api/report/${report.project.id}/${report.branch.id}.html`} download>
          <Download /> Download report
        </a>
      </Button>
    </div>
  );
  return (
    <div className="space-y-4">
      <PageHeader crumbs={[{ label: "Projects", to: "/" }, { label: report.project.name }]} title={report.project.name}>
        <div className="flex items-center gap-2 text-sm">
          <span className="text-muted-foreground">Branch</span>
          <Select value={report.branch.id} onValueChange={(b) => go(b, tab)}>
            <SelectTrigger aria-label="Branch" className="w-64">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {report.branches.map((b) => (
                <SelectItem key={b.id} value={b.id}>
                  {b.name}
                  {b.isDefault ? " (default)" : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </PageHeader>
      <div className="rp-root">
        <ReportHeader report={report} mode="app" actions={actions} />
      </div>
      <Tabs value={tab} onValueChange={(t) => go(report.branch.id, t)}>
        <TabsList className="h-auto flex-wrap">
          {TABS.map((t) => (
            <TabsTrigger key={t.id} value={t.id}>
              {t.label}
            </TabsTrigger>
          ))}
        </TabsList>
        {TABS.map((t) => {
          const Body = TAB_BODY[t.id];
          return (
            <TabsContent key={t.id} value={t.id} className="rp-root mt-4">
              {t.id === "queries" ? (
                <QueriesBody report={report} />
              ) : t.id === "waits" ? (
                <WaitsBody report={report} />
              ) : t.id === "fixes" ? (
                <FixesBody projectId={report.project.id} />
              ) : Body ? (
                <Body report={report} mode="app" />
              ) : null}
            </TabsContent>
          );
        })}
      </Tabs>
    </div>
  );
}

/** /projects/:id[/branches/:branch[/:tab]]: the default branch and the Health tab when not given. */
export default function ReportPage() {
  const { id, branch, tab } = useParams();
  const r = useApi<Report>(id ? `/api/report/${id}/${branch ?? "default"}` : null);
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <LoadingState label="Loading the health report" rows={10} />;
  if (!branch || !isTab(tab))
    return <Navigate replace to={`/projects/${r.data.project.id}/branches/${r.data.branch.id}/${isTab(tab) ? tab : "health"}`} />;
  return <ReportView report={r.data} tab={tab} />;
}
```

- [ ] **Step 4: Fixes without their own header, the nav label, the routes**

`app/client/src/views/Actions.tsx`:

Replace (1 of 2):

```tsx
  );
}

export function ActionsView({ data }: { data: ActionsData }) {
  const more = useShowAll<Row>((state) => `/api/projects/${data.project.id}/actions?state=${encodeURIComponent(state)}`, groupRows);
  return (
    <div className="space-y-6">
      <PageHeader
        crumbs={[
          { label: "Projects", to: "/" },
          { label: data.project.name, to: `/projects/${data.project.id}` },
```

with:

```tsx
  );
}

/** `embedded`: inside the project page's Fixes tab, which has its own header. */
export function ActionsView({ data, embedded = false }: { data: ActionsData; embedded?: boolean }) {
  const more = useShowAll<Row>((state) => `/api/projects/${data.project.id}/actions?state=${encodeURIComponent(state)}`, groupRows);
  return (
    <div className="space-y-6">
      {embedded ? null : <PageHeader
        crumbs={[
          { label: "Projects", to: "/" },
          { label: data.project.name, to: `/projects/${data.project.id}` },
```

Replace (2 of 2):

```tsx
        }
      >
        <SubNav items={projectNav(data.project.id)} />
      </PageHeader>
      {data.groups.length === 0 ? (
        <EmptyState title="No proposed fixes" icon={<Wrench />}>
          Fixes appear here when the health engine proposes one for a finding.
```

with:

```tsx
        }
      >
        <SubNav items={projectNav(data.project.id)} />
      </PageHeader>}
      {data.groups.length === 0 ? (
        <EmptyState title="No proposed fixes" icon={<Wrench />}>
          Fixes appear here when the health engine proposes one for a finding.
```

`app/client/src/lib/nav.ts`:

Replace:

```ts
/** Sub-navigation shared by a project's findings page and its fixes page. */
export function projectNav(id: string) {
  return [
    { label: "Findings", to: `/projects/${id}`, end: true },
    { label: "Fixes and approvals", to: `/projects/${id}/actions` },
  ];
}
```

with:

```ts
/** Sub-navigation shared by a project's findings page and its fixes page. */
export function projectNav(id: string) {
  return [
    { label: "Health report", to: `/projects/${id}`, end: true },
    { label: "Fixes and approvals", to: `/projects/${id}/actions` },
  ];
}
```

`app/client/src/App.tsx`:

Replace (1 of 2):

```tsx
import ActionsPage from "./views/Actions";
import FleetPage from "./views/Fleet";
import FindingPage from "./views/Finding";
import ProjectPage from "./views/Project";
import TimelinePage from "./views/Timeline";

function Freshness({ meta }: { meta: Meta | undefined }) {
```

with:

```tsx
import ActionsPage from "./views/Actions";
import FleetPage from "./views/Fleet";
import FindingPage from "./views/Finding";
import ReportPage from "./views/Report";
import TimelinePage from "./views/Timeline";

function Freshness({ meta }: { meta: Meta | undefined }) {
```

Replace (2 of 2):

```tsx
    element: <Layout />,
    children: [
      { path: "/", element: <FleetPage /> },
      { path: "/projects/:id", element: <ProjectPage /> },
      { path: "/projects/:id/actions", element: <ActionsPage /> },
      { path: "/findings/:fp", element: <FindingPage /> },
      { path: "/endpoints/:id/timeline", element: <TimelinePage /> },
```

with:

```tsx
    element: <Layout />,
    children: [
      { path: "/", element: <FleetPage /> },
      // The project page is the health report, one tab per URL: /projects/:id/branches/:branch/:tab
      { path: "/projects/:id", element: <ReportPage /> },
      { path: "/projects/:id/branches/:branch", element: <ReportPage /> },
      { path: "/projects/:id/branches/:branch/:tab", element: <ReportPage /> },
      { path: "/projects/:id/actions", element: <ActionsPage /> },
      { path: "/findings/:fp", element: <FindingPage /> },
      { path: "/endpoints/:id/timeline", element: <TimelinePage /> },
```

- [ ] **Step 5: Run the tests, typecheck, lint and build**

Run: `cd app && npx vitest run && npx tsc -b tsconfig.client.json tsconfig.server.json && npx eslint server client/src && npx vite build --config client/vite.config.ts && rm -rf client/dist`
Expected: all tests pass (live tests skipped); no type errors; eslint reports warnings only (array-index keys, fast-refresh exports), no errors; the client builds.

- [ ] **Step 6: Commit**

```bash
git add app/client/src/views/Report.tsx app/client/src/views/Report.test.tsx app/client/src/App.tsx \
  app/client/src/lib/nav.ts app/client/src/views/Actions.tsx
git commit -m "feat(app): the project page is the tabbed health report" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


### Task 17: Grants and documentation

The app service principal gets `MODIFY` on `audit_events` (and nothing else new); the README, `docs/COMPARISON.md` and `docs/FOLLOW_UPS.md` describe the report.

**Files:**
- Modify: `scripts/fixer_grants.sh` **[LLM-merge]**, `README.md` **[LLM-merge]**, `docs/COMPARISON.md`, `docs/FOLLOW_UPS.md`
- Test: `engine/tests/unit/test_fixer_grants.py` **[LLM-merge]** (append)

**Interfaces:**
- Consumes: the `audit_events` table (Task 5), the export's insert (Task 15).
- Produces: `GRANT MODIFY ON TABLE <catalog>.lakebase_health.audit_events TO <app SP>` in the grants script.

- [ ] **Step 1: Write the failing test** **[LLM-merge]**

`engine/tests/unit/test_fixer_grants.py`, append (after the LLM branch's last test):

Replace:

```python
    (tmp_path / "log.jsonl").unlink()
    stdout, calls = _run(tmp_path, None, LLM_ENABLED="false")
    assert not any(c[0] == "serving-endpoints" for c in calls) and "no model endpoint grant" in stdout
```

with:

```python
    (tmp_path / "log.jsonl").unlink()
    stdout, calls = _run(tmp_path, None, LLM_ENABLED="false")
    assert not any(c[0] == "serving-endpoints" for c in calls) and "no model endpoint grant" in stdout


def test_the_app_may_write_approvals_and_audit_events(tmp_path):
    _, calls = _run(tmp_path, None)
    app = [s for s in _statements(calls) if s.startswith("GRANT MODIFY") and s.endswith(f"TO `{UUID}`")]
    assert "GRANT MODIFY ON TABLE cat.lakebase_health.audit_events TO `" + UUID + "`" in app
    assert "GRANT MODIFY ON TABLE cat.lakebase_health.approvals TO `" + UUID + "`" in app
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_fixer_grants.py -v`
Expected: FAIL on the `audit_events` assertion.

- [ ] **Step 3: The grant** **[LLM-merge]**

`scripts/fixer_grants.sh` (the LLM branch widened the fixer's `for t in …` list just above; this line follows the app's `approvals` grant):

Replace:

```bash
  q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.$t TO \`$FIXER_SP\`"
done
q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.approvals TO \`$APP_SP\`"
# The bundle target to grant for (the CLI's own default is dev). The fixer job is the one this target deployed:
# its ID from `bundle summary`, never a name match, which could pick another target's job in the same workspace.
TARGET="${DATABRICKS_BUNDLE_TARGET:-dev}"
```

with:

```bash
  q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.$t TO \`$FIXER_SP\`"
done
q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.approvals TO \`$APP_SP\`"
q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.audit_events TO \`$APP_SP\`"  # one row per report download
# The bundle target to grant for (the CLI's own default is dev). The fixer job is the one this target deployed:
# its ID from `bundle summary`, never a name match, which could pick another target's job in the same workspace.
TARGET="${DATABRICKS_BUNDLE_TARGET:-dev}"
```

- [ ] **Step 4: Run the test**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_fixer_grants.py -v`
Expected: PASS.

- [ ] **Step 5: Documentation** **[LLM-merge]** (README)

`README.md`, section "Identities and privileges" (App SP row):

Replace:

```markdown
| App SP (created by Databricks Apps) | The app | `SELECT` on `<catalog>.lakebase_health`. `MODIFY` on `approvals` (Unity Catalog has no insert-only privilege; every consumed approval is recorded in the hash-chained ledger). `CAN_MANAGE_RUN` on the fixer job. `CAN_USE` on the SQL warehouse. | Connect to Postgres, hold Lakebase project permissions, or write any other table. Per-user checks (project visibility, approver rights, group membership) use the signed-in user's token and GET calls only. |
```

with:

```markdown
| App SP (created by Databricks Apps) | The app | `SELECT` on `<catalog>.lakebase_health`. `MODIFY` on `approvals` (Unity Catalog has no insert-only privilege; every consumed approval is recorded in the hash-chained ledger) and on `audit_events` (one row per report download). `CAN_MANAGE_RUN` on the fixer job. `CAN_USE` on the SQL warehouse. | Connect to Postgres, hold Lakebase project permissions, or write any other table. Per-user checks (project visibility, approver rights, group membership) use the signed-in user's token and GET calls only. |
```

`README.md`, the grants table:

Replace:

```markdown
| App SP | `MODIFY` on `approvals`; `CAN_MANAGE_RUN` on the fixer job | Catalog owner (`scripts/fixer_grants.sh`) |
```

with:

```markdown
| App SP | `MODIFY` on `approvals` and `audit_events`; `CAN_MANAGE_RUN` on the fixer job | Catalog owner (`scripts/fixer_grants.sh`) |
```

`README.md`, section "Using the app" (the Project row becomes two rows):

Replace:

```markdown
| Project | `/projects/<id>` | Open findings grouped by category, with severity, object, first and last seen, occurrences and confidence. Query findings show one row per query, with a badge per issue. Findings resolved in the last 7 days, collapsed. A computes panel: state, CU range, suspend timeout, HA, restore window, branch protection and **Postgres checks on/off**, with the grant to run when off. |
```

with:

```markdown
| Health report | `/projects/<id>/branches/<branch>/<tab>` | The project page, one branch at a time (default branch first; a picker lists the others). A header with the computes, Postgres version, data time, score, grade, "assessed N of 12", banners and **Download report**, then 11 tabs: Health (scorecard of 12 areas, where to focus, critical items and warnings), Overview, Tables & Indexes, Queries, Waits & Locks, Vacuum & Bloat, Scale to zero, System (24 h charts, `pg_stat_io`), Settings, Runbook (ordered actions with SQL) and Fixes. Every tab starts with KPI tiles; every card says in one line what it shows. `/projects/<id>` opens the default branch's Health tab. |
| Report download | `/api/report/<project uid>/<branch>.html` | One self-contained HTML file (inline CSS and SVG, no scripts, no external requests; print it to PDF from the browser) with the Health, Overview, Tables & Indexes, Queries, Vacuum & Bloat, Scale to zero, System, Settings and Runbook tabs. It never contains query text, whoever downloads it, and each download writes an `audit_events` row. |
```

`docs/COMPARISON.md`, section 3:

Replace:

```markdown
| Health score, tabbed project page, inventory panels, HTML / CSV export | Health Check app | Report spec (tabbed project page), next after the LLM rewrites deploy |
| Synced-table awareness, WAL bursts, getpage spikes, scale-to-zero blockers, autovacuum saturation, stats-reset guard | Health Check app | Same spec (six new checks) |
| `pg_stat_io` panel | Health Check app | Same spec (System tab, `inventory_io`) |
```

with:

```markdown
| Health score, tabbed project page, inventory panels, HTML / CSV export | Health Check app | Done on `feature/health-report`: score and grade, 11-tab project page, inventory panels, self-contained HTML download (CSV not planned) |
| Synced-table awareness, WAL bursts, getpage spikes, scale-to-zero blockers, autovacuum saturation, stats-reset guard | Health Check app | Done (six new checks, synced helper, stats-reset guard) |
| `pg_stat_io` panel | Health Check app | Done (System tab, `inventory_io`) |
```

`docs/FOLLOW_UPS.md`, the platform table:

Replace:

```markdown
| Synced tables (`__db_system` partitions) | In the report spec | High priority: avoid misleading drop-index advice |
```

with:

```markdown
| Synced tables (`__db_system` partitions) | Done (`synced.py`, `synced_index_review`) | Indexes on synced storage are reviewed, never dropped |
```

Replace:

```markdown
| Customer connection pools | Not visible | `suspend_blockers` check (report spec) |
```

with:

```markdown
| Customer connection pools | `suspend_blockers` check | Done; extend to pools that never go idle (min idle > 0 with traffic) |
```

Run: `engine/.venv/bin/python scripts/gen_checks_doc.py --check`
Expected: exit 0 (the README's generated checks summary between its markers is untouched by these edits).

- [ ] **Step 6: Commit**

```bash
git add scripts/fixer_grants.sh engine/tests/unit/test_fixer_grants.py README.md docs/COMPARISON.md docs/FOLLOW_UPS.md
git commit -m "docs: health report in the README; app SP may write audit_events" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```


---

# Phase 3: Deploy and acceptance

### Task 18: Deploy to my-profile and run the acceptance criteria on healthcheck

Deploy the bundle, let the engine create the new tables, grant, restart the app, run the live query test, then the eight acceptance criteria of spec §8 with `scripts/report_acceptance.py`. Creating the synced table (criterion 5) is outward-facing: **ask the user before creating the synced table**, and again before deleting it.

**Files:**
- Create: `scripts/report_acceptance.py`

**Interfaces:**
- Consumes: the deployed app (`/api/report/...`, `/api/fleet`), `my_catalog.lakebase_health` tables, the analysis job (`…lakebase-health-analysis`), `scripts/fix_acceptance.py` helpers `WarehouseSql`, `call`, `sp_token`; the test service principal from the app acceptance (CAN_USE on the app, a Postgres role on healthcheck only; its id and secret in `/tmp/test_sp_app` and `/tmp/test_sp_secret`).
- Produces: a PASS/FAIL line per criterion and `report-acceptance.html` (the downloaded file, gitignored by the step below) for the offline check.

- [ ] **Step 1: The acceptance script**

`scripts/report_acceptance.py` (new file, complete):

```python
"""Acceptance for the health report (spec 2026-10-28-lakebase-health-report-design.md, section 8) on healthcheck.

Usage: TEST_SP_SECRET=... engine/.venv/bin/python scripts/report_acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --test-sp-id <application id> --test-sp-secret-env TEST_SP_SECRET [--steps 1,2,3,4,5,6,7,8] \
         [--synced-pg-table lbh_acceptance.synced_orders --synced-pipeline <pipeline id>]
Needs the deployed bundle, the grants script run, healthcheck's production branch enrolled and the hc_test schema.
Step 4 holds one idle client connection; it is only testable when scale to zero is on and Postgres has been up for
24 h. Step 5 never creates anything: the operator creates the synced table first (plan Task 18, after asking the
user) and passes its Postgres name and pipeline id; the step then triggers one full refresh of that pipeline.
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fix_acceptance import WarehouseSql, call, sp_token  # noqa: E402
from lakebase_health.config import EngineConfig  # noqa: E402

from databricks.sdk import WorkspaceClient  # noqa: E402

RANK = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
IDLE_APP = "lbh-acceptance-idle"


def raw_get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, r.read().decode(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, "", dict(e.headers)


class Acceptance:
    def __init__(self, a):
        self.a = a
        self.w = WorkspaceClient(profile=a.profile)
        self.sql = WarehouseSql(self.w, a.warehouse)
        self.cfg = EngineConfig(catalog=a.catalog)
        self.url = self.w.apps.get("lakebase-health").url.rstrip("/")
        self.failures = []
        self.project = next(p for p in self.w.postgres.list_projects() if p.uid == a.project_uid).name
        branch = next(b for b in self.w.postgres.list_branches(parent=self.project)
                      if b.name.endswith("/branches/production"))
        self.branch_uid = branch.uid
        self.ep = f"{branch.name}/endpoints/primary"

    def owner(self):
        return self.w.config.authenticate()["Authorization"].split()[1]

    def check(self, crit, ok, msg):
        print(f"[{'PASS' if ok else 'FAIL'}] {crit}: {msg}", flush=True)
        if not ok:
            self.failures.append(f"{crit}: {msg}")
        return ok

    def report(self, token=None, branch="default"):
        t = time.time()
        status, body = call(f"{self.url}/api/report/{self.a.project_uid}/{branch}", token or self.owner())
        return status, body, time.time() - t

    def connect(self, app="lakebase-health-notebook"):
        """A Postgres session on the primary as the CLI user (the notebook's application name: never counted)."""
        import psycopg
        host = self.w.postgres.get_endpoint(name=self.ep).status.hosts.host
        token = self.w.postgres.generate_database_credential(endpoint=self.ep).token
        me = self.w.current_user.me().user_name
        return psycopg.connect(host=host, user=me, password=token, dbname="databricks_postgres", sslmode="require",
                               autocommit=True, application_name=app, prepare_threshold=None)

    def rows(self, stmt):
        with self.connect() as conn:
            return conn.execute("/* lakebase-health */ " + stmt).fetchall()

    def analysis_run(self):
        job = next(j.job_id for j in self.w.jobs.list() if j.settings.name.endswith("lakebase-health-analysis"))
        t = time.time()
        run = self.w.jobs.run_now(job_id=job).result(timeout=timedelta(minutes=60))
        print(f"  analysis run {run.run_id}: {time.time() - t:.0f} s")

    def open_findings(self, check_id=None):
        q = (f"SELECT fingerprint, check_id, object, severity, evidence FROM {self.cfg.table('findings')} "
             "WHERE project_id = :p AND (branch_id = :b OR branch_id IS NULL) AND status IN ('new', 'open')")
        args = {"p": self.a.project_uid, "b": self.branch_uid}
        if check_id:
            q += " AND check_id = :c"
            args["c"] = check_id
        return self.sql.query(q, args)

    def step1(self):
        """Score and grade; each area's state matches its open findings."""
        status, rep, _ = self.report()
        if not self.check("1", status == 200, f"report status {status}"):
            return
        s = rep["score"]
        self.check("1", s["assessed"] > 0 and s["grade"] in "ABCDF" and s["pct"] is not None,
                   f"score {s['pct']} %, grade {s['grade']}, assessed {s['assessed']} of {s['total']}")
        by_area = {}
        for f in rep["findings"]:
            if f["area"]:
                by_area.setdefault(f["area"], []).append(f["severity"])
        expect = {"critical": "fail", "warning": "warn", "info": "warn"}
        for a in rep["areas"]:
            if a["state"] == "not_assessed":
                continue
            sev = by_area.get(a["id"], [])
            want = "pass" if not sev else expect["critical" if "critical" in sev else "warning" if "warning" in sev
                                                else "info"]
            self.check("1", a["state"] == want, f"{a['id']}: {a['state']} with open {sorted(set(sev))}")
        sql_open = len(self.open_findings())
        self.check("1", len(rep["findings"]) == min(sql_open, 2000), f"{len(rep['findings'])} findings vs {sql_open} open")

    def step2(self):
        """Planted problems are in the runbook, ordered by severity, each with SQL or a note."""
        _, rep, _ = self.report()
        items = rep["runbook"]
        ranks = [RANK[i["priority"]] for i in items]
        self.check("2", ranks == sorted(ranks), f"{len(items)} items ordered by priority")
        planted = [i for i in items if "hc_test" in i["object"]]
        self.check("2", len(planted) >= 5, f"{len(planted)} runbook items on hc_test objects")
        self.check("2", all(i["sql"] or i["note"] for i in items), "every item has SQL or a note")
        with_sql = [i for i in planted if i["sql"]]
        self.check("2", len(with_sql) >= 3, f"{len(with_sql)} planted items with SQL")

    def step3(self):
        """Inventory lists the hc_test tables and indexes with sizes matching pg_relation_size."""
        _, rep, _ = self.report()
        tables = [t for t in rep["sections"]["tables"]["data"] if t["schema_name"] == "hc_test"]
        indexes = [i for i in rep["sections"]["indexes"]["data"] if i["schema_name"] == "hc_test"]
        self.check("3", len(tables) >= 4 and len(indexes) >= 4, f"{len(tables)} hc_test tables, {len(indexes)} indexes")
        live = dict(self.rows("SELECT c.relname, pg_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n "
                              "ON n.oid = c.relnamespace WHERE n.nspname = 'hc_test' AND c.relkind IN ('r', 'i')"))
        for t in tables + indexes:
            name = t.get("index_name") or t["table_name"]
            got = int(t.get("index_bytes") or t.get("data_bytes") or 0)
            want = live.get(name)
            self.check("3", want is not None and abs(got - want) <= max(0.05 * want, 8192),
                       f"{name}: inventory {got} B, pg_relation_size {want} B")

    def step4(self):
        """An idle client held past the suspend timeout raises suspend_blockers."""
        _, rep, _ = self.report()
        primary = next(e for e in rep["endpoints"] if e["type"] == "primary")
        uptime = rep["sections"]["endpoint"]["data"]["latest"].get("uptime_s") or 0
        if not primary["suspendTimeoutS"]:
            self.check("4", False, "NOT TESTABLE: scale to zero is off on the primary; ask the user before changing it")
            return
        if uptime < 24 * 3600:
            self.check("4", False, f"NOT TESTABLE YET: Postgres up {uptime / 3600:.1f} h; re-run step 4 after 24 h")
            return
        with self.connect(IDLE_APP) as conn:
            conn.execute("SELECT 1")
            wait = primary["suspendTimeoutS"] + 60
            print(f"  holding an idle {IDLE_APP} session {wait} s, then one analysis run")
            time.sleep(wait)
            self.analysis_run()
            found = [f for f in self.open_findings("suspend_blockers") if f["object"].startswith(f"{IDLE_APP} as ")]
        self.check("4", bool(found), f"suspend_blockers for {IDLE_APP}: {[f['object'] for f in found]}")
        if found:
            ev = json.loads(found[0]["evidence"])
            self.check("4", "pid" not in ev and ev.get("idle_sessions", 0) >= 1, f"evidence {ev}")

    def step5(self):
        """A synced table: named on its storage, its unused index reviewed without a fix, a full refresh found."""
        if not (self.a.synced_pg_table and self.a.synced_pipeline):
            self.check("5", False, "NOT RUN: create the synced table first (plan Task 18), then pass "
                                   "--synced-pg-table and --synced-pipeline")
            return
        name = self.a.synced_pg_table
        _, rep, _ = self.report()
        storage = [t for t in rep["sections"]["tables"]["data"] if t.get("synced_table")]
        self.check("5", any(t["synced_table"].replace('"', "") == name for t in storage),
                   f"storage named after the synced table: {[(t['table_name'], t['synced_table']) for t in storage]}")
        review = [f for f in self.open_findings("synced_index_review") if name in f["evidence"]]
        self.check("5", bool(review), f"synced_index_review findings: {[f['object'] for f in review]}")
        if review:
            fps = ", ".join(f"'{f['fingerprint']}'" for f in review)
            acts = self.sql.query(f"SELECT action_id FROM {self.cfg.table('proposed_actions')} WHERE fingerprint IN ({fps})")
            self.check("5", acts == [], f"no fix action for the reviewed index ({len(acts)} found)")
        drops = self.sql.query(f"""
            SELECT a.action_id FROM {self.cfg.table('proposed_actions')} a JOIN {self.cfg.table('findings')} f
            ON f.fingerprint = a.fingerprint WHERE f.project_id = :p AND a.sql LIKE 'DROP INDEX%'
              AND get_json_object(f.evidence, '$.synced_table') IS NOT NULL""", {"p": self.a.project_uid})
        self.check("5", drops == [], f"no DROP INDEX action on synced storage ({len(drops)} found)")
        print("  full refresh of the synced table's pipeline, then two analysis runs")
        upd = self.w.pipelines.start_update(pipeline_id=self.a.synced_pipeline, full_refresh=True)
        for _ in range(120):
            u = self.w.pipelines.get_update(pipeline_id=self.a.synced_pipeline, update_id=upd.update_id).update
            if str(getattr(u.state, "value", u.state)) in ("COMPLETED", "FAILED", "CANCELED"):
                break
            time.sleep(15)
        self.check("5", str(getattr(u.state, "value", u.state)) == "COMPLETED", f"refresh {u.state}")
        self.analysis_run()
        self.analysis_run()
        refresh = [f for f in self.open_findings("synced_full_refresh") if name in f["object"].replace('"', "")]
        self.check("5", bool(refresh), f"synced_full_refresh: {[(f['object'], f['severity']) for f in refresh]}")

    def step6(self):
        """The download opens offline: no scripts or external requests, no query text for a PII member; audited."""
        token = self.owner()
        t0 = self.sql.query("SELECT current_timestamp() AS now")[0]["now"]
        status, html, headers = raw_get(f"{self.url}/api/report/{self.a.project_uid}/default.html", token)
        if not self.check("6", status == 200, f"download status {status}"):
            return
        cd = headers.get("Content-Disposition", "")
        self.check("6", re.search(r'filename="lakebase-health-healthcheck-production-\d{8}T\d{4}Z\.html"', cd) is not None, cd)
        self.check("6", not re.search(r"<script|https?://|\bsrc=|url\(|<link", html, re.I), "no scripts or external URLs")
        texts = [r["query_text"] for r in self.sql.query(
            f"SELECT DISTINCT query_text FROM {self.cfg.table('findings')} WHERE project_id = :p "
            "AND query_text IS NOT NULL AND query_text NOT LIKE '<REDACTED%' LIMIT 50", {"p": self.a.project_uid})]
        leaked = [q for q in texts if len(q) > 12 and q in html]
        self.check("6", texts != [] and leaked == [], f"{len(texts)} known query texts, {len(leaked)} in the file")
        audit = self.sql.query(f"SELECT actor, detail FROM {self.cfg.table('audit_events')} WHERE action = 'report_export' "
                               "AND project_id = :p AND ts >= :t", {"p": self.a.project_uid, "t": t0})
        self.check("6", len(audit) >= 1, f"audit rows: {audit}")
        out = ROOT / "report-acceptance.html"
        out.write_text(html)
        print(f"  saved {out}: open it with the network off to confirm it renders")

    def step7(self):
        """The test SP sees only healthcheck's report; any other project or branch is 404."""
        sp = sp_token(self.w.config.host, self.a.test_sp_id, os.environ[self.a.test_sp_secret_env])
        status, _, _ = self.report(sp)
        self.check("7", status == 200, f"test SP healthcheck report: {status}")
        _, fleet = call(f"{self.url}/api/fleet", self.owner())
        others = [p["project_id"] for p in fleet["projects"] if p["project_id"] != self.a.project_uid][:3]
        for uid in others:
            s, _ = call(f"{self.url}/api/report/{uid}/default", sp)
            self.check("7", s == 404, f"test SP, project {uid}: {s}")
            s, _ = call(f"{self.url}/api/report/{uid}/default.html", sp)
            self.check("7", s == 404, f"test SP download, project {uid}: {s}")
        s, _, _ = self.report(sp, branch="br-not-a-branch-0000")
        self.check("7", s == 404, f"test SP, unknown branch: {s}")

    def step8(self):
        """Report data loads in under 5 s with a warm warehouse."""
        self.report()
        _, _, secs = self.report()
        self.check("8", secs < 5, f"report JSON in {secs:.1f} s")


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--test-sp-id", "--test-sp-secret-env"):
        ap.add_argument(a, required=True)
    ap.add_argument("--steps", default="1,2,3,4,5,6,7,8")
    ap.add_argument("--synced-pg-table", default="", help="schema.table of the synced table in Postgres")
    ap.add_argument("--synced-pipeline", default="", help="pipeline id of the synced table")
    a = ap.parse_args()
    acc = Acceptance(a)
    for step in a.steps.split(","):
        try:
            getattr(acc, f"step{step.strip()}")()
        except Exception as e:
            acc.check(step, False, f"{type(e).__name__}: {e}")
    print("\n".join(["FAILURES:"] + acc.failures) if acc.failures else "ALL REPORT ACCEPTANCE CHECKS PASSED")
    sys.exit(1 if acc.failures else 0)


if __name__ == "__main__":
    main()
```

Add `report-acceptance.html` to the repository's `.gitignore` (append the line `report-acceptance.html`).

Run: `engine/.venv/bin/python scripts/report_acceptance.py --help`
Expected: the usage text.

- [ ] **Step 2: Deploy, run the engine once, grant, restart the app**

```bash
VARS=(--var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 \
      --var warehouse_id=0123456789abcdef --var fixer_sp=00000000-0000-4000-8000-0000000000f1)
databricks bundle deploy "${VARS[@]}" --profile my-profile
databricks bundle run lakebase_health_engine "${VARS[@]}" --profile my-profile   # creates inventory_* and audit_events
PROFILE=my-profile CATALOG=my_catalog ENGINE_SP=00000000-0000-4000-8000-0000000000e1 \
  FIXER_SP=00000000-0000-4000-8000-0000000000f1 APP_SP=00000000-0000-4000-8000-0000000000b1 WAREHOUSE=0123456789abcdef \
  bash scripts/fixer_grants.sh
databricks bundle run lakebase_health_app "${VARS[@]}" --profile my-profile
```

Expected: deploy succeeds; the engine run is `SUCCESS`; the grants script prints `grants done`; the app is `RUNNING`. Confirm the service principal ids first (`databricks apps get lakebase-health -o json` field `service_principal_client_id` for the app; the fixer and engine ids are the notebook's `SERVICE_ROLES`). If the engine run's `errors` contain `pg inventory`, read them before going on.

- [ ] **Step 3: Inventory and new checks on the warehouse**

```bash
databricks api post /api/2.0/sql/statements --profile my-profile --json '{"warehouse_id": "0123456789abcdef",
  "wait_timeout": "50s", "statement": "SELECT (SELECT count(*) FROM my_catalog.lakebase_health.inventory_tables) AS t, (SELECT count(*) FROM my_catalog.lakebase_health.inventory_endpoint WHERE metric LIKE '\''source:%'\'') AS s, (SELECT count(*) FROM my_catalog.lakebase_health.check_results WHERE check_id IN ('\''wal_burst'\'', '\''getpage_latency_spike'\'', '\''autovacuum_saturation'\'', '\''synced_full_refresh'\'') AND status = '\''error'\'') AS broken"}'
```

Expected: `t` > 0, `s` ≥ 8, `broken` = 0. A `broken` > 0 means one of the new telemetry checks' SQL failed on the warehouse: read `check_results.error`, fix the YAML, add the case to `test_report_checks.py`, redeploy. Then look at the `source:*` rows for healthcheck's primary: `source:neon_perf_counters` should read `not available: no SELECT on neon_perf_counters` and the four neon functions 1 (the controller's live check, 2026-10-29).

- [ ] **Step 4: Live query test**

Run: `cd app && npm run test:live`
Expected: all live tests pass, including "report queries run on healthcheck's default branch within 5 s each". A query over 5 s: look at its plan on the warehouse; do not raise the limit.

- [ ] **Step 5: Criteria 1, 2, 3, 6, 7 and 8**

Run: `TEST_SP_SECRET=$(cat /tmp/test_sp_secret) engine/.venv/bin/python scripts/report_acceptance.py --profile my-profile --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 --test-sp-id $(cat /tmp/test_sp_app) --test-sp-secret-env TEST_SP_SECRET --steps 1,2,3,6,7,8`
Expected: every line `[PASS]`. Then open `report-acceptance.html` with the network off (for example in a browser with networking disabled): it renders fully, charts included, and the browser's network panel shows no request.

- [ ] **Step 6: Criterion 4 (idle client past the suspend timeout)**

Run the same command with `--steps 4`.
Expected: `[PASS] 4: suspend_blockers for lbh-acceptance-idle`. If it prints `NOT TESTABLE: scale to zero is off`, stop and ask the user whether to turn scale to zero on for healthcheck's primary (do not change it unasked). If it prints `NOT TESTABLE YET: Postgres up N h`, re-run after the remaining hours; the step holds one idle session for the suspend timeout plus a minute and starts one analysis run.

- [ ] **Step 7: Criterion 5 (synced table): ask the user before creating the synced table**

Ask the user, with this exact proposal, and wait for a yes: create the Delta table `my_catalog.lbh_acceptance.synced_orders_src` (2,000 rows), a SNAPSHOT synced table `my_catalog.lbh_acceptance.synced_orders` into healthcheck `production` / `databricks_postgres` (a new pipeline in `my_catalog.lbh_acceptance`), and one secondary index on it; one full refresh will run during the step. Only after the user agrees:

```sql
-- on the warehouse
CREATE SCHEMA IF NOT EXISTS my_catalog.lbh_acceptance;
CREATE OR REPLACE TABLE my_catalog.lbh_acceptance.synced_orders_src
  TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
SELECT id, id % 50 AS customer_id, concat('note ', id) AS note FROM range(1, 2001) AS t(id);
```

```bash
databricks postgres create-synced-table my_catalog.lbh_acceptance.synced_orders --profile my-profile --json '{
  "spec": {"branch": "projects/healthcheck/branches/production", "postgres_database": "databricks_postgres",
           "source_table_full_name": "my_catalog.lbh_acceptance.synced_orders_src", "primary_key_columns": ["id"],
           "scheduling_policy": "SNAPSHOT", "create_database_objects_if_missing": true,
           "new_pipeline_spec": {"storage_catalog": "my_catalog", "storage_schema": "lbh_acceptance"}}}'
databricks postgres get-synced-table synced_tables/my_catalog.lbh_acceptance.synced_orders --profile my-profile -o json \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("pipeline_id") or d.get("status", {}).get("pipeline_id"))'
PROFILE=my-profile EP=projects/healthcheck/branches/production/endpoints/primary PGAPPNAME=lakebase-health-notebook \
  scripts/pg.sh -c 'CREATE INDEX synced_orders_customer_idx ON lbh_acceptance.synced_orders (customer_id);'
```

Then check what the engine will see as synced storage (the same SQL as `synced.SYNCED_MAP_SQL`):

```bash
PROFILE=my-profile EP=projects/healthcheck/branches/production/endpoints/primary PGAPPNAME=lakebase-health-notebook \
  scripts/pg.sh -At -c "$(engine/.venv/bin/python -c 'import sys; sys.path.insert(0, "engine/src"); from lakebase_health.synced import SYNCED_MAP_SQL; print(SYNCED_MAP_SQL)')"
```

Expected: rows naming `lbh_acceptance.synced_orders` (its storage partitions under `__db_system`, or the table itself if the pipeline role owns it) and the index. If nothing maps, or `CREATE INDEX` is refused, stop and report to the user with the output of `\d+ lbh_acceptance.synced_orders`: the storage layout differs from the spec's assumption and `synced.py` needs their decision. `synced_index_review` also waits for 24 h of statistics (Task 2).

Run: the acceptance command with `--steps 5 --synced-pg-table lbh_acceptance.synced_orders --synced-pipeline <pipeline id>`
Expected: `[PASS]` for the storage naming, the `synced_index_review` finding, no fix action, no DROP INDEX action on synced storage, the refresh `COMPLETED` and the `synced_full_refresh` finding (warning if it overlapped a getpage spike, info otherwise).

Afterwards ask the user whether to keep or delete the synced table, its pipeline and the source table; delete only on a yes (`databricks postgres delete-synced-table synced_tables/my_catalog.lbh_acceptance.synced_orders --profile my-profile`, then `DROP TABLE my_catalog.lbh_acceptance.synced_orders_src`).

- [ ] **Step 8: Commit**

```bash
git add scripts/report_acceptance.py .gitignore
git commit -m "test(report): acceptance for the health report on healthcheck" \
  -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

Report the PASS/FAIL lines of every criterion, the neon `source:*` rows seen, and anything left "not testable" with its reason.


---

## Self-review

**1. Spec coverage.**

| Spec | Task |
|---|---|
| §1 six new checks, synced helper, inventory collector, report page and export | 3, 4, 7, 8, 9 (checks); 3 (helper); 5, 6 (collector); 16 (page); 14, 15 (export) |
| §2 units: `pg_inventory.py`, `synced.py`, check `area`, `server/report/*.ts` (`score`, `runbook`, `strip`), `queries/report_*.sql`, `client/src/report/*`, the report route | 5; 3; 1; 11, 12, 14; 12; 14; 13, 15 (the route lives in `routes.ts` and `handlers.ts` like every other route, not in a new `routes/report.ts`) |
| §3 six inventory tables, keys, latest per endpoint, 7-day cleanup, `inventory_io` with deltas and null times when timing is off; check YAML `area` | 5 (+ `inventory_endpoint`, `audit_events`), 12 (`ioOf` deltas, latest snapshot), 1 |
| §4 checks, thresholds in YAML; shared rules (synced naming and no DROP, stats-reset guard, telemetry fallback) | 3, 4, 7, 8, 9; 3; 2; decision "Telemetry fallback" and Tasks 8, 9 |
| §5 areas, scores, not assessed, grade, project-level checks; runbook order, priority, item contents, query ID instead of text | 1, 11; 11 (`runbook`, `safeSql`) |
| §6 tabs and URL, branch picker, header, KPI tiles and explained cards, each tab's content; export (strip, static render, inline CSS and SVG, no scripts or URLs, print CSS, file name, audit row); in-app text rules; access 404; app SP grants | 16, 14; 14, 15; existing `/queries` lookup (16); 13, 15; 17 |
| §7 not enrolled, stale or missing inventory, no telemetry, one section failing, suspended compute | 12 (banners, per-section errors), 11 (not assessed), 14 ("Section unavailable"), 5 (no wake, no partial snapshot) |
| §8 engine, app and query tests; acceptance 1–8 | every task's tests; 12 (live queries); 18 |
| §9 delivery | header (branch from `main` after the LLM merge) |

Not built, by the spec's own scope: stored snapshots, report comparison, PDF on the server, whole-project reports, LLM summaries, CSV.

**2. Placeholder scan.** No "TBD", "TODO", "similar to Task N" or prose-only code steps; every code step has the full file or the exact text it replaces. The only operator-supplied values are the synced table's pipeline id (printed by the command before it) and the test service principal files, both produced by earlier commands.

**3. Type consistency.** Checked across tasks: `AREAS` ids (engine `checkspec.AREAS`, `checks.json`, app `AreaId` and `catalog.AREAS`) are the same 12 strings in the same order; `skipped:stats_young` and its `error` text are produced in Task 2 and read in Tasks 11 and 12; `inventory_endpoint` metric names written in Task 5 are the ones read in Tasks 7 and 12 (`autovacuum_workers_busy`, `autovacuum_max_workers`, `oldest_xact_s`, `uptime_s`, `server_version`, `idle_clients`, `neon.perf.getpage_wait_seconds_bucket`, `source:*`); `buildReport` returns the `Report` shape of `types.ts`, which `tabs.tsx`, `Document.tsx` and `Report.tsx` render; `reportScope` (Task 13) is reused by `reportExport` (Task 15); `ActionsView`'s new `embedded` prop is optional, so existing callers are unchanged.

**4. Review Focus.** Each of the five lines has its tests in the owning task (Tasks 2, 5, 10, 11, 12 and 14). Also covered by tests but left out of the five: a hidden branch (Task 13), an export when the audit insert fails (Task 15), synced indexes never dropped (Task 3), a scale-to-zero-off endpoint (Task 4).

## Rulings on spec ambiguities

- **Delivery base.** Ruling: cut `feature/health-report` from `main` after `feature/llm-rewrites` merges, not from `feature/fix-loop` (spec §9). Why: both are in `main` by then and the LLM work touches the same files. Cost if wrong: none for the code; a rebase if the LLM branch changes after merge.
- **Telemetry fallback.** Ruling: WAL and full-page images from `pg_stat_statements_counters`, getpage from `plan_history`; `neon_perf_counters` only as an extra endpoint-level line when granted. Why: `compute_counters` has none of the three (checked), and 15-minute Postgres samples cannot see a 5-minute burst. Cost if wrong: WAL written outside statements (checkpoints, autovacuum) is not counted, so a burst caused by them is missed; getpage p99 reflects sampled queries only.
- **Extra table `inventory_endpoint`.** Ruling: one key/value snapshot table for endpoint scalars and source availability. Why: `autovacuum_saturation` needs 3 runs of worker counts, and the neon and idle-client values have no home in the six listed tables. Cost if wrong: one more table to grant and clean (done in the same code paths).
- **"Do not evaluate" for counter checks.** Ruling: skip with `skipped:stats_young`; findings are frozen, not resolved; one young database skips the endpoint. Why: resolving on young counters would close real findings after every scale-to-zero resume. Cost if wrong: a finding that truly went away stays open until the counters are 24 h old.
- **Guard on a telemetry check (`seq_scan_read_ratio`).** Ruling: "telemetry covers at least 20 of the 24 hours". Why: its counters are platform deltas that a reset does not distort; coverage is what matters. Cost if wrong: a database with exactly 20–24 h of history after a long suspend is judged on a short window.
- **Open findings of checks that did not run.** Ruling: they still count toward their area's score when the area is assessed by its other checks, and appear in the runbook; an area none of whose checks ran is not assessed even with open findings. Why: those findings are the last known state; the spec defines "not assessed" by checks run, not findings. Cost if wrong: an area can score lower than this run's own evidence supports.
- **Which checks "count"** in a not-enrolled branch's areas. Ruling: Postgres checks run where their status row for the branch's read-write endpoint is `ok`; telemetry checks where the fleet row is `ok` and the branch had telemetry in 24 h; `ok_stale_source` is not assessed. Cost if wrong: telemetry areas show "not assessed" while ingest catches up.
- **Areas for the engine's meta checks** (`check_broken`, `extension_missing`, `fix_regressed`). Ruling: no area; they appear in the runbook under "Engine" and never in the score. Why: they describe the engine, not the database. Cost if wrong: a critical `fix_regressed` does not lower the grade.
- **"Verified fix action exists"** in the runbook order. Ruling: `verified` ranks above `proposed`/`verifying`/`applying`, which rank above none. Cost if wrong: order among equal-severity items differs slightly.
- **"Impact".** Ruling: `(measure − threshold) / max(|threshold|, 1)` in the check's direction, threshold of the finding's severity. Why: comparable across checks with different units. Cost if wrong: order within a severity band.
- **"Support-managed note".** Ruling: shown instead of SQL for settings findings on `track_io_timing`, `log_min_duration_statement`, `shared_preload_libraries`, `autovacuum_max_workers`, `max_connections`, `shared_buffers`; other findings without SQL say "No SQL fix: follow the guidance on the finding page". Cost if wrong: a customer-changeable setting is labelled support-managed.
- **How to change a setting.** Ruling: `user` context → session; the support-managed list or `postmaster`/`sighup`/`internal` → support-managed; anything else → role or database. Cost if wrong: one column of the Settings tab is off for some superuser settings.
- **Exported tabs and order.** Ruling: the tabs in tab order minus Waits & Locks and Fixes (Queries exports its statement tables, IDs only). Why: "Health, Overview, Runbook and inventory tabs … in the same order" reads as the page's order; the two left out are interactive or per-user. Cost if wrong: the file's section order differs from what a reader expected.
- **Inventory panels for a branch with replicas.** Ruling: the read-write endpoint only. Cost if wrong: replica-only `pg_stat_io` and statements are not in the report.
- **"Inventory panels replaced by enrolment steps"** (§7, not enrolled). Ruling: a banner with the grant to run, and empty panels saying when inventory appears. Why: one place for the instruction instead of eleven copies. Cost if wrong: cosmetic.
- **Branch alias.** Ruling: `/api/report/:id/default` resolves the default branch so `/projects/:id` needs one request. Cost if wrong: none.
- **Synced storage detection.** Ruling: `__db_system` partitions whose `pg_partition_root()` is another relation, or tables owned by `databricks_writer_<n>`. Why: the spec names the first; the second covers unpartitioned synced tables. Cost if wrong: synced indexes are treated as ordinary ones (Task 18, Step 7 stops if nothing maps, before any finding is trusted).
- **`synced_full_refresh` window.** Ruling: any refresh in the last 24 h of inventory windows; a new storage partition counts from zero. Why: a refresh may create new partitions, and a finding should outlive the 15-minute window it happened in. Cost if wrong: a finding stays open up to 24 h after one refresh.
- **WAL burst "names the COPY or index build".** Ruling: name the statement (query id) that wrote the most WAL in the worst burst instead of `pg_stat_progress_*`. Why: a 15-minute snapshot of a progress view rarely catches a 5-minute burst; the statement id is exact. Cost if wrong: the reader looks up the id in the Queries tab instead of seeing "COPY".
- **Exports fail closed on audit.** Ruling: no audit row, no file (502). Cost if wrong: a download fails while the audit table is unwritable.
