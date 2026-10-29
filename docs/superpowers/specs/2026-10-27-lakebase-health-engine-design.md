# Lakebase Health — Engine (sub-project 1) Design

**Date:** 2026-10-27  **Status:** Draft for review  **Related:** `DESIGN.md` (product design, test results), `HEALTH_CHECKS.md` (tested check catalog)

## 1. Goal and scope

A self-serve, Lakebase-native health-check **engine** that, once installed as a Databricks Asset Bundle, produces a ranked, evidence-backed list of findings across all Lakebase projects in the account, with candidate fixes. It **proposes** actions and records shadow-mode decisions, but **never changes a customer database**.

The product has four sub-projects, built in order. This spec covers only #1:

| # | Sub-project | Depends on |
|---|---|---|
| **1** | **Engine:** ingestion, inventory, check runner, findings/actions/shadow/ledger tables | — |
| 2 | Fix loop: branch verification, trust-level policy gate, approval, apply, rollback, 24 h post-apply check | 1 |
| 3 | App: fleet view, finding detail, approval cards, ledger history | 1, 2 |
| 4 | LLM layer: rewrite and `pg_hint_plan` drafting through Unity Gateway; candidates go through the fix loop | 1, 2 |

**Out of scope for the engine:** executing any fix, branch creation, UI, LLM calls, alert channels (findings tables are queryable by SQL alerts; dedicated alerting comes with the app).

### Decisions this spec builds on

| Topic | Decision |
|---|---|
| Audience | Self-serve product for any Lakebase customer |
| Telemetry access | Service principal (SP) reads `system.lakebase` and is a member of `databricks_pii_access` |
| Write actions | Allowed later (fix loop), only with approval; **the engine is read-only** |
| LLM | May draft rewrites and hints (sub-project 4), via **Unity Gateway** (`/ai-gateway/mlflow/v1`, OpenAI-compatible; model service in UC). Client must accept string **and** list-of-blocks `content` |
| pg_sage | Borrow **ideas** (trust levels, shadow mode, verification ledger, extra rules). **No code is copied** (pg_sage is AGPL-3.0) |
| Telemetry read path | Never query `system.lakebase` live from checks; copy incrementally into Delta. Measured: checks on the copy ran in 2–3 s instead of ~23 min. Batch `INSERT … WHERE ts > max(ts)`, **one table at a time** (parallel statements made compile time ~10× worse; streaming was 4–5× slower) |
| Fix verification | Copy-on-write branches (HypoPG is not available on Lakebase) — used by the fix loop |
| Wake policy | Postgres-side checks run **only on computes that are already active**; never wake a suspended compute |
| Coverage | Telemetry and API checks: **all projects** in the account. Postgres checks: only projects where the SP has been granted a Postgres role (**enrolled**) |
| Cadence | Configurable bundle variable, **default 60 min**; documented floor ≈ 20 min (ingest time) |

## 2. Architecture

Approach: a Python package `lakebase_health`, run by one scheduled Lakeflow Job, deployed with a DABs bundle. (Rejected: SQL-only pipeline — cannot reach Postgres or the API; engine inside the app — hits the system-table compile cost and has no history when idle.)

```
system.lakebase.* ──► [1 ingest] ──► <catalog>.lakebase_health.raw_<table>  (10 tables, clustered by project_id, ts)
Lakebase API ───────► [2 discover] ─► inventory (projects, branches, endpoints, active?, enrolled?, settings)
                                         │
       raw_* ──────────┐                  │
       Postgres (active + enrolled) ──► [3 check runner] ◄── checks/*.yaml
       Lakebase API ───┘                  │
                                         ▼
                              [4 record] findings · proposed_actions · shadow_log · ledger · runs
```

| Module | Responsibility | Depends on |
|---|---|---|
| `ingest` | For each source table, in turn: `INSERT … WHERE ts > max(ts)`; 7-day backfill on first run; updates `ingest_watermarks` | Spark SQL |
| `inventory` | SDK listing of projects/branches/endpoints with `current_state`, CU range, suspend timeout, HA group, restore window, protection, role auth methods; marks `enrolled` when the SP can connect | Databricks SDK |
| `checks` | Loads and validates check files; dispatches to executors; returns typed results | `executors/*` |
| `executors/delta` | Runs telemetry checks on `raw_*` (system table names rewritten to the copy), ANSI mode on | Spark SQL |
| `executors/postgres` | Fresh OAuth token per connection; extension schemas on `search_path`; read-only session settings (§5); active + enrolled computes only | psycopg 3, SDK |
| `executors/api` | Backup, HA, branch protection, role auth, settings checks from SDK objects | Databricks SDK |
| `record` | Fingerprints results; `MERGE` into `findings`; writes `proposed_actions`, `shadow_log`, `ledger`, `metrics_daily` | Spark SQL |
| `jobs/run.py` | Orchestrates the four steps; per-unit error isolation | all |

The bundle deploys the job (schedule variable), the schema in an install-time catalog, the SP grants, and the Unity Catalog column masks.

Expected run time: ≈ 20 min ingest + seconds for telemetry checks + ~1–2 s per active enrolled project.

## 3. Check definitions

One YAML file per check, `checks/<category>/<id>.yaml`:

```yaml
id: missing_fk_index
title: "Foreign key without a supporting index"
category: indexing
source: postgres            # delta | postgres | api
schedule: every_run          # every_run | daily
sql: |                       # parameterized; {exclusions} and :project_id injected by the runner
  SELECT ... FROM pg_constraint c ...
object_key: [child_table, conname]      # → fingerprint = hash(check id, project, endpoint, object_key)
measure: child_size_bytes
thresholds: {warn: 0, crit: 1073741824}
guards:                                 # if any fails, confidence = low
  - stats_window_hours >= 24
explain: "Without an index on the referencing columns, every DELETE/UPDATE on the parent scans the child…"
fix:
  kind: ddl                              # ddl | setting | rewrite | hint | manual
  template: "CREATE INDEX CONCURRENTLY ON {child_table} ({fk_columns});"
  rollback: "DROP INDEX CONCURRENTLY IF EXISTS {index_name};"
  risk: moderate                         # safe | moderate | high
  reversible: true
  verify_on_branch: true
```

- **Shared exclusions** (`checks/exclusions.yaml`), referenced by every check: slots `wal_retention_slot`, `wal_proposer_slot`; roles `cloud_admin`, `databricks_*`; settings `databricks.*`, `neon.*`, `neon_*`, `compute_ctl.*`; platform schemas.
- **Risk tiers** — *safe*: `ANALYZE`, resetting a per-table setting; *moderate*: `CREATE INDEX CONCURRENTLY`, `ALTER ROLE … SET work_mem`; *high*: `DROP INDEX`, `VACUUM FULL`, column type changes. Irreversible operations (drop slot, drop schema/table) are never proposed as actions; they appear only as `fix.kind: manual` guidance.
- **Initial rule set (36):** the 31 tested checks in `HEALTH_CHECKS.md` (one file each; check 30's guardrail settings stay one check that emits one result per setting, so each setting has its own fingerprint), plus five reimplemented from pg_sage's ideas:
  1. never-analyzed / stale-statistics tables
  2. sorts reading far more rows than their `LIMIT` returns
  3. query-level `work_mem` advice for spilling queries
  4. privilege review (CREATE on schemas, BYPASSRLS, grants to PUBLIC)
  5. "autovacuum falling behind" linking dead tuples to the specific blocker (long transaction, slot, disabled autovacuum)

### 3a. Addendum (2026-10-28): incident checks from a customer escalation

A customer saw writes stall for 5–50 s, statement timeouts while locking a row, slow writes from GIN
pending-list merges, an unused 22 GB trigram index, and SSL EOF errors on a read replica. The rule set grows by
seven checks, and `unused_index` changes:

| Check | Source | Detects |
|---|---|---|
| `commit_quorum_wait` | `active_session_history` | Commits stalled in `SyncRep` (WAL storage quorum); platform side |
| `hot_row_lock` | `active_session_history` | Row-lock waits per query, with the blocker classified by joining its sample at the same instant: `commit_wait` (platform), `idle_in_transaction` (application), `active` (hot-key contention) |
| `statement_timeouts` | `postgres_logs` | SQLSTATE 57014 per endpoint, with how many were waiting for a row lock |
| `gin_cleanup_lock_wait` | `wait_event_counters` | Time writers spent queued on the GIN pending-list cleanup (page) lock; session history does not sample these waits |
| `connection_drops` | `postgres_logs` | SSL EOF, resets and lost clients per compute (primary vs. replica), alongside compute restarts |
| `gin_pending_list` | Postgres (`pgstattuple`) | Pending list fill vs. `gin_pending_list_limit`, fastupdate, index usage |
| `oversized_search_index` | Postgres | GIN/GiST indexes ≥ 1 GB, with scoping guidance |

**`unused_index` across computes.** Index usage counters are per compute, so an index can be unused on the
primary and serve every replica read. Postgres checks may set `replica_check: intersect`: the runner then runs
the check on each active, enrolled read replica of the same branch and keeps only objects that every replica also
reports. A replica that is suspended, unreachable, not enrolled or within its statistics guard window makes the
remaining results low confidence (`replicas_unverified` in the evidence). Other Postgres checks still run only on
read-write computes.

The session-history incident checks look back 24 hours so intermittent stalls do not resolve between hourly runs.

### 3b. Addendum (2026-10-28): noise and duplicate warnings

Spec fields added to keep one warning per problem (evidence and counts:
`.superpowers/sdd/2026-10-28-lakebase-health-fix-loop/noise-dedupe-report.md`):

- **`per_database: false`** (Postgres checks). The check reads instance-wide catalogs (roles, sessions,
  replication slots, `pg_db_role_setting`, prepared transactions), so it runs once per endpoint and its object has
  no `db:` prefix. Set on `privileged_roles`, `config_drift`, `long_transaction`, `idle_in_transaction`,
  `blocked_session`, `prepared_transaction`, `replication_slot_retention`, `xmin_horizon_holder` and
  `non_ssl_sessions`. Previously each ran in every database and reported the same role or session once per
  database.
- **`query_scoped: true`** (delta checks over per-query telemetry). The delta executor reads `plan_history`
  without platform and engine roles and application names (`query_users`, `query_user_regex`, `query_apps` in
  `exclusions.yaml`), and reads `pg_stat_statements_counters` and `active_session_history` without the role OIDs
  those names map to on the same branch. The job adds its own identity when it is a service principal, plus any
  `--service-roles` (engine and fixer application IDs). Per-query results carry `evidence.queryid` as a string so
  the app can group the query checks by query. The checks themselves are not merged: there is still one finding
  per (check, endpoint, queryid).
- **`title_template`**: a per-result title rendered from the row, for example
  `Row estimates far off for {queries} queries`.
- **`superseded_by: {key: object | database | endpoint, checks: [...]}`**. While one of the named checks has an
  open finding for the same key (same endpoint and object, same endpoint and database, or same endpoint) that is
  at least as severe, the result is not reported. The superseding result lists it under `evidence.supersedes`
  (check, fingerprint, measure, evidence). An already-open lower finding gets status `superseded` with
  `evidence.superseded_by`: its fingerprint, history, suppression and proposed action are kept, it is left out of
  open counts and not acted on by the fixer, and it returns to open under the same fingerprint once the higher
  finding resolves (or resolves itself after its clean runs). `load_checks` rejects unknown ids and cycles.

`row_misestimate` is one finding per endpoint. Its measure is the share of the endpoint's DB time spent in
qualifying queries, and the worst 10 queries are listed by query ID only. The floors and their justification are
in the check's YAML.

## 4. Data model (`<catalog>.lakebase_health`)

| Table | Grain | Key columns | Writer |
|---|---|---|---|
| `raw_<table>` (10) | source row | same columns as `system.lakebase.*`; clustered `(project_id, ts)` | ingest (append) |
| `ingest_watermarks` | source table | `source_table`, `max_ts`, `last_run_id`, `rows_added`, `seconds` | ingest |
| `inventory` | run × endpoint | project uid/name, branch, endpoint, type, `current_state`, CU range, suspend timeout, HA group, restore window, protected, `enrolled`, `last_pg_check_at` | discover |
| `runs` | run | `run_id`, start/end, status, per-step timings, counts, `errors` | run |
| `check_results` | run × check × object | `run_id`, `check_id`, `fingerprint`, project/endpoint, `measure`, `severity`, `confidence`, `status` (`ok`, `ok_stale_source`, `skipped:<reason>` where reason ∈ `compute_inactive`/`not_enrolled`/`extension_missing`/`opted_out`/`not_scheduled`, `error`, `timeout`), `evidence` JSON | check runner (append) |
| `findings` | fingerprint (current state) | `fingerprint`, `check_id`, object, `status` (`new`/`open`/`resolved`/`suppressed`), severity, confidence, `first_seen`, `last_seen`, `last_checked_at`, `occurrences`, `evidence`, `explain`, `candidate_sql`, `rollback_sql`, `suppressed_until` | record (`MERGE`) |
| `proposed_actions` | proposed fix | `action_id`, `fingerprint`, kind, sql, rollback, risk, reversible, `verify_on_branch`, `state` (engine writes `proposed`), `expires_at` | record |
| `shadow_log` | action × evaluation | `action_id`, `would_allow_level`, `would_execute`, `reason`, `outcome_score` (filled by fix loop) | record |
| `ledger` | event (append-only) | `event_id`, `action_id`, `event_type`, `actor`, `payload`, `ts`, `prev_hash`, `hash` | record, fix loop |
| `metrics_daily` | day × endpoint × metric | storage, XID age, peak connections, sequence `pct_used` | record |

**Lifecycle:** a finding becomes `resolved` after **2 consecutive runs in which its check ran successfully** for that object's scope and did not report it. Results with status `skipped:*`, `error` or `timeout` never resolve or change a finding; they update only `last_checked_at`/status display.

**Governance:** UC column masks on query-text and plan columns (`raw_plan_history`, `raw_pg_stat_statements_counters`, `raw_postgres_logs`, `raw_ddl_history`, and `findings.evidence` fields holding text) show values only to `databricks_pii_access` members.

**Retention (configurable):** `raw_*`, `check_results` 90 days; `findings`, `ledger`, `shadow_log`, `metrics_daily` indefinite.

## 5. Safety model

- **Read-only engine.** Every Postgres session sets `default_transaction_read_only = on`, `statement_timeout = 30s`, `lock_timeout = 1s`, `application_name = 'lakebase-health'`.
- **Least-privilege role.** The engine connects as a monitoring role with `pg_monitor` + `pg_read_all_stats`, no write grants. The fix loop will use a separate role.
- **Expensive checks bounded.** `pgstattuple` bloat checks: `schedule: daily`, top 15 tables by size, 60 s budget per project.
- **Trust levels (per project), adapted from pg_sage:**

  | Level | Meaning | Implemented in |
  |---|---|---|
  | L0 Observe | findings only (default) | engine |
  | L1 Script | proposed action + SQL + rollback, human-run | engine |
  | L2 Approve | one-click apply after successful branch verification | fix loop |
  | L3 Unattended | auto-apply *safe* actions after a proven track record | fix loop |

- **Shadow mode from day one.** For each proposed action the engine records what an L3 policy would have done and why; the fix loop later scores these. This evidence gates enabling L2/L3.
- **Off switches.** Bundle variable `engine_enabled`; per-project opt-out tag `lakebase_health=off`; every skip is logged in `runs`.
- **No external calls.** The engine calls only Databricks APIs and Lakebase endpoints.

## 6. Error handling

A run fails only if it cannot load the check files or write `runs`. Everything else is isolated per source table, per project and per check, and logged in `runs.errors`.

| Failure | Behaviour |
|---|---|
| Ingest of a table fails or times out | Watermark not advanced (next run retries from it); dependent checks run on existing data with status `ok_stale_source` |
| Compute suspends / connection drops | Project's Postgres checks `skipped:compute_inactive`; no retry; findings keep state |
| OAuth token expiry | New token per project connection |
| Check SQL error (e.g. Beta column rename) | Check `error`; findings unchanged; 3 consecutive runs → meta-finding `check_broken` |
| Statement timeout | `timeout`; findings unchanged |
| Extension missing | Dependent checks `skipped:extension_missing`; info finding with enablement steps |
| SP not enrolled on a project | Postgres checks `skipped:not_enrolled`; inventory shows how to grant access |
| Overlapping runs | Job `max_concurrent_runs = 1` |

All checks are written for ANSI/strict SQL (`NULLIF`, `try_*`, no unguarded array indexing).

## 7. Testing and acceptance

**Unit (pytest, local):** check-file validation (fields, risk, rollback when reversible, exclusions referenced); fingerprint stability and no cross-endpoint collisions; lifecycle (new → open → resolved after 2 clean runs; skipped/error never resolves; suppression expiry); ledger hash chain verify + tamper detection; executors mocked.

**Integration (test project `healthcheck` on `my-profile`, seeded by `scripts/seed_test_workload.sql` + `scripts/workload.py`):**
- *Expected-findings manifest:* all 15 planted problems + the 5 borrowed rules fire from the right check.
- *Must-not-find list* (fails the build): `wal_retention_slot`, `wal_proposer_slot`, `cloud_admin` sessions, `fsync`/`full_page_writes`, objects in the `sage` schema.
- *Strict mode:* telemetry checks under ANSI on a small fixture copy of `raw_*`; one full run on the real copy.

**Acceptance criteria:**
1. All planted problems detected; zero must-not-find hits.
2. Full run completes in < 30 min on `my-profile`.
3. Immediate re-run creates no duplicate findings or actions.
4. A suspended compute stays suspended (`compute_last_active_time` unchanged across a run).
5. After manually fixing one planted problem, its finding is `resolved` within 2 runs; others unchanged.
6. The engine role has no write privileges, and its sessions reject writes.

## 8. Open items (not blocking the engine)

- Positioning vs. Insights/Genie in the app (complement vs. wrap) — decide in sub-project 3.
- Model service name and default model for the LLM layer — decide in sub-project 4 (proposal: `<catalog>.lakebase_health.advisor_llm` → a `system.ai` Claude model).
- Packaging channel (public repo, Marketplace, internal) and licensing of this product.
