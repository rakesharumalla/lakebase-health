# Lakebase Health — Fix Loop (sub-project 2) Design

**Date:** 2026-10-28  **Status:** Draft for review  **Related:** `2026-10-27-lakebase-health-engine-design.md` (actions, shadow log, ledger, trust levels), `2026-10-28-lakebase-health-app-design.md` (app, access model), `scripts/verify_on_branch.py` (prototype)

## 1. Goal and scope

Turn the engine's proposed actions into verified, approved, applied and checked fixes. A project owner or manager
approves in the app. A separate fixer identity verifies the change on a copy-on-write branch, applies it to the
target branch only after a second approval, and the engine's next runs confirm the result or flag a regression,
which leads to an approved rollback.

**In scope (release 1):** `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, `ANALYZE`, and per-table
autovacuum reloptions (`ALTER TABLE … SET|RESET (autovacuum_*)`).

**Out of scope:** role/database settings, unattended (L3) apply (shadow mode continues), query rewrites and hints
(LLM layer, sub-project 4), irreversible operations (never proposed as actions).

### Decisions this spec builds on

| Topic | Decision |
|---|---|
| Who approves | The project owner, or users with `CAN_MANAGE` on the Lakebase project, in the app, checked with their own token |
| Who executes | A separate **fixer service principal**, enrolled per project with only the privileges fixes need; the engine stays read-only |
| Fix kinds | Index create/drop, `ANALYZE`, autovacuum table settings |
| Unattended | None: every verify, apply and rollback needs an approval; L3 stays shadow-only and is scored |
| Regression | Post-apply check marks `regressed`, raises a critical finding and a rollback card that needs approval |
| Approach | Fixer job + approval records (rejected: app executes DDL in web requests; engine executes in its hourly run) |

## 2. Architecture

```
App (approval cards) ──INSERT──► <catalog>.lakebase_health.approvals   (request: verify | apply | rollback | reject)
        │                                  │
        └── Jobs API: run now ──► lakebase-health-fixer job (runs as fixer SP; also every 15 min)
                                           │
              ┌────────────────────────────┼─────────────────────────────┐
              ▼                            ▼                             ▼
     verify on branch              apply on target branch          rollback on target
  (CoW branch, TTL 4 h)         (CONCURRENTLY, lock_timeout)     (stored rollback SQL)
              │                            │                             │
              └──────── action_runs + ledger events + proposed_actions.state ───────┘
                                           ▲
                      engine hourly run: post-apply check (2 runs) → confirmed | regressed
```

| Unit | Responsibility |
|---|---|
| `lakebase_health/fixer/queue.py` | Reads new approval rows; claims an action with a compare-and-set on its state |
| `lakebase_health/fixer/allowlist.py` | Parses action SQL; accepts only the four statement kinds on a single object |
| `lakebase_health/fixer/branch.py` | Creates `lbh-verify-<action8>` branches (TTL 4 h, 1–2 CU, scale to zero) and deletes them |
| `lakebase_health/fixer/verify.py` | Before/after measurements of affected queries on the branch; pass rules per kind; report |
| `lakebase_health/fixer/apply.py` | Applies or rolls back on the target branch with session guards; recovers interrupted applies |
| `lakebase_health/fixer/postcheck.py` | Rules for `confirmed` / `regressed`, called from the engine run |
| `lakebase_health/jobs/fix.py` | Fixer job entry point (sweep + run-now) |
| App: approval cards, Actions view | Buttons for verify / apply / reject / rollback, reports, state history |

The engine package gains only `postcheck` (read-only); the fixer job is a separate job with `run_as` the fixer SP.

### Action states

`proposed` → (verify request) `verifying` → `verified` | `verify_failed` → (apply request) `applying` → `applied`
| `apply_failed` → (post-apply check) `confirmed` | `regressed` → (rollback request) `rolling_back` → `rolled_back`
| `rollback_failed`.

Also `rejected` (approver declines), `expired` (7-day TTL or the finding resolved by itself), `blocked` (fixer not
enrolled, missing privileges, or SQL outside the allowlist). Each change is a ledger event. Actions whose spec has
`verify_on_branch: false` (autovacuum settings, `ANALYZE`) may go from `proposed` straight to an apply request; the
card still shows "not verifiable on a branch".

## 3. Data model additions (`<catalog>.lakebase_health`)

| Table | Grain | Key columns | Writer |
|---|---|---|---|
| `approvals` | request (append-only) | `approval_id`, `action_id`, `request` (`verify`/`apply`/`rollback`/`reject`), `sql_hash`, `approver`, `requested_at`, `note` | app SP (INSERT only) |
| `action_runs` | fixer attempt | `run_id`, `action_id`, `kind` (`verify`/`apply`/`rollback`/`postcheck`), `started_at`, `ended_at`, `outcome`, `report` JSON, `branch`, `error` | fixer, engine (postcheck) |

`proposed_actions.state` gains the states above; `shadow_log.outcome_score` is filled (1 confirmed, 0 regressed).
Ledger event types: `verify_requested`, `verify_started`, `verified`, `verify_failed`, `apply_requested`,
`applying`, `applied`, `apply_failed`, `confirmed`, `regressed`, `rollback_requested`, `rolled_back`,
`rollback_failed`, `rejected`, `blocked`, `stale_approval`.

## 4. Authorization and safety

- **Approver check.** The app shows buttons only to the project owner (`status.owner`) or users for whom
  `GET /api/2.0/permissions/database-projects/{uid}` succeeds with their token (only managers can read it). The
  server repeats the check on every click and writes the approver's user name and the SHA-256 of the action SQL
  into the approval row.
- **App write.** The app SP gets `MODIFY` on `approvals` only (INSERT); everything else stays SELECT. The app also
  gets `CAN_MANAGE_RUN` on the fixer job to trigger it.
- **Fixer trust.** It reads approval rows (written only by the app SP), checks that the action's current SQL hash
  equals the approved hash, the action is not expired and its finding is still open; otherwise it logs
  `stale_approval` and ignores the row. It executes only `proposed_actions.sql` / `rollback_sql`, never approval
  text.
- **Allowlist.** Accepted, one object each: `CREATE INDEX CONCURRENTLY …` (non-unique), `DROP INDEX CONCURRENTLY [IF
  EXISTS] …`, `ANALYZE <table>`, `ALTER TABLE <table> SET|RESET (autovacuum_… [, …])`. Anything else → `blocked`.
- **Session guards.** `lock_timeout = 5s`; `statement_timeout` 2 h for index builds and 10 min otherwise
  (configurable); `application_name = 'lakebase-health-fixer'`; one action at a time per project. Apply may wake a
  suspended compute (a person approved it); verification never connects to the target compute.
- **Cleanup.** A failed `CREATE INDEX CONCURRENTLY` leaves an invalid index; the fixer drops it with `DROP INDEX
  CONCURRENTLY IF EXISTS` and records the error.
- **Enrolment.** The owner creates a Postgres role for the fixer SP on the target branch and makes it a member of
  the role that owns the target tables, plus `CAN_MANAGE` on the project (creating the verification branch
  requires it; `CAN_USE` is rejected with PermissionDenied). When a person's login role owns the tables, that role
  cannot be granted (no ADMIN option on itself), so ownership first moves to a NOLOGIN owner role (for example
  `hc_test_owner`) that is granted to both the person and the fixer. Otherwise the action is
  `blocked: fixer not enrolled` and the app shows the steps.
- **Query evidence.** New branches start with empty statistics, so affected queries come from the telemetry copy
  (`raw_plan_history` text and sampled bind parameters, `raw_pg_stat_statements_counters`). The fixer SP is a
  member of `databricks_pii_access`; query text and parameters never leave the job (reports store query IDs and
  timings only).

## 5. Branch verification and post-apply check

- **Branch.** From the target branch's current state; TTL 4 h; 1–2 CU, scale to zero; `lbh-verify-<action8>`;
  deleted when done.
- **Affected queries.** Up to 10 query IDs touching the action's table, ranked by 24 h total time, each with up to 3
  sampled parameter sets, taken only from single SELECT/WITH/DML statements. Each read runs 3 times warm before and
  after (median), with `EXPLAIN (ANALYZE, BUFFERS)` inside a transaction that is rolled back. The write probe (the
  sampled INSERT/UPDATE/DELETE statements, rolled back) runs 3 warm rounds per side, alternating between the write
  statements in each round, and takes each statement's median; the report keeps the three samples per side. The
  write rule judges the bulk INSERT probe and UPDATEs that change a column of the new index, each with a median of
  at least 1 ms. DELETEs (Postgres does not maintain secondary indexes on delete; vacuum does), UPDATEs of other
  columns and sub-millisecond statements are reported in the evidence but not judged.
- **Regressed query.** A query counts as regressed only when (a) its plan changed between before and after, or its
  after plan uses the changed index, and (b) its median after/before ratio is > 1.10, and (c) it is > 1 ms slower.
  Slowdowns with an unchanged plan are reported as a *noise band* with their numbers and never fail (the branch
  compute is cold or shared; unchanged plans drift 10–30 % and sub-millisecond queries swing 2–3×). The post-apply
  check on production still catches a real regression on an unchanged plan.
- **Pass rules.**

  | Kind | Passes when |
  |---|---|
  | `CREATE INDEX` | ≥ 1 affected query's plan uses the new index with median speed-up ≥ 1.2×; no affected read regressed (see above; noise band reported); every judged write (INSERT probe, UPDATE of an indexed column, median ≥ 1 ms) median ≤ 25 % slower; unjudged writes reported; index size reported |
  | `DROP INDEX` | Before the drop no affected plan uses the index; after it no affected query regressed (see above; noise band reported); write-probe gain reported |
  | `ANALYZE` | Runs on the branch; plan changes shown; always passes |
  | autovacuum `SET`/`RESET` | Not verified on a branch (no measurable query effect); report says so |

- **Report** in `action_runs.report`: per-rule pass/fail, per-query before/after timings and plan-node changes,
  write probe, size, branch, duration.
- **Post-apply check** (engine runs, 2 runs after `applied`): `confirmed` when the action's finding is resolved or
  improving, the affected queries' mean time (from `pg_stat_statements_counters`) is at most 10 % worse than the
  24 h before apply, and write wait share has not risen; otherwise `regressed` with the evidence, a critical
  finding `fix_regressed`, and a rollback card. With too little traffic: `confirmed` with low confidence.
  Only runs in which the finding's check finished (`ok`/`ok_stale_source`) for its endpoint count, and the
  current run must be one of them: when the check did not run (compute suspended, check skipped) the action stays
  `applied` and is re-evaluated later; after 7 days without check data it is `confirmed` with low confidence.

## 6. Error handling

| Failure | Behaviour |
|---|---|
| Branch creation fails or hits the 10-branch limit | `verify_failed: branch_unavailable`; retried on the next sweep, up to 3 times |
| A verification query errors | That query's rule fails; the report shows the error, never the query text |
| Lock timeout on apply | `apply_failed: lock_timeout`, nothing changed; card offers "retry apply" |
| Index build fails or times out | Invalid index dropped; `apply_failed` with the error |
| Job crashes mid-apply | Next sweep finds `applying` older than its timeout, inspects the catalog (valid index → `applied`; invalid → cleanup → `apply_failed`) |
| Action changed since approval | Approval ignored; `stale_approval`; card asks to re-review |
| Fixer not enrolled / missing privileges | `blocked` with enrolment steps |
| Two approvals for one action | Compare-and-set claim; the second is a no-op |

## 7. Testing and acceptance

- **Unit (pytest):** state machine (all legal and illegal transitions), allowlist accept/reject cases, claim race,
  stale-approval rules, pass rules on synthetic reports, post-check rules, ledger events.
- **App (Vitest):** approver authorization (manager vs. role holder vs. none), approval row contents, cards per
  state.
- **Integration (healthcheck on my-profile):** the planted problems.

**Acceptance criteria:**
1. Missing FK index (`hc_test.orders.customer_id`): verify passes with the plan using the index; apply creates a
   valid index on production; `confirmed` within 2 engine runs; the finding resolves.
2. Duplicate index: verify shows no plan using it; apply removes it; the finding resolves; `confirmed`.
3. A forced regression (synthetic post-check data) yields `regressed`, a `fix_regressed` finding and a rollback
   card; approving the rollback restores the previous state.
4. A Postgres-role holder who is not a manager sees the cards but cannot approve (server returns 403); the engine
   and app SPs still have no database write privileges; a tampered `proposed_actions.sql` is `blocked`.
5. No `lbh-verify-*` branch remains after verification; every state change has a ledger event and the hash chain
   verifies.

## 8. Open items (not blocking)

- Notifications (email/Slack) when a card needs attention — with the alerting work.
- When L3 can be enabled, and its required shadow-mode track record (for example ≥ 20 confirmed and 0 regressed per
  kind) — a later release.
