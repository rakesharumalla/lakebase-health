# Lakebase Health Check: design options

Goal: give every Lakebase customer a health check that (1) runs deterministic checks, (2) explains findings with expected ranges, and (3) guides or safely automates the fix. The check catalog is in [HEALTH_CHECKS.md](HEALTH_CHECKS.md).

## 1. Constraints that shape the design

| Fact (from the docs) | Design consequence |
|---|---|
| Lakebase is fully managed and serverless. There is no host to install an agent on. | A literal "sidecar running next to Postgres" is not possible. Anything "standalone" runs elsewhere and connects over the network. |
| `system.lakebase.*` keeps **7 days**, is account‑wide, and needs account admin + metastore admin (or grants). Statement text and plans are masked unless the reader is in `databricks_pii_access`. | Read telemetry with an identity that has those grants. Persist rollups to Delta for trends beyond 7d. Design for masked text (key on `queryid`). |
| `pg_stat_*` and `pg_stat_statements` **reset on suspend/restart** (scale‑to‑zero). | Postgres‑side checks need a stats‑window guard and confidence labels. Prefer system tables for anything time‑based. |
| **HypoPG, pg_repack, pg_cron, auto_explain are not available.** pg_hint_plan, pgstattuple, pgrowlocks and pg_prewarm are. | Validate index and rewrite suggestions on a **copy‑on‑write branch**, not hypothetically. Bloat fixes are limited to REINDEX CONCURRENTLY, VACUUM FULL, or swap. |
| **Branches are instant, copy‑on‑write, and have a TTL.** A project allows 10 unarchived branches and 3 root branches. | This is the differentiator: test every proposed fix on real data at no risk to prod, then promote. |
| Built‑in **Insights** (LLM agent, Beta) and **Genie** already diagnose issues from the same telemetry. | Don't rebuild them. Add what they lack: **deterministic, thresholded, auditable checks**, a **fleet view across projects**, **long‑term trends and forecasts**, and **branch‑verified fixes**. Link to "Fix with Genie" where it fits. |
| The Postgres API / SDK (`w.postgres`) exposes restore window, protection, HA group, CU range, roles' auth method and pg_settings. | Backup, security and config checks come from the API. No SQL is needed for them. |

## 2. One check library, many surfaces

Whatever surface we choose, build **one check library** that every surface reuses:

```
lakebase_health/
  checks/
    cpu_utilization.yaml        # id, category, source (system_tables | postgres | api), sql/py,
    lfc_hit_rate.yaml           # thresholds {ok, warn, crit}, explanation, fix template,
    unused_indexes.yaml         # guard conditions (e.g. stats_window >= 24h), confidence rule
    ...
  runners/  system_tables.py  postgres.py  api.py
  model/    Finding(check_id, project, branch, endpoint, object, severity, value, threshold,
                    evidence_json, recommendation, candidate_ddl, rollback_ddl, confidence, run_id, ts)
```

Each check is data, not code: SQL, thresholds, explanation and fix template. Customers and field teams can then tune thresholds and add checks without forking. Outputs land in one `findings` Delta table plus a `metrics_daily` rollup for forecasting.

### 2a. Ingestion layer (required, from testing)

Live testing on 2026‑10‑27 showed that every `system.lakebase` statement spends **200–3,000 s compiling** and about 1 s executing, even `SELECT … LIMIT 1`. Running 16 statements in parallel made compile time worse. Running 33 check statements directly would take 30–50 minutes per project, which is unusable inside an app.

So the product needs an ingestion job in front of the checks:

```
system.lakebase.<10 tables>  ──(1 incremental MERGE/INSERT per table, every 15–60 min,
                                 watermark on ts, all projects at once)──►  <catalog>.lakebase_health.raw_<table>
                                                                                  │  fast: normal Delta tables
                                                                                  ▼
                                               checks (same SQL, table names templated) ──► findings, metrics_daily
```

This design has five properties:
- **One compile per table per run**, about 10 statements per run, instead of 33 per project.
- **Fleet‑wide by default**, because the system tables cover the whole account.
- **History past 7 days**, which the XID, storage and sequence forecasts need.
- The app and dashboards read only from `lakebase_health.*`, so responses take seconds.
- **Access stays narrow.** Only the job's service principal needs `system.lakebase` and `databricks_pii_access`. App users get grants on the derived tables, and the derived tables can keep the masking (store `queryid` plus redacted text) for users without PII access.

The check SQL in HEALTH_CHECKS.md stays the same. The runner swaps `system.lakebase.X` for `lakebase_health.raw_X`.

**Measured on 2026‑10‑27** (`scripts/copy_experiment.py`, `compute_gauges`, Serverless Starter warehouse):

| Step | Time | Notes |
|---|---|---|
| Backfill 7 days, all projects (`CREATE TABLE … CLUSTER BY (project_id, ts) AS SELECT`) | 22 min, once | 118,633 source files read. Result: 14.6M rows, 1,137 projects, **10 files / 313 MB** |
| Check 4 (working set): system table → copy | 1,410 s → **2.7 s** | Same hourly values |
| Check 6 (storage forecast): system table → copy | 1,410 s → **3.0 s** | Same used/limit |
| Check 27 (replica lag): system table → copy | 1,364 s → **2.2 s** | The system‑table run read only 1 file after 1,352 s of compile |
| Incremental append (`ts > max(ts)`) | 4 min | 964 files read, **891,857 still evaluated and skipped**: a time filter doesn't remove the planning cost |

Implications:
1. Never run several system‑table statements in parallel. The backfill compiled in 115 s alone; the same table took about 1,350 s per statement with three running at once.
2. Each ingest run costs about 4 min per heavy table (`compute_gauges`, `compute_counters`, `database_gauges`, `pg_stat_statements_counters`). That's acceptable on a 15–60 min schedule.
   **Streaming tested and rejected** (`notebooks/stream_experiment.py`, serverless, `availableNow`, 3 runs 5 min apart). Streaming works on system tables, but runs took **20.6 / 20.2 / 15.5 min** for 30–61k rows. Writing the rows took 3–17 s. Finding new data took 133–542 s, plus about 130 s to collect source metrics, plus 9–15 min of startup before the first progress report. A long‑running stream on classic compute would pay startup once, but would still spend about 4.5 min per micro‑batch finding new data. (A full notebook run was querying the same tables at the time, so contention may have inflated these numbers.) **Use batch `INSERT … WHERE ts > max(ts)`, one table at a time.**
3. The backfill covers every project in the account, so adding projects costs nearly nothing.

## 3. Options

### Option A: Notebook pack (one‑shot assessment)
A Databricks notebook (or a DABs‑deployable bundle) that takes `project_id` and a profile or secret, runs every check, and renders a scored report: traffic lights, a top‑10 findings list, and candidate DDL.

- **Pros:** a day or two to build. Zero infrastructure. Easy for SAs to run with a customer. Transparent, because the customer can read every query.
- **Cons:** point‑in‑time only. No history beyond 7d. Fixes are manual. Adoption depends on someone remembering to rerun it.
- **Best for:** health assessments, POCs, a pre‑go‑live review, and the first version of the check library.

### Option B: Scheduled job, Delta history, dashboard and alerts ("fleet monitor")
A Lakeflow Job (hourly or daily) runs the check library across **all projects in the account** (system tables are account‑wide). It writes `findings` and `metrics_daily` to Delta, with an AI/BI dashboard on top and SQL alerts on critical findings (Slack, email, PagerDuty). An optional step uses the Foundation Model API to write a weekly LLM summary of fleet health: what changed, top risks, and recommended actions.

- **Pros:** trends past 7 days, so forecasts work (storage, XID, sequences, connections). Fleet‑wide. Uses only native Databricks pieces and is cheap to run. Alerts catch problems before an incident.
- **Cons:** read‑only, so fixes are still manual. Postgres‑side checks need a credential per project (OAuth via SP, generated each run).
- **Best for:** customers with many projects, platform/DBA teams, and ongoing operations.

### Option C: Databricks App ("Lakebase Health Console")
An app (AppKit or FastAPI + React) that reads system tables through a SQL warehouse and connects to each Lakebase endpoint with the app's service principal (Postgres resource binding). It shows findings, drill‑downs (query → plans → waits → locks), and a remediation workflow.

- **Pros:** the best UX. Interactive drill‑down, with per‑user permissions through on‑behalf‑of auth. The natural home for the approval workflow in Option D.
- **Cons:** the most engineering. The app SP needs system‑table grants and Postgres roles in every project. Overlaps the Lakebase UI's Monitoring tab, so it must be clearly better: fleet view, history, verified fixes.
- **Best for:** the productized version.

### Option D: Assisted remediation with branch verification (layer on B + C)
The loop that turns findings into safe changes:

```
 deterministic check ──► candidate fix (DDL / GUC / rewrite / hint)
        │                       │  LLM drafts rewrites & explanation; never decides alone
        ▼                       ▼
   Finding (evidence)     user approves "test on branch"
                                │
                                ▼
      create branch from prod (TTL 4h) + endpoint sized like prod
                                │
      warm-up ► BASELINE: replay top-N affected queries (EXPLAIN ANALYZE, BUFFERS) ×3
                                │
      apply fix on branch ► CANDIDATE: replay same queries ×3
                                │
      measure: latency Δ, buffers Δ, plan change, write-cost Δ (index maintenance on
               DML replay), DDL duration, lock level, size added
                                │
                                ▼
      confidence score + report ──► user approves "apply to prod"
                                │      (CONCURRENTLY, lock_timeout, snapshot first,
                                ▼       rollback DDL stored, change logged)
      post-apply verification window (24h): re-run check, compare to baseline,
      auto-flag regression → suggest rollback
```

**Confidence score (index suggestion example)** combines:
- Measured speedup on the branch (p50/p95 Δ), weighted by the query's share of DB time.
- Plan evidence: the index is actually used, and estimate error dropped.
- Write penalty: extra time on DML replay against the affected table, and added size.
- Data quality of the evidence: stats window, sample count, and whether parameters were available (`plan_history.parameters` needs PII access; without it, use representative values and lower confidence).

**What it covers:** missing and FK indexes, dropping unused or duplicate indexes (verify no plan regresses), `work_mem` per role for spills, extended statistics, pg_hint_plan pins, query rewrites, and per‑table autovacuum settings (verified over the 24h window rather than on a branch).

**Guardrails:**
- Nothing auto‑applies to prod. Every action carries a stored rollback.
- Branch compute is billed, so cap concurrent test branches (the project limit is 10) and use TTLs.
- Destructive or locking fixes (VACUUM FULL, column type change) get a "maintenance window" plan instead of an apply button.

### Option E: Standalone external agent (not recommended as the primary path)
A container or VM outside Databricks that polls Lakebase and pushes metrics to Prometheus, Datadog or Grafana.
- It only makes sense for customers who standardize on an external observability stack. Even then, the cleaner route is to export from Option B's Delta tables or system tables than to run another credentialed poller.
- Lakebase can't co‑locate anything, so this is no closer to the database than a Job.

## 4. Comparison

| | A Notebook | B Job + Dashboard | C App | D Branch‑verified fixes | E External agent |
|---|---|---|---|---|---|
| Time to first value | Days | 1–2 weeks | 3–6 weeks | +3–4 weeks on C | 2–3 weeks |
| History / forecasting | 7d only | Yes | Yes (via B) | Yes | Depends on the stack |
| Fleet‑wide | Manual | Yes | Yes | Yes | Yes |
| Fix guidance | Text + DDL | Text + DDL | Interactive | Verified + apply | No |
| Ops burden | None | Low | Medium | Medium | High |
| Overlap with Insights/Genie | Low | Low | Medium | Low (unique) | Low |

## 5. Decisions (2026-10-27) and resulting plan

| Question | Decision | Consequence |
|---|---|---|
| Audience | **Self-serve product** | Option A becomes an internal test harness only. Ship B + C + D. |
| Access | **SP can read `system.lakebase` and join `databricks_pii_access`** | Full query text, plans and parameters. Fix verification can replay real parameters. |
| Write actions | **May create branches and apply DDL, with approval** | Option D is in scope. Prod apply stays behind an explicit per-change approval. |
| LLM scope | **May draft query rewrites and pg_hint_plan hints** | Rewrites and hints go through the same branch verification as DDL: same results, faster, plan changed as intended. |

Build order:
1. **Ingestion job + check runner + findings tables (Option B core, §2a).** Calibrate thresholds on the `healthcheck` test project and 1–2 design-partner projects.
2. **Dashboard + alerts** on `findings` and `metrics_daily`, plus forecasts (storage, XID, sequences, connections).
3. **App (Option C):** fleet view, project drill-down, finding detail with evidence.
4. **Fix loop (Option D):** a "Test on branch" button runs `verify_on_branch` logic, then an approval, an apply with stored rollback, and a 24h post-apply check. LLM-drafted rewrites and hints enter here as candidates.

Prototype status: `scripts/verify_on_branch.py` already runs the loop for one index. Tested on the FK finding: 72.9 ms → 0.05 ms, plan changed from Seq Scan to Index Only Scan, branch created in about 8 s, production untouched. Its write-penalty measure was noisy (one cold sample gave −33%), so it needs repeated, warmed probes before the confidence score can be trusted.

## 6. Remaining open decisions

1. **Positioning with Insights/Genie:** complement them (deterministic checks, fleet view, verified fixes) or wrap them (surface Insights findings in the same console)?
2. **Model:** which serving endpoint drafts rewrites and hints? Default proposal: Claude on Databricks Foundation Model APIs. Findings are sent as JSON; parameters are included only when the customer opts in.
3. **Packaging:** DABs bundle in a public repo (e.g. databricks-solutions), a Marketplace app, or internal field tooling?
4. **Where the derived tables live:** a catalog the customer chooses at install, or a fixed `lakebase_health` schema?

### 6a. Considered and rejected: OpenTelemetry export into UC instead of API calls (2026-10-28)

Idea: turn on the Lakebase [OpenTelemetry export](https://docs.databricks.com/aws/en/oltp/projects/opentelemetry) (Beta), land it in Delta through [Zerobus Ingest OTLP](https://docs.databricks.com/aws/en/ingestion/opentelemetry/), and read those tables instead of calling the Lakebase API and copying `system.lakebase`.

Rejected, for these reasons:

1. **It does not carry what the API calls provide.** Inventory (6.9 s per run) reads configuration and state: endpoint `current_state`, CU range, suspend timeout, HA, restore window, branch protection, role auth methods and enrolment. The export carries none of these. A suspended compute just stops emitting, so a gap cannot be told apart from a broken export, and the never-wake rule needs the real state.
2. **The metrics duplicate `system.lakebase`.** Exported series (`host_cpu*`, `host_memory*`, `lakebase_connection_counts`, `lakebase_db_total_size`, `lakebase_replication_delay_*`, `lakebase_lfc_*`) are a subset of `compute_counters`, `compute_gauges` and `database_gauges`, which have more columns and cover every project with no setup. Exported logs duplicate `postgres_logs`.
3. **It skips the expensive part of ingest.** Copy times from the first engine run:

   | Table | Copy time | In the export? |
   |---|---|---|
   | `pg_stat_statements_counters` | 665 s | No |
   | `wait_event_counters` | 119 s | No |
   | `plan_history` | 88 s | No |
   | `active_session_history` | 34 s | No |
   | compute/database tables, `postgres_logs` | 16–22 s each | Yes |

   Switching would save about 2 minutes of the cheapest copies.
4. **It does not connect directly, and it is not self-serve.** Zerobus needs an `x-databricks-zerobus-table-name` header and an OAuth token that expires after 1 hour. The Lakebase form accepts one static credential header. A customer-run OpenTelemetry Collector (token refresh + header injection), reachable from the internet, would have to sit in between. The export is configured per project in the Lakebase App UI only (no `databricks postgres` command or project-spec field), so a fleet tool cannot enable it.

When it is useful: near-real-time alerting on CPU, connections or replication lag for a few critical projects, which the hourly engine cannot do. If needed, add it as an optional feed alongside the engine, not as a replacement for inventory or the system-table copy.

## 7. Comparison: Lakebase Insights vs. pg_sage vs. these checks (tested 2026-10-27)

All three ran against the same `healthcheck` project (PG 17, 1 CU), with 15 planted problems and a 12‑minute incident workload (deadlocks, an idle transaction blocking an UPDATE, seq scans, disk spills).
- **Insights:** Beta, run on demand twice. The second run came right after the workload.
- **pg_sage:** v2.4.0, observation mode, executor off, no LLM, deterministic rules only.
- **These checks:** the `HEALTH_CHECKS.md` catalog.

**Bias to note:** the same person wrote the planted problems and these checks, so they have home advantage. Read the column for these checks as "the catalog covers what it was designed for", not as an independent score.

### 7.1 Detection scorecard

| # | Planted problem | Insights | pg_sage | These checks |
|---|---|---|---|---|
| 1 | Duplicate index | — | ✅ `duplicate_index` + DROP DDL | ✅ |
| 2 | Left‑prefix redundant index | — | — | ✅ |
| 3 | Unused indexes | — | ⏳ waits for a 7‑day window by design | ✅ (with stats‑window confidence guard) |
| 4 | Invalid index (failed CONCURRENTLY build) | — | ✅ `invalid_index` + DROP DDL (2nd cycle) | ✅ |
| 5 | FK without index | — | ✅ + CREATE DDL | ✅ + **verified on a branch** (72.9 → 0.05 ms) |
| 6 | Seq scans = 58% of DB time | — | ◐ `high_total_time` (info) raised, then auto‑resolved; not tied to the missing index | ✅ |
| 7 | int4 sequence at 75% | — | ◐ tracked as a runway (74.55%), no finding yet | ✅ with days‑left estimate |
| 8 | Autovacuum disabled on a table | — | ◐ reports "autovacuum falling behind" and suggests VACUUM; misses the root cause | ✅ suggests `RESET (autovacuum_enabled)` |
| 9 | 32% dead tuples / bloat | — | ✅ | ✅ |
| 10 | Table without PK | — | — | ✅ |
| 11 | PUBLIC CREATE on a schema | — | ✅ (AP‑07) | ✅ |
| 12 | Deadlocks (~12 per run) | — | — (no finding or incident after the full run) | ✅ with full `40P01` detail |
| 13 | Idle transaction blocking an UPDATE | — | — | ✅ live + history |
| 14 | Disk sorts / hash spills | — | ✅ work_mem tuning + temp‑file incident | ✅ |
| 15 | Write back‑pressure during bulk load | — | — (it can't see Lakebase storage) | ✅ |
| — | Table never analyzed | — | ✅ | ◐ visible in check 22 columns, but not raised as a finding (**gap to add**) |
| — | Sort processing 10× more rows than the LIMIT returns | — | ✅ `sort_without_index` | — (**gap to add**) |
| | **Score on the 15 planted** (full / partial) | **0 / 0** | **7 / 3** | **15 / 0** |

### 7.2 False positives on Lakebase

| Tool | False positive | Impact |
|---|---|---|
| pg_sage | `inactive_slot`: recommends `pg_drop_replication_slot('wal_retention_slot')`, labelled **safe** | That is a platform‑internal slot. Its policy layer refuses to run slot drops automatically, but a user could run the suggested SQL by hand |
| pg_sage | "No point‑in‑time recovery" (`archive_mode` off) | Wrong: Lakebase has PITR through its restore window (7 days here) |
| pg_sage | "Working set exceeds shared_buffers, cache hit 93.6%" | Misleading: the Lakebase local SSD cache hit rate was 100%; shared_buffers is only the first cache tier |
| pg_sage | `pg_stat_statements` unavailable when the extension lives outside `public` | Monitoring degraded until the search path was fixed by hand |
| These checks | Before testing: platform slots, `cloud_admin` sessions, ~120 platform settings | **Fixed** during testing (exclusions in checks 25, 29, 30) |
| Insights | None | It reported nothing at all |

### 7.3 Capabilities

| | Insights | pg_sage | These checks (as designed) |
|---|---|---|---|
| Setup | Built in; toggle per project | Always‑on host or container + its own login; creates a `sage` schema in the monitored DB | Databricks job + Delta + app; governed by Unity Catalog |
| Data sources | `system.lakebase` only | Postgres catalog (60 s polling) + logs + AWS/GCP/Azure APIs | `system.lakebase` + Postgres catalog + Lakebase API |
| Lakebase signals (local SSD cache, storage waits, write throttling, plan history, restore window, HA, OAuth roles) | Yes, as inputs | No (reports `cloud environment: unknown`) | Yes |
| Deterministic, tunable thresholds | No (fixed agent instructions) | Yes | Yes |
| Validating a fix | No | HypoPG what‑if (**unavailable on Lakebase**, so marked "unverified"), then post‑apply verify and rollback | Real execution on a copy‑on‑write branch before prod |
| Executing fixes | Via Genie, after confirmation | Mature trust‑ramped executor: shadow mode, approvals, rollback | Planned (approval → apply → 24 h verify) |
| LLM | Yes (built in) | Optional (any OpenAI‑compatible endpoint) | Planned (rewrites and hints, verified on a branch) |
| Fleet / history | Per project, 7 days | Fleet mode; history in `sage` schema or a meta DB | All projects; unlimited history in Delta |
| Effect on Lakebase cost | None | 60 s polling likely stops scale‑to‑zero (not measured) | Ingestion reads system tables, not the database; Postgres catalog checks run on a schedule |
| Auth on Lakebase | Managed | Static password (OAuth tokens expire after 1 h; no refresh found) | OAuth token per run |
| License | Databricks product | AGPL‑3.0 | Internal / to be decided |
| Maturity | Beta | High (v2.4, benches, e2e tests) | Early (validated catalog + prototypes) |

### 7.4 When to use which

| Situation | Use | Why |
|---|---|---|
| You want help **investigating an incident** you already know about, in conversation | **Insights / Genie** | Zero setup; reads the same telemetry; Genie can carry out a confirmed fix. Don't rely on it to *detect*: it flagged 0 of 15 here, including 5 that were clearly in its telemetry |
| A fleet that **mixes Lakebase with RDS / Cloud SQL / self‑managed Postgres**, and the team wants one mature autonomous DBA tool | **pg_sage** (with care on Lakebase) | Broadest rules, best executor safety design, Slack/MCP integration. On Lakebase, suppress `inactive_slot` for platform slots and ignore the PITR/archive_mode finding. Expect unverified index advice (no HypoPG), static‑password auth, and likely no scale‑to‑zero. Check AGPL before embedding |
| **Lakebase‑only** customers who want a health check and guided, verified fixes | **These checks** | Only option that sees Lakebase storage and cache signals, platform objects and backup posture; verifies fixes on real branches; runs natively in Databricks. Still needs building (ingestion job, app, fix loop) |
| A one‑time **health assessment** for a customer today | **These checks** (`lakebase_health_check` notebook) | Works now. Run checks against the Delta copy, or expect slow system‑table cells |

### 7.5 Recommendation

pg_sage results are final (collected 21:16 UTC, after the workload ended at 21:04).

Build the Lakebase‑native product, and **borrow from pg_sage on purpose**:
- **Ideas:** trust levels, shadow mode, an action and verification ledger, approval cards, the "facts" mechanism for app‑owned objects.
- **Rules to add:** never‑analyzed tables, sorts that read far more rows than the LIMIT returns, a stale‑statistics check, query‑level work_mem advice and agent‑posture checks.
- **Positioning:** keep Insights/Genie as the conversational layer, and link findings to "Fix with Genie" rather than competing with it.
- **Decide on licensing early:** if AGPL is acceptable, contributing a Lakebase adapter upstream (system‑table source, branch what‑if, OAuth refresh, platform exclusions) is a credible alternative.
