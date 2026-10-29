# Lakebase Health — Health Report (sub-project 3b) Design

**Date:** 2026-10-28  **Status:** Draft for review  **Related:** `2026-10-27-lakebase-health-engine-design.md` (engine, checks, tables), `2026-10-28-lakebase-health-app-design.md` (app, access, masking), `2026-10-28-lakebase-health-fix-loop-design.md` (actions, fix cards)

## 1. Goal and scope

A shareable health report for one branch of a Lakebase project: a score and grade, a scorecard by area, an ordered
runbook with SQL, inventory panels and system charts, shown live in the app and downloadable as one self-contained
HTML file for a customer health review. It follows the collector-based Lakebase Health report (sample:
`a sample customer report (HTML)`, reference: "Lakebase Health App: what the collector pulls, and
why") and closes the coverage gaps found against it.

**In scope:** six new checks and a shared synced-table helper; an inventory collector in the analysis run; a
report page and HTML export in the app.

**Out of scope:** stored report snapshots and report-to-report comparison; server-side PDF; whole-project reports;
LLM-written summaries (sub-project 4); support-managed settings (`track_io_timing`, `log_min_duration_statement`)
beyond a note.

### Decisions this spec builds on

| Topic | Decision |
|---|---|
| Audience | Shareable customer report (SA, support or customer DBA exports and sends it) |
| Export | Self-contained HTML (inline CSS and SVG, no scripts, no external requests); print to PDF from the browser |
| Query text in exports | Never, whatever the exporter's PII membership |
| Score | Category scorecard: 12 areas, 0–5 each from the worst open finding, grade from the percentage |
| Snapshots | Live only: the report is computed from the latest analysis run; no stored reports |
| Scope | One branch per report (default branch preselected), project-level checks in the header |
| Approach | Engine collects inventory; the app server builds the report (rejected: app-only without inventory; engine-rendered HTML in a volume) |

## 2. Architecture

```
analysis job (every 15 min, engine SP, enrolled branches)
  ├─ checks (existing + 6 new)       ──► findings, check_results
  └─ inventory collector (new)       ──► inventory_tables, inventory_indexes, inventory_settings,
                                          inventory_statements, inventory_activity
app (per-user scoped)
  /projects/:id/branches/:branch/report     live page
  /api/report/:project/:branch(.html)       same data → self-contained HTML download
     report builder (server, pure): findings + check area → scorecard, score, grade
                                    findings + actions    → ordered runbook
                                    inventory + gauges    → panels
```

| Unit | Responsibility |
|---|---|
| `engine/.../pg_inventory.py` (new; the existing `inventory.py` lists projects and computes) | Runs the inventory catalog queries through the read-only Postgres executor; writes one snapshot per endpoint |
| `engine/.../synced.py` | Maps `__db_system` storage partitions to synced tables (`pg_partition_root()` over `pg_class`); used by index/table checks and the action builder |
| `engine/.../checks/**` | Six new checks; every check YAML gains `area` |
| `app/server/report/*.ts` | Pure functions: `score(findings, areas)`, `runbook(findings, actions)`, `strip(report)` for exports |
| `app/server/queries/report_*.sql` | One batched SELECT set bounded by `(project_id, branch_id)` |
| `app/client/src/report/*` | Report sections as React components, used for both the page and the static export |
| `app/server/routes/report.ts` | Access check, data fetch, JSON for the page, `renderToStaticMarkup` for the download, audit row |

## 3. Data model additions (`<catalog>.lakebase_health`)

All inventory tables are keyed by `(project_id, branch_id, endpoint_id, collected_at)`. Each analysis run writes
a new snapshot; the app reads the latest per endpoint; rows older than 7 days are deleted by the run.

| Table | Content |
|---|---|
| `inventory_tables` | 25 largest tables by total size: schema, table, synced-table root (nullable), total/data/index/toast size, live and dead tuples, seq and index scans, last (auto)vacuum and (auto)analyze, vacuum and analyze counts |
| `inventory_indexes` | Indexes on those tables: name, method, size, scans, valid, primary/unique, synced flag, parent index (for partitions) |
| `inventory_settings` | Key settings (fixed list: `shared_buffers`, `work_mem`, `maintenance_work_mem`, `effective_cache_size`, `random_page_cost`, `max_connections`, `max_wal_size`, `checkpoint_timeout`, `default_statistics_target`, `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`, `autovacuum_max_workers`, `neon.file_cache_size_limit`, `track_io_timing`, `log_min_duration_statement`, `shared_preload_libraries`) plus any with `source` not `default`: value, unit, source, context, how to change (`session` / `role or database` / `support-managed`) |
| `inventory_statements` | Top 20 by total time and top 20 by shared blocks read: query ID, role, database, calls, mean/total ms, shared hit/read blocks, temp blocks written. **No query text.** Excludes platform roles (`databricks_writer_%`, `cloud_admin`, the engine and fixer roles) |
| `inventory_io` | `pg_stat_io` per endpoint: backend_type, object, context, reads, writes, extends, hits, evictions, reuses, fsyncs, read_time, write_time (null when `track_io_timing` is off), stats_reset; the System tab shows the delta since the previous snapshot when both exist |
| `inventory_activity` | Per-table insert/update/delete/seq/index scan deltas between this run and the previous snapshot of the same endpoint (window length stored) |

Check YAML gains `area` (one of the 12 below). `findings` gains nothing; the area comes from the check spec.

## 4. New checks

Thresholds live in each check's YAML like existing checks.

| Check | Source | Fires when | Severity | Candidate fix |
|---|---|---|---|---|
| `suspend_blockers` | `pg_stat_activity`; `compute_gauges` | Scale to zero enabled, no suspend in 24 h, and client sessions idle longer than the suspend timeout, grouped by role and application | warning | Guidance: pool minimum idle 0, stop polling agents; evidence has counts per application, no PIDs |
| `synced_full_refresh` | `inventory_activity`; WAL and full-page images from `compute_counters` | Insert delta ≥ 50 % of rows on a synced partition in one window | info; warning when it overlaps a `getpage_latency_spike` | Switch to triggered/continuous sync, drop unused secondary indexes first, schedule off-peak |
| `wal_burst` | `compute_counters` (WAL bytes, full-page images per minute) | 5-min WAL > 10× the 24 h median and > 1 GiB | info; warning when > 3 per day | Names the COPY or index build if `pg_stat_progress_*` caught one |
| `getpage_latency_spike` | `compute_counters` getpage wait histogram | 5-min p99 > 5× the 24 h baseline and > 20 ms | warning | Raise min CU when the working set is near the LFC; serve heavy reads from a replica during syncs |
| `autovacuum_saturation` | `pg_stat_activity` (autovacuum workers), `pg_settings` | All workers busy in 3 consecutive runs and a table over its vacuum threshold for > 1 h | warning | Session-level guidance; per-table `autovacuum_vacuum_cost_limit` (existing fixer kind) |
| `synced_index_review` | Replaces `unused_index` / `duplicate_index` findings on synced partitions | Same rules as those checks, on a synced partition | info | "Confirm unused across a full refresh cycle"; SQL finds the parent index; **no fix action** |

**Shared rules**

- **Synced tables.** Every finding on a storage partition names the synced table. The action builder never creates
  a DROP action for an index on a synced partition (Postgres refuses it, and the sync rebuilds indexes).
- **Stats-reset guard.** Counter-based Postgres checks (`unused_index`, `synced_index_review`, `seq_scan_heavy`,
  `dead_tuples`, `seq_scan_read_ratio`) do not evaluate when `pg_stat_database.stats_reset` or postmaster start is
  less than 24 h ago; the area shows "counters too young (reset N h ago)".
- **Telemetry fallback.** If `system.lakebase.compute_counters` lacks per-minute WAL, full-page images or getpage
  histogram buckets (checked during planning), `wal_burst` and `getpage_latency_spike` use two samples of
  `pg_stat_wal` and `neon_perf_counters` per analysis run from the Postgres executor (15-min resolution).

## 5. Score and runbook

**Areas (12):** cache & memory; storage latency; write path; connections & sessions; locks & contention; vacuum &
XID; indexes; tables & bloat; queries; configuration & security; scale to zero; resilience.

- Area score: 5 with no open finding; worst open finding info → 4, warning → 3, critical → 1.
- An area is **not assessed** when none of its checks ran for this branch (for example, Postgres areas on a branch
  that is not enrolled, or the stats-reset guard); it is left out of the total and shown as such.
- Score = sum / (5 × assessed areas); grade A ≥ 85 %, B ≥ 70 %, C ≥ 55 %, D ≥ 40 %, F below. The header shows
  "assessed N of 12".
- Project-level checks (HA, backup window, branch protection, password roles) count in resilience and
  configuration & security.

**Runbook:** one item per open finding, ordered by severity, then whether a verified fix action exists, then
impact (`measure` relative to its threshold). Item: priority (critical → HIGH, warning → MEDIUM, info → LOW), area,
title, one-line why, evidence numbers, candidate SQL or a "support-managed" note, and in the app a link to the fix
card. SQL that would quote user query text shows the query ID instead.

## 6. Report page and export

**The project page becomes the report, organized as tabs** (user decision 2026-10-29, after reviewing the
collector-based Lakebase Health Check app): `/projects/:id/branches/:branch/<tab>`, branch picker lists visible
branches, default preselected. A header above the tabs shows project, branch, endpoints (including read replicas),
PG version, data as of the last analysis run, score, grade, "assessed N of 12", banners (stale engine data, young
counters, not enrolled) and **Download report**. Every tab starts with a row of KPI tiles, then cards; each card has a
one-line plain-language explanation of what it shows and how to read it.

| Tab | Content |
|---|---|
| **Health** (default) | Score and grade; 12-area scorecard in a two-column grid, PASS (5) / WARN (3–4) / FAIL (1) / not assessed, with the worst finding's one-line detail; a short "where to focus" note; critical items and warnings |
| **Overview** | Tiles: LFC hit, working set vs. LFC, connections vs. max, database size, oldest transaction, getpage p99 (24 h), temp spill (24 h), commits per second; database sizes |
| **Tables & Indexes** | Inventory: top tables by size (live/dead tuples, scans, I/O), indexes with size and scans, unused, duplicate and redundant indexes, synced tables |
| **Queries** | One row per query with issue badges and query text (built), top statements by time and by I/O |
| **Waits & Locks** | Wait-event summary by class and the incident timeline (built) |
| **Vacuum & Bloat** | Vacuum and analyze timeline, autovacuum workers, XID age, table and index bloat |
| **Scale to zero** | Suspend blockers, idle clients by application, autovacuum overrides |
| **System** | 24 h charts: LFC hit, working set, getpage avg/p99, WAL and full-page images, active sessions by wait class; replica lag; **I/O by backend** (`pg_stat_io`: backend type × object × context with reads, writes, hits, evictions, extends, fsyncs; read/write times shown only when `track_io_timing` is on, otherwise marked "timing off"), with the note that counters reset on compute restart and that high autovacuum or bulkread reads point at cache churn |
| **Settings** | Key parameters, source, and how each can be changed (session / role or database / support-managed) |
| **Runbook** | Counts by priority; ordered actions (§5) with copyable SQL (one block or per item) linking to fix cards |
| **Fixes** | The existing fixes and approvals view |

Resilience and security findings appear in the Health scorecard and the Runbook. The **export** renders the Health,
Overview, Runbook and inventory tabs as one document, in the same order.

**Export** ("Download report"): the server fetches the same data, runs `strip()` (removes query text, statement text
in evidence and session samples, whatever the user's PII membership), and renders the same components with
`renderToStaticMarkup`. CSS inlined; charts rendered server-side to inline SVG; no `<script>`, no external URLs;
print stylesheet. File name `lakebase-health-<project>-<branch>-<UTC yyyymmddThhmmZ>.html`. Each export writes an
`audit_events` row (user, project, branch, time). In-app viewing shows query text to `databricks_pii_access` members, the project owner and workspace admins
(user decision 2026-10-28); others see the normalized skeleton with literals removed, or the query ID.

**Access:** the project and branch must be visible to the user (app spec §4), otherwise 404. The app SP stays
SELECT-only on engine tables; its only write is the `audit_events` insert (confirm in planning that the app already
holds it; if not, the grants script adds INSERT/MODIFY on that table only).

## 7. Error handling

| Case | Behaviour |
|---|---|
| Branch not enrolled for Postgres | Postgres areas not assessed; telemetry, API and resilience areas scored; inventory panels replaced by enrolment steps |
| Inventory missing or older than 2 analysis intervals | Panels show their age with a stale badge; the score uses findings only |
| No telemetry (new project or ingest stale) | Telemetry areas not assessed; banner |
| One section query fails | That section shows an error box; the page and the export still render, the export marks the section "unavailable" and never drops it silently |
| Suspended compute | Last snapshot shown with its time; nothing wakes the compute |

## 8. Testing and acceptance

- **Engine (pytest):** each new check on synthetic telemetry or fake Postgres rows; synced mapping; no DROP action on
  synced indexes; stats-reset skips; inventory collector with platform-role exclusion and 7-day cleanup.
- **App (Vitest):** scoring (each severity, not assessed, grade boundaries); runbook ordering; export has no
  `<script>`, no external URLs and none of the fixture's query texts, including for a PII user; 404 on a hidden
  branch; audit row written.
- **Query tests:** every new SQL file runs against `my_catalog.lakebase_health` on `my-profile` in < 5 s.

**Acceptance criteria (healthcheck, production branch, my-profile):**
1. The report shows a score and grade, and the area results match the open findings.
2. The planted problems appear in the runbook ordered by severity, with SQL.
3. Inventory lists the `hc_test` tables and indexes with sizes matching `pg_relation_size`.
4. An idle client held past the suspend timeout raises `suspend_blockers`.
5. A synced table from a small UC table: its partition is named after the synced table, its unused index is a
   `synced_index_review` with no fix action, and a full refresh raises `synced_full_refresh`.
6. The downloaded HTML opens offline, has no scripts or external requests, contains no query text when a PII member
   exports it, and an audit row exists.
7. The test service principal sees only healthcheck's report; any other project or branch returns 404.
8. Report data loads in < 5 s with a warm warehouse.

## 9. Delivery

Branch `feature/health-report` from `feature/fix-loop` (both touch the engine and the app), rebased if fix-loop
changes after its final review. One plan, two phases: (1) engine — `area` field, synced helper, stats-reset guard,
inventory collector, new checks; (2) app — report builder, page, export.

## 10. Open items (not blocking)

- Stored snapshots and "since last report" comparison, if customers ask for quarterly reviews.
- An LLM-written executive summary at the top of the report (sub-project 4).
