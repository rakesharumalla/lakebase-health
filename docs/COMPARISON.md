# Comparison: pg_sage, the collector-based Lakebase Health Check app, and Lakebase Health

Written 2026-10-29 for future reference.

Sources:
- **pg_sage:** v2.4.0, tested 2026-10-27 on the `healthcheck` test project in observation mode, executor off, no LLM
  (see `DESIGN.md` §7).
- **Lakebase Health Check app:** a separate, collector-based app (a `psql` collector script whose zip bundle is
  uploaded to a Databricks App). Reviewed from its metrics reference document, a sample report, and the live app
  with a bundle from the same `healthcheck` project.
- **Lakebase Health (this repo):** `main` on 2026-10-29, plus the LLM rewrites (built on `feature/llm-rewrites`,
  deploy pending) and the health report / tabbed project page (specced in
  `docs/superpowers/specs/2026-10-28-lakebase-health-report-design.md`).

Legend: ✅ covered · ◐ partial · — not covered.

## 1. Features

| Feature | pg_sage | Lakebase Health Check app | Lakebase Health |
|---|---|---|---|
| How it runs | Always-on host or container with its own login; creates a `sage` schema in the monitored database | `psql` collector run by hand; the zip bundle is uploaded to the app | Databricks jobs (15-minute analysis, hourly ingest), Delta tables, Databricks App; one bundle |
| Data sources | Postgres catalog (60 s polling), logs | Postgres catalog and `neon_*` functions over a short capture window | `system.lakebase` telemetry, Postgres catalog on enrolled branches, Lakebase API |
| Lakebase-aware signals (LFC, getpage, back-pressure, restore window, HA) | No ("cloud environment: unknown") | Yes (LFC, working set, getpage, back-pressure, LSN lag) | Yes (LFC, working set, storage waits, write throttling, plan history, HA, restore window, branch protection) |
| History | In the `sage` schema | Only the capture window (two samples or a 30-minute monitor series) | 7 days of telemetry, finding history, incident timeline |
| Fleet / many projects | Fleet mode | One bundle at a time | Every project in the workspace, scoped per user |
| Access control | Its own database login | Whoever can open the app sees the uploaded bundles | Workspace admin, project owner or Postgres-role holder; query-text masking switch |
| Database privileges needed | Static password login | `neon_superuser` member | Engine: `pg_monitor` only; fixer: separate least-privilege service principal |
| Health score / grade | No | Yes (11 checks × 5 points) | Specced (12 areas, tabbed project page) |
| Runbook with SQL | Findings with DDL | Yes, one copyable block | Candidate SQL per finding (built); ordered runbook (specced) |
| Export | No | HTML and CSV | HTML report specced (never includes query text) |
| Applying fixes | Mature executor: trust levels, shadow mode, approvals, rollback | No | Verify on a copy-on-write branch → approval → apply → post-apply check → approved rollback, with a hash-chained ledger |
| Validating a fix | HypoPG what-if (unavailable on Lakebase, so marked "unverified") | No | Real execution on a branch: same results, speed, write cost |
| Trust levels / shadow mode | Yes | No | L0–L2 built; L3 recorded in shadow mode only |
| LLM | Optional, any OpenAI-compatible endpoint | No | On-demand query rewrites through the AI Gateway, verified on a branch, advisory |
| `pg_hint_plan` hints | Uses them where it can | No | Not usable: the library is not preloaded and Lakebase refuses `LOAD` / `session_preload_libraries` (tested 2026-10-29) |
| Effect on scale-to-zero | 60 s polling likely keeps the compute awake | None between captures | Never wakes a suspended compute; telemetry comes from system tables |
| Platform-object awareness | No (suggested dropping a platform replication slot; claimed no PITR) | Partial (knows synced tables; suggested VACUUM on another tool's tables) | Yes (platform roles, slots and settings excluded) |
| One-off assessment notebook | No | No | Yes (generated from the same check definitions) |
| License | AGPL-3.0 | Internal | This repository |

## 2. Checks coverage

| Area | Check | pg_sage | Health Check app | Lakebase Health |
|---|---|---|---|---|
| Indexes | Duplicate index | ✅ | ✅ | ✅ (fixer can drop) |
| | Left-prefix redundant index | — | — | ✅ (fixer can drop) |
| | Unused index | ◐ waits 7 days | ✅ | ✅ (stats-age guard) |
| | Invalid index | ✅ | — | ✅ |
| | Missing FK index | ✅ | ◐ seq-scan tables | ✅ (verified on a branch; fixer can create) |
| | Index bloat | ✅ | ✅ | ✅ |
| | GIN pending list / oversized search index | — | — | ✅ |
| | Synced-table partition indexes (review, not drop) | — | ✅ | Specced |
| Tables | Dead tuples / bloat | ✅ | ✅ | ✅ |
| | Table without primary key | — | — | ✅ |
| | Heavy sequential scans / tuple fetch ratio | ◐ | ✅ | ✅ |
| | Stale or never-analyzed statistics | ✅ | ✅ (vacuum timeline) | ✅ |
| | Sequence exhaustion | ◐ runway only | — | ✅ (days left) |
| Vacuum / XID | Autovacuum disabled or aggressive overrides | ◐ | ◐ (scale-to-zero view) | ✅ (fixer can reset reloptions) |
| | Autovacuum blocked (xmin holder, long transaction) | ◐ | — | ✅ |
| | Autovacuum worker saturation | — | ✅ | Specced |
| | XID wraparound age and runway | ✅ | ✅ | ✅ |
| Queries | Top DB-time queries | ✅ | ✅ | ✅ |
| | Query regression over time | ◐ | — | ✅ |
| | Plan flips | — | — | ✅ |
| | Row misestimates | — | — | ✅ (per endpoint) |
| | Disk / temp spills (`work_mem`) | ✅ | ✅ | ✅ |
| | Sort processing far more rows than LIMIT returns | ✅ | — | ✅ |
| | Slowest application statements (platform traffic excluded) | — | ✅ | ✅ |
| | Query rewrites (LLM) | Optional | — | ✅ |
| Locks / sessions | Blocked sessions now | ✅ | ✅ | ✅ |
| | Lock-wait history and hot rows (blocker class) | — | — | ✅ |
| | Deadlocks | — (missed in the test) | — | ✅ |
| | Idle in transaction (now and time spent) | ◐ | ✅ | ✅ |
| | Long transactions | ✅ | ✅ | ✅ |
| | Statement timeouts, connection drops, auth failures, Postgres errors | — | — | ✅ |
| | Commit quorum (SyncRep) waits | — | — | ✅ |
| | Wait events by class | ◐ | ✅ (with monitor timeline) | ✅ (incident timeline) |
| Connections | Utilization vs. `max_connections` | ✅ | ✅ | ✅ |
| | Non-SSL sessions | — | — | ✅ |
| Cache / memory | LFC hit rate | — (read shared buffers instead) | ✅ (showed 0.0 % on `healthcheck`) | ✅ (99.998 % on the same project) |
| | Working set vs. LFC | — | ✅ | ✅ |
| | Buffer hit rate | ✅ | ✅ | ✅ |
| | CU sizing (under / over) | — | ◐ | ✅ (sustained pressure) |
| Storage / write path | Getpage latency (average / p99) | — | ✅ | ✅ per-query storage waits; spike detection specced |
| | Write back-pressure / throttling | — | ✅ | ✅ |
| | WAL bursts / full-page images | — | ✅ (monitor mode) | Specced |
| | Storage utilization / runway forecast | — | ◐ database sizes | ✅ |
| | `pg_stat_io` by backend | — | ✅ | Specced (System tab) |
| Replication | Replica lag | ✅ | ✅ | ✅ |
| | Replication slot retention | ✅ (flagged a platform slot) | — | ✅ (platform slots excluded) |
| Scale to zero | Idle pools or agents blocking suspend | — | ✅ | Specced |
| Configuration | Guardrail settings (`statement_timeout`, `idle_in_transaction_session_timeout` …) | ✅ | ✅ | ✅ |
| | Role / database overrides (config drift) | — | ◐ (all settings shown) | ✅ |
| | Full parameter list with how each can be changed | — | ✅ | Specced (Settings tab) |
| | Support-managed settings (`track_io_timing`, slow-query log) | — | ✅ | — |
| Security | PUBLIC CREATE on a schema | ✅ | — | ✅ |
| | Privileged roles | ✅ | — | ✅ |
| | Password (non-OAuth) roles | — | — | ✅ |
| Resilience | HA secondary, restore window, branch protection | — (claimed no PITR) | — | ✅ |
| | Prepared transactions | ✅ | — | ✅ |
| Report quality | Stats reset too recent to trust | — | ✅ (staleness banner) | ◐ (unused-index guard); full guard specced |

Score on the 15 problems planted in `healthcheck` (2026-10-27): pg_sage 7 full / 3 partial; Lakebase Health 15 / 15.
The Health Check app was not part of that test; on the same project later it caught the indexes, dead tuples and
sequential scans, and reported the LFC hit rate as 0.0 %.

## 3. Gaps in Lakebase Health and where they are tracked

| Gap | Seen in | Status |
|---|---|---|
| Health score, tabbed project page, inventory panels, HTML / CSV export | Health Check app | Report spec (tabbed project page), next after the LLM rewrites deploy |
| Synced-table awareness, WAL bursts, getpage spikes, scale-to-zero blockers, autovacuum saturation, stats-reset guard | Health Check app | Same spec (six new checks) |
| `pg_stat_io` panel | Health Check app | Same spec (System tab, `inventory_io`) |
| High-resolution capture during an incident (monitor mode) | Health Check app | Partly covered by 7-day telemetry; per-second capture not planned |
| Unattended apply (trust level 3) | pg_sage | Shadow-scored only; needs a track record (`docs/FOLLOW_UPS.md`) |
| Alerts (Slack / email) | pg_sage | `docs/FOLLOW_UPS.md` |
| Works without `system.lakebase` | Health Check app, pg_sage | Postgres-only mode in `docs/FOLLOW_UPS.md` |
| `pg_hint_plan` hints | pg_sage | Blocked on Lakebase loading the library |
