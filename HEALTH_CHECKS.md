> The original check catalogue, kept for reference. What the engine actually runs is in [docs/CHECKS.md](docs/CHECKS.md) and the generated notebook [notebooks/lakebase_health_check.py](notebooks/lakebase_health_check.py) (built from the check YAMLs by `scripts/build_notebook.py`).

# Lakebase Health Check Catalog

A catalog of health checks for a Lakebase (Autoscaling) Postgres project. For each check:
**what it measures**, **the query**, **the expected range**, and **the recommended fix**.

Sources used (as of the docs dated Sep–Oct 2026):
- [Observability](https://docs.databricks.com/aws/en/oltp/projects/monitor), [Metrics](https://docs.databricks.com/aws/en/oltp/projects/metrics), [Insights](https://docs.databricks.com/aws/en/oltp/projects/observability-ai-insights)
- [Telemetry system tables reference](https://docs.databricks.com/aws/en/oltp/projects/observability-telemetry-reference) (`system.lakebase.*`, Beta)
- [pg_stat_statements](https://docs.databricks.com/aws/en/oltp/projects/pg-stat-statements), [Extensions](https://docs.databricks.com/aws/en/oltp/projects/extensions), [Computes](https://docs.databricks.com/aws/en/oltp/projects/manage-computes), [Point-in-time restore](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore), [Snapshots](https://docs.databricks.com/aws/en/oltp/projects/snapshots)

> **Validation status (2026‑10‑27):** every SQL block was run against a live PG 17 project (`healthcheck`, 1 CU) with a seeded workload. Results: 31/31 Postgres statements and 33/33 `system.lakebase` statements execute, and each planted anti‑pattern was detected. Corrections from testing are marked **"Tested:"** inline. Run the checks yourself with `scripts/test_checks.py`. Column names in `system.lakebase` are Beta and "might change before GA".

---

## 0. Prerequisites and things specific to Lakebase

| Topic | What to know |
|---|---|
| Two data planes | **(A) `system.lakebase.*`** is queried from Databricks SQL. It keeps a 7‑day history and survives scale‑to‑zero. A workspace admin must enable *Lakebase Observability in System Tables*. Reading it needs account admin + metastore admin, or a grant. Statement text is `<REDACTED>` unless the reader is in `databricks_pii_access`. **(B) Postgres catalog (`pg_stat_*`, `pg_catalog`)** is queried over a Postgres connection. It is live, but counters **reset when the compute suspends or restarts**. |
| Telemetry not appearing | Run `SHOW neon_monitor.log_capture_enabled; SHOW databricks.o11y_exporter_enabled;`. Both must be `on`. If not, restart the endpoint. |
| Counters vs gauges | `*_counters` tables hold **deltas for the interval `[previous_ts, ts]`**. Sum them, or divide by the interval to get a rate. `*_gauges` tables hold point‑in‑time readings. |
| IDs | **`project_id` is the project's `uid` (a UUID), not its name.** Get it with `databricks postgres get-project projects/<name>` → `uid`. `branch_id`/`endpoint_id` are internal IDs (`br-…`, `ep-…`); map them with `list-branches`/`list-endpoints`. `compute_id` changes on every resume. |
| Branches share query IDs | Every branch of a project reports into the same tables with the same `queryid`. Per‑query checks must group or filter by `endpoint_id`, or test branches will pollute production findings. |
| Query cost | Tested: `system.lakebase` statements spent **200–3,000 s compiling** and about 1 s executing, even for `LIMIT 1`. Running 16 statements in parallel made compile time worse. Copy rows into Delta first, then run checks on the copies (DESIGN.md §2a). Don't query these tables live from an app. |
| Planning stats | `pg_stat_statements.track_planning` is off, so `total_plan_time`/`plans` are 0 or NULL. `mean_plan_ms` will be empty. |
| Two caches | **Postgres shared buffers** (`shared_blks_hit/read`) sit in front of the **Local File Cache (LFC)**, the "compute cache / local SSD cache" in the UI (`lfc_hits/misses`). The LFC sits in front of the remote **pageserver** (`neon_getpage_*`). The LFC hit rate is the one the UI's 99% target refers to. |
| Extensions | Available: `pg_stat_statements`, `pgstattuple`, `pgrowlocks`, `pg_prewarm`. `pg_hint_plan` can be created but its library is not loaded, and `LOAD`, `ALTER ROLE/DATABASE … SET session_preload_libraries` and `pg_hint_plan.enable_hint_table` are refused for non-superusers (tested 2026-10-29): hint comments do not change plans. **Not available:** `hypopg`, `pg_repack`, `pg_cron`, `auto_explain` (per‑execution plans come from `system.lakebase.plan_history` instead). |
| Connection limits | 0.5 CU=105, 1=218, 2=443, 4=894, 8=1,795, 16=3,597, ≥24 CU=3,993. With autoscaling the limit is the limit for `min(max_CU, 8 × min_CU)`. `SHOW max_connections` reads higher because it includes reserved slots. |
| Sizing facts | 1 CU ≈ 2 GB RAM. LFC ≤ 75% of RAM. Autoscale 0.5–32 CU, and max − min ≤ 16 CU. Fixed sizes run 36–112 CU. 8 TB logical data per branch. |

In Databricks SQL, set a parameter `:project_id` (and optionally `:endpoint_id`). Every `system.lakebase` query below filters on it.

### 0.1 Statistics window (run first, on Postgres)
Every `pg_stat_*` check is only as good as the window it covers. An "unused index" seen after 10 minutes of uptime means nothing.

```sql
SELECT pg_postmaster_start_time()                         AS compute_started,
       now() - pg_postmaster_start_time()                 AS uptime,
       (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()) AS db_stats_reset,
       (SELECT stats_reset FROM pg_stat_statements_info)  AS pgss_reset;   -- requires pg_stat_statements
```
**Expected:** a window of at least one full business cycle (24h, or 7d for weekly batch).
**If too short:** label Postgres‑side findings "low confidence". Lean on `system.lakebase.*` (7d), or keep the compute warm (`no_suspension`) during an assessment window.

---

## Threshold summary

| # | Check | Healthy | Warn | Critical |
|---|---|---|---|---|
| 1 | CPU utilization (p95, vs allocated) | < 70% | 70–85% | > 85% sustained, or pinned at max CU |
| 2 | Local SSD (LFC) cache hit rate | ≥ 99% | 95–99% | < 95% |
| 3 | Buffer (shared_buffers) cache hit rate | ≥ 95%* | 90–95% | < 90% |
| 4 | Working set (60m) vs LFC size | < 80% | 80–100% | > 100% |
| 5 | Connections used vs limit | < 70% | 70–85% | > 85%, or any 53300 error |
| 6 | Storage used vs limit | < 70% | 70–85% | > 85%, or < 30 days to full |
| 7 | Write back‑pressure throttling | 0 s | > 0 s occasionally | sustained > 0 |
| 8 | TPS / rollback ratio | rollback < 1% | 1–5% | > 5% |
| 9 | Rows/sec; returned÷fetched | ratio < 10 | 10–100 | > 100 (seq‑scan heavy) |
| 10 | Deadlocks | 0 | > 0/day | > 1/hour |
| 11 | Session errors, idle‑in‑transaction time | idle‑in‑tx < 5% of session time | 5–20% | > 20%, or sessions killed |
| 12 | Top queries / mean latency | per SLO (OLTP p95 < 50 ms typical) | — | — |
| 13 | Query regressions / plan flips | none | mean +50% | mean +100%, or a plan flip on a hot query |
| 14 | Wait‑event profile | CPU/IO dominated | Lock/LWLock > 10% | Lock > 25% |
| 15 | Disk spills, bad plans | none on hot paths | occasional | spills on top‑10 queries |
| 16 | Pageserver wait per query | < 10% of duration | 10–30% | > 30% |
| 17 | Blocking, long transactions | none > 1 min | > 1 min | > 5 min, or idle‑in‑tx holding locks |
| 18 | Errors in Postgres logs | ~0 infra errors | app errors trending | 53xxx / 57Pxx / XX000 present |
| 19 | Unused / duplicate / invalid indexes | none | any | large (> 1 GB) unused/dup |
| 20 | Missing FK indexes | none | small child tables | large child tables |
| 21 | Seq‑scan‑heavy tables | — | avg rows/seq scan > 10k | > 1M on a hot table |
| 22 | Dead tuples / autovacuum lag | dead < 10% | 10–20% | > 20%, or no autovacuum in 24h on a hot table |
| 23 | Table/index bloat | free < 20% | 20–40% | > 40% |
| 24 | XID / MXID wraparound runway | age < 200M | 200M–1B | > 1B (≈50% of 2^31) |
| 25 | Freeze blockers | none | xmin age > 50M | xmin age > 200M |
| 26 | Sequence / identity exhaustion | < 50% | 50–75% | > 75%, or < 90 days left |
| 27 | Replica lag, replication slots | lag < 1 s, no inactive slots | 1–30 s | > 30 s, or inactive slot retaining WAL |
| 28 | Backup / recovery posture | restore window ≥ 7d, prod protected, HA on prod | — | window 2d, unprotected prod |
| 29 | Security audit | OAuth roles, no PUBLIC CREATE | password roles | non‑SSL sessions, broad superuser |
| 30 | Config drift / guardrail GUCs | timeouts set | — | no `idle_in_transaction_session_timeout` |
| 31 | Schema change audit | changes tied to releases | — | DDL in prod outside a change window |

\* On Lakebase, shared_buffers is a small first tier in front of the LFC. A lower buffer hit rate matters less than the LFC hit rate.

---

## A. Capacity, sizing and resource utilization

### 1. CPU utilization %
**What:** CPU seconds consumed per second of wall time, divided by provisioned logical CPUs. `host_busiest_cpu_seconds` shows single‑core saturation, which is typical of one hot query or of autovacuum.

```sql
-- Databricks SQL
WITH c AS (
  SELECT endpoint_id, compute_id, ts,
         host_cpu_seconds         / NULLIF(timestampdiff(MILLISECOND, previous_ts, ts) / 1000.0, 0) AS cores_used,
         host_busiest_cpu_seconds / NULLIF(timestampdiff(MILLISECOND, previous_ts, ts) / 1000.0, 0) AS busiest_core
  FROM system.lakebase.compute_counters
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
),
g AS (
  SELECT compute_id, date_trunc('MINUTE', ts) AS m, max(host_logical_cpus) AS cpus
  FROM system.lakebase.compute_gauges
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY ALL
)
SELECT c.endpoint_id, date_trunc('HOUR', c.ts) AS hour,
       round(100 * avg(cores_used / cpus), 1)                     AS avg_cpu_pct,
       round(100 * percentile_approx(cores_used / cpus, 0.95), 1) AS p95_cpu_pct,
       round(100 * max(cores_used / cpus), 1)                     AS max_cpu_pct,
       round(100 * max(busiest_core), 1)                          AS busiest_core_pct
FROM c JOIN g ON c.compute_id = g.compute_id AND date_trunc('MINUTE', c.ts) = g.m
GROUP BY ALL ORDER BY hour DESC;
```
**Expected:** p95 < 70%. Short bursts to 100% are fine on autoscaling, as long as the compute scales up.
**If high:**
1. Find the CPU consumers with check 12 (sort by `total_exec_time`) and `plan_history.cpu_user_time_ms`.
2. Fix the plans first: indexes (checks 19–21) and spills (check 15).
3. If CPU is pinned at the **max CU** of the autoscale range, raise `autoscaling_limit_max_cu`. If it is pinned at a fixed size, move up a size.
4. A single busiest core near 100% with low average CPU means one serial query. Optimize it, or allow parallelism (`max_parallel_workers_per_gather`).

### 2. Local SSD cache (LFC) hit rate %
**What:** the share of page reads served from the compute's local file cache instead of the remote pageserver. This is the "Compute cache hit rate" graph in the UI.

```sql
SELECT endpoint_id, date_trunc('HOUR', ts) AS hour,
       sum(lfc_hits) AS hits, sum(lfc_misses) AS misses, sum(lfc_evictions) AS evictions,
       round(100.0 * sum(lfc_hits) / NULLIF(sum(lfc_hits) + sum(lfc_misses), 0), 2) AS lfc_hit_pct
FROM system.lakebase.compute_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY hour DESC;
```
**Expected:** ≥ 99% for OLTP. Large analytic seq scans can run lower acceptably.
**If low:**
1. Check the working set (check 4). If it exceeds the cache, raise **min CU**, not only max, so the cache doesn't shrink between bursts.
2. A dip right after resume is a cold cache. Use `pg_prewarm` on hot tables after resume, or disable scale‑to‑zero on latency‑sensitive prod.
3. Reduce pages read: add indexes for seq‑scan queries (check 21) and remove bloat (check 23).

### 3. Buffer cache hit rate %
**What:** the hit ratio of the Postgres shared_buffers tier, overall and per query.

```sql
-- Databricks SQL, per database
SELECT datname, date_trunc('HOUR', ts) AS hour,
       round(100.0 * sum(shared_blks_hit) / NULLIF(sum(shared_blks_hit) + sum(shared_blks_read), 0), 2) AS buffer_hit_pct
FROM system.lakebase.database_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY ALL ORDER BY hour DESC, datname;
```
```sql
-- Postgres, live, per table (find the tables driving misses)
SELECT schemaname, relname,
       heap_blks_read, heap_blks_hit,
       round(100.0 * heap_blks_hit / NULLIF(heap_blks_hit + heap_blks_read, 0), 2) AS heap_hit_pct,
       idx_blks_read,
       round(100.0 * idx_blks_hit  / NULLIF(idx_blks_hit  + idx_blks_read, 0), 2) AS idx_hit_pct
FROM pg_statio_user_tables
ORDER BY heap_blks_read + coalesce(idx_blks_read, 0) DESC LIMIT 20;
```
**Expected:** ≥ 95% (OLTP), but **judge LFC first**. A shared_buffers miss that hits the LFC is still cheap.
**If low:** the same levers as LFC. Per table, a low `heap_hit_pct` with high `seq_scan` points to a missing index.

### 4. Working set vs cache size (memory pressure)
**What:** the distinct pages touched in 5/15/60 minutes, compared with LFC capacity. This is the best signal for whether RAM (CU) is the bottleneck.

```sql
SELECT endpoint_id, date_trunc('HOUR', ts) AS hour,
       max(lfc_working_set_size_60m) AS ws_60m, max(lfc_size_limit) AS lfc_limit,
       max(lfc_used) AS lfc_used, max(host_memory_total_bytes) / 1024 / 1024 / 1024 AS ram_gb,
       round(100.0 * max(lfc_working_set_size_60m) / NULLIF(max(lfc_size_limit), 0), 1) AS ws_vs_cache_pct
FROM system.lakebase.compute_gauges
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY hour DESC;
```
> Tested: `lfc_working_set_size_*`, `lfc_size_limit` and `lfc_used` are all in **bytes**. At peak load the 60m working set was 340.6 MB with `lfc_used` at 351.6 MB. On a 1 CU compute `lfc_size_limit` is 1.72 GB, and an idle compute reports about 40 KB. Use hourly **max**, not avg, so idle stretches do not hide peaks.

**Expected:** working set (60m) < 80% of the LFC limit.
**If above:** raise min/max CU (each CU adds ~1.5 GB of LFC), or shrink the working set: drop unused indexes, fix bloat, partition and archive cold data.

### 5. Connection utilization
**What:** open backends compared with the endpoint's limit, plus the breakdown by state.

```sql
-- Databricks SQL: 7-day trend
WITH conn AS (
  SELECT compute_id, endpoint_id, ts, sum(numbackends) AS backends
  FROM system.lakebase.database_gauges
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY ALL
),
lim AS (
  SELECT compute_id, date_trunc('MINUTE', ts) AS m, max(max_connections) AS max_conn
  FROM system.lakebase.compute_gauges
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY ALL
)
SELECT conn.endpoint_id, date_trunc('HOUR', conn.ts) AS hour,
       max(backends) AS peak_backends, max(max_conn) AS max_connections,
       round(100.0 * max(backends) / max(max_conn), 1) AS peak_pct
FROM conn JOIN lim ON conn.compute_id = lim.compute_id AND date_trunc('MINUTE', conn.ts) = lim.m
GROUP BY ALL ORDER BY hour DESC;
```
```sql
-- Postgres: what the connections are doing right now
SELECT usename, application_name, state, count(*) AS n,
       max(now() - state_change) AS longest_in_state
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND usename <> 'cloud_admin'   -- cloud_admin = platform agents (exporter, monitor)
GROUP BY usename, application_name, state
ORDER BY n DESC;
```
**Expected:** peak < 70% of the **usable** limit (the docs table, not `SHOW max_connections`). Mostly `active` or briefly `idle`.
**If high:**
1. Add a client‑side pool (HikariCP, pgbouncer in the app tier, SQLAlchemy pool) with a sane `max_pool_size` per app instance.
2. Close idle connections, and set `idle_session_timeout` for ad‑hoc roles.
3. Remember the autoscale rule. The limit is the limit for `min(max_CU, 8×min_CU)`, so a 0.5–16 CU range gets only the 4 CU limit (894). Raise min CU if you need more slots.

### 6. Storage utilization and time‑to‑full forecast
**What:** current size against the limit, with a linear growth trend over 7 days used to predict days until full.

```sql
WITH s AS (
  SELECT endpoint_id, ts, total_size, total_size_limit
  FROM system.lakebase.compute_gauges
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS AND total_size IS NOT NULL
)
SELECT endpoint_id,
       max_by(total_size, ts) / power(1024, 3)                                    AS used_gb,
       max_by(total_size_limit, ts) / power(1024, 3)                              AS limit_gb,
       round(100.0 * max_by(total_size, ts) / max_by(total_size_limit, ts), 1)   AS used_pct,
       regr_slope(total_size, unix_timestamp(ts)) * 86400 / power(1024, 3)       AS growth_gb_per_day,
       (max_by(total_size_limit, ts) - max_by(total_size, ts))
         / NULLIF(regr_slope(total_size, unix_timestamp(ts)) * 86400, 0)         AS days_to_full
FROM s GROUP BY endpoint_id;
```
```sql
-- Postgres: what is taking the space
SELECT n.nspname, c.relname,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
       pg_size_pretty(pg_relation_size(c.oid))       AS heap,
       pg_size_pretty(pg_indexes_size(c.oid))        AS indexes
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 20;
```
Also available through the API: `branch.status.logical_size_bytes` against `project.status.branch_logical_size_limit_bytes`.
**Expected:** < 70% used, and > 90 days to full. The docs say write performance drops at the quota.
**If high:** drop unused/duplicate indexes and reclaim bloat (checks 19, 23). Archive or partition cold data, for example to Delta via Lakehouse Sync. Shorten the restore window if history storage is the driver.

### 7. Write back‑pressure throttling
**What:** seconds during which the storage layer throttled writes, because WAL volume exceeded what the pageserver could absorb.

```sql
SELECT endpoint_id, date_trunc('HOUR', ts) AS hour,
       round(sum(backpressure_throttling_seconds), 1) AS throttled_s,
       round(sum(host_disk_written_bytes) / power(1024, 2), 1) AS disk_written_mb
FROM system.lakebase.compute_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL HAVING sum(backpressure_throttling_seconds) > 0
ORDER BY hour DESC;
```
```sql
-- Who generates the WAL
SELECT queryid, max(query) AS query, sum(calls) AS calls,
       round(sum(wal_bytes) / power(1024, 3), 2) AS wal_gb, sum(wal_fpi) AS full_page_images
FROM system.lakebase.pg_stat_statements_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY queryid ORDER BY wal_gb DESC LIMIT 20;
```
**Expected:** 0.
**If > 0:**
1. Batch and throttle bulk loads; prefer `COPY` over row‑by‑row inserts.
2. Avoid updating indexed columns needlessly, since that produces non‑HOT updates.
3. Drop unused indexes, because each one multiplies WAL.
4. Split huge single‑statement `UPDATE`/`DELETE` into chunks.

### 8. Appropriate CU sizing (composite recommendation)
**What:** combines checks 1, 2, 4 and 5 into a sizing verdict per endpoint.

```sql
WITH cpu AS (
  SELECT c.endpoint_id,
         percentile_approx(c.host_cpu_seconds / NULLIF(timestampdiff(MILLISECOND, c.previous_ts, c.ts) / 1000.0, 0) / g.host_logical_cpus, 0.95) AS p95_cpu,
         100.0 * sum(c.lfc_hits) / NULLIF(sum(c.lfc_hits) + sum(c.lfc_misses), 0) AS lfc_hit_pct
  FROM system.lakebase.compute_counters c
  JOIN system.lakebase.compute_gauges g
    ON c.compute_id = g.compute_id AND date_trunc('MINUTE', c.ts) = date_trunc('MINUTE', g.ts)
  WHERE c.project_id = :project_id AND c.ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY c.endpoint_id
),
mem AS (
  SELECT endpoint_id,
         percentile_approx(lfc_working_set_size_60m / NULLIF(lfc_size_limit, 0), 0.95) AS p95_ws_ratio,
         max(host_memory_total_bytes) / power(1024, 3) AS max_ram_gb
  FROM system.lakebase.compute_gauges
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY endpoint_id
)
SELECT cpu.endpoint_id, round(100 * p95_cpu, 1) AS p95_cpu_pct, round(lfc_hit_pct, 2) AS lfc_hit_pct,
       round(100 * p95_ws_ratio, 1) AS p95_ws_vs_cache_pct, round(max_ram_gb, 1) AS max_ram_gb,
       CASE
         WHEN p95_cpu > 0.85 OR p95_ws_ratio > 1.0 OR lfc_hit_pct < 95 THEN 'UNDERSIZED: raise max CU (and min CU if cache-bound)'
         WHEN p95_cpu < 0.20 AND p95_ws_ratio < 0.40 AND lfc_hit_pct >= 99 THEN 'OVERSIZED: lower min CU / enable scale-to-zero on non-prod'
         ELSE 'RIGHT-SIZED'
       END AS verdict
FROM cpu JOIN mem USING (endpoint_id);
```
Read the configured range from the API: `endpoint.status.autoscaling_limit_min_cu / max_cu`, `suspend_timeout_duration`.
**Guidance:**
- **Cache‑bound** (LFC < 99%, working set > cache): raise **min CU**, because the cache size follows the current CU.
- **CPU‑bound** with a healthy cache: raise **max CU**.
- **Connection‑bound** (check 5): raise min CU so that `8×min ≥ max`.
- Constraint: max − min ≤ 16 CU. Above 32 CU, use a fixed size.
- **Non‑prod:** scale‑to‑zero on. **Latency‑sensitive prod:** `no_suspension`, to avoid cold caches and lost `pg_stat_*`.

---

## B. Throughput and transaction health

### 9. Transactions per second (TPS) and rollback ratio
```sql
WITH per_interval AS (   -- sum across databases first, then turn into a rate
  SELECT endpoint_id, compute_id, ts, previous_ts,
         sum(xact_commit) AS commits, sum(xact_rollback) AS rollbacks
  FROM system.lakebase.database_counters
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY ALL
)
SELECT endpoint_id, date_trunc('HOUR', ts) AS hour,
       round(avg((commits + rollbacks) / NULLIF(timestampdiff(MILLISECOND, previous_ts, ts) / 1000.0, 0)), 1) AS avg_tps,
       round(max((commits + rollbacks) / NULLIF(timestampdiff(MILLISECOND, previous_ts, ts) / 1000.0, 0)), 1) AS peak_tps,
       round(100.0 * sum(rollbacks) / NULLIF(sum(commits + rollbacks), 0), 2)                          AS rollback_pct
FROM per_interval GROUP BY ALL ORDER BY hour DESC;
```
**Expected:** TPS follows the business pattern; use it as a baseline for anomaly detection. Rollback < 1%.
**If rollbacks are high:** look up the cause in `postgres_logs` (check 18). Common causes are serialization failures (40001), deadlocks (40P01), constraint violations (23xxx), and ORMs rolling back read‑only transactions. A sudden TPS drop alongside high CPU means saturation, so see check 8.

### 10. Rows per second (DML and read mix)
```sql
WITH per_interval AS (
  SELECT endpoint_id, ts, previous_ts,
         sum(rows_inserted) ins, sum(rows_updated) upd, sum(rows_deleted) del,
         sum(rows_fetched) fetched, sum(rows_returned) returned
  FROM system.lakebase.database_counters
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY ALL
)
SELECT endpoint_id, date_trunc('HOUR', ts) AS hour,
       round(sum(ins) / NULLIF(sum(timestampdiff(SECOND, previous_ts, ts)), 0), 1) AS inserts_per_s,
       round(sum(upd) / NULLIF(sum(timestampdiff(SECOND, previous_ts, ts)), 0), 1) AS updates_per_s,
       round(sum(del) / NULLIF(sum(timestampdiff(SECOND, previous_ts, ts)), 0), 1) AS deletes_per_s,
       round(sum(returned) / NULLIF(sum(fetched), 0), 1)                AS returned_to_fetched_ratio
FROM per_interval GROUP BY ALL ORDER BY hour DESC;
```
**What `returned_to_fetched_ratio` means:** `rows_returned` counts rows *read* (it is dominated by seq scans), while `rows_fetched` counts rows fetched through index scans. A ratio well above 10 means the database reads far more rows than it uses.
**Expected:** ratio < 10. Delete spikes should match known jobs.
**If off:** for a high ratio, go to check 21 (seq‑scan tables) and check 15 (plans). An update‑heavy workload needs autovacuum tuning (check 22) and a lower `fillfactor` on hot tables for HOT updates.

### 11. Deadlocks per second / per hour
```sql
SELECT endpoint_id, datname, date_trunc('HOUR', ts) AS hour,
       sum(deadlocks) AS deadlocks,
       sum(deadlocks) / NULLIF(sum(timestampdiff(SECOND, previous_ts, ts)), 0) AS deadlocks_per_s
FROM system.lakebase.database_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL HAVING sum(deadlocks) > 0 ORDER BY hour DESC;
```
```sql
-- The deadlock details (which statements and which relations)
SELECT ts, datname, usename, application_name, message, detail, context
FROM system.lakebase.postgres_logs
WHERE project_id = :project_id AND sqlstate = '40P01' AND ts >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY ts DESC;
```
**Expected:** 0.
**If > 0:**
1. Make every code path lock rows in the same order, for example by sorting the keys before a batch update.
2. Keep transactions short, and use `SELECT … FOR UPDATE` early.
3. Add retry‑on‑40P01 in the app.
4. Missing FK indexes (check 20) widen lock scope and are a frequent cause.

### 12. Session health: idle‑in‑transaction time and abnormal session endings
```sql
SELECT endpoint_id, datname, date_trunc('DAY', ts) AS day,
       round(100.0 * sum(idle_in_transaction_time_ms) / NULLIF(sum(session_time_ms), 0), 2) AS idle_in_tx_pct,
       round(100.0 * sum(active_time_ms) / NULLIF(sum(session_time_ms), 0), 2)              AS active_pct,
       sum(sessions_established) AS established, sum(sessions_fatal) AS fatal,
       sum(sessions_killed) AS killed, sum(sessions_abandoned) AS abandoned
FROM system.lakebase.database_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY day DESC;
```
**Expected:** idle‑in‑tx < 5% of session time, and few fatal or killed sessions. A high `established` count compared with TPS means connection churn with no pooling.
**If off:** set `idle_in_transaction_session_timeout` (30–60 s per the Insights guidance), fix app transaction scoping, and add pooling. Killed sessions mean someone is terminating backends or timeouts are firing; correlate with logs.

---

## C. Query performance

### 13. Top queries by total time, mean latency and I/O (7‑day, survives restarts)
```sql
SELECT queryid, max(query) AS query, sum(calls) AS calls,
       round(sum(total_exec_time) / 1000, 1)                         AS total_exec_s,
       round(sum(total_exec_time) / NULLIF(sum(calls), 0), 2)        AS mean_exec_ms,
       round(sum(total_plan_time) / NULLIF(sum(plans), 0), 2)        AS mean_plan_ms,
       round(sum(rows) / NULLIF(sum(calls), 0), 1)                   AS rows_per_call,
       round(100.0 * sum(shared_blks_hit) / NULLIF(sum(shared_blks_hit) + sum(shared_blks_read), 0), 2) AS hit_pct,
       sum(temp_blks_written)                                        AS temp_blks_written,
       round(100.0 * sum(total_exec_time) / sum(sum(total_exec_time)) OVER (), 1) AS pct_of_db_time
FROM system.lakebase.pg_stat_statements_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS AND toplevel
GROUP BY queryid ORDER BY total_exec_s DESC LIMIT 25;
```
```sql
-- p50/p95/p99 latency of the *captured* executions (plan_history is sampled; see the note below)
SELECT queryid, max(query_text) AS query, count(*) AS samples,
       percentile_approx(duration_ms, 0.50) AS p50_ms,
       percentile_approx(duration_ms, 0.95) AS p95_ms,
       percentile_approx(duration_ms, 0.99) AS p99_ms
FROM system.lakebase.plan_history
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY queryid ORDER BY p95_ms DESC LIMIT 25;
```
> Tested: `plan_history` does **not** record every execution, and it leans toward slow ones. In the test, a sub‑millisecond PK lookup that ran thousands of times was captured 4 times, while a ~210 ms seq‑scan query was captured 2,921 times. Use it for plans and the slow tail. Take calls, means and DB‑time share from `pg_stat_statements_counters`.

The live Postgres equivalent is in the [pg_stat_statements doc](https://docs.databricks.com/aws/en/oltp/projects/pg-stat-statements). Run `CREATE EXTENSION IF NOT EXISTS pg_stat_statements;` first.
**Expected:** the top 5 queries account for most DB time (that is normal). Mean and p95 should be within the app SLO; typical OLTP point queries run < 5 ms and p95 < 50 ms.
**If off:** for each top query, take its plan (check 15). Check `hit_pct` (I/O bound) and `temp_blks_written` (spills). A high `rows_per_call` means pagination or filtering is missing. A high `mean_plan_ms` compared with exec time means the app should use prepared statements.

### 14. Query regressions and plan flips
```sql
-- Mean latency in the last 24h vs the previous 6 days, per queryid
WITH w AS (
  SELECT queryid,
         CASE WHEN ts >= current_timestamp() - INTERVAL 24 HOURS THEN 'recent' ELSE 'baseline' END AS win,
         sum(calls) AS calls, sum(total_exec_time) AS t
  FROM system.lakebase.pg_stat_statements_counters
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS AND toplevel
  GROUP BY ALL
)
SELECT r.queryid,
       round(b.t / b.calls, 2) AS baseline_mean_ms, round(r.t / r.calls, 2) AS recent_mean_ms,
       round((r.t / r.calls) / (b.t / b.calls), 2) AS slowdown_x, r.calls AS recent_calls
FROM w r JOIN w b ON r.queryid = b.queryid AND r.win = 'recent' AND b.win = 'baseline'
WHERE r.calls >= 50 AND b.calls >= 50 AND (r.t / r.calls) > 1.5 * (b.t / b.calls)
ORDER BY (r.t - r.calls * b.t / b.calls) DESC   -- rank by extra time the regression costs
LIMIT 25;
```
```sql
-- Plan flips: queries with more than one plan shape *on the same endpoint*, and how each shape performs
WITH flips AS (
  SELECT endpoint_id, queryid FROM system.lakebase.plan_history
  WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY endpoint_id, queryid HAVING count(DISTINCT plan_fingerprint) > 1
)
SELECT p.endpoint_id, p.queryid, p.plan_fingerprint, count(*) AS execs,
       min(p.ts) AS first_seen, max(p.ts) AS last_seen,
       percentile_approx(p.duration_ms, 0.5) AS p50_ms
FROM system.lakebase.plan_history p
JOIN flips f ON f.endpoint_id = p.endpoint_id AND f.queryid = p.queryid
WHERE p.project_id = :project_id AND p.ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY p.endpoint_id, p.queryid, first_seen;
```
> Tested: the first version grouped only by `queryid`, and reported a "plan flip" that was really the indexed plan from a verification branch. Branches share `queryid` values, so per‑query checks must group or filter by `endpoint_id` (or `branch_id`).
**Expected:** no queries slowed by more than 1.5×; one stable plan per hot query.
**If regressed:**
1. Correlate with `ddl_history` (check 31) and with stats changes: a stale `ANALYZE` after a bulk load is a common cause.
2. Fix with `ANALYZE <table>`, extended statistics (`CREATE STATISTICS`) for correlated columns, or a higher `default_statistics_target` on skewed columns.
3. If the plan stays bad, rewrite the query: the app's **Draft a rewrite** verifies an LLM-drafted rewrite on a branch. (pg_hint_plan pins need its library loaded, which Lakebase refuses without a support request.)

### 15. Bad plans: disk spills, hash spills, misestimates, missed index scans
Uses `plan_history.plan_json`, which requires `databricks_pii_access` membership.

```sql
SELECT queryid, max(query_text) AS query, count(*) AS execs,
       percentile_approx(duration_ms, 0.95) AS p95_ms,
       max(temp_blks_written) AS max_temp_blks,
       max(CASE WHEN plan_json RLIKE '"Sort Space Type":\\s*"Disk"'         THEN 1 ELSE 0 END) AS disk_sort,
       max(CASE WHEN plan_json RLIKE '"Hash Batches":\\s*([2-9]|[1-9][0-9]+)' THEN 1 ELSE 0 END) AS hash_spill,
       max(CASE WHEN plan_json RLIKE '"Node Type":\\s*"Seq Scan"'            THEN 1 ELSE 0 END) AS has_seq_scan,
       max(CASE WHEN plan_json RLIKE '"Node Type":\\s*"Nested Loop"'         THEN 1 ELSE 0 END) AS has_nested_loop,
       max(greatest(actual_rows, 1) / greatest(plan_rows, 1))                                   AS max_underestimate_x,
       max(greatest(plan_rows, 1) / greatest(actual_rows, 1))                                   AS max_overestimate_x
FROM system.lakebase.plan_history
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS AND analyze_enabled
  -- writes: the top node (ModifyTable) returns 0 rows, so row-estimate ratios are meaningless
  AND NOT plan_json RLIKE '"Node Type":\\s*"ModifyTable"'
GROUP BY queryid
HAVING disk_sort = 1 OR hash_spill = 1 OR max_underestimate_x > 10 OR max_overestimate_x > 10
ORDER BY p95_ms DESC LIMIT 25;
```
Live per‑DB spill totals: `SELECT datname, temp_files, pg_size_pretty(temp_bytes) FROM pg_stat_database;`

**Expected:** no disk sorts or hash spills on hot queries; estimates within 10× of actuals.

The engine's `row_misestimate` check reports one finding per endpoint, not one per query. A query counts only
with ≥ 50 calls, ≥ 1 s total time, ≥ 3 sampled executions over ≥ 2 hours and a **median** misestimate factor of
≥ 10×. Severity is those queries' share of the endpoint's DB time: info above 0%, warning above 10%, critical
above 50%. The evidence lists the 10 worst queries by time (queryid, calls, total ms, estimated vs. actual rows,
factor). Platform roles such as `databricks_writer_<n>` are excluded.

**Fix playbook (each fix should be tested on a branch, see DESIGN.md):**

| Symptom | Fix | pg_hint_plan directive (only if Lakebase loads the library) |
|---|---|---|
| Disk sort / hash batches > 1 | Raise `work_mem` **per role or session** (`ALTER ROLE app SET work_mem='64MB'`), not globally. Add an index that delivers the sort order. | `/*+ Set(work_mem "64MB") */` |
| Seq scan on a selective predicate | Create the index (B‑tree, partial, or covering `INCLUDE`). | `/*+ IndexScan(t idx_name) */` |
| Nested loop with a 100× underestimate | `ANALYZE`, `CREATE STATISTICS (dependencies, ndistinct)` on the correlated columns. | `/*+ HashJoin(a b) */` or `/*+ Rows(a b #10000) */` |
| Wrong join order | Rewrite the query (CTE `MATERIALIZED`, explicit join), or fix the estimates. | `/*+ Leading((a b) c) */` |

pg_hint_plan on Lakebase (tested 2026-10-29): the extension installs, but its library is not loaded and non-superusers may not `LOAD` it, add it to `session_preload_libraries` or set `pg_hint_plan.enable_hint_table`, so the directives above do nothing until a support request loads it per endpoint. Where it is loaded: `CREATE EXTENSION pg_hint_plan;`, then prefix the query with the hint comment. Alternatively, register hints in `hint_plan.hints` by `query_id` with `pg_hint_plan.enable_hint_table = on`, which pins a plan without changing app code. Treat hints as technical debt: record why each one exists, and re‑validate after major version upgrades.

### 16. Storage (pageserver) wait per query (Lakebase‑specific)
**What:** how much of each query's time goes to fetching pages from remote storage after an LFC miss.

```sql
SELECT queryid, max(query_text) AS query, count(*) AS execs,
       round(avg(duration_ms), 2)                                   AS avg_ms,
       round(avg(neon_getpage_wait_us) / 1000, 2)                   AS avg_getpage_wait_ms,
       round(100 * avg(neon_getpage_wait_us / 1000.0 / NULLIF(duration_ms, 0)), 1) AS pct_time_in_storage,
       round(100.0 * sum(neon_file_cache_hits) / NULLIF(sum(neon_file_cache_hits) + sum(neon_getpage_count), 0), 1) AS lfc_hit_pct
FROM system.lakebase.plan_history
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY queryid HAVING count(*) >= 10
ORDER BY sum(neon_getpage_wait_us) DESC LIMIT 25;
```
**Expected:** < 10% of query time in storage waits.
**If high:** the query reads cold pages. Index it so it reads fewer pages, increase the cache (min CU), or `pg_prewarm` the hot relations after resume.

### 17. Wait‑event profile (where time goes)
```sql
SELECT wait_class_name, wait_event_name,
       sum(wait_event_count) AS waits, round(sum(wait_event_time), 1) AS wait_s,
       round(100.0 * sum(wait_event_time) / sum(sum(wait_event_time)) OVER (), 1) AS pct_of_wait_time
FROM system.lakebase.wait_event_counters
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
  -- exclude idle loops of background/platform processes (autovacuum launcher, Neon monitors, etc.)
  AND wait_class_name NOT IN ('Activity')
  AND NOT (wait_class_name = 'Extension' AND wait_event_name RLIKE '^Neon')
GROUP BY ALL ORDER BY wait_s DESC LIMIT 20;
```
```sql
-- Which queries are waiting on what (ASH sampling, like AWS Performance Insights)
SELECT coalesce(wait_event_type, 'CPU') AS wait_type, coalesce(wait_event, 'running') AS wait_event,
       queryid, count(*) AS samples,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_samples
FROM system.lakebase.active_session_history
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY ALL ORDER BY samples DESC LIMIT 25;
```
> Tested: `wait_event_time` is in **seconds**. Always‑on background waits added up to ~2,041 per event over a ~34‑minute window. Unfiltered, platform background waits (`Activity:*Main`, `Extension:Neon*`) made up about 30% of the profile, which is why the query excludes them.

**Expected:** CPU and IO dominate; `Lock` < 10%; `LWLock` small; `Client:ClientRead` is app think‑time.
**If off:**

| Wait type | Fix |
|---|---|
| `Lock` | Check 17b (blocking) and 20 (FK indexes). |
| `LWLock:BufferMapping`, `LWLock:WALWrite` | Memory or WAL pressure; see checks 4 and 7. |
| `IO` / storage waits | Check 16. |
| `IPC`, parallel waits | Tune parallelism. |
| `Timeout:PgSleep` | App code is sleeping in SQL. |

### 17b. Blocking chains, long‑running and idle‑in‑transaction sessions (live)
```sql
-- Who blocks whom
SELECT a.pid AS blocked_pid, a.usename, a.application_name,
       now() - a.query_start AS blocked_for, left(a.query, 120) AS blocked_query,
       b.pid AS blocking_pid, b.state AS blocking_state,
       now() - b.xact_start AS blocking_xact_age, left(b.query, 120) AS blocking_query
FROM pg_stat_activity a
CROSS JOIN LATERAL unnest(pg_blocking_pids(a.pid)) AS bp(pid)
JOIN pg_stat_activity b ON b.pid = bp.pid
ORDER BY blocked_for DESC;

-- Long transactions and idle-in-transaction sessions
SELECT pid, usename, application_name, state,
       now() - xact_start  AS xact_age, now() - state_change AS in_state_for,
       wait_event_type, wait_event, left(query, 120) AS query
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND (state LIKE 'idle in transaction%' OR now() - xact_start > interval '5 minutes')
ORDER BY xact_start;

-- Lock inventory
SELECT locktype, mode, granted, count(*) FROM pg_locks GROUP BY locktype, mode, granted ORDER BY count(*) DESC;
```
Historical view: `active_session_history` rows where `blocking_pid IS NOT NULL`, or `plan_history.lock_wait_time_ms > 0`.
**Expected:** no blocker older than about 1 minute; no idle‑in‑transaction session older than about 1 minute.
**If off:** in an emergency, `SELECT pg_cancel_backend(pid)` first, then `pg_terminate_backend(pid)`. Durable fixes: `idle_in_transaction_session_timeout`, `lock_timeout` for DDL (`SET lock_timeout='5s'` before an `ALTER`), `statement_timeout` per role, and short transactions.

### 18. Errors from Postgres logs
```sql
SELECT sqlstate, severity, regexp_replace(message, '[0-9]+', 'N') AS message_pattern,
       count(*) AS occurrences, min(ts) AS first_seen, max(ts) AS last_seen,
       any_value(usename, true) AS sample_user, any_value(application_name, true) AS sample_app  -- not collect_set(..)[0]: fails on empty arrays under ANSI mode
FROM system.lakebase.postgres_logs
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
  AND severity IN ('ERROR', 'FATAL', 'PANIC')
GROUP BY ALL ORDER BY occurrences DESC LIMIT 50;
```
| SQLSTATE | Meaning | Fix |
|---|---|---|
| `53300` | too many connections | Check 5 |
| `53100`/`53200` | disk full / out of memory | Checks 6, 4, 15 |
| `57014` | statement_timeout cancel | Check 13 (query perf) |
| `25P03` | idle‑in‑tx timeout | App transaction scoping |
| `40P01` / `40001` | deadlock / serialization | Check 11, retries |
| `55P03` | lock_not_available | DDL lock contention |
| `XX000` / `XX001` | internal / data corruption | Open a support case |
| `28P01`/`28000` | auth failures | Security audit (check 29) |
| `42xxx`, `23xxx` | app bugs / constraint violations | Route to the app team |

Also watch `LOG`‑level messages containing `canceling autovacuum task` (autovacuum keeps being preempted, so see check 22).

---

## D. Indexing strategy

### 19. Unused, duplicate, redundant and invalid indexes (Postgres)
Check the statistics window (0.1) before acting.
```sql
-- 19a. Unused indexes (excludes PK/unique/constraint-backing indexes)
SELECT s.schemaname, s.relname AS table, s.indexrelname AS index, s.idx_scan,
       pg_size_pretty(pg_relation_size(s.indexrelid)) AS size,
       'DROP INDEX CONCURRENTLY ' || quote_ident(s.schemaname) || '.' || quote_ident(s.indexrelname) || ';' AS candidate_ddl
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0 AND NOT i.indisunique AND NOT i.indisprimary
  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid)
ORDER BY pg_relation_size(s.indexrelid) DESC;

-- 19b. Exact duplicates (same table, columns, opclasses, expressions, predicate)
SELECT pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS total_size,
       array_agg(idx::text ORDER BY idx::text) AS duplicate_set
FROM (SELECT indexrelid::regclass AS idx,
             indrelid::text || '|' || indclass::text || '|' || indkey::text || '|' ||
             coalesce(indexprs::text, '') || '|' || coalesce(indpred::text, '') AS sig
      FROM pg_index) d
GROUP BY sig HAVING count(*) > 1 ORDER BY sum(pg_relation_size(idx)) DESC;

-- 19c. Left-prefix redundant: (a) is covered by (a,b) on the same table and access method
SELECT a.indrelid::regclass AS table, a.indexrelid::regclass AS redundant_index,
       b.indexrelid::regclass AS covered_by, pg_size_pretty(pg_relation_size(a.indexrelid)) AS size
FROM pg_index a
JOIN pg_index b  ON a.indrelid = b.indrelid AND a.indexrelid <> b.indexrelid
JOIN pg_class ca ON ca.oid = a.indexrelid
JOIN pg_class cb ON cb.oid = b.indexrelid
WHERE ca.relam = cb.relam AND NOT a.indisunique
  AND a.indpred IS NULL AND b.indpred IS NULL AND a.indexprs IS NULL AND b.indexprs IS NULL
  AND b.indkey::text LIKE a.indkey::text || ' %';

-- 19d. Invalid indexes (failed CREATE INDEX CONCURRENTLY); they cost writes and serve no reads
SELECT indexrelid::regclass AS index, indrelid::regclass AS table
FROM pg_index WHERE NOT indisvalid;
```
**Expected:** none.
**Fix:** `DROP INDEX CONCURRENTLY`. For invalid indexes, use `REINDEX INDEX CONCURRENTLY` or drop and recreate. **Guardrails:**
- The stats window must cover a full business cycle.
- Check read replicas too. Scans on replicas don't show up in the primary's `idx_scan`.
- Test the drop on a branch first, by replaying the top queries.
- Keep the `CREATE INDEX` DDL (`pg_get_indexdef(indexrelid)`) for rollback.

### 20. Missing indexes on foreign keys
```sql
SELECT c.conrelid::regclass AS child_table, c.conname, pg_get_constraintdef(c.oid) AS fk,
       pg_size_pretty(pg_relation_size(c.conrelid)) AS child_size,
       'CREATE INDEX CONCURRENTLY ON ' || c.conrelid::regclass || ' (' ||
         (SELECT string_agg(quote_ident(attname), ', ' ORDER BY array_position(c.conkey, attnum))
          FROM pg_attribute WHERE attrelid = c.conrelid AND attnum = ANY (c.conkey)) || ');' AS candidate_ddl
FROM pg_constraint c
WHERE c.contype = 'f'
  AND NOT EXISTS (
    SELECT 1 FROM pg_index i
    WHERE i.indrelid = c.conrelid
      AND (i.indkey::text || ' ') LIKE array_to_string(c.conkey, ' ') || ' %')
ORDER BY pg_relation_size(c.conrelid) DESC;
```
**Why it matters:** without an index on the referencing columns, every `DELETE` or key `UPDATE` on the parent seq‑scans the child table and holds locks the whole time. That is a classic source of lock waits and deadlocks.
**Fix:** create the index (`CONCURRENTLY`). Small, rarely‑modified parents can be skipped.

### 21. Seq‑scan‑heavy tables (missing‑index candidates)
```sql
SELECT schemaname, relname, seq_scan, idx_scan, n_live_tup,
       seq_tup_read, round(seq_tup_read::numeric / NULLIF(seq_scan, 0)) AS avg_rows_per_seq_scan,
       round(100.0 * seq_scan / NULLIF(seq_scan + coalesce(idx_scan, 0), 0), 1) AS seq_scan_pct,
       pg_size_pretty(pg_relation_size(relid)) AS size
FROM pg_stat_user_tables
WHERE n_live_tup > 10000 AND seq_scan > 0
ORDER BY seq_tup_read DESC LIMIT 20;
```
Then find the queries that scan those tables: check 15 filtered with `plan_json RLIKE '"Relation Name":\\s*"<table>"'`, or check 13 with `hit_pct` / `rows_per_call`.
**Expected:** a large table with `avg_rows_per_seq_scan` > 10k on an OLTP path is a finding.
**Fix and suggestion engine:**
1. Extract the filter and join columns from the `Seq Scan` nodes in `plan_json` (`"Filter"`, `"Rows Removed by Filter"`).
2. Propose a B‑tree on the equality columns first, then range columns, with `INCLUDE` for covered columns.
3. **HypoPG is not available on Lakebase.** Validate the index instead with a real `CREATE INDEX` on a **copy‑on‑write branch**, replay the queries with `EXPLAIN (ANALYZE, BUFFERS)`, and compare cost and time. That is stronger evidence than a hypothetical index, because it measures real execution. See DESIGN.md for the confidence score.

---

## E. Maintenance: vacuum, bloat, wraparound, sequences

### 22. Dead tuples and autovacuum lag (with per‑table tuning candidates)
```sql
SELECT schemaname, relname, n_live_tup, n_dead_tup,
       round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 1) AS dead_pct,
       n_mod_since_analyze, last_autovacuum, last_autoanalyze, autovacuum_count,
       round(current_setting('autovacuum_vacuum_threshold')::int
             + current_setting('autovacuum_vacuum_scale_factor')::float * n_live_tup) AS autovac_trigger_at,
       (SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid = relid) AS table_overrides,
       CASE WHEN (SELECT reloptions FROM pg_class WHERE oid = relid) @> ARRAY['autovacuum_enabled=false']
            THEN format('ALTER TABLE %I.%I RESET (autovacuum_enabled);', schemaname, relname)  -- autovacuum is DISABLED on this table
            WHEN n_live_tup > 100000 AND n_dead_tup > 0.1 * n_live_tup
            THEN format('ALTER TABLE %I.%I SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.01);', schemaname, relname)
       END AS tuning_candidate
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC LIMIT 25;
```
**Expected:** dead < 10% (< 20% on very high‑churn tables), and `last_autovacuum` recent on hot tables.
**If high:**
1. Find what blocks cleanup first (check 25). A long transaction makes vacuum useless.
2. On big tables, lower the per‑table `autovacuum_vacuum_scale_factor`. The default of 0.2 means a 100M‑row table waits for 20M dead rows.
3. Run a manual `VACUUM (ANALYZE, VERBOSE) <table>` as a one‑off.
4. **Verification plan:** in the next 24h, `autovacuum_count` should increase, `dead_pct` should stay under 10%, and the table's top queries should hold their latency.

### 23. Table and index bloat
```sql
CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- Tables: approximate scan of the 15 largest (reads pages, so run off-peak or on a branch)
SELECT t.schemaname, t.relname, pg_size_pretty(pg_table_size(t.relid)) AS size,
       round(a.approx_free_percent::numeric, 1) AS free_pct,
       round(a.dead_tuple_percent::numeric, 1) AS dead_pct,
       pg_size_pretty((pg_table_size(t.relid) * a.approx_free_percent / 100)::bigint) AS reclaimable_est
FROM (SELECT relid, schemaname, relname FROM pg_stat_user_tables ORDER BY pg_table_size(relid) DESC LIMIT 15) t
CROSS JOIN LATERAL pgstattuple_approx(t.relid) a
ORDER BY a.approx_free_percent DESC;

-- B-tree indexes: leaf density (pgstatindex reads the whole index, so limit it to large ones)
SELECT s.schemaname, s.indexrelname, pg_size_pretty(pg_relation_size(s.indexrelid)) AS size,
       x.avg_leaf_density, x.leaf_fragmentation
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
JOIN pg_class c ON c.oid = s.indexrelid
JOIN pg_am am ON am.oid = c.relam AND am.amname = 'btree'
CROSS JOIN LATERAL pgstatindex(s.indexrelid) x
WHERE pg_relation_size(s.indexrelid) > 100 * 1024 * 1024
ORDER BY x.avg_leaf_density ASC LIMIT 15;
```
**Expected:** table free space < 20%; B‑tree `avg_leaf_density` > 70%. A fresh B‑tree sits around 90%.
**Fix (pg_repack is not available):**

| Object | Fix |
|---|---|
| Index | `REINDEX INDEX CONCURRENTLY <idx>` (online). |
| Table | `VACUUM` only makes space reusable; it does not shrink the table. To shrink: `VACUUM FULL` (takes an ACCESS EXCLUSIVE lock, so use a maintenance window), or create a new table, backfill it, and swap. |

Then prevent recurrence with check 22 tuning and a `fillfactor` of 80–90 on update‑heavy tables.

### 24. Transaction ID (XID) and multixact wraparound runway, with forecast
```sql
-- Postgres: per database and the worst tables
SELECT datname, age(datfrozenxid) AS xid_age, mxid_age(datminmxid) AS mxid_age,
       round(100.0 * age(datfrozenxid) / 2147483647, 2) AS pct_of_wraparound,
       current_setting('autovacuum_freeze_max_age')::bigint AS freeze_max_age
FROM pg_database ORDER BY xid_age DESC;

SELECT c.oid::regclass AS table, age(c.relfrozenxid) AS xid_age, mxid_age(c.relminmxid) AS mxid_age,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM pg_class c WHERE c.relkind IN ('r', 'm', 't')
ORDER BY age(c.relfrozenxid) DESC LIMIT 15;
```
```sql
-- Databricks SQL: trend and days until 1.5B (the point where Postgres starts warning hard)
SELECT datname,
       max_by(oldest_frozen_xid_age, ts)                         AS current_xid_age,
       regr_slope(oldest_frozen_xid_age, unix_timestamp(ts)) * 86400 AS xid_age_growth_per_day,
       (1500000000 - max_by(oldest_frozen_xid_age, ts))
         / NULLIF(regr_slope(oldest_frozen_xid_age, unix_timestamp(ts)) * 86400, 0) AS days_to_1_5B,
       max_by(oldest_mxid_age, ts)                                AS current_mxid_age
FROM system.lakebase.database_gauges
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY datname;
```
**Expected:** age stays near or below `autovacuum_freeze_max_age` (200M default) and saw‑tooths down as anti‑wraparound vacuums complete. A steadily rising line is the alarm.
**If rising:** remove freeze blockers (check 25), then `VACUUM (FREEZE, VERBOSE)` the oldest tables, largest‑age first. Lower `autovacuum_freeze_max_age` per table on huge append‑only tables so freezing happens in smaller increments.

### 25. Freeze and vacuum blockers
```sql
-- Sessions holding back the xmin horizon
SELECT pid, usename, application_name, state, backend_xmin, age(backend_xmin) AS xmin_age,
       now() - xact_start AS xact_age, left(query, 100) AS query
FROM pg_stat_activity WHERE backend_xmin IS NOT NULL
ORDER BY age(backend_xmin) DESC LIMIT 10;

-- Forgotten two-phase transactions
SELECT gid, prepared, owner, database, age(transaction) AS xid_age FROM pg_prepared_xacts;

-- Replication slots holding xmin / catalog_xmin / WAL (logical replication, CDC, Lakehouse Sync)
SELECT slot_name, slot_type, plugin, active, xmin, age(xmin) AS xmin_age,
       catalog_xmin, age(catalog_xmin) AS catalog_xmin_age,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots
WHERE slot_name NOT IN ('wal_proposer_slot', 'wal_retention_slot');   -- Lakebase-internal physical slots; never drop
```
> Tested: every Lakebase compute has the platform slots `wal_proposer_slot` (active) and `wal_retention_slot` (normally **inactive**, retaining a few MB). They are not findings, so the query excludes them.
**Expected:** no session xmin older than about 1h of XIDs; no prepared transactions; no inactive slots.
**Fix:**
- Terminate the stale session.
- `ROLLBACK PREPARED '<gid>'` for orphaned two‑phase transactions.
- Drop abandoned slots with `pg_drop_replication_slot`, but **confirm first** that the slot isn't owned by Lakehouse Sync or another CDC consumer.
- On read replicas, long queries with `hot_standby_feedback` also hold back the primary's horizon. Set a `statement_timeout` for replica roles.

### 26. Sequence and identity exhaustion, with forecast
```sql
-- Exhaustion against the *column* type (an int4 column on a bigint sequence still fails at 2^31-1)
WITH seq_cols AS (
  SELECT n.nspname AS schemaname, sc.relname AS sequencename,
         d.refobjid::regclass AS table_name, a.attname AS column_name,
         format_type(a.atttypid, a.atttypmod) AS column_type,
         CASE a.atttypid WHEN 'int2'::regtype THEN 32767
                         WHEN 'int4'::regtype THEN 2147483647
                         ELSE 9223372036854775807 END AS column_max,
         d.refobjid AS table_oid
  FROM pg_depend d
  JOIN pg_class sc     ON sc.oid = d.objid AND sc.relkind = 'S'
  JOIN pg_namespace n  ON n.oid = sc.relnamespace
  JOIN pg_attribute a  ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
  WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
)
SELECT sc.table_name, sc.column_name, sc.column_type, ps.last_value,
       least(sc.column_max, ps.max_value) AS effective_max,
       round(100.0 * ps.last_value / least(sc.column_max, ps.max_value), 2) AS pct_used,
       -- rough runway: insert rate since stats reset
       round((least(sc.column_max, ps.max_value) - ps.last_value)
             / NULLIF(t.n_tup_ins / NULLIF(extract(epoch FROM now() - coalesce(
                 (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()),
                 pg_postmaster_start_time())), 0), 0) / 86400) AS est_days_left
FROM seq_cols sc
JOIN pg_sequences ps ON ps.schemaname = sc.schemaname AND ps.sequencename = sc.sequencename
LEFT JOIN pg_stat_user_tables t ON t.relid = sc.table_oid
WHERE ps.last_value IS NOT NULL
ORDER BY pct_used DESC;
```
**Expected:** < 50% used, and more than 1 year left.
**Fix:** `ALTER TABLE … ALTER COLUMN id TYPE bigint`. This rewrites the table, so on large tables add a new bigint column, backfill in batches, and swap during a window, and rehearse it on a branch. Also `ALTER SEQUENCE … AS bigint`. For a more accurate runway, snapshot `last_value` daily (DESIGN.md, history store).

### 27. Replication: read‑replica lag and replication slots
```sql
SELECT endpoint_id, date_trunc('HOUR', ts) AS hour,
       max(replication_lag_bytes) / power(1024, 2) AS max_lag_mb,
       round(max(replication_lag_time_ms) / 1000, 1) AS max_lag_s,
       round(percentile_approx(replication_lag_time_ms, 0.95) / 1000, 1) AS p95_lag_s
FROM system.lakebase.compute_gauges
WHERE project_id = :project_id AND compute_mode <> 'primary'
  AND ts >= current_timestamp() - INTERVAL 24 HOURS AND replication_lag_bytes IS NOT NULL
GROUP BY endpoint_id, date_trunc('HOUR', ts) ORDER BY hour DESC;

-- Replica cache churn caused by redo (replica_lfc_redo_evictions)
SELECT endpoint_id, date_trunc('HOUR', ts) AS hour, sum(replica_lfc_redo_evictions) AS redo_evictions
FROM system.lakebase.compute_counters
WHERE project_id = :project_id AND compute_mode <> 'primary' AND ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY ALL ORDER BY hour DESC;
```
> Tested: the read-write compute reports `compute_mode = 'primary'` (not 'read-write' as the docs example suggests). Replica rows are everything else.

Replication slots are covered in check 25.
**Expected:** p95 lag < 1 s.
**If high:** a replica sized smaller than the primary can't keep up with redo, so match the replica's CU. Long replica queries conflict with replay. Bursty bulk writes on the primary are the third cause; see check 7.

---

## F. Resilience, security, configuration, change

### 28. Backup and recovery posture (Lakebase API)
Postgres can't see this; it comes from the Postgres API (Python SDK `w.postgres`, verified against `databricks-sdk` 0.96).
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile="<PROFILE>")
proj = w.postgres.get_project(name="projects/<PROJECT_ID>")
print("restore window:", proj.status.history_retention_duration)          # 2–30 days, default 7d
print("branch size limit:", proj.status.branch_logical_size_limit_bytes)
for b in w.postgres.list_branches(parent=proj.name):
    print(b.name, "protected:", b.status.is_protected, "default:", b.status.default,
          "expires:", b.status.expire_time, "size:", b.status.logical_size_bytes)
    for e in w.postgres.list_endpoints(parent=b.name):
        s = e.status
        print("  ", e.name, s.endpoint_type, s.current_state,
              "CU:", s.autoscaling_limit_min_cu, "-", s.autoscaling_limit_max_cu,
              "suspend:", s.suspend_timeout_duration, "HA group:", s.group)   # group.min/max = HA computes
# Snapshots: the snapshots API (create/list) is in newer SDK versions. Check w.postgres for list_snapshots.
```
**Expected for production:**
- A restore window of 7 days or more; 14–30 for regulated workloads.
- The production branch has `is_protected = true`.
- An HA group (`group.min ≥ 2`) on the prod read‑write endpoint.
- Scheduled snapshots on the root branch, plus a manual snapshot before every migration.
- At least one rehearsed PITR (create a branch at a timestamp, verify it, delete it) per quarter.
- Root‑branch headroom: the maximum is 3, and each PITR consumes one.

**Fix:** set these through `update-project`, `update-branch` and `update-endpoint`. Automate the restore drill as a job.

### 29. Security audit
```sql
-- Members of databricks_superuser (keep small)
SELECT m.rolname AS member, m.rolcanlogin
FROM pg_auth_members am JOIN pg_roles g ON g.oid = am.roleid JOIN pg_roles m ON m.oid = am.member
WHERE g.rolname = 'databricks_superuser';

-- Powerful role attributes (excludes platform-managed roles)
SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolbypassrls, rolreplication, rolcanlogin, rolvaliduntil
FROM pg_roles
WHERE rolname NOT LIKE 'pg\_%' AND rolname NOT IN ('cloud_admin', 'databricks_control_plane', 'databricks_superuser')
  AND (rolcreaterole OR rolcreatedb OR rolbypassrls OR rolreplication OR rolsuper);

-- PUBLIC may create in schemas
SELECT nspname FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND nspname NOT LIKE 'pg\_%'
  AND has_schema_privilege('public', oid, 'CREATE');

-- Non-SSL client sessions (platform agents connect locally as cloud_admin without SSL; ignore them)
SELECT a.usename, a.client_addr, s.ssl, s.version FROM pg_stat_ssl s JOIN pg_stat_activity a USING (pid)
WHERE a.backend_type = 'client backend' AND NOT s.ssl
  AND a.usename <> 'cloud_admin' AND a.client_addr IS DISTINCT FROM '127.0.0.1' AND a.client_addr IS DISTINCT FROM '::1';

-- Tables without a primary key (also breaks logical replication/CDC)
SELECT c.oid::regclass FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = c.oid AND contype = 'p');
```
```sql
-- Databricks SQL: authentication failures
SELECT usename, client_addr, count(*) AS failures, max(ts) AS last_seen
FROM system.lakebase.postgres_logs
WHERE project_id = :project_id AND sqlstate IN ('28P01', '28000') AND ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY failures DESC;
```
API: in `w.postgres.list_roles(parent=branch)`, each role's `status.auth_method` should be `LAKEBASE_OAUTH_V1`, not `PG_PASSWORD_SCRAM_SHA_256`. Also check `project.status.enable_pg_native_login`.
**Expected:** OAuth roles only; superuser membership limited to admins; no PUBLIC CREATE; every session uses SSL.
**Fix:** migrate password roles to OAuth roles. `REVOKE CREATE ON SCHEMA public FROM PUBLIC`. Use least‑privilege group roles. Disable native PG login if it isn't needed.

### 30. Config drift and guardrail settings
```sql
-- Everything that differs from defaults, and where it was set (platform-internal GUCs excluded)
SELECT name, setting, unit, source, boot_val
FROM pg_settings
WHERE source NOT IN ('default', 'override')
  AND name !~ '^(databricks|neon|neon_monitor|compute_ctl|lakebase)\.' AND name NOT LIKE 'neon\_%'
ORDER BY name;

-- Per-role and per-database overrides
SELECT coalesce(r.rolname, '*') AS role, coalesce(d.datname, '*') AS db, s.setconfig
FROM pg_db_role_setting s LEFT JOIN pg_roles r ON r.oid = s.setrole LEFT JOIN pg_database d ON d.oid = s.setdatabase;

-- Guardrail baseline
SELECT name, setting, unit,
       CASE
         WHEN name = 'idle_in_transaction_session_timeout' AND (setting = '0' OR setting::bigint > 60000) THEN 'lower to 30-60s (Lakebase default is 5min)'
         WHEN name = 'statement_timeout' AND setting = '0' THEN 'consider per-role timeout'
         WHEN name = 'lock_timeout' AND setting = '0' THEN 'set per-session for DDL'
         WHEN name = 'track_io_timing' AND setting = 'off' THEN 'enable for I/O attribution'
         ELSE 'ok'
       END AS advice
FROM pg_settings
WHERE name IN ('idle_in_transaction_session_timeout', 'statement_timeout', 'lock_timeout', 'idle_session_timeout',
               'work_mem', 'maintenance_work_mem', 'random_page_cost', 'track_io_timing',
               'autovacuum', 'autovacuum_vacuum_scale_factor', 'autovacuum_freeze_max_age',
               'max_parallel_workers_per_gather', 'default_statistics_target', 'log_min_duration_statement');
```
**Drift detection:** Postgres defaults are the wrong baseline. Tested: a fresh Lakebase compute already differs from them in about 55 settings, all set by the platform (for example `fsync=off` and `full_page_writes=off`, which are expected on Lakebase storage). Store the output per branch and endpoint, then diff it against **the previous run** and **between prod and staging branches**. The API exposes Lakebase‑managed overrides as `endpoint.status.settings.pg_settings` and `project.status.default_endpoint_settings`.
**Fix:** set guardrails via endpoint `pg_settings` or `ALTER ROLE … SET`, and keep the baseline in source control.

### 31. Schema change audit (and regression correlation)
```sql
SELECT ts, usename, application_name, command_tag, object_type, schema_name, object_name,
       round(duration_ms) AS duration_ms, query_text
FROM system.lakebase.ddl_history
WHERE project_id = :project_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY ts DESC;
```
Join this with check 14. A regression that starts within minutes of a `DROP INDEX`, an `ALTER TABLE` or a bulk load is the likely cause.
**Expected:** DDL only from the migration tool or service principal, inside release windows.
**Fix:**
1. Route DDL through migrations.
2. Take a snapshot before each migration.
3. Use `lock_timeout` on DDL.
4. Use `CREATE INDEX CONCURRENTLY`.

Note that DDL run right around a compute resume can be missed in this table.

---

## Predictive limits summary ("warn before it happens")

| Limit | Source | Forecast method |
|---|---|---|
| Storage | `compute_gauges.total_size` / `total_size_limit` (7d) | `regr_slope` → days to full (check 6) |
| XID / MXID | `database_gauges.oldest_frozen_xid_age` / `oldest_mxid_age` | slope → days to 1.5B (check 24) |
| Sequences | `pg_sequences` + column type | insert rate → days left (check 26); better with daily snapshots |
| Connections | `database_gauges.numbackends` vs `compute_gauges.max_connections` | p99 by hour‑of‑week plus trend; alert when the projected peak exceeds 85% |
| CPU / cache | `compute_counters` / `compute_gauges` | p95 trend against the autoscale ceiling; alert when the ceiling is hit more than N times per day |

`system.lakebase` keeps 7 days. For forecasts over weeks or months, persist a daily rollup of these gauges into a Delta table; DESIGN.md covers this.
