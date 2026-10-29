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
