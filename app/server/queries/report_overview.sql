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
