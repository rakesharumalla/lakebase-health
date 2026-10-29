SELECT collected_at, metric, dims, value, note FROM {catalog}.lakebase_health.inventory_endpoint
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 24 HOURS
  AND (metric NOT LIKE 'neon.perf.%' OR metric LIKE 'neon.perf.getpage_wait_seconds_bucket')
ORDER BY collected_at, metric
