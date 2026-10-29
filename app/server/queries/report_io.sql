SELECT * FROM {catalog}.lakebase_health.inventory_io
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 1 DAY
QUALIFY dense_rank() OVER (ORDER BY collected_at DESC) <= 2
ORDER BY collected_at DESC, backend_type, object, context
