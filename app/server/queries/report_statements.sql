SELECT * FROM {catalog}.lakebase_health.inventory_statements
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND collected_at >= current_timestamp() - INTERVAL 7 DAYS
QUALIFY collected_at = max(collected_at) OVER ()
ORDER BY ranking, rank
LIMIT 500
