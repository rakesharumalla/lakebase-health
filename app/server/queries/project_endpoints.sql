SELECT DISTINCT endpoint_id FROM (
  SELECT project_id, endpoint_id FROM {catalog}.lakebase_health.inventory
  WHERE project_id = :project_id AND array_contains(split(:endpoint_ids, ','), endpoint_id)
  UNION ALL
  SELECT project_id, endpoint_id FROM {catalog}.lakebase_health.raw_compute_gauges
  WHERE project_id = :project_id AND array_contains(split(:endpoint_ids, ','), endpoint_id)
    AND ts >= current_timestamp() - INTERVAL 7 DAYS
)
WHERE array_contains(split(:project_ids, ','), project_id)
