SELECT project_id FROM (
  SELECT project_id FROM {catalog}.lakebase_health.inventory WHERE endpoint_id = :endpoint_id
  UNION
  SELECT project_id FROM {catalog}.lakebase_health.raw_compute_gauges
  WHERE endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
)
WHERE array_contains(split(:project_ids, ','), project_id)
LIMIT 1
