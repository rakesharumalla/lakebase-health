WITH g AS (
  SELECT date_trunc('MINUTE', ts) AS minute, max(max_connections) AS max_connections
  FROM {catalog}.lakebase_health.raw_compute_gauges
  WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts AND array_contains(split(:project_ids, ','), project_id)
  GROUP BY ALL
), c AS (
  SELECT date_trunc('MINUTE', ts) AS minute, sum(host_cpu_seconds) / 60.0 AS cpu_cores_used
  FROM {catalog}.lakebase_health.raw_compute_counters
  WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts AND array_contains(split(:project_ids, ','), project_id)
  GROUP BY ALL
)
SELECT coalesce(g.minute, c.minute) AS minute, g.max_connections, c.cpu_cores_used
FROM g FULL OUTER JOIN c ON c.minute = g.minute
ORDER BY minute
