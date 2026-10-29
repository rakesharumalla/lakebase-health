SELECT datname, max_by(size, ts) AS size_bytes, max_by(numbackends, ts) AS backends,
       max_by(oldest_frozen_xid_age, ts) AS xid_age, max(ts) AS at
FROM {catalog}.lakebase_health.raw_database_gauges
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY datname
ORDER BY size_bytes DESC
