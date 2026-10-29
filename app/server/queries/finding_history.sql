SELECT ts, measure, severity, status
FROM {catalog}.lakebase_health.check_results
WHERE fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), project_id)
  AND ts >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY ts
