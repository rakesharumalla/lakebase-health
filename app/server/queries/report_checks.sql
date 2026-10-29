SELECT check_id, endpoint_id, status, error, ts,
  -- the last successful run in the last 24 h: a suspended compute (skipped:compute_inactive) keeps that assessment
  max(CASE WHEN status = 'ok' AND ts >= current_timestamp() - INTERVAL 24 HOURS THEN ts END)
    OVER (PARTITION BY check_id, coalesce(endpoint_id, '*')) AS last_ok_at
FROM {catalog}.lakebase_health.check_results
WHERE fingerprint IS NULL AND ts >= current_timestamp() - INTERVAL 2 DAYS AND status <> 'skipped:not_scheduled'
  AND (project_id IS NULL OR (project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)))
QUALIFY row_number() OVER (PARTITION BY check_id, coalesce(endpoint_id, '*') ORDER BY ts DESC) = 1
