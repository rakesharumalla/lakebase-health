SELECT category, CASE WHEN status = 'resolved' THEN 'resolved' ELSE severity END AS bucket, count(*) AS n
FROM {catalog}.lakebase_health.findings
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND (status IN ('new', 'open') OR (status = 'resolved' AND resolved_at >= current_timestamp() - INTERVAL 7 DAYS))
GROUP BY category, CASE WHEN status = 'resolved' THEN 'resolved' ELSE severity END
