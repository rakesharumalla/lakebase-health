SELECT fingerprint, check_id, title, category, branch_id, endpoint_id, object, status, severity, confidence, measure,
       first_seen, last_seen, occurrences, evidence, explain, candidate_sql,
       get_json_object(evidence, '$.queryid') AS queryid,
       to_json(transform(from_json(get_json_object(evidence, '$.top_queries'), 'array<struct<queryid:string>>'), x -> x.queryid)) AS top_queryids,
       coalesce(CAST(get_json_object(evidence, '$.total_ms') AS DOUBLE), CAST(get_json_object(evidence, '$.misestimated_ms') AS DOUBLE)) AS db_ms
FROM {catalog}.lakebase_health.findings
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND (branch_id = :branch_id OR branch_id IS NULL) AND status IN ('new', 'open')
ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, last_seen DESC
LIMIT 2000
