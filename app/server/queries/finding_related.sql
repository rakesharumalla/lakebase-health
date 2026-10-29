SELECT o.fingerprint, o.check_id, o.title, o.category, o.endpoint_id, o.object, o.status, o.severity, o.confidence,
       o.measure, o.first_seen, o.last_seen, o.occurrences, o.resolved_at,
       get_json_object(o.evidence, '$.queryid') AS queryid,
       to_json(transform(from_json(get_json_object(o.evidence, '$.top_queries'), 'array<struct<queryid:string>>'), x -> x.queryid)) AS top_queryids,
       coalesce(CAST(get_json_object(o.evidence, '$.total_ms') AS DOUBLE), CAST(get_json_object(o.evidence, '$.misestimated_ms') AS DOUBLE)) AS db_ms,
       count(*) OVER () AS total_rows
FROM {catalog}.lakebase_health.findings f
JOIN {catalog}.lakebase_health.findings o ON o.endpoint_id = f.endpoint_id AND o.fingerprint <> f.fingerprint
WHERE f.fingerprint = :fingerprint AND o.status IN ('new', 'open')
  AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY CASE o.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, o.last_seen DESC
LIMIT 2000
