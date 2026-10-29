WITH last AS (SELECT * FROM {catalog}.lakebase_health.runs ORDER BY started_at DESC LIMIT 1)
SELECT (SELECT max(ended_at) FROM {catalog}.lakebase_health.runs WHERE status IN ('ok', 'ok_with_errors')) AS last_success_at,
       last.status AS last_status, last.started_at AS last_started_at,
       (SELECT count(*) FROM {catalog}.lakebase_health.findings
         WHERE status IN ('new', 'open') AND array_contains(split(:project_ids, ','), project_id)) AS open_findings
FROM last
