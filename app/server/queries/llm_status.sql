-- Not project data; :project_ids keeps every server query scoped (an empty allow-list never reaches here).
SELECT endpoint, enabled, ready, reason, checked_at
FROM {catalog}.lakebase_health.llm_status
WHERE endpoint = :endpoint AND size(split(:project_ids, ',')) > 0
