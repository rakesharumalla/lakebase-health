SELECT fingerprint, check_id, title, category, project_id, branch_id, endpoint_id, object, status, severity,
       confidence, measure, first_seen, last_seen, last_checked_at, occurrences, evidence, query_text, explain,
       candidate_sql, rollback_sql, rollback_note, resolved_at
FROM {catalog}.lakebase_health.findings
WHERE fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), project_id)
