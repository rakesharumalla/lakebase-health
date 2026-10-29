SELECT a.action_id, a.fingerprint, a.check_id, a.kind, a.sql, a.rollback_sql, a.rollback_note, a.risk, a.reversible,
       a.verify_on_branch, a.state, a.created_at, a.updated_at, a.expires_at, f.project_id, f.endpoint_id, f.title, f.object,
       f.status AS finding_status
FROM {catalog}.lakebase_health.proposed_actions a
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
WHERE a.action_id = :action_id AND array_contains(split(:project_ids, ','), f.project_id)
