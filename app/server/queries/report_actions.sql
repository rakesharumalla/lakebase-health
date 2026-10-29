SELECT a.action_id, a.fingerprint, a.state
FROM {catalog}.lakebase_health.proposed_actions a
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
WHERE f.project_id = :project_id AND array_contains(split(:project_ids, ','), f.project_id)
  AND (f.branch_id = :branch_id OR f.branch_id IS NULL) AND f.status IN ('new', 'open')
  AND coalesce(a.origin, 'check') = 'check'
  AND a.state IN ('proposed', 'verifying', 'verified', 'verify_failed', 'applying', 'apply_failed', 'blocked')
