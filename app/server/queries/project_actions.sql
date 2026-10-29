SELECT a.action_id, a.fingerprint, a.check_id, a.kind, a.risk, a.reversible, a.verify_on_branch, a.state,
       a.created_at, a.updated_at, a.expires_at, f.title, f.object, f.severity,
       count(*) OVER (PARTITION BY a.state) AS state_total
FROM {catalog}.lakebase_health.proposed_actions a
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
WHERE f.project_id = :project_id AND array_contains(split(:project_ids, ','), f.project_id)
  AND coalesce(a.origin, 'check') = 'check'
  AND (:state = '' OR a.state = :state)
QUALIFY row_number() OVER (
  PARTITION BY a.state
  ORDER BY CASE f.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, a.updated_at DESC
) <= CAST(:row_limit AS INT)
ORDER BY a.state, CASE f.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, a.updated_at DESC
