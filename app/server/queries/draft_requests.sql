SELECT ap.approval_id, ap.requested_at, ap.approver, r.outcome
FROM {catalog}.lakebase_health.approvals ap
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = ap.fingerprint
LEFT JOIN {catalog}.lakebase_health.action_runs r ON r.approval_id = ap.approval_id
WHERE ap.fingerprint = :fingerprint AND ap.request = 'draft' AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY ap.requested_at DESC
LIMIT 20
