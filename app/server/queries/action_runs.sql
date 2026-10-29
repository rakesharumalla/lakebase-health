SELECT r.run_id, r.approval_id, r.kind, r.started_at, r.ended_at, r.outcome, r.report, r.branch, r.error, ap.approver
FROM {catalog}.lakebase_health.action_runs r
JOIN {catalog}.lakebase_health.proposed_actions a ON a.action_id = r.action_id
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
LEFT JOIN {catalog}.lakebase_health.approvals ap ON ap.approval_id = r.approval_id
WHERE r.action_id = :action_id AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY r.started_at DESC
LIMIT 50
