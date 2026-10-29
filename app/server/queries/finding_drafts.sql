WITH v AS (
  SELECT action_id, max_by(report, started_at) AS report, max_by(error, started_at) AS error
  FROM {catalog}.lakebase_health.action_runs
  WHERE kind = 'verify' AND outcome <> 'claimed'
  GROUP BY action_id
), adoption AS (
  SELECT action_id, max_by(report, started_at) AS adoption_report
  FROM {catalog}.lakebase_health.action_runs
  WHERE kind = 'postcheck'
  GROUP BY action_id
)
SELECT d.draft_id, d.attempt, d.state, d.reason, d.rationale, d.expected_plan_change, d.requested_by,
       d.requested_at, d.ended_at, d.endpoint, d.prompt_version, a.sql, v.report, v.error, ad.adoption_report
FROM {catalog}.lakebase_health.drafts d
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = d.finding_fingerprint
LEFT JOIN {catalog}.lakebase_health.proposed_actions a ON a.action_id = d.action_id
LEFT JOIN v ON v.action_id = d.action_id
LEFT JOIN adoption ad ON ad.action_id = d.action_id
WHERE d.finding_fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY d.attempt DESC
LIMIT 20
