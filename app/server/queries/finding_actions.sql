WITH sl AS (
  SELECT action_id, max_by(would_allow_level, evaluated_at) AS would_allow_level,
         max_by(would_execute, evaluated_at) AS would_execute, max_by(reason, evaluated_at) AS reason,
         max(evaluated_at) AS evaluated_at
  FROM {catalog}.lakebase_health.shadow_log GROUP BY action_id
)
SELECT a.action_id, a.kind, a.sql, a.rollback_sql, a.rollback_note, a.risk, a.reversible, a.verify_on_branch, a.state,
       a.expires_at,
       sl.would_allow_level, sl.would_execute, sl.reason, sl.evaluated_at
FROM {catalog}.lakebase_health.proposed_actions a
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
LEFT JOIN sl ON sl.action_id = a.action_id
WHERE a.fingerprint = :fingerprint AND coalesce(a.origin, 'check') = 'check'
  AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY a.created_at DESC
LIMIT 5
