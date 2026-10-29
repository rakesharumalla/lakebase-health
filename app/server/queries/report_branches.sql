SELECT branch_id, branch_name, is_default_branch, endpoint_id, endpoint_type, current_state, min_cu, max_cu,
       suspend_timeout_s, enrolled, captured_at
FROM {catalog}.lakebase_health.inventory
WHERE run_id = (SELECT max_by(run_id, captured_at) FROM {catalog}.lakebase_health.inventory)
  AND project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
ORDER BY is_default_branch DESC, branch_name, endpoint_type DESC, endpoint_id
