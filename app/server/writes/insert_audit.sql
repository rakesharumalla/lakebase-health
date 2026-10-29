INSERT INTO {catalog}.lakebase_health.audit_events (event_id, ts, actor, action, project_id, branch_id, detail)
VALUES (:event_id, current_timestamp(), :actor, :action, :project_id, :branch_id, :detail)
