INSERT INTO {catalog}.lakebase_health.approvals (approval_id, action_id, request, sql_hash, approver, requested_at, note)
VALUES (:approval_id, :action_id, :request, :sql_hash, :approver, current_timestamp(), :note)
