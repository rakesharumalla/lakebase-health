INSERT INTO {catalog}.lakebase_health.approvals (approval_id, action_id, fingerprint, request, sql_hash, approver,
                                                  requested_at, note)
VALUES (:approval_id, NULLIF(:action_id, ''), NULLIF(:fingerprint, ''), :request, :sql_hash, :approver,
        current_timestamp(), :note)
