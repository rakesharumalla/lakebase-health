SELECT date_trunc('MINUTE', w.ts) AS minute,
       CASE WHEN b.pid IS NULL THEN 'not_sampled' WHEN b.wait_event = 'SyncRep' THEN 'commit_wait'
            WHEN b.backend_state IN (3, 4) THEN 'idle_in_transaction' ELSE 'active' END AS blocker_class,
       count(*) AS samples, max(w.query_age_ms) / 1000.0 AS max_wait_s
FROM {catalog}.lakebase_health.raw_active_session_history w
LEFT JOIN {catalog}.lakebase_health.raw_active_session_history b
  ON b.endpoint_id = w.endpoint_id AND b.ts = w.ts AND b.pid = w.blocking_pid AND b.ts >= :from_ts AND b.ts < :to_ts
WHERE w.endpoint_id = :endpoint_id AND w.ts >= :from_ts AND w.ts < :to_ts
  AND array_contains(split(:project_ids, ','), w.project_id)
  AND w.wait_event_type = 'Lock' AND w.wait_event IN ('tuple', 'transactionid')
GROUP BY ALL
ORDER BY minute
