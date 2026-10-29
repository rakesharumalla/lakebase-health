SELECT ts, pid, cast(queryid AS STRING) AS queryid, wait_event_type, wait_event, backend_state, blocking_pid,
       query_age_ms, xact_age_ms
FROM {catalog}.lakebase_health.raw_active_session_history
WHERE endpoint_id = :endpoint_id AND ts >= :at_ts AND ts < :at_ts + INTERVAL 1 MINUTE
  AND array_contains(split(:project_ids, ','), project_id)
ORDER BY query_age_ms DESC
LIMIT 200
