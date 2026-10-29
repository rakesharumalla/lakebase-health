SELECT date_trunc('MINUTE', ts) AS minute, coalesce(wait_event_type, 'CPU') AS wait_class, count(*) / 60.0 AS avg_sessions
FROM {catalog}.lakebase_health.raw_active_session_history
WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts
  AND array_contains(split(:project_ids, ','), project_id) AND backend_state = 2
GROUP BY ALL
ORDER BY minute
