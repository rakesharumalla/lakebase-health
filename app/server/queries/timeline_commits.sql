SELECT date_trunc('MINUTE', ts) AS minute, count(*) AS samples, count(DISTINCT pid) AS sessions,
       max(query_age_ms) / 1000.0 AS max_statement_age_s
FROM {catalog}.lakebase_health.raw_active_session_history
WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts
  AND array_contains(split(:project_ids, ','), project_id) AND wait_event = 'SyncRep'
GROUP BY ALL
ORDER BY minute
