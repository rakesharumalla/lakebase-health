SELECT ts, kind, sqlstate, compute_mode FROM (
  SELECT ts, sqlstate, compute_mode,
         CASE WHEN sqlstate = '57014' AND message LIKE 'canceling statement due to statement timeout%' THEN 'statement_timeout'
              WHEN message RLIKE '(?i)^(SSL error: unexpected eof|could not accept SSL connection: (EOF detected|Connection reset))' THEN 'ssl_eof'
              WHEN message RLIKE '^(could not receive data from client|could not send data to client)' THEN 'connection_reset'
              WHEN message RLIKE '^(connection to client lost|unexpected EOF on client connection)' THEN 'client_lost'
              WHEN message RLIKE '^(the database system is starting up|terminating walreceiver)' THEN 'compute_restart' END AS kind
  FROM {catalog}.lakebase_health.raw_postgres_logs
  WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts
    AND array_contains(split(:project_ids, ','), project_id)
    AND sqlstate IN ('57014', '08006', '08P01', '57P01', '57P03')
)
WHERE kind IS NOT NULL
ORDER BY ts
LIMIT 5000
