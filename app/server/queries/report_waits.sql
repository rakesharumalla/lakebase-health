WITH s AS (
  SELECT timestamp_seconds(floor(unix_timestamp(ts) / 900) * 900) AS bucket, ts,
         coalesce(wait_event_type, 'CPU') AS wait_class
  FROM {catalog}.lakebase_health.raw_active_session_history
  WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
    AND endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 24 HOURS
), n AS (SELECT bucket, count(DISTINCT ts) AS samples FROM s GROUP BY bucket)
SELECT s.bucket, s.wait_class, count(*) / max(n.samples) AS avg_active
FROM s JOIN n ON n.bucket = s.bucket
GROUP BY s.bucket, s.wait_class
ORDER BY s.bucket, s.wait_class
