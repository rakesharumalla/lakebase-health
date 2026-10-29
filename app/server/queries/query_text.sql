WITH ask AS (
  SELECT DISTINCT element_at(split(p, ':'), 1) AS endpoint_id, CAST(element_at(split(p, ':'), 2) AS BIGINT) AS queryid
  FROM (SELECT explode(split(:pairs, ',')) AS p)
), plan AS (
  SELECT h.endpoint_id, h.queryid, max_by(h.query_text, h.ts) AS plan_text, max_by(h.truncated, h.ts) AS plan_truncated
  FROM {catalog}.lakebase_health.raw_plan_history h
  JOIN ask ON ask.endpoint_id = h.endpoint_id AND ask.queryid = h.queryid
  WHERE h.project_id = :project_id AND array_contains(split(:project_ids, ','), h.project_id)
    AND h.ts >= current_timestamp() - INTERVAL 7 DAYS
    AND h.query_text IS NOT NULL AND h.query_text NOT LIKE '<REDACTED%'
  GROUP BY h.endpoint_id, h.queryid
), stmt AS (
  SELECT s.endpoint_id, s.queryid, max_by(s.query, s.ts) AS stat_text
  FROM {catalog}.lakebase_health.raw_pg_stat_statements_counters s
  JOIN ask ON ask.endpoint_id = s.endpoint_id AND ask.queryid = s.queryid
  WHERE s.project_id = :project_id AND array_contains(split(:project_ids, ','), s.project_id)
    AND s.ts >= current_timestamp() - INTERVAL 7 DAYS
    AND s.query IS NOT NULL AND s.query NOT LIKE '<REDACTED%'
  GROUP BY s.endpoint_id, s.queryid
)
SELECT ask.endpoint_id, CAST(ask.queryid AS STRING) AS queryid, plan.plan_text, plan.plan_truncated, stmt.stat_text
FROM ask
LEFT JOIN plan ON plan.endpoint_id = ask.endpoint_id AND plan.queryid = ask.queryid
LEFT JOIN stmt ON stmt.endpoint_id = ask.endpoint_id AND stmt.queryid = ask.queryid
