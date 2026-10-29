WITH ask AS (
  SELECT DISTINCT element_at(split(p, ':'), 1) AS endpoint_id, CAST(element_at(split(p, ':'), 2) AS BIGINT) AS queryid
  FROM (SELECT explode(split(:pairs, ',')) AS p)
), qt AS (
  -- normalized pg_stat_statements text the analysis job copies from enrolled branches (system.lakebase masks it)
  SELECT q.endpoint_id, q.queryid, max_by(q.query, q.captured_at) AS qt_text
  FROM {catalog}.lakebase_health.query_texts q
  JOIN ask ON ask.endpoint_id = q.endpoint_id AND ask.queryid = q.queryid
  WHERE q.project_id = :project_id AND array_contains(split(:project_ids, ','), q.project_id)
    AND q.captured_at >= current_timestamp() - INTERVAL 7 DAYS
    AND q.query IS NOT NULL AND q.query NOT LIKE '<REDACTED%'
  GROUP BY q.endpoint_id, q.queryid
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
SELECT ask.endpoint_id, CAST(ask.queryid AS STRING) AS queryid, qt.qt_text, plan.plan_text, plan.plan_truncated,
       stmt.stat_text
FROM ask
LEFT JOIN qt ON qt.endpoint_id = ask.endpoint_id AND qt.queryid = ask.queryid
LEFT JOIN plan ON plan.endpoint_id = ask.endpoint_id AND plan.queryid = ask.queryid
LEFT JOIN stmt ON stmt.endpoint_id = ask.endpoint_id AND stmt.queryid = ask.queryid
