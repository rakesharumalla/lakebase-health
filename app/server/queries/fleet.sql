WITH inv AS (
  SELECT project_id, any_value(project_name) AS project_name,
         sum(CASE WHEN current_state = 'ACTIVE' THEN 1 ELSE 0 END) AS active_computes,
         count(*) AS computes, max(CAST(enrolled AS INT)) AS enrolled
  FROM {catalog}.lakebase_health.inventory
  WHERE run_id = (SELECT max_by(run_id, captured_at) FROM {catalog}.lakebase_health.inventory)
    AND array_contains(split(:project_ids, ','), project_id)
  GROUP BY project_id
), f AS (
  SELECT project_id,
         sum(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical,
         sum(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) AS warning,
         sum(CASE WHEN severity = 'info' THEN 1 ELSE 0 END) AS info,
         max(first_seen) AS newest_finding_at
  FROM {catalog}.lakebase_health.findings
  WHERE status IN ('new', 'open') AND array_contains(split(:project_ids, ','), project_id)
  GROUP BY project_id
)
SELECT coalesce(f.project_id, inv.project_id) AS project_id, inv.project_name,
       coalesce(f.critical, 0) AS critical, coalesce(f.warning, 0) AS warning, coalesce(f.info, 0) AS info,
       f.newest_finding_at, inv.active_computes, inv.computes, inv.enrolled
FROM f FULL OUTER JOIN inv ON inv.project_id = f.project_id
ORDER BY critical DESC, warning DESC
