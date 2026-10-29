import { useEffect, useRef, useState } from "react";
import { queryKey, type QueryLabel, type QueryRef } from "./findings";

/** The server's per-request limit (server/lib/querytext.ts MAX_QUERY_PAIRS). */
export const LABEL_BATCH = 200;

/**
 * Query text labels for every (endpoint, queryid) a page shows, fetched in batches: one request for all the
 * pairs not yet known (split only past the server limit), never one per row. Labels already loaded are kept, so
 * expanding a list fetches only the new pairs.
 */
export function useQueryLabels(projectId: string | null | undefined, refs: QueryRef[]): Map<string, QueryLabel> {
  const [labels, setLabels] = useState<Map<string, QueryLabel>>(() => new Map());
  const asked = useRef(new Set<string>());
  const missing = [...new Set(refs.map((r) => queryKey(r.endpoint_id, r.queryid)))]
    .filter((k) => !labels.has(k))
    .sort();
  const want = missing.join(",");

  useEffect(() => {
    if (!projectId || !want) return;
    const keys = want.split(",").filter((k) => !asked.current.has(`${projectId}|${k}`));
    for (const k of keys) asked.current.add(`${projectId}|${k}`);
    for (let i = 0; i < keys.length; i += LABEL_BATCH) {
      const ids = keys.slice(i, i + LABEL_BATCH).join(",");
      fetch(`/api/projects/${projectId}/queries?ids=${encodeURIComponent(ids)}`)
        .then(async (r) => (r.ok ? ((await r.json()) as { queries?: QueryLabel[] }) : { queries: [] }))
        .then((body) => {
          const got = body.queries ?? [];
          if (got.length === 0) return;
          setLabels((prev) => {
            const next = new Map(prev);
            for (const q of got) next.set(queryKey(q.endpoint_id, q.queryid), q);
            return next;
          });
        })
        .catch(() => undefined);
    }
  }, [projectId, want]);

  return labels;
}
