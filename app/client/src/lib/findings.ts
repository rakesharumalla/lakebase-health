/** Shapes returned by the server's grouping (server/lib/group.ts) and query text lookup (server/lib/querytext.ts). */
export type Issue = { fingerprint: string; check_id: string; label: string; severity: string; status: string; title: string };
export type GroupRow = {
  key: string;
  kind: "query" | "finding";
  endpoint_id: string | null;
  queryid: string | null;
  fingerprint: string;
  check_id: string;
  title: string;
  category: string;
  object: string;
  severity: string;
  status: string;
  confidence: string | null;
  first_seen: string | null;
  last_seen: string | null;
  occurrences: number;
  measure: number | null;
  db_ms: number | null;
  resolved_at: string | null;
  issues: Issue[];
};
export type Page<T> = {
  total: number;
  rows: T[];
  /** Findings counted in SQL (the list may be cut at the query's LIMIT). */
  count?: number;
  /** Findings fetched before grouping; less than count when truncated. */
  shown?: number;
  truncated?: boolean;
  limit?: number;
};
export type QueryLabel = {
  endpoint_id: string;
  queryid: string;
  text: string | null;
  label: string;
  kind: "full" | "skeleton" | "none";
  truncated: boolean;
};
export type QueryRef = { endpoint_id: string; queryid: string };

export const queryKey = (endpointId: string, queryid: string) => `${endpointId}:${queryid}`;

/** The (endpoint, queryid) pairs a list of grouped rows needs labels for. */
export const queryRefs = (rows: GroupRow[]): QueryRef[] =>
  rows.flatMap((r) => (r.kind === "query" && r.endpoint_id && r.queryid ? [{ endpoint_id: r.endpoint_id, queryid: r.queryid }] : []));

export type SeverityCounts = { critical: number; warning: number; info: number };

/**
 * Open findings per severity in a fully loaded list of grouped rows: each finding row counts once, a query
 * row counts each of its issues, deduplicated by fingerprint. Returns null when the list is cut (a top-N page
 * or a LIMIT) or when the tally disagrees with the SQL count, so a header never shows a partial breakdown.
 */
export function severityCounts(p: Page<GroupRow>, rows: GroupRow[] = p.rows): SeverityCounts | null {
  if (rows.length < p.total || p.truncated) return null;
  const seen = new Map<string, string>();
  for (const r of rows) {
    const items = r.kind === "query" && r.issues.length > 0 ? r.issues : [r];
    for (const i of items) if (!seen.has(i.fingerprint)) seen.set(i.fingerprint, i.severity);
  }
  const out: SeverityCounts = { critical: 0, warning: 0, info: 0 };
  for (const sev of seen.values()) if (sev === "critical" || sev === "warning" || sev === "info") out[sev]++;
  if (p.count !== undefined && out.critical + out.warning + out.info !== p.count) return null;
  return out;
}
