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
