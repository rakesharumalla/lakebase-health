import type { Row } from "./sql";

/** Rows shown before "Show all N". */
export const TOP_N = 10;

/** Checks that raise one finding per Postgres queryid (row_misestimate did until it became per endpoint). */
export const QUERY_CHECKS: ReadonlySet<string> = new Set([
  "plan_flip",
  "storage_wait",
  "query_regression",
  "top_db_time_query",
  "disk_spill",
  "sort_limit_waste",
  "row_misestimate",
  "hot_row_lock",
]);

const ISSUE_LABELS: Record<string, string> = {
  plan_flip: "plan flip",
  storage_wait: "storage wait",
  query_regression: "regression",
  top_db_time_query: "top DB time",
  disk_spill: "disk spill",
  sort_limit_waste: "sort + limit waste",
  row_misestimate: "row estimates",
  hot_row_lock: "row lock wait",
};
export const issueLabel = (checkId: string) => ISSUE_LABELS[checkId] ?? checkId.replace(/_/g, " ");

export type Issue = { fingerprint: string; check_id: string; label: string; severity: string; status: string; title: string };
export type GroupRow = {
  /** Stable React key: q:<endpoint>:<queryid> or f:<fingerprint>. */
  key: string;
  kind: "query" | "finding";
  endpoint_id: string | null;
  queryid: string | null;
  /** The worst issue's finding: the row links here. */
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

const s = (v: unknown) => (typeof v === "string" || typeof v === "number" || typeof v === "boolean" ? String(v) : "");
const numOrNull = (v: unknown) => {
  if (v === null || v === undefined || v === "") return null;
  const x = Number(v);
  return Number.isFinite(x) ? x : null;
};
const RANK: Record<string, number> = { critical: 0, warning: 1, info: 2 };
const rank = (sev: unknown) => RANK[s(sev)] ?? 3;

/** new and open findings; resolved and superseded (a higher-severity overlapping finding is open) are not. */
export const isOpen = (r: { status?: unknown }) => r.status === "new" || r.status === "open";

const LEGACY_OBJECT = /^(?:ep-[a-z0-9-]+\/)?(-?\d{1,20})(?:@.*)?$/;

/** evidence.queryid (engine), else the object of an older per-query finding. */
export function queryIdOf(r: Row): string | null {
  const q = s(r.queryid);
  if (/^-?\d{1,20}$/.test(q)) return q;
  if (!QUERY_CHECKS.has(s(r.check_id))) return null;
  const m = LEGACY_OBJECT.exec(s(r.object));
  return m ? m[1] : null;
}

export function topQueryIds(r: Row): string[] {
  const raw = r.top_queryids;
  let list: unknown = raw;
  if (typeof raw === "string") {
    try {
      list = JSON.parse(raw);
    } catch {
      return [];
    }
  }
  return Array.isArray(list) ? list.map(s).filter((q) => /^-?\d{1,20}$/.test(q)) : [];
}

const issueOf = (r: Row): Issue => ({
  fingerprint: s(r.fingerprint),
  check_id: s(r.check_id),
  label: issueLabel(s(r.check_id)),
  severity: s(r.severity),
  status: s(r.status),
  title: s(r.title),
});
const byIssue = (a: Issue, b: Issue) => rank(a.severity) - rank(b.severity);

function compare(a: GroupRow, b: GroupRow) {
  return (
    rank(a.severity) - rank(b.severity) ||
    (b.db_ms ?? -1) - (a.db_ms ?? -1) ||
    (b.measure ?? -Infinity) - (a.measure ?? -Infinity) ||
    b.occurrences - a.occurrences ||
    s(b.last_seen).localeCompare(s(a.last_seen)) ||
    a.key.localeCompare(b.key)
  );
}

function build(members: Row[], key: string, kind: GroupRow["kind"], queryid: string | null): GroupRow {
  const sorted = [...members].sort((a, b) => rank(a.severity) - rank(b.severity));
  const p = sorted[0];
  const times = (k: string) => members.map((m) => s(m[k])).filter(Boolean).sort();
  const first = times("first_seen");
  const last = times("last_seen");
  const dbs = members.map((m) => numOrNull(m.db_ms)).filter((x): x is number => x !== null);
  return {
    key,
    kind,
    endpoint_id: s(p.endpoint_id) || null,
    queryid,
    fingerprint: s(p.fingerprint),
    check_id: s(p.check_id),
    title: s(p.title),
    category: s(p.category),
    object: s(p.object),
    severity: s(p.severity),
    status: members.some((m) => m.status === "new") ? "new" : s(p.status),
    confidence: s(p.confidence) || null,
    first_seen: first[0] ?? null,
    last_seen: last[last.length - 1] ?? null,
    occurrences: Math.max(0, ...members.map((m) => Number(m.occurrences ?? 0) || 0)),
    measure: numOrNull(p.measure),
    db_ms: dbs.length ? Math.max(...dbs) : null,
    resolved_at: s(p.resolved_at) || null,
    issues: sorted.map(issueOf).sort(byIssue),
  };
}

/**
 * One row per (endpoint, queryid) with an issue per check, worst severity first; other findings pass through.
 * Open endpoint-level row_misestimate findings (in `rows` or `context`) add a "row estimates" issue to the open
 * query rows listed in their evidence.top_queries.
 */
export function groupFindings(rows: Row[], context: Row[] = []): GroupRow[] {
  const estimates = new Map<string, Row>();
  for (const r of [...context, ...rows]) {
    if (r.check_id !== "row_misestimate" || !isOpen(r)) continue;
    for (const q of topQueryIds(r)) {
      const k = `${s(r.endpoint_id)}:${q}`;
      if (!estimates.has(k)) estimates.set(k, r);
    }
  }
  const groups = new Map<string, Row[]>();
  const out: GroupRow[] = [];
  for (const r of rows) {
    const q = queryIdOf(r);
    if (q === null) {
      out.push(build([r], `f:${s(r.fingerprint)}`, "finding", null));
      continue;
    }
    const k = `${s(r.endpoint_id)}:${q}`;
    const g = groups.get(k);
    if (g) g.push(r);
    else groups.set(k, [r]);
  }
  for (const [k, members] of groups) {
    const row = build(members, `q:${k}`, "query", k.slice(k.lastIndexOf(":") + 1));
    const est = estimates.get(k);
    if (est && members.some(isOpen) && !row.issues.some((i) => i.check_id === "row_misestimate")) {
      row.issues.push(issueOf(est));
    }
    out.push(row);
  }
  return out.sort(compare);
}

export function page<T>(rows: T[], all = false): { total: number; rows: T[] } {
  return { total: rows.length, rows: all ? rows : rows.slice(0, TOP_N) };
}
