import { parseEndpointId } from "./validate";

export const MAX_QUERY_PAIRS = 200;

export type QueryPair = { endpoint_id: string; queryid: string };
export type QueryLabel = QueryPair & {
  /** Full text, the normalized skeleton, or null when there is none. */
  text: string | null;
  /** What to show: the text, else "query <id>". */
  label: string;
  kind: "full" | "skeleton" | "none";
  truncated: boolean;
};

const QUERYID = /^-?\d{1,19}$/;
const IDENT = /[A-Za-z0-9_$]/;

/**
 * A normalized statement skeleton for users who may not read query text: string literals (including E'' and
 * dollar-quoted) and numbers become ?, comments are dropped, $n parameters and identifiers stay, whitespace
 * collapses.
 */
export function skeleton(sql: string): string {
  let out = "";
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const c = sql[i];
    const prev = out.length ? out[out.length - 1] : "";
    if (c === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") i++;
      out += " ";
    } else if (c === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? n : end + 2;
      out += " ";
    } else if (c === "'" || ((c === "E" || c === "e") && sql[i + 1] === "'" && !IDENT.test(prev))) {
      if (c !== "'") i++;
      i++;
      while (i < n) {
        if (sql[i] === "\\" && c !== "'") i += 2;
        else if (sql[i] === "'" && sql[i + 1] === "'") i += 2;
        else if (sql[i] === "'") break;
        else i++;
      }
      i++;
      out += "?";
    } else if (c === '"') {
      const end = sql.indexOf('"', i + 1);
      const stop = end < 0 ? n : end + 1;
      out += sql.slice(i, stop);
      i = stop;
    } else if (c === "$" && !IDENT.test(prev) && /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.test(sql.slice(i))) {
      const tag = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i))![0];
      const end = sql.indexOf(tag, i + tag.length);
      i = end < 0 ? n : end + tag.length;
      out += "?";
    } else if (c === "$" && /\d/.test(sql[i + 1] ?? "")) {
      let j = i + 1;
      while (j < n && /\d/.test(sql[j])) j++;
      out += sql.slice(i, j);
      i = j;
    } else if (/\d/.test(c) && !IDENT.test(prev)) {
      const m = /^(0x[0-9a-fA-F]+|\d+(\.\d*)?([eE][+-]?\d+)?|\.\d+)/.exec(sql.slice(i))!;
      i += m[0].length;
      out += "?";
    } else if (IDENT.test(c)) {
      let j = i;
      while (j < n && IDENT.test(sql[j])) j++;
      out += sql.slice(i, j);
      i = j;
    } else {
      out += c;
      i++;
    }
  }
  return out.replace(/\s+/g, " ").replace(/\s+([,)])/g, "$1").trim();
}

const usable = (v: unknown): string | null => {
  if (typeof v !== "string") return null;
  const t = v.trim();
  return t === "" || t.startsWith("<REDACTED") ? null : t;
};
const isTrue = (v: unknown) => v === true || v === "true";

/**
 * Chooses the text for one (endpoint, queryid) from the query_text lookup row: complete plan_history text, else
 * pg_stat_statements text, else truncated plan text. `full` false returns only the skeleton.
 */
export function queryLabel(row: Record<string, unknown>, full: boolean): QueryLabel {
  const endpoint_id = typeof row.endpoint_id === "string" ? row.endpoint_id : "";
  const queryid = typeof row.queryid === "string" || typeof row.queryid === "number" ? String(row.queryid) : "";
  const plan = usable(row.plan_text);
  const stat = usable(row.stat_text);
  const planTruncated = isTrue(row.plan_truncated);
  let chosen: { text: string; truncated: boolean } | null = null;
  if (plan && !planTruncated) chosen = { text: plan, truncated: false };
  else if (stat) chosen = { text: stat, truncated: false };
  else if (plan) chosen = { text: plan, truncated: true };
  if (!chosen) return { endpoint_id, queryid, text: null, label: `query ${queryid}`, kind: "none", truncated: false };
  const text = full ? chosen.text : skeleton(chosen.text);
  return { endpoint_id, queryid, text, label: text.replace(/\s+/g, " "), kind: full ? "full" : "skeleton", truncated: chosen.truncated };
}

/** "ep-a:42,ep-b:-7" -> pairs; null for anything malformed or over the batch limit. */
export function parseQueryPairs(raw: unknown): QueryPair[] | null {
  if (typeof raw !== "string" || raw === "") return null;
  const seen = new Set<string>();
  const out: QueryPair[] = [];
  for (const part of raw.split(",")) {
    const i = part.lastIndexOf(":");
    if (i < 0) return null;
    const endpoint_id = parseEndpointId(part.slice(0, i));
    const queryid = part.slice(i + 1);
    if (!endpoint_id || !QUERYID.test(queryid)) return null;
    if (queryid.replace("-", "").length === 19 && BigInt(queryid) > 9223372036854775807n) return null;
    if (BigInt(queryid) < -9223372036854775808n) return null;
    const key = `${endpoint_id}:${queryid}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ endpoint_id, queryid });
  }
  return out.length > MAX_QUERY_PAIRS ? null : out;
}
