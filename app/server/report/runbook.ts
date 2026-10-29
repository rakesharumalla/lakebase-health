import type { Priority, ReportFinding, RunbookItem, Severity } from "../../client/src/report/types";
import { TEXT_FIELDS } from "../lib/mask";
import { CATALOG, areaLabel, type Catalog } from "./catalog";

/** An open fix action of a finding (proposed_actions joined to its finding). */
export type ActionRow = { action_id: string; fingerprint: string; state: string };

const PRIORITY: Record<Severity, Priority> = { critical: "HIGH", warning: "MEDIUM", info: "LOW" };
const SEV_RANK: Record<Severity, number> = { critical: 2, warning: 1, info: 0 };
/** A fix verified on a branch beats one only proposed, which beats none. */
const FIX_RANK: Record<string, number> = { verified: 2, proposed: 1, verifying: 1, verify_failed: 0, applying: 1 };
/** Settings only Databricks support can change on Lakebase. */
const SUPPORT_MANAGED = new Set(["track_io_timing", "log_min_duration_statement", "shared_preload_libraries",
  "autovacuum_max_workers", "max_connections", "shared_buffers"]);

/** How far past its threshold a finding is, comparable across checks: (measure - threshold) / max(|threshold|, 1). */
export function impact(f: ReportFinding, catalog: Catalog = CATALOG): number {
  const info = catalog[f.check_id];
  const t = info?.thresholds[f.severity];
  if (f.measure === null || t === undefined) return 0;
  const d = info.direction === "below" ? t - f.measure : f.measure - t;
  return d / Math.max(Math.abs(t), 1);
}

function parse(evidence: string | null): Record<string, unknown> {
  try {
    const e: unknown = JSON.parse(evidence ?? "{}");
    return typeof e === "object" && e !== null && !Array.isArray(e) ? (e as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

const fmt = (v: number) => (Number.isInteger(v) ? v.toLocaleString("en-US") : v.toLocaleString("en-US", { maximumFractionDigits: 2 }));

/** Up to 4 "key: value" numbers from the evidence (never text). */
export function evidenceNumbers(evidence: string | null): string[] {
  return Object.entries(parse(evidence))
    .filter(([k, v]) => typeof v === "number" && Number.isFinite(v) && !TEXT_FIELDS.has(k))
    .slice(0, 4)
    .map(([k, v]) => `${k.replace(/_/g, " ")}: ${fmt(v as number)}`);
}

/** The explain text's first sentence. */
export const firstSentence = (s: string | null) => {
  const t = (s ?? "").trim();
  const m = /^(.+?[.!?])(\s|$)/s.exec(t);
  return (m ? m[1] : t).replace(/\s+/g, " ");
};

/** SQL to show for a finding: never the user's query text (that becomes "query <id>"). */
export function safeSql(f: ReportFinding): string | null {
  const sql = f.candidate_sql;
  if (!sql) return null;
  const text = f.query_text;
  if (text && text.length > 8 && sql.includes(text)) return sql.split(text).join(`query ${f.queryid ?? "?"}`);
  return sql;
}

/** One item per open finding: severity, then a verified fix, then impact. */
export function runbook(findings: ReportFinding[], actions: ActionRow[], catalog: Catalog = CATALOG): RunbookItem[] {
  const best = new Map<string, ActionRow>();
  for (const a of actions) {
    const cur = best.get(a.fingerprint);
    if (!cur || (FIX_RANK[a.state] ?? 0) > (FIX_RANK[cur.state] ?? 0)) best.set(a.fingerprint, a);
  }
  const open = findings.filter((f) => f.status === "new" || f.status === "open");
  const fixRank = (f: ReportFinding) => {
    const a = best.get(f.fingerprint);
    return a ? (FIX_RANK[a.state] ?? 0) + 1 : 0;
  };
  return [...open]
    .sort((x, y) => SEV_RANK[y.severity] - SEV_RANK[x.severity] || fixRank(y) - fixRank(x) || impact(y, catalog) - impact(x, catalog) || x.title.localeCompare(y.title))
    .map((f) => {
      const a = best.get(f.fingerprint) ?? null;
      const sql = safeSql(f);
      const setting = parse(f.evidence).name;
      const supportManaged = typeof setting === "string" && SUPPORT_MANAGED.has(setting);
      return {
        priority: PRIORITY[f.severity],
        severity: f.severity,
        fingerprint: f.fingerprint,
        checkId: f.check_id,
        area: catalog[f.check_id]?.area ?? null,
        areaLabel: areaLabel(catalog[f.check_id]?.area),
        title: f.title,
        object: f.object,
        why: firstSentence(f.explain),
        evidence: evidenceNumbers(f.evidence),
        sql: supportManaged ? null : sql,
        note: supportManaged
          ? `support-managed: ${setting} cannot be changed from a customer session; open a support request`
          : sql
            ? null
            : "No SQL fix: follow the guidance on the finding page",
        actionId: a?.action_id ?? null,
        actionState: a?.state ?? null,
      };
    });
}

export function runbookCounts(items: RunbookItem[]): Record<Priority, number> {
  const out: Record<Priority, number> = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const i of items) out[i.priority] += 1;
  return out;
}
