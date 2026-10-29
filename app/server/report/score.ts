import type { AreaId, AreaResult, AreaState, ReportFinding, Score, Severity } from "../../client/src/report/types";
import { AREAS, CATALOG, type Catalog } from "./catalog";

/** The latest check_results status row of one check: fleet-wide (endpoint_id null) or for one endpoint. */
export type CheckStatus = {
  check_id: string;
  endpoint_id: string | null;
  status: string;
  error: string | null;
  /** The last "ok" run in the last 24 h (report_checks.sql), kept while the compute is suspended. */
  last_ok_at?: string | null;
};

/** What decides whether a check ran for this branch. */
export type Coverage = {
  /** The branch's read-write endpoint(s): Postgres checks run there. */
  primaryEndpoints: string[];
  /** Some telemetry arrived for the branch in the last 24 h. */
  telemetry: boolean;
  /** The branch's primary is enrolled for Postgres checks. */
  enrolled: boolean;
};

const RANK: Record<Severity, number> = { info: 0, warning: 1, critical: 2 };
const AREA_SCORE: Record<Severity, number> = { info: 4, warning: 3, critical: 1 };

/** Whether a check ran for the branch, and why not. */
export function checkRan(checkId: string, statuses: CheckStatus[], cov: Coverage, catalog: Catalog = CATALOG) {
  const info = catalog[checkId];
  if (!info) return { ran: false, note: null as string | null };
  if (info.source === "postgres") {
    const mine = statuses.filter((s) => s.check_id === checkId && s.endpoint_id !== null && cov.primaryEndpoints.includes(s.endpoint_id));
    if (mine.some((s) => s.status === "ok")) return { ran: true, note: null };
    // a scale-to-zero compute asleep at the last run keeps its last assessment (within 24 h) instead of
    // dropping out of the score until it wakes
    const kept = mine.filter((s) => s.status === "skipped:compute_inactive" && s.last_ok_at).map((s) => s.last_ok_at as string).sort();
    if (kept.length) return { ran: true, note: null, lastAssessed: kept[0] };
    const young = mine.find((s) => s.status === "skipped:stats_young");
    if (young) return { ran: false, note: young.error ?? "counters too young" };
    return { ran: false, note: cov.enrolled ? "Postgres checks did not run (compute suspended or unreachable)" : "Postgres checks are not enabled on this branch" };
  }
  const fleet = statuses.find((s) => s.check_id === checkId && s.endpoint_id === null);
  const ok = fleet?.status === "ok";
  if (info.source === "api") return { ran: ok, note: ok ? null : "API checks did not run" };
  if (!cov.telemetry) return { ran: false, note: "no telemetry for this branch in the last 24 h" };
  if (ok) return { ran: true, note: null };
  return { ran: false, note: fleet?.status === "ok_stale_source" ? "telemetry is stale" : "telemetry checks did not run" };
}

const stateOf = (score: number | null): AreaState =>
  score === null ? "not_assessed" : score === 5 ? "pass" : score >= 3 ? "warn" : "fail";

/** One result per area: the worst open finding decides the score; an area none of whose checks ran is not assessed. */
export function scoreAreas(findings: ReportFinding[], statuses: CheckStatus[], cov: Coverage, catalog: Catalog = CATALOG): AreaResult[] {
  const open = findings.filter((f) => f.status === "new" || f.status === "open");
  return AREAS.map(({ id, label, blurb }) => {
    const checks = Object.keys(catalog).filter((c) => catalog[c].area === id);
    const runs: { ran: boolean; note: string | null; lastAssessed?: string }[] = checks.map((c) => checkRan(c, statuses, cov, catalog));
    // the oldest carried-forward time among the area's checks; none when every check ran in the last run
    const lastAssessed = runs.map((r) => r.lastAssessed).filter((t): t is string => !!t).sort()[0];
    const assessed = runs.some((r) => r.ran);
    const mine = open.filter((f) => catalog[f.check_id]?.area === id);
    const worst = mine.reduce<ReportFinding | null>(
      (w, f) => (w === null || RANK[f.severity] > RANK[w.severity] || (RANK[f.severity] === RANK[w.severity] && (f.measure ?? 0) > (w.measure ?? 0)) ? f : w),
      null,
    );
    const score = assessed ? (worst ? AREA_SCORE[worst.severity] : 5) : null;
    const notes = [...new Set(runs.map((r) => r.note).filter((n): n is string => !!n))];
    const young = notes.find((n) => n.startsWith("counters too young"));
    return {
      id,
      label,
      blurb,
      state: stateOf(score),
      score,
      open: mine.length,
      worst: worst ? { fingerprint: worst.fingerprint, title: worst.title, object: worst.object, severity: worst.severity } : null,
      note: assessed ? (young ?? null) : (young ?? notes[0] ?? "no checks ran"),
      ...(assessed && lastAssessed ? { lastAssessed } : {}),
    };
  });
}

export function grade(pct: number): Score["grade"] {
  if (pct >= 85) return "A";
  if (pct >= 70) return "B";
  if (pct >= 55) return "C";
  if (pct >= 40) return "D";
  return "F";
}

/** Score = sum / (5 x assessed areas), as a whole percentage; no grade when nothing was assessed. */
export function score(areas: AreaResult[]): Score {
  const assessed = areas.filter((a) => a.score !== null);
  if (assessed.length === 0) return { pct: null, grade: null, assessed: 0, total: areas.length };
  const pct = Math.round((100 * assessed.reduce((n, a) => n + (a.score ?? 0), 0)) / (5 * assessed.length));
  return { pct, grade: grade(pct), assessed: assessed.length, total: areas.length };
}

/** The short "where to focus" note under the scorecard. */
export function focus(areas: AreaResult[]): string {
  const fail = areas.filter((a) => a.state === "fail");
  const warn = areas.filter((a) => a.state === "warn").sort((x, y) => (x.score ?? 5) - (y.score ?? 5));
  const names = (xs: AreaResult[]) => xs.slice(0, 3).map((a) => a.label.toLowerCase()).join(", ");
  if (fail.length) return `Start with ${names(fail)}: ${fail.length === 1 ? "it has" : "they have"} a critical finding. Then work down the runbook.`;
  if (warn.some((a) => a.score === 3)) return `Next: ${names(warn.filter((a) => a.score === 3))}. Warnings there are worth a fix this week.`;
  if (warn.length) return `Only informational findings, in ${names(warn)}. Review them when convenient.`;
  if (areas.every((a) => a.state === "not_assessed")) return "Nothing was assessed yet: see the banners above.";
  return "No open findings in the assessed areas.";
}

export const AREA_IDS: AreaId[] = AREAS.map((a) => a.id);
