import { createHash } from "node:crypto";
import type { Http } from "./access";

export type RequestKind = "verify" | "apply" | "rollback" | "reject";

// Mirror of engine/src/lakebase_health/fixer/states.py REQUEST_FROM / can_request.
const FROM: Record<RequestKind, string[]> = {
  verify: ["proposed", "verify_failed"],
  apply: ["verified", "apply_failed"],
  rollback: ["regressed", "applied", "confirmed", "rollback_failed"],
  reject: ["proposed", "verified", "verify_failed", "apply_failed"],
};
const ORDER: RequestKind[] = ["verify", "apply", "rollback", "reject"];

export const isRequestKind = (v: unknown): v is RequestKind => typeof v === "string" && (ORDER as string[]).includes(v);

/**
 * Whether the action has a rollback statement to run. Empty or comment-only text (a fix with nothing to undo,
 * e.g. "-- VACUUM changes no data") is never runnable; the fixer also refuses SQL outside its allowlist.
 */
export function hasRollback(sql: unknown): boolean {
  const s = typeof sql === "string" ? sql.trim() : "";
  return s !== "" && !s.startsWith("--") && !s.startsWith("/*");
}

export function legalRequests(state: string, verifyOnBranch: boolean, retry: RequestKind | null = null,
                              rollbackRunnable = true): RequestKind[] {
  if (state === "blocked") return retry ? [retry] : [];
  return ORDER.filter((r) => (r !== "rollback" || rollbackRunnable)
    && (FROM[r].includes(state) || (r === "apply" && state === "proposed" && !verifyOnBranch)));
}

/** Mirror of fixer.queue FINDING_ACTIONABLE: the fixer runs verify/apply only for these finding statuses. */
const FINDING_ACTIONABLE = ["new", "open"];
const WHY: Record<string, string> = {
  superseded: "the finding is superseded by a related finding, so the fixer will not run this fix",
  resolved: "the finding is resolved, so there is nothing to fix",
  suppressed: "the finding is suppressed, so the fixer will not run this fix",
};

/**
 * Why verify/apply cannot run (the fixer would consume the approval as stale), or null when they can. Mirrors
 * fixer.queue.staleness: an expired action, or a finding that is not new/open.
 */
export function notActionable(findingStatus: unknown, expiresAt: unknown, now: Date): string | null {
  const exp = typeof expiresAt === "string" || expiresAt instanceof Date ? new Date(expiresAt) : null;
  if (exp && !Number.isNaN(exp.getTime()) && exp.getTime() <= now.getTime())
    return "Not actionable: this fix expired. The engine proposes it again if the finding is still open.";
  const st = typeof findingStatus === "string" ? findingStatus : "";
  if (FINDING_ACTIONABLE.includes(st)) return null;
  return `Not actionable: ${WHY[st] ?? `the finding is ${st || "gone"}, so the fixer will not run this fix`}.`;
}

/** verify and apply need an actionable finding; reject and rollback do not. */
export const NEEDS_ACTIONABLE: RequestKind[] = ["verify", "apply"];

const LEGACY_PRIVILEGE_ERROR = "fixer not enrolled or missing privileges";
const txt = (v: unknown) => (typeof v === "string" ? v : "");

/** Mirror of fixer.states.block_retry over the action's runs: the request a privilege/enrolment block may retry. */
export function blockRetry(runs: Record<string, unknown>[]): RequestKind | null {
  const blocked = runs
    .filter((r) => r.outcome === "blocked")
    .sort((a, b) => txt(b.started_at).localeCompare(txt(a.started_at)))[0];
  if (!blocked) return null;
  let reason: unknown;
  try {
    reason = (JSON.parse(txt(blocked.report) || "{}") as Record<string, unknown>)?.blocked_reason;
  } catch {
    reason = undefined;
  }
  if (reason === undefined && txt(blocked.error).startsWith(LEGACY_PRIVILEGE_ERROR)) reason = "privileges";
  const kind = blocked.kind;
  return reason === "privileges" && (kind === "verify" || kind === "apply") ? kind : null;
}

/** sha256 hex of the exact SQL the approval covers; matches engine fixer.queue.sql_hash. */
export const sqlHash = (sql: string) => createHash("sha256").update(sql).digest("hex");

/**
 * Owner, or anyone allowed to read the project's ACL (only CAN_MANAGE can). GET-only with the user's token.
 * The permissions API addresses a project by its id (e.g. "healthcheck"); its uid returns 404.
 */
export async function canManage(http: Http, token: string, project: { id: string; owner?: string }, userName: string) {
  if (project.owner && project.owner === userName) return true;
  const r = await http(`/api/2.0/permissions/database-projects/${encodeURIComponent(project.id)}`, token);
  return r.status === 200;
}
