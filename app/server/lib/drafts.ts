import { createHash } from "node:crypto";
import type { QueryLabel } from "./querytext";
import type { Row } from "./sql";

// Mirror of engine/src/lakebase_health/fixer/draft/states.py.
export const REWRITE_CHECKS = [
  "top_db_time_query", "query_regression", "plan_flip", "storage_wait", "disk_spill", "sort_limit_waste",
  "row_misestimate",
];
export const RUNNING = ["drafting", "verifying"];
export const FINAL = ["draft_failed", "verified", "verify_failed", "adopted", "not_adopted"];

/** What a draft approval's sql_hash holds: the attempt it asks for (states.draft_token). */
export const draftToken = (fingerprint: string, attempt: number) =>
  createHash("sha256").update(`draft:${fingerprint}:${attempt}`).digest("hex");

const QUERYID = /^-?\d{1,20}$/;
const isObj = (x: unknown): x is Record<string, unknown> => typeof x === "object" && x !== null && !Array.isArray(x);
const sv = (v: unknown) => (typeof v === "string" || typeof v === "number" ? String(v) : "");

/** The query a rewrite is drafted for (states.finding_queryid): evidence.queryid, row_misestimate's top query, the object. */
export function findingQueryId(checkId: unknown, evidence: unknown, object: unknown): string | null {
  if (typeof checkId !== "string" || !REWRITE_CHECKS.includes(checkId)) return null;
  let ev: unknown = {};
  try {
    ev = typeof evidence === "string" ? JSON.parse(evidence) : (evidence ?? {});
  } catch {
    ev = {};
  }
  const e = isObj(ev) ? ev : {};
  let q: unknown;
  if (checkId === "row_misestimate") {
    const top = Array.isArray(e.top_queries) ? e.top_queries : [];
    q = isObj(top[0]) ? top[0].queryid : undefined;
  } else {
    q = "queryid" in e ? e.queryid : sv(object).split("@")[0];
  }
  const s = sv(q);
  return QUERYID.test(s) ? s : null;
}

export type SetUp = { ready: boolean; reason: string | null };

/** Whether drafting can run: LLM_ENABLED, and the fixer's last probe of the endpoint (llm_status). */
export function setUpOf(enabled: boolean, status: Row | undefined): SetUp {
  if (!enabled) return { ready: false, reason: "LLM drafting is turned off (llm_enabled is false)." };
  if (!status) return { ready: false, reason: "The fixer has not checked the model endpoint yet." };
  if (status.ready !== true && status.ready !== "true")
    return { ready: false, reason: sv(status.reason) || "The model endpoint is not ready." };
  return { ready: true, reason: null };
}

export type RequestState = {
  pending: boolean;
  latestFinal: boolean;
  nextAttempt: number;
  refused: { reason: string; at: string } | null;
};

/**
 * From the attempts (newest first) and the draft approvals (newest first, with the outcome of the run that consumed
 * them): an unconsumed request is pending; the newest refused request after the latest attempt says why.
 */
export function requestState(attempts: Row[], requests: Row[]): RequestState {
  const latest = attempts[0];
  const since = latest ? sv(latest.requested_at) : "";
  const refusedRow = requests.find((r) => sv(r.outcome).startsWith("stale:") && sv(r.requested_at) > since);
  return {
    pending: requests.some((r) => r.outcome === null || r.outcome === undefined),
    latestFinal: !latest || FINAL.includes(sv(latest.state)),
    nextAttempt: (latest ? Number(latest.attempt) : 0) + 1,
    refused: refusedRow ? { reason: sv(refusedRow.outcome).slice("stale:".length), at: sv(refusedRow.requested_at) } : null,
  };
}

export type DraftView = {
  rewritable: boolean;
  setUp: SetUp;
  canManage: boolean;
  canRequest: boolean;
  pending: boolean;
  refused: { reason: string; at: string } | null;
  nextAttempt: number;
  original: QueryLabel | null;
  attempts: Row[];
};
