import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { Access } from "../lib/access";
import { draftToken, findingQueryId, FINAL, requestState, REWRITE_CHECKS, RUNNING, setUpOf } from "../lib/drafts";
import { createHandlers } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const FP = "f".repeat(32);
const AID = "d".repeat(32);
const REWRITE = "SELECT o.id FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 AND o.status <> 'void' ORDER BY o.id";
const ctx = { token: "t", user: "me@x.com", now: new Date("2026-10-29T12:00:00Z") };
const finding = { fingerprint: FP, project_id: HC, check_id: "top_db_time_query", endpoint_id: "ep-a", object: "42",
                  status: "open", evidence: '{"queryid": "42"}' };
const attempt = (n: number, state: string, o: Record<string, unknown> = {}) => ({
  draft_id: `${n}`.repeat(32), attempt: String(n), state, reason: null, requested_at: `2026-10-29T1${n}:00:00Z`,
  sql: REWRITE, report: null, ...o,
});

function setup(o: {
  manager?: boolean; pii?: boolean; ready?: boolean; enabled?: boolean; attempts?: Record<string, unknown>[];
  requests?: Record<string, unknown>[]; finding?: Record<string, unknown> | null; started?: boolean;
} = {}) {
  const writes: Record<string, string>[] = [];
  const calls: { name: string; params: Record<string, unknown> }[] = [];
  const access: Access = { userKey: "me@x.com", userName: "me@x.com", projectIds: [HC],
                           projectNames: { [HC]: "healthcheck" }, pii: !!o.pii };
  const h = createHandlers({
    access: { async resolve() { return access; }, refresh() {} },
    runner: {
      async run(name, _ids, params = {}) {
        calls.push({ name, params });
        if (name === "finding") return o.finding === null ? [] : [o.finding ?? finding];
        if (name === "finding_drafts") return o.attempts ?? [];
        if (name === "draft_requests") return o.requests ?? [];
        if (name === "llm_status") return o.ready === undefined || o.ready ? [{ endpoint: "m", ready: "true" }]
          : [{ endpoint: "m", ready: "false", reason: "serving endpoint m is NOT_READY" }];
        if (name === "query_text") return [{ endpoint_id: "ep-a", queryid: "42", plan_text: "SELECT x FROM t WHERE a = 'secret'" }];
        if (name === "action") return [{ action_id: AID, fingerprint: FP, project_id: HC, kind: "query_rewrite",
                                         state: "verified", sql: REWRITE, verify_on_branch: "true" }];
        return [];
      },
    },
    scheduleMinutes: 60,
    approvals: {
      canManage: async () => o.manager ?? true,
      insert: async (row) => { writes.push(row); },
      trigger: async () => o.started,
      newId: () => "ap-9",
    },
    llm: { enabled: o.enabled ?? true, endpoint: "m" },
  });
  return { h, writes, calls };
}

describe("draft helpers mirror the engine", () => {
  it("lists the same rewritable checks and states as fixer/draft/states.py", () => {
    const py = readFileSync(join(__dirname, "..", "..", "..", "engine", "src", "lakebase_health", "fixer", "draft",
                                 "states.py"), "utf8");
    const tuple = (name: string) => [...(new RegExp(`${name} = \\(([^)]*)\\)`, "s").exec(py)?.[1] ?? "")
      .matchAll(/"([a-z_]+)"/g)].map((m) => m[1]);
    expect(tuple("REWRITE_CHECKS")).toEqual(REWRITE_CHECKS);
    expect(tuple("RUNNING")).toEqual(RUNNING);
    expect(tuple("FINAL")).toEqual(FINAL);
    expect(draftToken(FP, 2)).toBe(createHash("sha256").update(`draft:${FP}:2`).digest("hex"));
  });

  it("finds the query of each rewritable check", () => {
    expect(findingQueryId("top_db_time_query", '{"queryid": "42"}', "42")).toBe("42");
    expect(findingQueryId("disk_spill", "{}", "-17@app")).toBe("-17");
    expect(findingQueryId("row_misestimate", '{"top_queries": [{"queryid": "7"}]}', "ep-a")).toBe("7");
    expect(findingQueryId("missing_fk_index", '{"queryid": "42"}', "x")).toBeNull();
  });

  it("explains why drafting is not set up", () => {
    expect(setUpOf(false, { ready: "true" })).toMatchObject({ ready: false, reason: expect.stringMatching(/turned off/) });
    expect(setUpOf(true, undefined)).toMatchObject({ ready: false, reason: expect.stringMatching(/not checked/) });
    expect(setUpOf(true, { ready: "false", reason: "no CAN_QUERY" })).toEqual({ ready: false, reason: "no CAN_QUERY" });
    expect(setUpOf(true, { ready: true })).toEqual({ ready: true, reason: null });
  });

  it("derives pending, final, the next attempt and the last refusal", () => {
    const s = requestState([attempt(2, "verify_failed"), attempt(1, "verified")],
                           [{ requested_at: "2026-10-29T13:00:00Z", outcome: "stale:attempt_changed" },
                            { requested_at: "2026-10-29T12:00:00Z", outcome: "claimed" }]);
    expect(s).toEqual({ pending: false, latestFinal: true, nextAttempt: 3,
                        refused: { reason: "attempt_changed", at: "2026-10-29T13:00:00Z" } });
    expect(requestState([attempt(1, "verifying")], [{ outcome: null }])).toMatchObject({ pending: true, latestFinal: false });
    expect(requestState([], [])).toEqual({ pending: false, latestFinal: true, nextAttempt: 1, refused: null });
  });
});

describe("drafts handler", () => {
  it("returns the card data for a manager, newest attempt first, with the original query", async () => {
    const view = await setup({ pii: true, attempts: [attempt(2, "verified"), attempt(1, "draft_failed")] }).h.drafts(ctx, FP);
    expect(view).toMatchObject({ rewritable: true, setUp: { ready: true }, canManage: true, canRequest: true,
                                 pending: false, nextAttempt: 3 });
    expect(view.original).toMatchObject({ queryid: "42", kind: "full", text: "SELECT x FROM t WHERE a = 'secret'" });
    expect(view.attempts.map((a) => a.attempt)).toEqual(["2", "1"]);
    expect(view.attempts[0].sql).toBe(REWRITE);
  });

  it("masks the rewrite for users who may not read query text", async () => {
    const view = await setup({ attempts: [attempt(1, "verified")] }).h.drafts(ctx, FP);
    expect(view.attempts[0].sql).toBe("SELECT o.id FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 AND o.status <> ? ORDER BY o.id");
    expect(view.original).toMatchObject({ kind: "skeleton", text: "SELECT x FROM t WHERE a = ?" });
  });

  it("offers no request while one is pending or running, to non-managers, or when not set up", async () => {
    expect((await setup({ requests: [{ outcome: null }] }).h.drafts(ctx, FP)).canRequest).toBe(false);
    expect((await setup({ attempts: [attempt(1, "drafting")] }).h.drafts(ctx, FP)).canRequest).toBe(false);
    expect((await setup({ manager: false }).h.drafts(ctx, FP))).toMatchObject({ canRequest: false, canManage: false });
    expect((await setup({ ready: false }).h.drafts(ctx, FP))).toMatchObject({
      canRequest: false, setUp: { ready: false, reason: "serving endpoint m is NOT_READY" } });
    expect((await setup({ enabled: false }).h.drafts(ctx, FP)).setUp.ready).toBe(false);
    const other = await setup({ finding: { ...finding, check_id: "missing_fk_index" } }).h.drafts(ctx, FP);
    expect(other).toMatchObject({ rewritable: false, canRequest: false, original: null });
  });

  it("404s a hidden finding and 400s a malformed fingerprint", async () => {
    await expect(setup({ finding: null }).h.drafts(ctx, FP)).rejects.toMatchObject({ status: 404 });
    await expect(setup().h.drafts(ctx, "nope")).rejects.toMatchObject({ status: 400 });
  });
});

describe("draft requests", () => {
  it("records a draft approval for the next attempt and starts the fixer", async () => {
    const s = setup({ attempts: [attempt(1, "verify_failed")] });
    expect(await s.h.requestDraft(ctx, FP)).toEqual({ approvalId: "ap-9", attempt: 2 });
    expect(s.writes).toEqual([{ approval_id: "ap-9", action_id: "", fingerprint: FP, request: "draft",
                                sql_hash: draftToken(FP, 2), approver: "me@x.com", note: "" }]);
  });

  it("says the schedule will pick it up when the job could not start", async () => {
    expect(await setup({ started: false }).h.requestDraft(ctx, FP)).toMatchObject({ message: expect.stringMatching(/15-minute/) });
  });

  it("refuses non-managers with 403 and impossible requests with 409, writing nothing", async () => {
    const cases: [Parameters<typeof setup>[0], number][] = [
      [{ manager: false }, 403], [{ ready: false }, 409], [{ requests: [{ outcome: null }] }, 409],
      [{ attempts: [attempt(1, "verifying")] }, 409], [{ finding: { ...finding, status: "resolved" } }, 409],
      [{ finding: { ...finding, check_id: "missing_fk_index" } }, 409],
    ];
    for (const [o, status] of cases) {
      const s = setup(o);
      await expect(s.h.requestDraft(ctx, FP)).rejects.toMatchObject({ status });
      expect(s.writes).toHaveLength(0);
    }
  });
});

describe("rewrite actions through the fix routes", () => {
  it("refuses fix requests on a rewrite and offers none", async () => {
    const s = setup({ pii: true });
    expect(await s.h.action(ctx, AID)).toMatchObject({ requests: [] });
    await expect(s.h.request(ctx, AID, { request: "verify" })).rejects.toMatchObject({ status: 409 });
    expect(s.writes).toHaveLength(0);
  });

  it("masks a rewrite's SQL on the action route too", async () => {
    expect((await setup().h.action(ctx, AID)).action.sql).toContain("o.status <> ?");
  });
});
