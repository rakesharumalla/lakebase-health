import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { blockRetry, canManage, hasRollback, legalRequests } from "../lib/approvals";
import { createHandlers } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const AID = "0123456789abcdef0123456789abcdef";
const SQL = "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);";
const ROLLBACK = "DROP INDEX CONCURRENTLY IF EXISTS i;";
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

describe("canManage", () => {
  it("accepts the owner, or a 200 on the project ACL read by project id (not uid)", async () => {
    const paths: string[] = [];
    const http = async (path: string) => {
      paths.push(path);
      return { status: path === "/api/2.0/permissions/database-projects/healthcheck" ? 200 : 404, json: {} };
    };
    expect(await canManage(http, "t", { id: "x", owner: "me@x.com" }, "me@x.com")).toBe(true);
    expect(await canManage(http, "t", { id: "healthcheck", owner: "o@x.com" }, "me@x.com")).toBe(true);
    expect(await canManage(http, "t", { id: "other", owner: "o@x.com" }, "me@x.com")).toBe(false);
    expect(paths).toEqual([
      "/api/2.0/permissions/database-projects/healthcheck",
      "/api/2.0/permissions/database-projects/other",
    ]);
  });
});

describe("legalRequests", () => {
  it("mirrors the fixer's state rules", () => {
    expect(legalRequests("proposed", true)).toEqual(["verify", "reject"]);
    expect(legalRequests("proposed", false)).toEqual(["verify", "apply", "reject"]);
    expect(legalRequests("verified", true)).toEqual(["apply", "reject"]);
    expect(legalRequests("verify_failed", true)).toEqual(["verify", "reject"]);
    expect(legalRequests("apply_failed", true)).toEqual(["apply", "reject"]);
    expect(legalRequests("regressed", true)).toEqual(["rollback"]);
    expect(legalRequests("applied", true)).toEqual(["rollback"]);
    expect(legalRequests("confirmed", true)).toEqual(["rollback"]);
    expect(legalRequests("rollback_failed", true)).toEqual(["rollback"]);
    expect(legalRequests("applying", true)).toEqual([]);
    expect(legalRequests("rejected", false)).toEqual([]);
    expect(legalRequests("blocked", true)).toEqual([]);
    expect(legalRequests("blocked", true, "verify")).toEqual(["verify"]);
    expect(legalRequests("blocked", true, "apply")).toEqual(["apply"]);
  });

  it("never offers a rollback when there is nothing runnable to roll back to", () => {
    expect(legalRequests("applied", true, null, false)).toEqual([]);
    expect(legalRequests("regressed", true, null, false)).toEqual([]);
    expect(legalRequests("proposed", false, null, false)).toEqual(["verify", "apply", "reject"]);
    expect(hasRollback("DROP INDEX CONCURRENTLY IF EXISTS s.i;")).toBe(true);
    for (const v of [null, undefined, "", "  ", "-- VACUUM changes no data; nothing to roll back", "/* none */"])
      expect(hasRollback(v)).toBe(false);
  });
});

describe("blockRetry", () => {
  const blocked = (kind: string, report: unknown, error: string | null = null) =>
    [{ kind: "verify", outcome: "claimed" }, { kind, outcome: "blocked", report: JSON.stringify(report), error }];
  it("retries the blocked request after a privilege block, never after an allowlist block", () => {
    expect(blockRetry(blocked("verify", { blocked_reason: "privileges" }))).toBe("verify");
    expect(blockRetry(blocked("apply", { blocked_reason: "privileges" }))).toBe("apply");
    expect(blockRetry(blocked("verify", { blocked_reason: "not_allowed" }))).toBeNull();
    expect(blockRetry(blocked("verify", { approval_id: "x" }, "fixer not enrolled or missing privileges on this project: 42501"))).toBe("verify");
    expect(blockRetry(blocked("rollback", { blocked_reason: "privileges" }))).toBeNull();
    expect(blockRetry([])).toBeNull();
  });
});

describe("request handler", () => {
  function setup(manager: boolean, state = "proposed",
                 opts: { started?: boolean; visible?: boolean; runs?: Record<string, unknown>[]; rollback?: string | null;
                         findingStatus?: string; expiresAt?: string } = {}) {
    const writes: Record<string, unknown>[] = [];
    const calls: string[] = [];
    let triggered = 0;
    const h = createHandlers({
      access: { async resolve() { return { userKey: "me@x.com", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: false }; }, refresh() {} },
      runner: { async run(name: string) {
        calls.push(name);
        if (name === "action" && opts.visible !== false)
          return [{ action_id: AID, project_id: HC, state, sql: SQL, rollback_sql: opts.rollback === undefined ? ROLLBACK : opts.rollback, verify_on_branch: "true",
                    finding_status: opts.findingStatus ?? "open", expires_at: opts.expiresAt ?? "2026-11-04T12:00:00Z" }];
        if (name === "action_runs") return opts.runs ?? [{ run_id: "r1", kind: "verify" }];
        if (name === "project_actions") return [{ action_id: AID, state }];
        return [];
      } },
      scheduleMinutes: 60,
      approvals: {
        canManage: async () => manager,
        insert: async (row) => { writes.push(row); },
        trigger: async () => { triggered++; return opts.started; },
        newId: () => "ap-1",
      },
    });
    return { h, writes, calls, triggered: () => triggered };
  }
  const ctx = { token: "t", user: "me@x.com", now: new Date("2026-10-28T12:00:00Z") };

  it("records a manager's verify request with the SQL hash and triggers the fixer", async () => {
    const s = setup(true);
    expect(await s.h.request(ctx, AID, { request: "verify" })).toEqual({ approvalId: "ap-1" });
    expect(s.writes[0]).toMatchObject({ action_id: AID, request: "verify", approver: "me@x.com", sql_hash: sha(SQL) });
    expect(s.triggered()).toBe(1);
  });

  it("hashes the rollback SQL for a rollback request and truncates the note", async () => {
    const s = setup(true, "regressed");
    await s.h.request(ctx, AID, { request: "rollback", note: "x".repeat(600) });
    expect(s.writes[0]).toMatchObject({ request: "rollback", sql_hash: sha(ROLLBACK) });
    expect(String(s.writes[0].note)).toHaveLength(500);
  });

  it("says the fixer schedule will pick the request up when the job could not be started", async () => {
    const r = await setup(true, "proposed", { started: false }).h.request(ctx, AID, { request: "verify" });
    expect(r).toMatchObject({ approvalId: "ap-1", message: expect.stringMatching(/15-minute schedule/) });
  });

  it("offers and accepts no rollback for a fix with nothing to undo", async () => {
    for (const rb of [null, "-- ANALYZE only refreshes planner statistics; nothing to roll back"]) {
      const s = setup(true, "applied", { rollback: rb });
      expect(await s.h.action(ctx, AID)).toMatchObject({ requests: [] });
      await expect(s.h.request(ctx, AID, { request: "rollback" })).rejects.toMatchObject({ status: 409 });
      expect(s.writes).toHaveLength(0);
    }
  });

  it("refuses non-managers with 403 and illegal requests with 409", async () => {
    await expect(setup(false).h.request(ctx, AID, { request: "verify" })).rejects.toMatchObject({ status: 403 });
    await expect(setup(true).h.request(ctx, AID, { request: "apply" })).rejects.toMatchObject({ status: 409 });
    await expect(setup(true).h.request(ctx, AID, { request: "drop" })).rejects.toMatchObject({ status: 400 });
  });

  it("returns 404 for actions outside the caller's projects and 400 for malformed ids", async () => {
    const s = setup(true, "proposed", { visible: false });
    await expect(s.h.request(ctx, AID, { request: "verify" })).rejects.toMatchObject({ status: 404 });
    await expect(s.h.action(ctx, AID)).rejects.toMatchObject({ status: 404 });
    await expect(s.h.request(ctx, "nope", { request: "verify" })).rejects.toMatchObject({ status: 400 });
    expect(s.writes).toHaveLength(0);
  });

  it("accepts a retry on an action blocked for privileges and refuses one blocked by the allowlist", async () => {
    const run = (reason: string) => [{ run_id: "r2", kind: "verify", outcome: "blocked", report: JSON.stringify({ blocked_reason: reason }) }];
    const ok = setup(true, "blocked", { runs: run("privileges") });
    expect(await ok.h.request(ctx, AID, { request: "verify" })).toEqual({ approvalId: "ap-1" });
    expect(await ok.h.action(ctx, AID)).toMatchObject({ requests: ["verify"], retryAfterEnrolment: true });
    await expect(setup(true, "blocked", { runs: run("not_allowed") }).h.request(ctx, AID, { request: "verify" }))
      .rejects.toMatchObject({ status: 409 });
  });

  it("returns an action with its runs, manager flag and legal requests", async () => {
    expect(await setup(true, "verified").h.action(ctx, AID)).toMatchObject({
      action: { action_id: AID }, runs: [{ run_id: "r1" }], canManage: true, requests: ["apply", "reject"],
    });
    expect(await setup(false, "verified").h.action(ctx, AID)).toMatchObject({ canManage: false });
  });

  // Final review app-I1: the fixer refuses verify/apply unless the finding is new/open and the action has not
  // expired, so the app neither offers nor accepts them; reject and rollback stay.
  it("refuses verify and apply with 409 not_actionable when the finding is not new/open or the fix expired", async () => {
    for (const [opts, reason] of [
      [{ findingStatus: "superseded" }, /superseded/],
      [{ findingStatus: "resolved" }, /resolved/],
      [{ findingStatus: "suppressed" }, /suppressed/],
      [{ expiresAt: "2026-10-28T11:59:00Z" }, /expired/],
    ] as const) {
      const s = setup(true, "proposed", opts);
      await expect(s.h.request(ctx, AID, { request: "verify" })).rejects.toMatchObject({ status: 409, code: "not_actionable" });
      expect(s.writes).toHaveLength(0);
      expect(s.triggered()).toBe(0);
      const view = await s.h.action(ctx, AID);
      expect(view.requests).toEqual(["reject"]);
      expect(view.notActionable).toMatch(reason);
    }
    const ok = await setup(true, "proposed", { findingStatus: "new" }).h.action(ctx, AID);
    expect(ok).toMatchObject({ requests: ["verify", "reject"], notActionable: null });
  });

  it("still offers and accepts a rollback once the finding has resolved", async () => {
    const s = setup(true, "applied", { findingStatus: "resolved" });
    expect(await s.h.action(ctx, AID)).toMatchObject({ requests: ["rollback"] });
    expect(await s.h.request(ctx, AID, { request: "rollback" })).toEqual({ approvalId: "ap-1" });
  });

  it("lists a visible project's actions and 404s an invisible one", async () => {
    const s = setup(true);
    expect(await s.h.actions(ctx, HC)).toMatchObject({ groups: [{ rows: [{ action_id: AID }] }], canManage: true });
    await expect(s.h.actions(ctx, "11111111-2222-3333-4444-555555555555")).rejects.toMatchObject({ status: 404 });
  });
});
