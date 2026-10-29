import { describe, expect, it } from "vitest";
import type { Access } from "../lib/access";
import { createHandlers } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const OTHER = "11111111-2222-3333-4444-555555555555";
const ctx = { token: "t", user: "alice", now: new Date("2026-10-29T12:00:00Z") };
const BRANCH = { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a",
  endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", enrolled: "true" };

function deps(rows: Record<string, Record<string, unknown>[]> = {}) {
  const calls: string[] = [];
  return {
    calls,
    access: {
      async resolve() {
        const a: Access = { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: false };
        return a;
      },
      refresh() {},
    },
    runner: {
      async run(name: string) {
        calls.push(name);
        return rows[name] ?? [];
      },
    },
    scheduleMinutes: 60,
  };
}

describe("report handler", () => {
  it("returns the report of a visible branch", async () => {
    const out = await createHandlers(deps({ report_branches: [BRANCH] })).report(ctx, HC, "br-prod");
    expect(out.project).toEqual({ id: HC, name: "healthcheck" });
    expect(out.branch.id).toBe("br-prod");
    expect(out.areas).toHaveLength(12);
  });

  it("returns 404 for a hidden project without querying, and for a branch the project does not have", async () => {
    const d = deps({ report_branches: [BRANCH] });
    await expect(createHandlers(d).report(ctx, OTHER, "br-prod")).rejects.toMatchObject({ status: 404 });
    expect(d.calls).toEqual([]);
    await expect(createHandlers(d).report(ctx, HC, "br-elsewhere")).rejects.toMatchObject({ status: 404 });
  });

  it("rejects malformed ids with 400", async () => {
    const h = createHandlers(deps());
    await expect(h.report(ctx, "nope", "default")).rejects.toMatchObject({ status: 400 });
    await expect(h.report(ctx, HC, "main; DROP")).rejects.toMatchObject({ status: 400 });
  });
});

describe("report export handler", () => {
  const audited = () => {
    const rows: Record<string, string>[] = [];
    return { rows, insert: async (r: Record<string, string>) => void rows.push(r), newId: () => "e1" };
  };

  it("writes an audit row and returns the file", async () => {
    const audit = audited();
    const out = await createHandlers({ ...deps({ report_branches: [BRANCH] }), audit }).reportExport(ctx, HC, "default");
    expect(out.filename).toBe("lakebase-health-healthcheck-production-20261029T1200Z.html");
    expect(out.html).toMatch(/^<!DOCTYPE html>/);
    expect(audit.rows).toEqual([{ event_id: "e1", actor: "alice", action: "report_export", project_id: HC, branch_id: "br-prod",
      detail: JSON.stringify({ filename: out.filename, unavailable: [] }) }]);
  });

  it("records failed findings and checks queries in the audit row's unavailable list (I1)", async () => {
    const audit = audited();
    const d = deps({ report_branches: [BRANCH] });
    const ok = { report_branches: [BRANCH] } as Record<string, Record<string, unknown>[]>;
    d.runner.run = async (name: string) => {
      if (name === "report_findings" || name === "report_checks") throw new Error("timeout");
      return ok[name] ?? [];
    };
    const out = await createHandlers({ ...d, audit }).reportExport(ctx, HC, "default");
    expect(JSON.parse(audit.rows[0].detail).unavailable).toEqual(["findings", "checks"]);
    expect(out.html).toContain("Score unavailable");
  });

  it("returns 404 for a hidden project or branch and writes no audit row", async () => {
    const audit = audited();
    const h = createHandlers({ ...deps({ report_branches: [BRANCH] }), audit });
    await expect(h.reportExport(ctx, OTHER, "default")).rejects.toMatchObject({ status: 404 });
    await expect(h.reportExport(ctx, HC, "br-elsewhere")).rejects.toMatchObject({ status: 404 });
    expect(audit.rows).toEqual([]);
  });

  it("fails when the audit row cannot be written", async () => {
    const audit = { insert: async () => { throw new Error("no MODIFY"); }, newId: () => "e1" };
    await expect(createHandlers({ ...deps({ report_branches: [BRANCH] }), audit }).reportExport(ctx, HC, "default")).rejects.toThrow("no MODIFY");
  });
});
