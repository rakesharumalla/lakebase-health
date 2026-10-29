import { describe, expect, it } from "vitest";
import { AccessError, type Access } from "../lib/access";
import { createHandlers, HttpError } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const OTHER = "11111111-2222-3333-4444-555555555555";
const FP = "cc59509434e9e437c838cc12b394bafa";
const NOW = new Date("2026-10-28T12:00:00Z");
const ctx = { token: "t", user: "alice", now: NOW };

function deps(opts: { pii?: boolean; rows?: Record<string, Record<string, unknown>[]>; fail?: Error } = {}) {
  const calls: { name: string; ids: string[]; params: Record<string, unknown> }[] = [];
  return {
    calls,
    access: {
      async resolve(token?: string) {
        if (opts.fail) throw opts.fail;
        if (!token) throw new AccessError(401, "no token");
        const a: Access = { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: !!opts.pii };
        return a;
      },
      refresh() {},
    },
    runner: {
      async run(name: string, ids: string[], params: Record<string, string | Date> = {}) {
        calls.push({ name, ids, params });
        return opts.rows?.[name] ?? [];
      },
    },
    scheduleMinutes: 60,
  };
}

describe("handlers", () => {
  it("always passes the caller's allow-list, never a client value", async () => {
    const d = deps({ rows: { fleet: [{ project_id: HC, critical: "2" }] } });
    const out = await createHandlers(d).fleet(ctx);
    expect(d.calls[0]).toMatchObject({ name: "fleet", ids: [HC] });
    expect(out.projects[0].project_name).toBe("healthcheck");
  });

  it("returns 404 for a project outside the allow-list without querying", async () => {
    const d = deps();
    await expect(createHandlers(d).project(ctx, OTHER)).rejects.toMatchObject({ status: 404 });
    expect(d.calls).toEqual([]);
  });

  it("returns 404 for a fingerprint the user cannot see, and 400 for a malformed one", async () => {
    const h = createHandlers(deps({ rows: { finding: [] } }));
    await expect(h.finding(ctx, FP)).rejects.toMatchObject({ status: 404 });
    await expect(h.finding(ctx, "zz")).rejects.toMatchObject({ status: 400 });
  });

  it("masks finding text for non-PII users and not for PII members", async () => {
    const rows = { finding: [{ fingerprint: FP, project_id: HC, query_text: "SELECT ssn", evidence: '{"statement":"x"}' }] };
    const masked = await createHandlers(deps({ rows })).finding(ctx, FP);
    expect(masked.finding.query_text).toBe("[masked]");
    expect(JSON.parse(String(masked.finding.evidence)).statement).toBe("[masked]");
    const open = await createHandlers(deps({ rows, pii: true })).finding(ctx, FP);
    expect(open.finding.query_text).toBe("SELECT ssn");
  });

  it("checks the timeline endpoint's project, clamps the window and masks samples", async () => {
    const d = deps({ rows: { endpoint_project: [{ project_id: HC }], timeline_samples: [{ pid: "1", query_text: "q" }] } });
    const h = createHandlers(d);
    const out = await h.timeline(ctx, "ep-sample-primary-a1b2c3d4", "2026-10-01T00:00:00Z", undefined);
    expect(out.from).toBe("2026-10-21T12:00:00.000Z");
    expect(d.calls.find((c) => c.name === "timeline_locks")?.params).toMatchObject({ endpoint_id: "ep-sample-primary-a1b2c3d4" });
    await expect(h.timeline(ctx, "ep-sample-primary-a1b2c3d4", "2026-10-28T10:00:00Z", "2026-10-28T09:00:00Z")).rejects.toMatchObject({ status: 400 });
    const s = await h.samples(ctx, "ep-sample-primary-a1b2c3d4", "2026-10-28T11:00:00Z");
    expect(s.samples[0].query_text).toBe("[masked]");
  });

  it("returns 404 for an endpoint whose project is not visible", async () => {
    const h = createHandlers(deps({ rows: { endpoint_project: [] } }));
    await expect(h.timeline(ctx, "ep-other-x", undefined, undefined)).rejects.toMatchObject({ status: 404 });
  });

  it("propagates 401 without a token and never falls back", async () => {
    const h = createHandlers(deps());
    await expect(h.fleet({ now: NOW })).rejects.toMatchObject({ status: 401 });
    await expect(createHandlers(deps({ fail: new AccessError(502, "api down") })).fleet(ctx)).rejects.toMatchObject({ status: 502 });
  });

  it("flags the engine as stale after two missed schedules", async () => {
    const fresh = await createHandlers(deps({ rows: { meta: [{ last_success_at: "2026-10-28T11:00:00Z", open_findings: "3" }] } })).meta(ctx);
    expect(fresh).toMatchObject({ stale: false, openFindings: 3 });
    const stale = await createHandlers(deps({ rows: { meta: [{ last_success_at: "2026-10-28T09:30:00Z" }] } })).meta(ctx);
    expect(stale.stale).toBe(true);
    const never = await createHandlers(deps({ rows: { meta: [] } })).meta(ctx);
    expect(never).toMatchObject({ lastSuccessAt: null, stale: true });
  });

  it("HttpError carries the status", () => {
    expect(new HttpError(404, "x").status).toBe(404);
  });
});

describe("meta with no visible projects", () => {
  it("still reports the engine's last run so the UI shows the access explanation, not the setup page", async () => {
    const d = deps({ rows: { meta: [{ last_success_at: "2026-10-28T11:00:00Z", open_findings: "0" }] } });
    const empty: Access = { userKey: "u", projectIds: [], projectNames: {}, pii: false };
    d.access.resolve = async () => empty;
    const m = await createHandlers(d).meta(ctx);
    expect(m).toMatchObject({ lastSuccessAt: "2026-10-28T11:00:00.000Z", stale: false, openFindings: 0 });
    expect(d.calls[0].ids).toEqual(["-"]);
  });
});

describe("grouped project findings", () => {
  const perf = (i: number, o: Record<string, unknown> = {}) => ({
    fingerprint: `${i}`.padStart(32, "0"), check_id: "plan_flip", title: "Plan flip", category: "performance",
    endpoint_id: "ep-a", object: String(i), status: "open", severity: "warning", measure: String(i), occurrences: "1",
    queryid: String(i), ...o,
  });

  it("returns the top 10 grouped rows per category with totals, and counts superseded as not open", async () => {
    const findings = [
      ...Array.from({ length: 14 }, (_, i) => perf(i)),
      perf(3, { fingerprint: "a".repeat(32), check_id: "storage_wait", severity: "critical" }),
      perf(99, { status: "superseded", severity: "critical", category: "storage" }),
      perf(98, { status: "resolved", resolved_at: "2026-10-27T00:00:00Z" }),
    ];
    // counts come from SQL (findings, superseded excluded by the query), not from the grouped rows
    const project_finding_counts = [{ category: "performance", bucket: "warning", n: "14" },
      { category: "performance", bucket: "critical", n: "1" }, { category: "performance", bucket: "resolved", n: "1" }];
    const out = await createHandlers(deps({ rows: { project_findings: findings, project_finding_counts } })).project(ctx, HC);
    expect(out.sections.map((s) => s.category)).toEqual(["performance"]);
    const [p] = out.sections;
    expect(p.total).toBe(14);
    expect(p.rows).toHaveLength(10);
    expect(p.rows[0]).toMatchObject({ queryid: "3", severity: "critical" });
    expect(p.rows[0].issues.map((i) => i.check_id)).toEqual(["storage_wait", "plan_flip"]);
    expect(p.rows.slice(1).map((r) => r.queryid)).toEqual(["13", "12", "11", "10", "9", "8", "7", "6", "5"]);
    expect(out.counts).toEqual({ critical: 1, warning: 14, info: 0, resolved: 1 });
    expect(out.resolved.total).toBe(1);
  });

  it("returns a whole section on demand, rejecting bad sections and hidden projects", async () => {
    const findings = Array.from({ length: 14 }, (_, i) => perf(i));
    const d = deps({ rows: { project_findings: findings } });
    const h = createHandlers(d);
    const all = await h.projectFindings(ctx, HC, "performance");
    expect(all).toMatchObject({ section: "performance", total: 14 });
    expect(all.rows).toHaveLength(14);
    await expect(h.projectFindings(ctx, HC, "x'; --")).rejects.toMatchObject({ status: 400 });
    await expect(h.projectFindings(ctx, OTHER, "performance")).rejects.toMatchObject({ status: 404 });
  });

  it("groups related findings on the finding page and pages them", async () => {
    const related = Array.from({ length: 12 }, (_, i) => perf(i));
    const rows = { finding: [{ fingerprint: FP, project_id: HC, check_id: "row_misestimate", status: "open", endpoint_id: "ep-a", evidence: '{"top_queries":[{"queryid":"2"}]}' }], finding_related: related };
    const h = createHandlers(deps({ rows }));
    const out = await h.finding(ctx, FP);
    expect(out.related.total).toBe(12);
    expect(out.related.rows).toHaveLength(10);
    const q2 = (await h.findingRelated(ctx, FP)).rows.find((r) => r.queryid === "2");
    expect(q2?.issues.map((i) => i.fingerprint)).toEqual([perf(2).fingerprint, FP]);
  });
});

// Final review app-I2: lists stop at a SQL LIMIT, so totals come from SQL counts and responses say when a list
// was cut ("showing first N of M").
describe("totals from SQL counts and truncated lists", () => {
  const perf = (i: number, o: Record<string, unknown> = {}) => ({
    fingerprint: `${i}`.padStart(32, "0"), check_id: "plan_flip", title: "Plan flip", category: "performance",
    endpoint_id: "ep-a", object: String(i), status: "open", severity: "warning", measure: String(i), occurrences: "1",
    queryid: String(i), ...o,
  });
  const counts = [
    { category: "performance", bucket: "warning", n: "5200" },
    { category: "performance", bucket: "critical", n: "3" },
    { category: "storage", bucket: "info", n: "40" },
    { category: "performance", bucket: "resolved", n: "12" },
  ];

  it("takes the KPI counts and section totals from SQL and flags the sections the LIMIT cut", async () => {
    const findings = Array.from({ length: 5000 }, (_, i) => perf(i));
    const d = deps({ rows: { project_findings: findings, project_finding_counts: counts } });
    const out = await createHandlers(d).project(ctx, HC);
    expect(d.calls.map((c) => c.name)).toContain("project_finding_counts");
    expect(out.counts).toEqual({ critical: 3, warning: 5200, info: 40, resolved: 12 });
    expect(out).toMatchObject({ truncated: true, limit: 5000 });
    const by = Object.fromEntries(out.sections.map((s) => [s.category, s]));
    expect(Object.keys(by)).toEqual(["performance", "storage"]); // storage fell off the list entirely
    expect(by.performance).toMatchObject({ count: 5203, shown: 5000, truncated: true });
    expect(by.storage).toMatchObject({ count: 40, shown: 0, truncated: true, rows: [] });
    expect(out.resolved).toMatchObject({ count: 12, shown: 0, truncated: true });
  });

  it("is not truncated below the LIMIT", async () => {
    const findings = [perf(1), perf(2, { status: "resolved" })];
    const c = [{ category: "performance", bucket: "warning", n: "1" }, { category: "performance", bucket: "resolved", n: "1" }];
    const out = await createHandlers(deps({ rows: { project_findings: findings, project_finding_counts: c } })).project(ctx, HC);
    expect(out).toMatchObject({ truncated: false, limit: 5000 });
    expect(out.sections[0]).toMatchObject({ count: 1, shown: 1, truncated: false });
  });

  it("says when the related list on a finding page was cut", async () => {
    const related = Array.from({ length: 2000 }, (_, i) => perf(i, { total_rows: "2600" }));
    const rows = { finding: [{ fingerprint: FP, project_id: HC, check_id: "x", status: "open", endpoint_id: "ep-a" }], finding_related: related };
    const h = createHandlers(deps({ rows }));
    expect((await h.finding(ctx, FP)).related).toMatchObject({ count: 2600, shown: 2000, truncated: true, limit: 2000 });
    expect(await h.findingRelated(ctx, FP)).toMatchObject({ count: 2600, truncated: true });
  });

  it("says when one state's full list was cut at its limit", async () => {
    const rows = { project_actions: Array.from({ length: 1000 }, (_, i) => ({ action_id: `${i}`, state: "expired", state_total: "1500" })) };
    const out = await createHandlers(deps({ rows })).actions(ctx, HC, "expired");
    expect(out).toMatchObject({ limit: 1000 });
    expect(out.groups[0]).toMatchObject({ total: 1500, truncated: true });
    const top = await createHandlers(deps({ rows: { project_actions: [{ action_id: "1", state: "expired", state_total: "1500" }] } })).actions(ctx, HC);
    expect(top.groups[0]).toMatchObject({ total: 1500, truncated: false }); // top 10 + "Show all", not a cut list
  });

  it("keeps each SQL LIMIT and the related count in the query files", async () => {
    const { readFileSync } = await import("node:fs");
    const q = (n: string) => readFileSync(new URL(`../queries/${n}.sql`, import.meta.url), "utf8");
    expect(q("project_findings")).toMatch(/LIMIT 5000\s*$/);
    expect(q("finding_related")).toMatch(/LIMIT 2000\s*$/);
    expect(q("finding_related")).toContain("count(*) OVER () AS total_rows");
    expect(q("project_finding_counts")).toMatch(/GROUP BY/);
  });
});

describe("fixes grouped by state", () => {
  it("asks for 10 per state, orders states by what needs a decision, and fetches one state in full", async () => {
    const rows = {
      project_actions: [
        { action_id: "1", state: "applied", state_total: "1" },
        { action_id: "2", state: "proposed", state_total: "40" },
        { action_id: "3", state: "proposed", state_total: "40" },
      ],
    };
    const d = deps({ rows });
    const h = createHandlers(d);
    const out = await h.actions(ctx, HC);
    expect(d.calls[0].params).toMatchObject({ project_id: HC, state: "", row_limit: "10" });
    expect(out.groups.map((g) => [g.state, g.total, g.rows.length])).toEqual([["proposed", 40, 2], ["applied", 1, 1]]);
    await h.actions(ctx, HC, "proposed");
    expect(d.calls[d.calls.length - 1].params).toMatchObject({ state: "proposed", row_limit: "1000" });
    await expect(h.actions(ctx, HC, "x y")).rejects.toMatchObject({ status: 400 });
  });
});

describe("query text lookup", () => {
  const text = [{ endpoint_id: "ep-a", queryid: "42", plan_text: "SELECT * FROM t WHERE ssn = '123'", plan_truncated: "false" }];
  const withAccess = (extra: Partial<Access>) => {
    const d = deps({ rows: { project_endpoints: [{ endpoint_id: "ep-a" }], query_text: text } });
    d.access.resolve = async () => ({ userKey: "u", projectIds: [HC], projectNames: {}, pii: false, ...extra });
    return d;
  };

  it("looks every pair up in one batch per request", async () => {
    const d = withAccess({ pii: true });
    const out = await createHandlers(d).queries(ctx, HC, "ep-a:42,ep-a:7,ep-a:42");
    expect(d.calls.filter((c) => c.name === "query_text")).toHaveLength(1);
    expect(d.calls.find((c) => c.name === "query_text")?.params).toEqual({ project_id: HC, pairs: "ep-a:42,ep-a:7" });
    expect(d.calls.find((c) => c.name === "project_endpoints")?.params).toEqual({ project_id: HC, endpoint_ids: "ep-a" });
    expect(out.queries.map((q) => q.label)).toEqual(["SELECT * FROM t WHERE ssn = '123'", "query 7"]);
  });

  it("shows full text with masking off, to PII members, the owner and admins; others get the skeleton", async () => {
    const label = async (extra: Partial<Access>) => (await createHandlers(withAccess(extra)).queries(ctx, HC, "ep-a:42")).queries[0];
    // Masking off: access sets pii for everyone (PII_MASKING=false).
    expect((await label({ pii: true })).kind).toBe("full");
    expect((await label({ owned: [HC] })).text).toBe("SELECT * FROM t WHERE ssn = '123'");
    expect((await label({ admin: true })).kind).toBe("full");
    const other = await label({ owned: [OTHER] });
    expect(other).toMatchObject({ kind: "skeleton", text: "SELECT * FROM t WHERE ssn = ?" });
  });

  it("enforces the access scope: 404 for a hidden project or an endpoint outside the project, 400 for bad ids", async () => {
    const d = withAccess({ pii: true });
    const h = createHandlers(d);
    await expect(h.queries(ctx, OTHER, "ep-a:42")).rejects.toMatchObject({ status: 404 });
    expect(d.calls).toEqual([]);
    await expect(h.queries(ctx, HC, "ep-a:42,ep-hidden:1")).rejects.toMatchObject({ status: 404 });
    await expect(h.queries(ctx, HC, "ep-a:1;DROP")).rejects.toMatchObject({ status: 400 });
    await expect(h.queries(ctx, HC, undefined)).rejects.toMatchObject({ status: 400 });
  });

  it("finding and sample text follow the same rule (owner sees text with masking on)", async () => {
    const d = deps({ rows: { finding: [{ fingerprint: FP, project_id: HC, query_text: "SELECT ssn" }] } });
    d.access.resolve = async () => ({ userKey: "u", projectIds: [HC], projectNames: {}, pii: false, owned: [HC] });
    expect((await createHandlers(d).finding(ctx, FP)).finding.query_text).toBe("SELECT ssn");
  });
});
