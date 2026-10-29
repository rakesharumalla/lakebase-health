// Opt-in: npm run test:live (runs every server query on my-profile as the CLI user)
import { join } from "node:path";
import { WorkspaceClient } from "@databricks/sdk-experimental";
import { describe, expect, it } from "vitest";
import { queryIdOf } from "../../lib/group";
import { createRunner, loadQueries, statementExec } from "../../lib/sql";

const live = process.env.LIVE === "1";
const HC = "11111111-2222-4333-8444-555555555555";
const EP = "ep-sample-primary-a1b2c3d4";
const to = new Date();
const from = new Date(to.getTime() - 7 * 24 * 3600 * 1000 + 60_000);

describe.runIf(live)("queries on my-profile", () => {
  const runner = createRunner(
    loadQueries(join(__dirname, "..", "..", "queries"), "my_catalog"),
    statementExec(new WorkspaceClient({ profile: "my-profile" }), "0123456789abcdef"),
  );
  const timed = async (name: string, params: Record<string, string | Date> = {}) => {
    const t = Date.now();
    const rows = await runner.run(name, [HC], params);
    return { rows, ms: Date.now() - t };
  };

  it("fleet and project queries return healthcheck rows quickly", async () => {
    const cases: [string, Record<string, string>, string][] = [
      ["meta", {}, "last_success_at"],
      ["fleet", {}, "project_id"],
      ["project_findings", { project_id: HC }, "fingerprint"],
      ["project_computes", { project_id: HC }, "endpoint_id"],
    ];
    for (const [name, params, col] of cases) {
      const { rows, ms } = await timed(name, params);
      expect(rows.length, name).toBeGreaterThan(0);
      expect(Object.keys(rows[0]), name).toContain(col);
      expect(ms, name).toBeLessThan(5000);
    }
  }, 120_000);

  it("finding queries work for a real healthcheck fingerprint", async () => {
    const [f] = (await timed("project_findings", { project_id: HC })).rows;
    const fp = String(f.fingerprint);
    for (const name of ["finding", "finding_history", "finding_actions", "finding_related"]) {
      const { ms } = await timed(name, { fingerprint: fp });
      expect(ms, name).toBeLessThan(5000);
    }
    expect((await timed("finding", { fingerprint: fp })).rows[0].fingerprint).toBe(fp);
  }, 120_000);

  it("timeline queries cover 7 days within 5 s each and map the endpoint", async () => {
    expect((await timed("endpoint_project", { endpoint_id: EP })).rows[0].project_id).toBe(HC);
    for (const name of ["timeline_waits", "timeline_locks", "timeline_commits", "timeline_events", "timeline_gauges"]) {
      const { ms } = await timed(name, { endpoint_id: EP, from_ts: from, to_ts: to });
      expect(ms, name).toBeLessThan(5000);
    }
    const { rows } = await timed("timeline_locks", { endpoint_id: EP, from_ts: from, to_ts: to });
    expect(rows.some((r) => r.blocker_class === "idle_in_transaction" || r.blocker_class === "not_sampled")).toBe(true);
    const at = new Date(String(rows[0].minute));
    expect((await timed("timeline_samples", { endpoint_id: EP, at_ts: at })).ms).toBeLessThan(5000);
  }, 180_000);

  it("query text, project endpoints and per-state fixes run on live data", async () => {
    const { rows: eps } = await timed("project_endpoints", { project_id: HC, endpoint_ids: `${EP},ep-not-there` });
    expect(eps.map((r) => r.endpoint_id)).toEqual([EP]);
    const { rows: found } = await timed("project_findings", { project_id: HC });
    const ids = [...new Set(found.filter((r) => r.endpoint_id === EP).map(queryIdOf).filter((q): q is string => !!q))].slice(0, 8);
    expect(ids.length).toBeGreaterThan(0);
    const pairs = [...ids, "1"].map((q) => `${EP}:${q}`).join(",");
    const { rows, ms } = await timed("query_text", { project_id: HC, pairs });
    expect(ms).toBeLessThan(8000);
    expect(rows.map((r) => r.queryid).sort()).toEqual([...ids, "1"].sort());
    expect(rows.find((r) => r.queryid === "1")?.plan_text ?? null).toBeNull();
    // PII masking is off on my-profile: real text comes back for the queries the findings name.
    expect(rows.filter((r) => r.plan_text || r.stat_text).length).toBeGreaterThan(0);
    const { rows: acts } = await timed("project_actions", { project_id: HC, state: "", row_limit: "10" });
    const perState = new Map<string, number>();
    for (const r of acts) perState.set(String(r.state), (perState.get(String(r.state)) ?? 0) + 1);
    for (const [st, k] of perState) expect(k, st).toBeLessThanOrEqual(10);
    const { rows: grouped } = await timed("project_findings", { project_id: HC });
    expect(Object.keys(grouped[0])).toEqual(expect.arrayContaining(["queryid", "top_queryids", "db_ms"]));
  }, 120_000);

  it("report queries run on healthcheck's default branch within 5 s each", async () => {
    const { rows: br } = await timed("report_branches", { project_id: HC });
    const prod = br.find((r) => r.is_default_branch === true || r.is_default_branch === "true");
    expect(prod).toBeDefined();
    const branch = String(prod?.branch_id);
    const ep = String(br.find((r) => r.branch_id === branch && String(r.endpoint_type).endsWith("READ_WRITE"))?.endpoint_id);
    for (const name of ["report_findings", "report_actions", "report_checks"]) {
      const { ms } = await timed(name, { project_id: HC, branch_id: branch });
      expect(ms, name).toBeLessThan(5000);
    }
    const panels = ["report_overview", "report_databases", "report_tables", "report_indexes", "report_statements",
      "report_settings", "report_io", "report_endpoint", "report_series", "report_waits"];
    for (const name of panels) {
      const { rows, ms } = await timed(name, { project_id: HC, branch_id: branch, endpoint_id: ep });
      expect(ms, name).toBeLessThan(5000);
      if (name === "report_overview") expect(rows).toHaveLength(1);
    }
  }, 300_000);

  it("returns nothing for a project outside the allow-list", async () => {
    const rows = await runner.run("project_findings", ["00000000-0000-0000-0000-000000000000"], { project_id: HC });
    expect(rows).toEqual([]);
  }, 60_000);
});
