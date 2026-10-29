import { describe, expect, it } from "vitest";
import type { Access } from "../lib/access";
import { ReportNotFound, buildReport, endpointOf, histogramP99, ioOf, seriesOf } from "../report/build";

const HC = "11111111-2222-4333-8444-555555555555";
const NOW = new Date("2026-10-29T12:00:00Z");
const A: Access = { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: true };
const BRANCHES = [
  { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", min_cu: "1", max_cu: "4", suspend_timeout_s: "300", enrolled: "true" },
  { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-r", endpoint_type: "ENDPOINT_TYPE_READ_ONLY", current_state: "ACTIVE", min_cu: "1", max_cu: "1", suspend_timeout_s: "300", enrolled: "true" },
  { branch_id: "br-dev", branch_name: "dev", is_default_branch: "false", endpoint_id: "ep-d", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "IDLE", min_cu: "0.5", max_cu: "1", suspend_timeout_s: "300", enrolled: "false" },
];
const FINDING = {
  fingerprint: "f1", check_id: "dead_tuples", title: "High share of dead tuples", category: "maintenance", endpoint_id: "ep-a",
  object: "db:hc_test.orders", status: "open", severity: "warning", measure: "15", evidence: JSON.stringify({ n_dead_tup: 5000, statement: "SELECT ssn FROM t" }),
  explain: "Dead tuples pile up. More.", candidate_sql: "VACUUM (ANALYZE) hc_test.orders;", queryid: null,
};

function runner(rows: Record<string, Record<string, unknown>[]>, fail: string[] = []) {
  const calls: { name: string; params: Record<string, unknown> }[] = [];
  return {
    calls,
    async run(name: string, _ids: string[], params: Record<string, string | Date> = {}) {
      calls.push({ name, params });
      if (fail.includes(name)) throw new Error(`${name} exploded`);
      return rows[name] ?? [];
    },
  };
}
const fresh = {
  report_overview: [{ telemetry_at: "2026-10-29T11:50:00Z", lfc_hit_pct: "99.5" }],
  meta: [{ last_success_at: "2026-10-29T11:50:00Z" }],
  report_tables: [{ collected_at: "2026-10-29T11:50:00Z", schema_name: "hc_test", table_name: "orders" }],
};
const okStatus = [{ check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok", error: null }];

describe("buildReport", () => {
  it("resolves the default branch, scopes every query to it and scores the findings", async () => {
    const r = runner({ report_branches: BRANCHES, report_findings: [FINDING], report_checks: okStatus, ...fresh });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "default", now: NOW, scheduleMinutes: 60 });
    expect(rep.branch).toEqual({ id: "br-prod", name: "production", isDefault: true });
    expect(rep.primary).toBe("ep-a");
    expect(rep.endpoints.map((e) => e.type)).toEqual(["primary", "replica"]);
    expect(r.calls.find((c) => c.name === "report_tables")?.params).toEqual({ project_id: HC, branch_id: "br-prod", endpoint_id: "ep-a" });
    expect(r.calls.find((c) => c.name === "report_findings")?.params).toEqual({ project_id: HC, branch_id: "br-prod" });
    const vac = rep.areas.find((a) => a.id === "vacuum_xid");
    expect([vac?.state, vac?.score, vac?.worst?.fingerprint]).toEqual(["warn", 3, "f1"]);
    expect(rep.score).toEqual({ pct: 60, grade: "C", assessed: 1, total: 12 });
    expect(rep.runbook[0]).toMatchObject({ fingerprint: "f1", priority: "MEDIUM", sql: "VACUUM (ANALYZE) hc_test.orders;" });
    expect(rep.banners).toEqual([]);
  });

  it("returns 404 (ReportNotFound) for a branch outside the project", async () => {
    const r = runner({ report_branches: BRANCHES });
    await expect(buildReport({ runner: r, access: A, projectId: HC, branch: "br-other", now: NOW, scheduleMinutes: 60 })).rejects.toBeInstanceOf(ReportNotFound);
    await expect(buildReport({ runner: runner({}), access: A, projectId: HC, branch: "default", now: NOW, scheduleMinutes: 60 })).rejects.toBeInstanceOf(ReportNotFound);
  });

  it("never carries statement text in evidence, even for a PII member", async () => {
    const r = runner({ report_branches: BRANCHES, report_findings: [FINDING], report_checks: okStatus, ...fresh });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(JSON.stringify(rep)).not.toContain("SELECT ssn");
  });

  it("keeps rendering when one section fails, and marks that section", async () => {
    const r = runner({ report_branches: BRANCHES, report_findings: [FINDING], report_checks: okStatus, ...fresh }, ["report_tables"]);
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(rep.sections.tables).toEqual({ status: "error", data: [], error: "This panel could not be loaded." });
    expect(rep.sections.settings.status).toBe("ok");
    expect(rep.runbook).toHaveLength(1);
  });

  it("gives no score or grade when the findings query fails, never a false A (I1)", async () => {
    const r = runner({ report_branches: BRANCHES, report_checks: okStatus, ...fresh }, ["report_findings"]);
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(rep.score).toEqual({ pct: null, grade: null, assessed: 0, total: 12, unavailable: true });
    expect(rep.areas.every((x) => x.state === "not_assessed" && x.score === null && x.note === "findings unavailable")).toBe(true);
    expect(rep.unavailable).toEqual(["findings"]);
    expect(rep.banners).toContain("Findings could not be loaded: the scorecard and runbook are incomplete.");
    const c = await buildReport({ runner: runner({ report_branches: BRANCHES, report_findings: [FINDING], ...fresh }, ["report_checks"]), access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(c.unavailable).toEqual(["checks"]);
    expect([c.score.pct, c.score.grade]).toEqual([null, null]);
  });

  it("carries last_ok_at from report_checks into the area's last assessed time (I2)", async () => {
    const asleep = [{ check_id: "dead_tuples", endpoint_id: "ep-a", status: "skipped:compute_inactive", error: null, last_ok_at: "2026-10-29T03:00:00Z" }];
    const r = runner({ report_branches: BRANCHES, report_findings: [FINDING], report_checks: asleep, ...fresh });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    const vac = rep.areas.find((x) => x.id === "vacuum_xid");
    expect([vac?.state, vac?.score, vac?.lastAssessed]).toEqual(["warn", 3, "2026-10-29T03:00:00.000Z"]);
    expect(rep.score.grade).toBe("C");
  });

  it("explains a branch that is not enrolled and a suspended compute, without inventory queries failing", async () => {
    const r = runner({ report_branches: BRANCHES, ...fresh });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-dev", now: NOW, scheduleMinutes: 60 });
    expect(rep.banners.some((b) => b.startsWith("Postgres checks are not enabled on this branch"))).toBe(true);
    expect(rep.banners.some((b) => b.startsWith("The compute is idle"))).toBe(true);
  });

  it("warns about young counters, stale engine data and missing telemetry", async () => {
    const r = runner({
      report_branches: BRANCHES,
      report_checks: [{ check_id: "unused_index", endpoint_id: "ep-a", status: "skipped:stats_young", error: "counters too young (reset 3 h ago)" }],
      meta: [{ last_success_at: "2026-10-29T08:00:00Z" }],
    });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect(rep.banners).toEqual(expect.arrayContaining([
      expect.stringMatching(/^The health engine has not completed a run/),
      expect.stringMatching(/reset 3 h ago/),
      expect.stringMatching(/^No telemetry for this branch/),
    ]));
    expect(rep.areas.find((a) => a.id === "indexes")?.note).toBe("counters too young (reset 3 h ago)");
    expect(rep.score.assessed).toBe(0);
  });

  it("marks inventory older than two analysis runs as stale", async () => {
    const r = runner({ report_branches: BRANCHES, ...fresh, report_tables: [{ collected_at: "2026-10-29T10:00:00Z", table_name: "t" }] });
    const rep = await buildReport({ runner: r, access: A, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });
    expect([rep.sections.tables.asOf, rep.sections.tables.stale]).toEqual(["2026-10-29T10:00:00.000Z", true]);
    expect(rep.banners.some((b) => b.startsWith("The inventory is older than two analysis runs"))).toBe(true);
  });
});

describe("section shapers", () => {
  it("pivots the long-format series", () => {
    expect(seriesOf([{ bucket: "2026-10-29T11:15:00.000Z", metric: "wal_bytes", value: "10" }, { bucket: "2026-10-29T11:00:00.000Z", metric: "wal_bytes", value: "5" }]))
      .toEqual({ wal_bytes: [{ t: "2026-10-29T11:00:00.000Z", v: 5 }, { t: "2026-10-29T11:15:00.000Z", v: 10 }] });
  });

  it("shows pg_stat_io as the change since the previous snapshot, or totals after a reset", () => {
    const row = (at: string, reads: string, reset = "2026-10-28T00:00:00Z") => ({
      collected_at: at, backend_type: "autovacuum worker", object: "relation", context: "bulkread", reads, writes: "0",
      extends: "0", hits: "10", evictions: "1", reuses: "0", fsyncs: "0", read_time: null, write_time: null, stats_reset: reset,
    });
    const d = ioOf([row("2026-10-29T12:00:00Z", "150"), row("2026-10-29T11:45:00Z", "100")]);
    expect([d.delta, d.windowS, d.rows[0].reads, d.rows[0].hits, d.timing]).toEqual([true, 900, 50, 0, false]);
    const reset = ioOf([row("2026-10-29T12:00:00Z", "20", "2026-10-29T11:50:00Z"), row("2026-10-29T11:45:00Z", "100")]);
    expect([reset.delta, reset.rows[0].reads]).toEqual([false, 20]);
  });

  it("computes p99 from histogram bucket deltas", () => {
    const prev = new Map([[0.001, 0], [0.01, 0], [0.1, 0], [Infinity, 0]]);
    const cur = new Map([[0.001, 50], [0.01, 98], [0.1, 100], [Infinity, 100]]);
    expect(histogramP99(prev, cur)).toBe(100);
    expect(histogramP99(cur, prev)).toBeNull();
  });

  it("reads endpoint metrics: latest values, sources, idle clients, getpage p99", () => {
    const m = endpointOf([
      { collected_at: "2026-10-29T11:45:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.01}', value: "10" },
      { collected_at: "2026-10-29T11:45:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.1}', value: "10" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.01}', value: "110" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "neon.perf.getpage_wait_seconds_bucket", dims: '{"le": 0.1}', value: "110" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "autovacuum_workers_busy", dims: null, value: "2" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "source:neon_perf_counters", dims: null, value: "0", note: "not available: no SELECT on neon_perf_counters" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "idle_clients", dims: '{"role": "app", "application": "worker", "max_idle_s": 900}', value: "3" },
      { collected_at: "2026-10-29T12:00:00Z", metric: "server_version", dims: null, value: null, note: "17.5" },
    ]);
    expect(m.latest).toEqual({ autovacuum_workers_busy: 2 });
    expect(m.sources.neon_perf_counters).toEqual({ available: false, note: "not available: no SELECT on neon_perf_counters" });
    expect(m.idleClients).toEqual([{ role: "app", application: "worker", sessions: 3, maxIdleS: 900 }]);
    expect(m.getpageP99).toEqual([{ t: "2026-10-29T12:00:00.000Z", v: 10 }]);
    expect(m.serverVersion).toBe("17.5");
  });
});
