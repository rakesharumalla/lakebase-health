import { describe, expect, it } from "vitest";
import { TABS } from "../../client/src/report/tabs";
import type { Access } from "../lib/access";
import { buildReport } from "../report/build";
import { exportCss, exportFilename, renderExport } from "../report/export";

const HC = "11111111-2222-4333-8444-555555555555";
const NOW = new Date("2026-10-29T12:07:00Z");
const PII: Access = { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: true, admin: true, owned: [HC] };
const SECRET = "SELECT ssn FROM hc_test.customers WHERE email = 'a@b.c'";
const ROWS: Record<string, Record<string, unknown>[]> = {
  report_branches: [{ branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", min_cu: "1", max_cu: "4", suspend_timeout_s: "300", enrolled: "true" }],
  report_findings: [
    { fingerprint: "f1", check_id: "top_db_time_query", title: "Query uses the most DB time", category: "performance", endpoint_id: "ep-a", object: "42", status: "open", severity: "warning", measure: "50", evidence: JSON.stringify({ queryid: "42", query_text: SECRET, total_ms: 5000 }), explain: "A query dominates. More.", candidate_sql: null, queryid: "42", query_text: SECRET },
    { fingerprint: "f2", check_id: "dead_tuples", title: "High share of dead tuples", category: "maintenance", endpoint_id: "ep-a", object: "db:hc_test.orders", status: "open", severity: "critical", measure: "25", evidence: JSON.stringify({ statement: SECRET, n_dead_tup: 900 }), explain: "Dead tuples. More.", candidate_sql: "VACUUM (ANALYZE) hc_test.orders;" },
  ],
  report_checks: [{ check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok" }, { check_id: "top_db_time_query", endpoint_id: null, status: "ok" }],
  report_overview: [{ telemetry_at: "2026-10-29T12:00:00Z", lfc_hit_pct: "98.7", working_set_bytes: "1000", lfc_size_bytes: "2000" }],
  report_series: [{ bucket: "2026-10-29T11:00:00Z", metric: "lfc_hit_pct", value: "99" }, { bucket: "2026-10-29T11:15:00Z", metric: "lfc_hit_pct", value: "97" }],
  report_statements: [{ collected_at: "2026-10-29T12:00:00Z", ranking: "time", rank: "1", queryid: "42", role: "app", datname: "db", calls: "10", mean_ms: "5", total_ms: "50" }],
  meta: [{ last_success_at: "2026-10-29T12:00:00Z" }],
};
const runner = (fail: string[] = []) => ({
  async run(name: string) {
    if (fail.includes(name)) throw new Error("boom");
    return ROWS[name] ?? [];
  },
});
const report = (fail: string[] = []) => buildReport({ runner: runner(fail), access: PII, projectId: HC, branch: "br-prod", now: NOW, scheduleMinutes: 60 });

describe("HTML export", () => {
  it("is one self-contained document: inline CSS, no scripts, no external URLs", async () => {
    const html = renderExport(await report());
    expect(html.startsWith("<!DOCTYPE html><html")).toBe(true);
    expect(html).toContain("<style>");
    expect(html).toContain("--lbh-sev-critical-fg");
    expect(html).not.toMatch(/<script/i);
    expect(html).not.toMatch(/https?:\/\//i);
    expect(html).not.toMatch(/\bsrc=|url\(|<link|@import/i);
    expect(html).toContain("<svg");
  });

  it("never contains query text, even when a PII member exports it", async () => {
    const html = renderExport(await report());
    expect(html).not.toContain("ssn");
    expect(html).not.toContain("a@b.c");
    expect(html).toContain("42"); // the query id stays
  });

  it("renders the exported tabs in tab order with tiles and explained cards", async () => {
    const html = renderExport(await report());
    const labels = TABS.filter((t) => t.exported).map((t) => `<h2 class="rp-tab-title">${t.label.replace("&", "&amp;")}</h2>`);
    const at = labels.map((l) => html.indexOf(l));
    expect(at.every((i) => i > 0)).toBe(true);
    expect([...at].sort((a, b) => a - b)).toEqual(at);
    expect(html).not.toContain(">Fixes</h2>");
    expect(html.split('class="rp-tiles"').length - 1).toBe(labels.length);
    expect(html.split('class="rp-card"').length).toBe(html.split('class="rp-explain"').length);
    expect(html).toContain("HIGH");
    expect(html).toContain("VACUUM (ANALYZE) hc_test.orders;");
  });

  it("says the score is unavailable, with no grade, when the findings query failed (I1)", async () => {
    const html = renderExport(await report(["report_findings"]));
    expect(html).toContain("Score unavailable");
    expect(html).not.toMatch(/>A<|100 %/);
    expect(html).toContain("findings unavailable");
  });

  it("shows when a suspended compute's area was last assessed (I2)", async () => {
    ROWS.report_checks[0] = { check_id: "dead_tuples", endpoint_id: "ep-a", status: "skipped:compute_inactive", last_ok_at: "2026-10-29T03:00:00Z" };
    try {
      expect(renderExport(await report())).toMatch(/last assessed 2026-10-29 03:00/);
    } finally {
      ROWS.report_checks[0] = { check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok" };
    }
  });

  it("carries the bold severity scheme: solid CRITICAL/WARNING badges, row edges and tints, tile tints (I4)", async () => {
    const html = renderExport(await report());
    // markup: severity and priority badges are solid, severity rows and runbook items carry an edge and a tint
    expect(html).toMatch(/class="rp-badge rp-sev rp-sev-critical"[^>]*>.*?critical</);
    expect(html).toMatch(/<tr class="rp-row-critical">/);
    expect(html).toMatch(/<tr class="rp-row-warning">/);
    expect(html).toMatch(/<li class="rp-runbook-item rp-row-critical">/);
    expect(html).toMatch(/class="rp-tile rp-tile-critical"/);
    // the inline CSS defines those classes on the same tokens, and the tokens carry the light-mode colours
    const rule = (sel: string) => new RegExp(`${sel.replace(/[.>:-]/g, (c) => `\\${c}`)}[^{]*\\{[^}]*`);
    expect(html).toMatch(new RegExp(rule(".rp-sev-critical").source + "background: var\\(--lbh-sev-critical-solid-bg\\)"));
    expect(html).toMatch(new RegExp(rule(".rp-sev-warning").source + "background: var\\(--lbh-sev-warning-solid-bg\\)"));
    expect(html).toMatch(new RegExp(rule(".rp-row-critical").source + "background: var\\(--lbh-sev-critical-row\\)"));
    expect(html).toMatch(/inset 4px 0 0 var\(--lbh-sev-critical-edge\)/);
    expect(html).toMatch(/inset 4px 0 0 var\(--lbh-sev-warning-edge\)/);
    expect(html).toMatch(new RegExp(rule(".rp-tile-critical").source + "background: var\\(--lbh-sev-critical-bg\\)"));
    expect(html).toContain("--lbh-sev-critical-solid-bg: #b42318;");
    expect(html).toContain("--lbh-sev-warning-solid-bg: #fdb022;");
    expect(html).toContain("--lbh-sev-critical-edge: #b42318;");
  });

  it("marks a failed section unavailable instead of dropping it", async () => {
    const html = renderExport(await report(["report_tables"]));
    expect(html).toContain("Section unavailable");
    expect(html).toContain("Largest tables");
  });

  it("names the file after project, branch and UTC minute", async () => {
    expect(exportFilename(await report(), NOW)).toBe("lakebase-health-healthcheck-production-20261029T1207Z.html");
  });

  it("ships CSS without imports or URLs", () => {
    expect(exportCss()).not.toMatch(/@import|url\(/);
  });
});
