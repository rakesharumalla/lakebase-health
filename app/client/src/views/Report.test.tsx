// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { buildReport } from "../../../server/report/build";
import type { Report } from "../report/types";
import ReportPage, { ReportView } from "./Report";

const HC = "11111111-2222-4333-8444-555555555555";
const ROWS: Record<string, Record<string, unknown>[]> = {
  report_branches: [
    { branch_id: "br-prod", branch_name: "production", is_default_branch: "true", endpoint_id: "ep-a", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", min_cu: "1", max_cu: "4", suspend_timeout_s: "300", enrolled: "true" },
    { branch_id: "br-dev", branch_name: "dev", is_default_branch: "false", endpoint_id: "ep-d", endpoint_type: "ENDPOINT_TYPE_READ_WRITE", current_state: "ACTIVE", enrolled: "true" },
  ],
  report_findings: [
    { fingerprint: "f1", check_id: "dead_tuples", title: "High share of dead tuples", category: "maintenance", endpoint_id: "ep-a", object: "db:hc_test.orders", status: "open", severity: "critical", measure: "25", evidence: "{}", explain: "Dead tuples. More.", candidate_sql: "VACUUM (ANALYZE) hc_test.orders;" },
    { fingerprint: "f2", check_id: "top_db_time_query", title: "Query uses the most DB time", category: "performance", endpoint_id: "ep-a", object: "42", status: "open", severity: "warning", measure: "50", evidence: '{"queryid":"42"}', queryid: "42", top_queryids: "[]", occurrences: "3" },
  ],
  report_checks: [{ check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok" }],
  report_overview: [{ telemetry_at: new Date().toISOString(), lfc_hit_pct: "98.5" }],
  meta: [{ last_success_at: new Date().toISOString() }],
};
let report: Report;
beforeAll(async () => {
  report = await buildReport({
    runner: { run: async (n: string) => ROWS[n] ?? [] },
    access: { userKey: "a", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: false },
    projectId: HC, branch: "default", now: new Date(), scheduleMinutes: 60,
  });
});

let calls: string[] = [];
beforeEach(() => {
  calls = [];
  vi.stubGlobal("fetch", async (url: string) => {
    calls.push(url);
    if (url.startsWith("/api/report/")) return new Response(JSON.stringify(report), { status: 200 });
    if (url.includes("/queries?ids=")) {
      return new Response(JSON.stringify({ queries: [{ endpoint_id: "ep-a", queryid: "42", text: "SELECT $1", label: "SELECT $1", kind: "skeleton", truncated: false }] }), { status: 200 });
    }
    if (url.endsWith("/actions")) return new Response(JSON.stringify({ project: { id: HC, name: "healthcheck" }, groups: [], canManage: false }), { status: 200 });
    return new Response("{}", { status: 404 });
  });
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function Where() {
  return <span data-testid="where">{useLocation().pathname}</span>;
}
const at = (path: string) =>
  render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/projects/:id" element={<ReportPage />} />
        <Route path="/projects/:id/branches/:branch/:tab" element={<><ReportPage /><Where /></>} />
      </Routes>
    </MemoryRouter>,
  );

describe("the project page is the tabbed report", () => {
  it("opens the default branch on the Health tab", async () => {
    at(`/projects/${HC}`);
    await waitFor(() => expect(screen.getByTestId("where").textContent).toBe(`/projects/${HC}/branches/br-prod/health`));
    expect(calls[0]).toBe(`/api/report/${HC}/default`);
    expect((await screen.findAllByRole("tab")).map((t) => t.textContent)).toEqual([
      "Health", "Overview", "Tables & Indexes", "Queries", "Waits & Locks", "Vacuum & Bloat", "Scale to zero", "System",
      "Settings", "Runbook", "Fixes"]);
    expect(screen.getByText("Scorecard")).toBeTruthy();
    expect(document.querySelectorAll(".rp-area")).toHaveLength(12);
  });

  it("has a header with score, assessed count and the download link", () => {
    render(<MemoryRouter><ReportView report={report} tab="health" /></MemoryRouter>);
    expect(screen.getByText(`assessed ${report.score.assessed} of 12`)).toBeTruthy();
    const link = screen.getByRole("link", { name: /Download report/ });
    expect(link.getAttribute("href")).toBe(`/api/report/${HC}/br-prod.html`);
    expect(link.hasAttribute("download")).toBe(true);
  });

  it("switches tabs through the URL", async () => {
    at(`/projects/${HC}/branches/br-prod/health`);
    const runbook = await screen.findByRole("tab", { name: "Runbook" });
    fireEvent.mouseDown(runbook);
    await waitFor(() => expect(screen.getByTestId("where").textContent).toBe(`/projects/${HC}/branches/br-prod/runbook`));
    expect(await screen.findByText("Actions in order")).toBeTruthy();
    expect(screen.getByText("VACUUM (ANALYZE) hc_test.orders;")).toBeTruthy();
  });

  it("starts every tab with KPI tiles and explains every card", async () => {
    for (const tab of ["health", "overview", "tables", "vacuum", "scale-to-zero", "system", "settings", "runbook"] as const) {
      const { unmount } = render(<MemoryRouter><ReportView report={report} tab={tab} /></MemoryRouter>);
      const panel = screen.getByRole("tabpanel");
      expect(panel.querySelector(".rp-tiles"), tab).toBeTruthy();
      expect(panel.querySelectorAll(".rp-card").length, tab).toBe(panel.querySelectorAll(".rp-card .rp-explain").length);
      unmount();
    }
  });

  it("lists query findings with labels fetched in one batch on the Queries tab", async () => {
    render(<MemoryRouter><ReportView report={report} tab="queries" /></MemoryRouter>);
    const card = screen.getByRole("heading", { name: "Queries with issues" }).closest("[data-slot=card]") as HTMLElement;
    await waitFor(() => expect(within(card).getByText("SELECT $1")).toBeTruthy());
    expect(calls.filter((c) => c.includes("/queries?ids="))).toHaveLength(1);
  });

  it("shows the fixes view without its own header on the Fixes tab", async () => {
    render(<MemoryRouter><ReportView report={report} tab="fixes" /></MemoryRouter>);
    expect(await screen.findByText("No proposed fixes")).toBeTruthy();
    expect(calls).toContain(`/api/projects/${HC}/actions`);
  });
});
