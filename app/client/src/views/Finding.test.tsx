// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import findingJson from "../fixtures/finding.json";
import maskedJson from "../fixtures/finding_masked.json";
import { FindingView, type FindingData } from "./Finding";

const finding = findingJson as unknown as FindingData;
const masked = maskedJson as unknown as FindingData;
let calls: string[] = [];

beforeEach(() => {
  calls = [];
  vi.stubGlobal("fetch", async (url: string) => {
    calls.push(url);
    if (url.endsWith("/related")) {
      const extra = finding.related.rows.map((r) => ({ ...r, key: `${r.key}-x` }));
      return new Response(JSON.stringify({ total: 20, rows: [...finding.related.rows, ...extra] }), { status: 200 });
    }
    if (url.includes("/queries?ids=")) {
      const ids = decodeURIComponent(url.split("ids=")[1]).split(",");
      const queries = ids.map((k) => {
        const [endpoint_id, queryid] = k.split(":");
        return { endpoint_id, queryid, text: `SELECT ${queryid}`, label: `SELECT ${queryid}`, kind: "full", truncated: false };
      });
      return new Response(JSON.stringify({ queries }), { status: 200 });
    }
    return new Response("{}", { status: 404 });
  });
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const view = (data: FindingData) => render(<MemoryRouter><FindingView data={data} /></MemoryRouter>);

describe("FindingView", () => {
  it("shows explanation, evidence and the likely cause", () => {
    view(finding);
    expect(screen.getByRole("heading", { name: finding.finding.title as string })).toBeTruthy();
    expect(screen.getByText("likely_cause")).toBeTruthy();
  });

  it("links incident findings to the endpoint timeline around last_seen", () => {
    view(finding);
    const link = screen.getByRole("link", { name: /incident timeline/i });
    expect(link.getAttribute("href")).toContain(`/endpoints/${finding.finding.endpoint_id}/timeline?from=`);
  });

  it("renders masked text as masked and candidate SQL as copyable text only", () => {
    view({ ...masked, finding: { ...masked.finding, candidate_sql: "DROP INDEX CONCURRENTLY s.i;" } });
    expect(screen.getAllByText("[masked]").length).toBeGreaterThan(0);
    expect(screen.getByText("DROP INDEX CONCURRENTLY s.i;")).toBeTruthy();
    expect(screen.getByRole("button", { name: /copy sql/i })).toBeTruthy();
    expect(screen.queryByRole("button", { name: /apply|approve|run/i })).toBeNull();
  });

  it("marks candidate SQL the fixer cannot run as apply by hand, with the rollback note as text", () => {
    view({ ...finding, actions: [], finding: { ...finding.finding, candidate_sql: "VACUUM (ANALYZE) hc_test.t;",
                                              rollback_sql: null, rollback_note: "Not needed: VACUUM changes no data" } });
    expect(screen.getByText(/Apply by hand/)).toBeTruthy();
    expect(screen.getByText("Rollback: not needed — VACUUM changes no data")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /apply|approve|run/i })).toBeNull();
  });

  it("does not say apply by hand when the fixer has an action for it", () => {
    view({ ...finding, actions: [{ action_id: "a1", state: "proposed", risk: "moderate", verify_on_branch: "true" }],
           finding: { ...finding.finding, candidate_sql: "CREATE INDEX CONCURRENTLY ON s.t (a);" } });
    expect(screen.queryByText(/Apply by hand/)).toBeNull();
  });

  it("shows the finding's own query and related queries by text, from one batched lookup", async () => {
    view(finding);
    const own = "-3196267159430279274";
    expect(await screen.findByText(`SELECT ${own}`)).toBeTruthy();
    const lookups = calls.filter((c) => c.includes("/queries?ids="));
    expect(lookups).toHaveLength(1);
    expect(lookups[0]).toContain(`/api/projects/${finding.finding.project_id}/queries`);
    expect(decodeURIComponent(lookups[0])).toContain(`${finding.finding.endpoint_id}:${own}`);
  });

  it("lists the top 10 related rows and loads the rest on Show all", async () => {
    view(finding);
    const card = screen.getByRole("heading", { name: "Related findings on this endpoint" }).closest("[data-slot=card]") as HTMLElement;
    expect(within(card).getAllByRole("listitem")).toHaveLength(10);
    expect(calls.some((c) => c.endsWith("/related"))).toBe(false);
    fireEvent.click(within(card).getByRole("button", { name: "Show all 20" }));
    await waitFor(() => expect(within(card).getAllByRole("listitem")).toHaveLength(20));
    expect(calls.filter((c) => c.endsWith(`/api/findings/${finding.finding.fingerprint}/related`))).toHaveLength(1);
  });

  it("says when the related list was cut at the limit (app-I2)", () => {
    view({ ...finding, related: { ...finding.related, count: 2600, shown: 2000, truncated: true, limit: 2000 } });
    expect(screen.getByText("2600 open")).toBeTruthy();
    expect(screen.getByText("Showing the first 2000 of 2600: the list stops at 2000 rows.")).toBeTruthy();
  });
});
