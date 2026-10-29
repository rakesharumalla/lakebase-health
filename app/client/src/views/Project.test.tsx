// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import project from "../fixtures/project.json";
import { ProjectView, type ProjectData } from "./Project";

const data = project as unknown as ProjectData;
const perf = data.sections.find((s) => s.category === "performance")!;
const SPILL = "-5273821145808649494";

type Call = string;
let calls: Call[] = [];
function stubFetch(extra: (url: string) => unknown = () => undefined) {
  calls = [];
  vi.stubGlobal("fetch", async (url: string) => {
    calls.push(url);
    const custom = extra(url);
    if (custom) return new Response(JSON.stringify(custom), { status: 200 });
    if (url.includes("/queries?ids=")) {
      const ids = decodeURIComponent(url.split("ids=")[1]).split(",");
      return new Response(
        JSON.stringify({
          queries: ids.map((k) => {
            const [endpoint_id, queryid] = k.split(":");
            const text = queryid === SPILL ? "SELECT * FROM hc_test.orders ORDER BY note LIMIT $1" : null;
            return { endpoint_id, queryid, text, label: text ?? `query ${queryid}`, kind: text ? "full" : "none", truncated: false };
          }),
        }),
        { status: 200 },
      );
    }
    return new Response("{}", { status: 404 });
  });
}

beforeEach(() => stubFetch());
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const view = (d: ProjectData = data) => render(<MemoryRouter><ProjectView data={d} /></MemoryRouter>);
const perfCard = () => screen.getByRole("heading", { name: /performance/i }).closest("[data-slot=card]") as HTMLElement;

describe("ProjectView", () => {
  it("groups findings by category and shows the computes panel", () => {
    view();
    expect(screen.getByRole("heading", { name: "healthcheck" })).toBeTruthy();
    expect(screen.getByRole("heading", { name: /indexing/i })).toBeTruthy();
    expect(screen.getAllByText("ep-sample-primary-a1b2c3d4").length).toBeGreaterThan(0);
  });

  it("shows enrolment instructions for a compute that is not enrolled", () => {
    view({ ...data, computes: [{ ...data.computes[0], enrolled: "false" }] });
    expect(screen.getByText(/GRANT pg_monitor/)).toBeTruthy();
  });

  it("shows one row per query with a badge per check, each linking to its finding", async () => {
    view();
    const card = perfCard();
    const row = perf.rows.find((r) => r.queryid === SPILL)!;
    expect(row.issues.map((i) => i.label)).toEqual(["sort + limit waste", "disk spill", "row estimates"]);
    const text = await within(card).findByText("SELECT * FROM hc_test.orders ORDER BY note LIMIT $1");
    const tr = text.closest("tr")!;
    expect(within(tr).getByText(SPILL)).toBeTruthy();
    for (const i of row.issues) {
      expect(within(tr).getByRole("link", { name: i.label }).getAttribute("href")).toBe(`/findings/${i.fingerprint}`);
    }
    // Queries without text fall back to their id.
    expect(within(card).getAllByText(/^query -?\d+$/).length).toBeGreaterThan(0);
  });

  it("marks each row with its severity (edge and tint) and keeps the text label", () => {
    view();
    const card = perfCard();
    const rows = within(card).getAllByRole("row").slice(1);
    expect(rows.map((r) => r.getAttribute("data-severity"))).toEqual(perf.rows.map((r) => r.severity));
    const critical = rows.find((r) => r.getAttribute("data-severity") === "critical")!;
    expect(critical.className).toContain("bg-sev-critical-row");
    expect(within(critical).getByText("critical")).toBeTruthy();
  });

  it("shows per-severity chips on a fully loaded section only", () => {
    const complete = data.sections.find((s) => s.rows.length === s.total && s.rows.some((r) => r.severity === "warning"))!;
    const warnings = new Set(
      complete.rows
        .flatMap((r): { severity: string; fingerprint: string }[] => (r.kind === "query" && r.issues.length ? r.issues : [r]))
        .filter((i) => i.severity === "warning")
        .map((i) => i.fingerprint),
    ).size;
    view({ ...data, sections: data.sections.map((s) => ({ ...s, count: undefined })) });
    const card = screen.getByRole("heading", { name: new RegExp(complete.category, "i") }).closest("[data-slot=card]") as HTMLElement;
    expect(within(card).getByText(`${warnings} warning`)).toBeTruthy();
    expect(within(perfCard()).queryByText(/^\d+ (critical|warning|info)$/)).toBeNull();
  });

  it("lists the top 10 and fetches the rest only on Show all", async () => {
    stubFetch((url) => (url.includes("/findings?section=performance") ? { section: "performance", total: perf.total, rows: [...perf.rows, ...perf.rows.map((r) => ({ ...r, key: `${r.key}-more` }))] } : undefined));
    view();
    const card = perfCard();
    expect(perf.total).toBeGreaterThan(10);
    expect(within(card).getAllByRole("row")).toHaveLength(11);
    expect(calls.some((c) => c.includes("/findings?section="))).toBe(false);
    fireEvent.click(within(card).getByRole("button", { name: `Show all ${perf.total}` }));
    await waitFor(() => expect(within(card).getAllByRole("row")).toHaveLength(21));
    expect(calls.filter((c) => c.includes("/findings?section=performance"))).toHaveLength(1);
    expect(within(card).getByRole("button", { name: "Show top 10" })).toBeTruthy();
  });

  it("asks for every visible query's text in one batched request", async () => {
    view();
    await waitFor(() => expect(calls.some((c) => c.includes("/queries?ids="))).toBe(true));
    const lookups = calls.filter((c) => c.includes("/queries?ids="));
    expect(lookups).toHaveLength(1);
    const ids = decodeURIComponent(lookups[0].split("ids=")[1]).split(",");
    const visible = new Set([...data.sections, data.resolved].flatMap((s) => s.rows.filter((r) => r.kind === "query").map((r) => `${r.endpoint_id}:${r.queryid}`)));
    expect(new Set(ids)).toEqual(visible);
    expect(lookups[0]).toContain(`/api/projects/${data.project.id}/queries`);
  });

  // Final review app-I2: totals are SQL counts; a list cut at the SQL LIMIT says so.
  it("shows SQL counts and says when a section was cut at the limit", () => {
    const cutData: ProjectData = {
      ...data,
      truncated: true,
      limit: 5000,
      sections: data.sections.map((s) =>
        s.category === "performance" ? { ...s, count: 5203, shown: 5000, truncated: true } : { ...s, count: s.total, shown: s.total, truncated: false }),
      resolved: { ...data.resolved, count: 12, shown: 0, truncated: true },
    };
    view(cutData);
    expect(within(perfCard()).getByText("5203 open")).toBeTruthy();
    expect(within(perfCard()).getByText("Showing the first 5000 of 5203: the list stops at 5000 rows.")).toBeTruthy();
    expect(screen.getByText("Recently resolved (7 days): 12")).toBeTruthy();
    expect(screen.getAllByText(/the list stops at 5000 rows/)).toHaveLength(2);
  });

  it("says nothing about limits when no list was cut", () => {
    view();
    expect(screen.queryByText(/the list stops at/)).toBeNull();
  });
});
