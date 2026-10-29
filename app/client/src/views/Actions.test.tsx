// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActionsView, type ActionsData } from "./Actions";

const row = (state: string, i: number) => ({
  action_id: `${state}-${i}`, fingerprint: "0".repeat(32), title: `Fix ${state} ${i}`, kind: "create_index", object: "t", risk: "low", state,
  updated_at: "2026-10-28T12:00:00Z",
});
const data: ActionsData = {
  project: { id: "11111111-2222-4333-8444-555555555555", name: "healthcheck" },
  canManage: true,
  groups: [
    { state: "proposed", total: 25, rows: Array.from({ length: 10 }, (_, i) => row("proposed", i)) },
    { state: "applied", total: 2, rows: [row("applied", 0), row("applied", 1)] },
  ],
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("ActionsView", () => {
  it("groups fixes by state, shows 10 per state and fetches one state in full on Show all", async () => {
    const calls: string[] = [];
    vi.stubGlobal("fetch", async (url: string) => {
      calls.push(url);
      return new Response(JSON.stringify({ groups: [{ state: "proposed", total: 25, rows: Array.from({ length: 25 }, (_, i) => row("proposed", i)) }] }), { status: 200 });
    });
    render(<MemoryRouter><ActionsView data={data} /></MemoryRouter>);
    const proposed = screen.getByText("proposed").closest("[data-slot=card]") as HTMLElement;
    const applied = screen.getByText("applied").closest("[data-slot=card]") as HTMLElement;
    expect(within(proposed).getAllByRole("row")).toHaveLength(11);
    expect(within(applied).queryByRole("button", { name: /show all/i })).toBeNull();
    fireEvent.click(within(proposed).getByRole("button", { name: "Show all 25" }));
    await waitFor(() => expect(within(proposed).getAllByRole("row")).toHaveLength(26));
    expect(calls).toEqual([`/api/projects/${data.project.id}/actions?state=proposed`]);
  });

  it("says when one state's full list was cut at the limit (app-I2)", async () => {
    vi.stubGlobal("fetch", async () => new Response(JSON.stringify({
      limit: 1000, groups: [{ state: "proposed", total: 1500, truncated: true, rows: Array.from({ length: 1000 }, (_, i) => row("proposed", i)) }],
    }), { status: 200 }));
    const d: ActionsData = { ...data, groups: [{ state: "proposed", total: 1500, rows: data.groups[0].rows }] };
    render(<MemoryRouter><ActionsView data={d} /></MemoryRouter>);
    expect(screen.queryByText(/the list stops at/)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Show all 1500" }));
    await waitFor(() => expect(screen.getByText("Showing the first 1000 of 1500: the list stops at 1000 rows.")).toBeTruthy());
  });
});
