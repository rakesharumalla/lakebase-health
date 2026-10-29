// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DraftCard, type DraftData } from "./DraftCard";

const ADVISORY = "Change it in your application code; this tool does not deploy rewrites.";

const ORIGINAL = "SELECT o.id FROM hc_test.orders o WHERE o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2) ORDER BY o.id";
const REWRITE = "WITH a AS (SELECT avg(amount) AS m FROM hc_test.orders) SELECT o.id FROM hc_test.orders o, a WHERE o.amount > a.m ORDER BY o.id";
const report = JSON.stringify({
  sets: [{ set: 1, rows_original: 1803, rows_rewrite: 1803, same_checksum: true, original_ms: 512.5, rewrite_ms: 2.25 }],
  rules: [{ rule: "equivalence", passed: true, detail: "1 of 1 parameter sets: same rows in order" },
          { rule: "speed", passed: true, detail: "median 512.5 -> 2.25 ms, 227.78x faster (needs 1.2x)" },
          { rule: "plan", passed: true, warning: false, detail: "removed SubPlan; added Hash Join" }],
  plan: { removed: ["SubPlan"], added: ["Hash Join"], expected_nodes: ["Hash Join", "SubPlan"], found: ["Hash Join", "SubPlan"] },
});
const verified = { draft_id: "2".repeat(32), attempt: "2", state: "verified", reason: null, requested_by: "owner@x",
                   requested_at: "2026-10-29T12:00:00Z", rationale: "Computes the average once.", sql: REWRITE, report };
const failed = { draft_id: "1".repeat(32), attempt: "1", state: "draft_failed", reason: "invalid_response: not JSON",
                 requested_by: "owner@x", requested_at: "2026-10-29T11:00:00Z", sql: null, report: null };
const base: DraftData = {
  rewritable: true, setUp: { ready: true, reason: null }, canManage: true, canRequest: true, pending: false,
  refused: null, nextAttempt: 1,
  original: { endpoint_id: "ep-a", queryid: "42", text: ORIGINAL, label: ORIGINAL, kind: "full", truncated: false },
  attempts: [],
};

afterEach(cleanup);

function card(o: Partial<DraftData> = {}) {
  const onRequest = vi.fn();
  render(<MemoryRouter><DraftCard data={{ ...base, ...o }} onRequest={onRequest} /></MemoryRouter>);
  return onRequest;
}

describe("DraftCard", () => {
  it("a manager drafts the first rewrite", () => {
    const onRequest = card();
    fireEvent.click(screen.getByRole("button", { name: "Draft a rewrite" }));
    expect(onRequest).toHaveBeenCalledOnce();
  });

  it("role holder sees no button, with the reason", () => {
    card({ canManage: false, canRequest: false });
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    expect(screen.getByText("Only the project owner or a manager can draft a rewrite.")).toBeTruthy();
  });

  it("not set up: no button and the reason", () => {
    card({ setUp: { ready: false, reason: "serving endpoint m is NOT_READY" }, canRequest: false });
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    expect(screen.getByText("LLM drafting not set up.")).toBeTruthy();
    expect(screen.getByText(/NOT_READY/)).toBeTruthy();
  });

  it("a verified rewrite shows its SQL, diff, evidence and the advisory note, with Retry", () => {
    const onRequest = card({ attempts: [verified], nextAttempt: 3 });
    expect(screen.getByText("verified")).toBeTruthy();
    expect(screen.getByText("Computes the average once.")).toBeTruthy();
    expect(screen.getByText(REWRITE).tagName).toBe("PRE");
    expect(screen.getByLabelText("Diff with the original").querySelector("ins")?.textContent).toContain("WITH");
    expect(screen.getByText("1803", { selector: "td:nth-child(2)" })).toBeTruthy();
    expect(screen.getByText("512.5")).toBeTruthy();
    expect(screen.getByText("2.25")).toBeTruthy();
    expect(screen.getByText(/removed SubPlan; added Hash Join\. Expected Hash Join, SubPlan; found/)).toBeTruthy();
    expect(screen.getByText(ADVISORY)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRequest).toHaveBeenCalledOnce();
  });

  it("while a draft runs, Retry is disabled", () => {
    card({ attempts: [{ ...verified, state: "verifying", report: null }], canRequest: false });
    expect(screen.getByRole("button", { name: "Drafting…" }).hasAttribute("disabled")).toBe(true);
    cleanup();
    card({ attempts: [verified], pending: true, canRequest: false });
    expect(screen.getByRole("button", { name: "Drafting…" }).hasAttribute("disabled")).toBe(true);
  });

  it("a failed draft shows why", () => {
    card({ attempts: [failed], nextAttempt: 2 });
    expect(screen.getByText("invalid_response: not JSON")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Retry" })).toBeTruthy();
  });

  it("history lists every attempt and shows the one picked", () => {
    card({ attempts: [verified, failed], nextAttempt: 3 });
    expect(screen.getByRole("button", { name: "Attempt 2" })).toBeTruthy();
    expect(screen.queryByText("invalid_response: not JSON")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Attempt 1" }));
    expect(screen.getByText("invalid_response: not JSON")).toBeTruthy();
  });

  it("shows the adoption outcome and a refused request", () => {
    card({ attempts: [{ ...verified, state: "adopted" }], refused: { reason: "attempt_changed", at: "2026-10-29T13:00:00Z" } });
    expect(screen.getByText("Adopted: the workload now runs the rewritten query.")).toBeTruthy();
    expect(screen.getByText("The last request was not run: attempt_changed.")).toBeTruthy();
  });

  it("renders nothing for a finding without a query", () => {
    const { container } = render(<MemoryRouter><DraftCard data={{ ...base, rewritable: false }} onRequest={vi.fn()} /></MemoryRouter>);
    expect(container.textContent).toBe("");
  });
});
