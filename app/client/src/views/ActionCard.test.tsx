// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActionCard } from "./ActionCard";

const action = {
  action_id: "0123456789abcdef0123456789abcdef",
  state: "proposed",
  kind: "create_index",
  sql: "CREATE INDEX CONCURRENTLY ON hc_test.orders (customer_id);",
  rollback_sql: "DROP INDEX CONCURRENTLY IF EXISTS i;",
  risk: "low",
  verify_on_branch: "true",
};
const report = JSON.stringify({
  branch: "lbh-verify-0123",
  rules: [
    { rule: "index_used_with_speedup", passed: true, detail: "best 3.10x" },
    { rule: "write_cost", passed: false, detail: "worst write ratio 1.40" },
  ],
  queries: [{ queryid: "q1", before_ms: 120.5, after_ms: 12.25 }],
});
const runs = [{ run_id: "r1", kind: "verify", outcome: "verify_failed", started_at: "2026-10-28T11:00:00Z", report }];

afterEach(cleanup);

function renderCard(props: Partial<Parameters<typeof ActionCard>[0]> = {}) {
  const onRequest = vi.fn();
  render(
    <MemoryRouter>
      <ActionCard action={action} runs={runs} canManage requests={["verify", "reject"]} onRequest={onRequest} {...props} />
    </MemoryRouter>,
  );
  return onRequest;
}

describe("ActionCard", () => {
  it("shows buttons only for legal requests, and clicking asks for that request", () => {
    const onRequest = renderCard();
    expect(screen.getByRole("button", { name: "Verify on a branch" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Reject" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Apply" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Roll back" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Verify on a branch" }));
    expect(onRequest).toHaveBeenCalledWith("verify");
  });

  it("shows no buttons to non-managers, with the reason", () => {
    renderCard({ canManage: false, requests: ["verify", "apply", "rollback", "reject"] });
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    expect(screen.getByText("Only the project owner or a manager can approve.")).toBeTruthy();
  });

  it("shows the state and the latest verification report", () => {
    renderCard();
    expect(screen.getByText("proposed")).toBeTruthy();
    expect(screen.getByText("index_used_with_speedup")).toBeTruthy();
    expect(screen.getByText("write_cost")).toBeTruthy();
    expect(screen.getByText("fail")).toBeTruthy();
    expect(screen.getByText("120.5")).toBeTruthy();
    expect(screen.getByText("12.25")).toBeTruthy();
  });

  it("shows the verification report's queries by text with the id as secondary text", () => {
    const labels = new Map([["ep-a:q1", { endpoint_id: "ep-a", queryid: "q1", text: "SELECT * FROM hc_test.orders", label: "SELECT * FROM hc_test.orders", kind: "full" as const, truncated: false }]]);
    renderCard({ action: { ...action, endpoint_id: "ep-a" }, labels });
    expect(screen.getByRole("button", { name: /SELECT \* FROM hc_test.orders/ })).toBeTruthy();
    expect(screen.getByText("q1")).toBeTruthy();
  });

  it("falls back to 'query <id>' when the text is not known", () => {
    renderCard();
    expect(screen.getByText("query q1")).toBeTruthy();
  });

  it("shows a fix with nothing to undo as a plain-text rollback note, never as SQL", () => {
    for (const a of [
      { ...action, rollback_sql: null, rollback_note: "Not needed: VACUUM changes no data" },
      { ...action, rollback_sql: "-- VACUUM changes no data; nothing to roll back", rollback_note: null },
    ]) {
      const { container } = render(
        <MemoryRouter>
          <ActionCard action={a} runs={[]} canManage requests={[]} onRequest={vi.fn()} />
        </MemoryRouter>,
      );
      expect(screen.getByText("Rollback: not needed — VACUUM changes no data")).toBeTruthy();
      expect([...container.querySelectorAll("code, pre")].some((n) => /VACUUM|nothing to roll back/.test(n.textContent ?? ""))).toBe(false);
      cleanup();
    }
  });

  it("shows a runnable rollback as SQL", () => {
    renderCard();
    expect(screen.getByText("DROP INDEX CONCURRENTLY IF EXISTS i;").tagName).toBe("CODE");
  });

  it("labels the retry on a card blocked for privileges", () => {
    const onRequest = renderCard({ action: { ...action, state: "blocked" }, requests: ["verify"], retryAfterEnrolment: true });
    fireEvent.click(screen.getByRole("button", { name: "Retry after enrolment" }));
    expect(onRequest).toHaveBeenCalledWith("verify");
  });

  // Final review app-I1: the server drops verify/apply for a finding that is not new/open or an expired fix.
  it("says why a fix cannot run instead of offering verify or apply", () => {
    const why = "Not actionable: the finding is superseded by a related finding, so the fixer will not run this fix.";
    renderCard({ requests: ["reject"], notActionable: why });
    expect(screen.getByText(why)).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Verify on a branch" })).toBeNull();
    expect(screen.getByRole("button", { name: "Reject" })).toBeTruthy();
  });

  it("says why even when no request is left", () => {
    renderCard({ requests: [], notActionable: "Not actionable: this fix expired." });
    expect(screen.getByText("Not actionable: this fix expired.")).toBeTruthy();
    expect(screen.queryAllByRole("button")).toHaveLength(0);
  });
});
