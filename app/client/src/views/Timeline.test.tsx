// @vitest-environment happy-dom
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import samples from "../fixtures/samples.json";
import timeline from "../fixtures/timeline.json";
import { pivot } from "../lib/pivot";
import { SamplesTable, TimelineView } from "./Timeline";

describe("TimelineView", () => {
  it("renders the tracks and the freshness note", () => {
    render(<TimelineView data={timeline} onPick={() => {}} />);
    for (const name of [/active sessions/i, /lock waits/i, /commit waits/i, /events/i, /cpu/i]) {
      expect(screen.getByRole("heading", { name })).toBeTruthy();
    }
    expect(screen.getByText(/up to one engine cycle behind/i)).toBeTruthy();
  });

  it("counts statement timeouts and lists blocker classes in the legend", () => {
    render(<TimelineView data={timeline} onPick={() => {}} />);
    const n = timeline.events.filter((e) => e.kind === "statement_timeout").length;
    expect(screen.getByText(new RegExp(`${n} statement timeouts?`))).toBeTruthy();
    expect(screen.getAllByText(/idle in transaction/i).length).toBeGreaterThan(0);
  });

  it("asks for the samples of a minute when a lock bucket is picked", () => {
    const onPick = vi.fn();
    render(<TimelineView data={timeline} onPick={onPick} />);
    fireEvent.click(screen.getAllByRole("button", { name: /show sessions/i })[0]);
    expect(onPick).toHaveBeenCalledWith(timeline.locks[0].minute);
  });

  it("shows sampled sessions with masked text", () => {
    render(<SamplesTable samples={[...samples.samples, { pid: "9", query_text: "[masked]" }]} />);
    expect(screen.getByText("[masked]")).toBeTruthy();
  });
});

describe("pivot", () => {
  it("turns long rows into one row per minute with a column per series, filling zeros", () => {
    const rows = [
      { minute: "m1", blocker_class: "active", samples: "2" },
      { minute: "m1", blocker_class: "commit_wait", samples: "1" },
      { minute: "m2", blocker_class: "active", samples: "3" },
    ];
    expect(pivot(rows, "minute", "blocker_class", "samples", ["active", "commit_wait"])).toEqual([
      { minute: "m1", active: 2, commit_wait: 1 },
      { minute: "m2", active: 3, commit_wait: 0 },
    ]);
  });
});
