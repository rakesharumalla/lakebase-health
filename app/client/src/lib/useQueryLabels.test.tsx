// @vitest-environment happy-dom
import { cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { QueryRef } from "./findings";
import { LABEL_BATCH, useQueryLabels } from "./useQueryLabels";

function Probe({ refs, onLabels }: { refs: QueryRef[]; onLabels: (n: number) => void }) {
  onLabels(useQueryLabels("p1", refs).size);
  return null;
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("useQueryLabels", () => {
  it("fetches all pairs of a page in one request, batches past the server limit, and refetches only new pairs", async () => {
    const calls: string[][] = [];
    vi.stubGlobal("fetch", async (url: string) => {
      const ids = decodeURIComponent(url.split("ids=")[1]).split(",");
      calls.push(ids);
      const queries = ids.map((k) => ({ endpoint_id: k.split(":")[0], queryid: k.split(":")[1], text: null, label: "q", kind: "none", truncated: false }));
      return new Response(JSON.stringify({ queries }), { status: 200 });
    });
    const refs = (n: number, from = 0) => Array.from({ length: n }, (_, i) => ({ endpoint_id: "ep-a", queryid: String(from + i) }));
    let size = 0;
    const { rerender } = render(<Probe refs={[...refs(25), ...refs(5)]} onLabels={(n) => (size = n)} />);
    await waitFor(() => expect(size).toBe(25));
    expect(calls).toHaveLength(1);
    rerender(<Probe refs={refs(LABEL_BATCH + 75)} onLabels={(n) => (size = n)} />);
    await waitFor(() => expect(size).toBe(LABEL_BATCH + 75));
    expect(calls.slice(1).map((c) => c.length)).toEqual([LABEL_BATCH, 50]);
    expect(calls.slice(1).flat().some((k) => Number(k.split(":")[1]) < 25)).toBe(false);
  });
});
