import { describe, expect, it } from "vitest";
import { DEFAULT_WINDOW_MS, MAX_WINDOW_MS, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "../lib/validate";

const NOW = new Date("2026-10-28T12:00:00Z");

describe("ids", () => {
  it("accepts only 32-char lowercase hex fingerprints", () => {
    expect(parseFingerprint("cc59509434e9e437c838cc12b394bafa")).toBe("cc59509434e9e437c838cc12b394bafa");
    expect(parseFingerprint("CC59509434E9E437C838CC12B394BAFA")).toBeNull();
    expect(parseFingerprint("cc59' OR 1=1 --")).toBeNull();
    expect(parseFingerprint(undefined)).toBeNull();
  });
  it("accepts Lakebase endpoint ids and project uuids", () => {
    expect(parseEndpointId("ep-sample-primary-a1b2c3d4")).toBe("ep-sample-primary-a1b2c3d4");
    expect(parseEndpointId("ep-x; DROP")).toBeNull();
    expect(parseProjectId("11111111-2222-4333-8444-555555555555")).toBe("11111111-2222-4333-8444-555555555555");
    expect(parseProjectId("healthcheck")).toBeNull();
  });
});

describe("parseWindow", () => {
  it("defaults to the last 6 hours", () => {
    const w = parseWindow(undefined, undefined, NOW);
    expect("error" in w).toBe(false);
    if (!("error" in w)) {
      expect(w.to.toISOString()).toBe(NOW.toISOString());
      expect(w.to.getTime() - w.from.getTime()).toBe(DEFAULT_WINDOW_MS);
    }
  });
  it("rejects malformed dates and from > to", () => {
    expect(parseWindow("yesterday", undefined, NOW)).toEqual({ error: "invalid from" });
    expect(parseWindow("2026-10-28T10:00:00Z", "2026-10-28T09:00:00Z", NOW)).toEqual({ error: "from must be before to" });
  });
  it("clamps to the 7-day retention and a 7-day span", () => {
    const w = parseWindow("2026-10-01T00:00:00Z", "2026-10-28T13:00:00Z", NOW);
    if ("error" in w) throw new Error(w.error);
    expect(w.to.getTime()).toBe(NOW.getTime());
    expect(w.from.getTime()).toBe(NOW.getTime() - MAX_WINDOW_MS);
  });
  it("rejects a window entirely older than retention", () => {
    expect(parseWindow("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", NOW)).toEqual({ error: "window is older than 7 days" });
  });
});
