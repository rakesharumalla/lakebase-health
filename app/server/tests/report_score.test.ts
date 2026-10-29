import { describe, expect, it } from "vitest";
import type { ReportFinding } from "../../client/src/report/types";
import { AREAS, CATALOG, type Catalog } from "../report/catalog";
import { checkRan, focus, grade, score, scoreAreas, type CheckStatus, type Coverage } from "../report/score";

const CAT: Catalog = {
  lfc_hit_rate: { area: "cache_memory", source: "delta", title: "LFC", thresholds: { warning: 99, critical: 95 }, direction: "below" },
  unused_index: { area: "indexes", source: "postgres", title: "Unused", thresholds: { info: 0 }, direction: "above" },
  dead_tuples: { area: "vacuum_xid", source: "postgres", title: "Dead", thresholds: { warning: 10, critical: 20 }, direction: "above" },
  primary_without_ha: { area: "resilience", source: "api", title: "HA", thresholds: { warning: 0 }, direction: "above" },
};
const COV: Coverage = { primaryEndpoints: ["ep-a"], telemetry: true, enrolled: true };
const OK: CheckStatus[] = [
  { check_id: "lfc_hit_rate", endpoint_id: null, status: "ok", error: null },
  { check_id: "unused_index", endpoint_id: "ep-a", status: "ok", error: null },
  { check_id: "dead_tuples", endpoint_id: "ep-a", status: "ok", error: null },
  { check_id: "primary_without_ha", endpoint_id: null, status: "ok", error: null },
];

const f = (check_id: string, severity: ReportFinding["severity"], extra: Partial<ReportFinding> = {}): ReportFinding => ({
  fingerprint: `${check_id}-${severity}`, check_id, title: `${check_id} title`, category: "c", area: null, endpoint_id: "ep-a",
  object: "o", status: "open", severity, confidence: "high", measure: 1, first_seen: null, last_seen: null, evidence: "{}",
  explain: "Why it matters. More.", candidate_sql: null, queryid: null, ...extra,
});
const byId = (areas: ReturnType<typeof scoreAreas>) => Object.fromEntries(areas.map((a) => [a.id, a]));

describe("report scoring", () => {
  it("has the 12 areas in report order and every catalog check maps to one", () => {
    expect(AREAS.map((a) => a.id)).toEqual(["cache_memory", "storage_latency", "write_path", "connections", "locks", "vacuum_xid",
      "indexes", "tables_bloat", "queries", "config_security", "scale_to_zero", "resilience"]);
    expect(Object.keys(CATALOG).length).toBeGreaterThan(60);
    expect(Object.values(CATALOG).every((c) => c.area === null || AREAS.some((a) => a.id === c.area))).toBe(true);
  });

  it("scores 5 with no open finding and 4 / 3 / 1 by the worst open severity", () => {
    const a = byId(scoreAreas([f("lfc_hit_rate", "info"), f("dead_tuples", "warning"), f("dead_tuples", "critical"),
      f("unused_index", "warning", { status: "resolved" })], OK, COV, CAT));
    expect([a.cache_memory.score, a.cache_memory.state]).toEqual([4, "warn"]);
    expect([a.vacuum_xid.score, a.vacuum_xid.state, a.vacuum_xid.worst?.severity]).toEqual([1, "fail", "critical"]);
    expect([a.indexes.score, a.indexes.state, a.indexes.open]).toEqual([5, "pass", 0]);
    expect([a.resilience.score, a.resilience.state]).toEqual([5, "pass"]);
  });

  it("leaves an area none of whose checks ran out of the total", () => {
    const a = byId(scoreAreas([], OK, COV, CAT));
    expect([a.queries.state, a.queries.score, a.queries.note]).toEqual(["not_assessed", null, "no checks ran"]);
    expect(score(Object.values(a))).toEqual({ pct: 100, grade: "A", assessed: 4, total: 12 });
  });

  it("does not assess Postgres areas on a branch that is not enrolled, and says why", () => {
    const st = OK.filter((s) => s.endpoint_id === null);
    const a = byId(scoreAreas([f("dead_tuples", "critical")], st, { ...COV, enrolled: false }, CAT));
    expect(a.vacuum_xid.state).toBe("not_assessed");
    expect(a.vacuum_xid.note).toBe("Postgres checks are not enabled on this branch");
    expect(a.cache_memory.state).toBe("pass");
  });

  it("shows young counters as the reason an area is not assessed", () => {
    const st = OK.map((s) => (s.check_id === "unused_index" ? { ...s, status: "skipped:stats_young", error: "counters too young (reset 3 h ago)" } : s));
    const a = byId(scoreAreas([], st, COV, CAT));
    expect([a.indexes.state, a.indexes.note]).toEqual(["not_assessed", "counters too young (reset 3 h ago)"]);
  });

  it("keeps the last successful assessment within 24 h while the compute is suspended, and says when (I2)", () => {
    const asleep: CheckStatus[] = OK.map((s) => s.check_id === "dead_tuples"
      ? { ...s, status: "skipped:compute_inactive", last_ok_at: "2026-10-29T03:00:00Z" } : s);
    const areas = byId(scoreAreas([f("dead_tuples", "critical")], asleep, COV, CAT));
    expect([areas.vacuum_xid.state, areas.vacuum_xid.score, areas.vacuum_xid.lastAssessed]).toEqual(["fail", 1, "2026-10-29T03:00:00Z"]);
    expect(areas.indexes.lastAssessed).toBeUndefined();
    // no successful run in the last 24 h: not assessed, as before
    const stale = OK.map((s) => s.check_id === "dead_tuples" ? { ...s, status: "skipped:compute_inactive", last_ok_at: null } : s);
    expect(byId(scoreAreas([f("dead_tuples", "critical")], stale, COV, CAT)).vacuum_xid.state).toBe("not_assessed");
  });

  it("does not assess telemetry areas without telemetry or when the source is stale", () => {
    expect(checkRan("lfc_hit_rate", OK, { ...COV, telemetry: false }, CAT)).toEqual({ ran: false, note: "no telemetry for this branch in the last 24 h" });
    const stale = OK.map((s) => (s.check_id === "lfc_hit_rate" ? { ...s, status: "ok_stale_source" } : s));
    expect(checkRan("lfc_hit_rate", stale, COV, CAT)).toEqual({ ran: false, note: "telemetry is stale" });
    expect(checkRan("dead_tuples", OK, { ...COV, primaryEndpoints: ["ep-other"] }, CAT).ran).toBe(false);
  });

  it("grades on the percentage boundaries", () => {
    expect([85, 84, 70, 69, 55, 54, 40, 39].map(grade)).toEqual(["A", "B", "B", "C", "C", "D", "D", "F"]);
    const areas = scoreAreas([f("dead_tuples", "critical")], OK, COV, CAT);
    expect(score(areas)).toEqual({ pct: 80, grade: "B", assessed: 4, total: 12 }); // (5 + 5 + 1 + 5) / 20
    expect(score(scoreAreas([], [], { ...COV, telemetry: false, enrolled: false }, CAT))).toEqual({ pct: null, grade: null, assessed: 0, total: 12 });
  });

  it("points the focus note at failing areas first", () => {
    expect(focus(scoreAreas([f("dead_tuples", "critical")], OK, COV, CAT))).toMatch(/^Start with vacuum & xid/);
    expect(focus(scoreAreas([f("lfc_hit_rate", "warning")], OK, COV, CAT))).toMatch(/^Next: cache & memory/);
    expect(focus(scoreAreas([], OK, COV, CAT))).toBe("No open findings in the assessed areas.");
  });
});
