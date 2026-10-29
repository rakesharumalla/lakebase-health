import { describe, expect, it } from "vitest";
import type { Report, ReportFinding } from "../../client/src/report/types";
import type { Catalog } from "../report/catalog";
import { evidenceNumbers, firstSentence, impact, runbook, runbookCounts, safeSql } from "../report/runbook";
import { strip } from "../report/strip";

const CAT: Catalog = {
  dead_tuples: { area: "vacuum_xid", source: "postgres", title: "Dead", thresholds: { warning: 10, critical: 20 }, direction: "above" },
  lfc_hit_rate: { area: "cache_memory", source: "delta", title: "LFC", thresholds: { warning: 99, critical: 95 }, direction: "below" },
  guardrail_settings: { area: "config_security", source: "postgres", title: "G", thresholds: { info: 0, warning: 1 }, direction: "above" },
};
const f = (fp: string, check_id: string, severity: ReportFinding["severity"], measure: number, extra: Partial<ReportFinding> = {}): ReportFinding => ({
  fingerprint: fp, check_id, title: `T ${fp}`, category: "c", area: null, endpoint_id: "ep-a", object: `db:public.${fp}`,
  status: "open", severity, confidence: "high", measure, first_seen: null, last_seen: null,
  evidence: JSON.stringify({ n_dead_tup: 12000, statement: "SELECT secret", table_ident: "public.t" }),
  explain: "Dead tuples pile up. Vacuum is behind.", candidate_sql: "VACUUM (ANALYZE) public.t;", queryid: null, ...extra,
});

describe("runbook", () => {
  it("orders by severity, then a verified fix, then impact", () => {
    const items = runbook([
      f("a", "dead_tuples", "warning", 11),
      f("b", "dead_tuples", "warning", 19),
      f("c", "dead_tuples", "critical", 21),
      f("d", "dead_tuples", "warning", 12),
      f("e", "lfc_hit_rate", "info", 99.5, { status: "resolved" }),
    ], [{ action_id: "x1", fingerprint: "a", state: "verified" }, { action_id: "x2", fingerprint: "d", state: "proposed" }], CAT);
    expect(items.map((i) => i.fingerprint)).toEqual(["c", "a", "d", "b"]);
    expect(items.map((i) => i.priority)).toEqual(["HIGH", "MEDIUM", "MEDIUM", "MEDIUM"]);
    expect(items[1]).toMatchObject({ actionId: "x1", actionState: "verified", areaLabel: "Vacuum & XID", why: "Dead tuples pile up." });
    expect(runbookCounts(items)).toEqual({ HIGH: 1, MEDIUM: 3, LOW: 0 });
  });

  it("measures impact against the threshold in the check's direction", () => {
    expect(impact(f("a", "dead_tuples", "warning", 15), CAT)).toBe(0.5);
    expect(impact(f("a", "lfc_hit_rate", "critical", 90), CAT)).toBeCloseTo(5 / 95);
    expect(impact(f("a", "unknown", "warning", 15), CAT)).toBe(0);
  });

  it("shows evidence numbers, never text", () => {
    expect(evidenceNumbers(JSON.stringify({ n_dead_tup: 12000, ratio: 0.12345, query: "SELECT 1", statement: "x" }))).toEqual([
      "n dead tup: 12,000", "ratio: 0.12"]);
    expect(evidenceNumbers("not json")).toEqual([]);
  });

  it("gives SQL, or a support-managed note, or the guidance note", () => {
    const [sql] = runbook([f("a", "dead_tuples", "warning", 11)], [], CAT);
    expect([sql.sql, sql.note]).toEqual(["VACUUM (ANALYZE) public.t;", null]);
    const [sm] = runbook([f("s", "guardrail_settings", "info", 1, { candidate_sql: null, evidence: JSON.stringify({ name: "track_io_timing" }) })], [], CAT);
    expect(sm.sql).toBeNull();
    expect(sm.note).toMatch(/^support-managed: track_io_timing/);
    const [none] = runbook([f("n", "dead_tuples", "warning", 11, { candidate_sql: null })], [], CAT);
    expect(none.note).toBe("No SQL fix: follow the guidance on the finding page");
  });

  it("never quotes query text in SQL", () => {
    expect(safeSql(f("q", "dead_tuples", "warning", 11, { candidate_sql: "EXPLAIN SELECT * FROM t WHERE ssn = 1", query_text: "SELECT * FROM t WHERE ssn = 1", queryid: "42" })))
      .toBe("EXPLAIN query 42");
    expect(firstSentence("One. Two.")).toBe("One.");
  });
});

describe("strip", () => {
  it("removes query text and statement text in evidence, and query groups", () => {
    const report = { findings: [f("a", "dead_tuples", "warning", 11, { query_text: "SELECT secret" })], queryGroups: [{ x: 1 }] } as unknown as Report;
    const out = strip(report);
    expect(out.findings[0].query_text).toBeNull();
    expect(JSON.parse(out.findings[0].evidence ?? "{}")).toEqual({ n_dead_tup: 12000, statement: "[masked]", table_ident: "public.t" });
    expect(out.queryGroups).toEqual([]);
    expect(JSON.stringify(out)).not.toContain("SELECT secret");
  });
});
