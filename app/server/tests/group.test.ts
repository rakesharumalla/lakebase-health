import { describe, expect, it } from "vitest";
import { groupFindings, isOpen, page, queryIdOf, TOP_N } from "../lib/group";

const EP = "ep-sample-primary-a1b2c3d4";
let n = 0;
const f = (o: Record<string, unknown>) => ({
  fingerprint: `fp${n++}`,
  check_id: "plan_flip",
  title: "t",
  category: "performance",
  endpoint_id: EP,
  object: "",
  status: "open",
  severity: "warning",
  measure: "1",
  occurrences: "1",
  first_seen: "2026-10-28T01:00:00Z",
  last_seen: "2026-10-28T02:00:00Z",
  queryid: null,
  top_queryids: null,
  db_ms: null,
  ...o,
});

describe("queryIdOf", () => {
  it("uses evidence.queryid, and the object of legacy query-check findings", () => {
    expect(queryIdOf(f({ queryid: "42" }))).toBe("42");
    expect(queryIdOf(f({ check_id: "row_misestimate", object: "-5246853206147450390" }))).toBe("-5246853206147450390");
    expect(queryIdOf(f({ check_id: "hot_row_lock", object: `${EP}/-3196267159430279274` }))).toBe("-3196267159430279274");
    expect(queryIdOf(f({ check_id: "disk_spill", object: "77@alice" }))).toBe("77");
    expect(queryIdOf(f({ check_id: "dead_tuples", object: "123" }))).toBeNull();
    expect(queryIdOf(f({ check_id: "row_misestimate", object: EP, top_queryids: '["1"]' }))).toBeNull();
  });
});

describe("groupFindings", () => {
  it("collapses several checks on the same (endpoint, queryid) into one row with the worst severity", () => {
    const rows = groupFindings([
      f({ fingerprint: "a", check_id: "plan_flip", queryid: "42", severity: "warning" }),
      f({ fingerprint: "b", check_id: "storage_wait", queryid: "42", severity: "critical", status: "new" }),
      f({ fingerprint: "c", check_id: "top_db_time_query", queryid: "42", severity: "info", db_ms: "90000" }),
      f({ fingerprint: "d", check_id: "plan_flip", queryid: "42", endpoint_id: "ep-other" }),
    ]);
    expect(rows).toHaveLength(2);
    const [g] = rows;
    expect(g).toMatchObject({ kind: "query", endpoint_id: EP, queryid: "42", severity: "critical", fingerprint: "b", status: "new", db_ms: 90000 });
    expect(g.issues.map((i) => [i.check_id, i.severity, i.fingerprint])).toEqual([
      ["storage_wait", "critical", "b"],
      ["plan_flip", "warning", "a"],
      ["top_db_time_query", "info", "c"],
    ]);
    expect(g.issues[0].label).toBe("storage wait");
  });

  it("shows the endpoint-level row_misestimate once and badges the queries in its top_queries", () => {
    const est = f({ fingerprint: "est", check_id: "row_misestimate", object: EP, top_queryids: '["42","7"]', severity: "warning" });
    const rows = groupFindings([est, f({ fingerprint: "a", queryid: "42" }), f({ fingerprint: "b", queryid: "9" })]);
    expect(rows.filter((r) => r.fingerprint === "est")).toHaveLength(1);
    expect(rows.find((r) => r.fingerprint === "est")).toMatchObject({ kind: "finding", queryid: null });
    const q42 = rows.find((r) => r.queryid === "42")!;
    expect(q42.issues.map((i) => [i.check_id, i.label, i.fingerprint])).toEqual([
      ["plan_flip", "plan flip", "a"],
      ["row_misestimate", "row estimates", "est"],
    ]);
    expect(rows.find((r) => r.queryid === "9")!.issues).toHaveLength(1);
    // A query only listed in top_queries (no per-query finding of its own) gets no row.
    expect(rows.find((r) => r.queryid === "7")).toBeUndefined();
  });

  it("can take the row_misestimate index from findings outside the grouped list", () => {
    const est = f({ fingerprint: "est", check_id: "row_misestimate", object: EP, top_queryids: '["42"]' });
    const [g] = groupFindings([f({ queryid: "42" })], [est]);
    expect(g.issues.map((i) => i.fingerprint)).toContain("est");
  });

  it("groups legacy per-query row_misestimate findings with the other checks, without a second badge", () => {
    const est = f({ fingerprint: "est", check_id: "row_misestimate", object: EP, top_queryids: '["42"]' });
    const rows = groupFindings([
      est,
      f({ fingerprint: "old", check_id: "row_misestimate", object: "42", severity: "critical" }),
      f({ fingerprint: "a", check_id: "plan_flip", queryid: "42" }),
    ]);
    const q = rows.find((r) => r.queryid === "42")!;
    expect(q.severity).toBe("critical");
    expect(q.issues.filter((i) => i.check_id === "row_misestimate").map((i) => i.fingerprint)).toEqual(["old"]);
  });

  it("passes findings without a queryid through as single rows", () => {
    const rows = groupFindings([f({ fingerprint: "x", check_id: "dead_tuples", object: "db:t" }), f({ fingerprint: "y", check_id: "dead_tuples", object: "db:u" })]);
    expect(rows.map((r) => [r.kind, r.fingerprint, r.issues.length])).toEqual([
      ["finding", "x", 1],
      ["finding", "y", 1],
    ]);
  });

  it("orders by worst severity, then DB time, then measure, then occurrences", () => {
    const rows = groupFindings([
      f({ fingerprint: "w-low", queryid: "1", measure: "2" }),
      f({ fingerprint: "info", queryid: "2", severity: "info", db_ms: "999999" }),
      f({ fingerprint: "w-db", queryid: "3", db_ms: "5000", measure: "1" }),
      f({ fingerprint: "crit", queryid: "4", severity: "critical" }),
      f({ fingerprint: "w-high", queryid: "5", measure: "9" }),
      f({ fingerprint: "w-occ", check_id: "dead_tuples", measure: "2", occurrences: "8" }),
    ]);
    expect(rows.map((r) => r.fingerprint)).toEqual(["crit", "w-db", "w-high", "w-occ", "w-low", "info"]);
  });
});

describe("open status and paging", () => {
  it("treats superseded like resolved: not open", () => {
    expect(["new", "open"].every((s) => isOpen({ status: s }))).toBe(true);
    expect(["resolved", "superseded", "ignored"].some((s) => isOpen({ status: s }))).toBe(false);
  });

  it("returns the top 10 and the total, or everything when asked", () => {
    const rows = Array.from({ length: 25 }, (_, i) => i);
    expect(page(rows)).toEqual({ total: 25, rows: rows.slice(0, TOP_N) });
    expect(page(rows, true)).toEqual({ total: 25, rows });
    expect(TOP_N).toBe(10);
  });
});
