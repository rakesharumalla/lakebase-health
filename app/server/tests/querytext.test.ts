import { describe, expect, it } from "vitest";
import { MAX_QUERY_PAIRS, parseQueryPairs, queryLabel, skeleton } from "../lib/querytext";

describe("skeleton", () => {
  it("replaces string literals and numbers, keeps $n parameters and collapses whitespace", () => {
    expect(skeleton("SELECT *\n  FROM orders\tWHERE email = 'a@b.com' AND id = 42 AND amount > 3.5e2 AND c = $1")).toBe(
      "SELECT * FROM orders WHERE email = ? AND id = ? AND amount > ? AND c = $1",
    );
  });

  it("handles escaped quotes, E'' and dollar-quoted strings and drops comments", () => {
    expect(skeleton("SELECT 'it''s', E'a\\'b', $$secret$$, $tag$x y$tag$ -- note ssn 123\nFROM t /* who */")).toBe(
      "SELECT ?, ?, ?, ? FROM t",
    );
  });

  it("does not touch digits inside identifiers or quoted identifiers", () => {
    expect(skeleton('SELECT col_2, "Table 1".x FROM t1 WHERE t1.v = -7')).toBe('SELECT col_2, "Table 1".x FROM t1 WHERE t1.v = -?');
  });

  it("is stable on already-normalized pg_stat_statements text", () => {
    const s = "INSERT INTO hc_test.orders (customer_id, status, amount) VALUES ($1, $2, $3)";
    expect(skeleton(s)).toBe(s);
  });
});

describe("queryLabel", () => {
  const row = (o: Record<string, unknown>) => ({ endpoint_id: "ep-a", queryid: "42", ...o });

  it("prefers complete plan text, then pg_stat_statements text, then truncated plan text", () => {
    expect(queryLabel(row({ plan_text: "SELECT 1", plan_truncated: "false", stat_text: "SELECT $1" }), true)).toMatchObject({
      text: "SELECT 1", label: "SELECT 1", kind: "full", truncated: false,
    });
    expect(queryLabel(row({ plan_text: "SELECT a FR", plan_truncated: "true", stat_text: "SELECT a FROM t" }), true).text).toBe(
      "SELECT a FROM t",
    );
    expect(queryLabel(row({ plan_text: "SELECT a FR", plan_truncated: true, stat_text: null }), true)).toMatchObject({
      text: "SELECT a FR", truncated: true,
    });
  });

  it("returns the normalized skeleton to users who may not see text", () => {
    const l = queryLabel(row({ plan_text: "SELECT * FROM users WHERE ssn = '123-45-6789'", plan_truncated: "false" }), false);
    expect(l).toMatchObject({ text: "SELECT * FROM users WHERE ssn = ?", kind: "skeleton" });
    expect(JSON.stringify(l)).not.toContain("6789");
  });

  it("falls back to 'query <id>' when there is no text, including UC's <REDACTED>", () => {
    for (const r of [row({}), row({ plan_text: "<REDACTED>", stat_text: "<REDACTED>" }), row({ plan_text: "  " })]) {
      expect(queryLabel(r, true)).toEqual({ endpoint_id: "ep-a", queryid: "42", text: null, label: "query 42", kind: "none", truncated: false });
    }
  });
});

describe("parseQueryPairs", () => {
  it("accepts endpoint:queryid pairs, dedupes them and rejects anything else", () => {
    expect(parseQueryPairs("ep-a-1:42,ep-a-1:-7,ep-a-1:42")).toEqual([
      { endpoint_id: "ep-a-1", queryid: "42" },
      { endpoint_id: "ep-a-1", queryid: "-7" },
    ]);
    for (const bad of ["", undefined, ["ep-a:1"], "ep-a", "ep-a:1;DROP", "x:1", "ep-a:1.5", "ep-a:99999999999999999999999"]) {
      expect(parseQueryPairs(bad), String(bad)).toBeNull();
    }
    const many = Array.from({ length: MAX_QUERY_PAIRS + 1 }, (_, i) => `ep-a:${i}`).join(",");
    expect(parseQueryPairs(many)).toBeNull();
  });
});
