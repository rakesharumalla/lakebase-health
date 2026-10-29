import { readFileSync } from "node:fs";
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

  it("prefers the analysis job's query_texts copy of pg_stat_statements over system.lakebase text", () => {
    expect(
      queryLabel(row({ qt_text: "SELECT a FROM t WHERE b = $1", plan_text: "SELECT 1", plan_truncated: "false", stat_text: "x" }), true),
    ).toMatchObject({ text: "SELECT a FROM t WHERE b = $1", kind: "full", truncated: false });
    expect(queryLabel(row({ qt_text: "<REDACTED>", plan_text: "<REDACTED>", stat_text: "SELECT $1" }), true).text).toBe("SELECT $1");
    expect(queryLabel(row({ qt_text: "SELECT x FROM t WHERE s = $1" }), false)).toMatchObject({
      text: "SELECT x FROM t WHERE s = $1", kind: "skeleton",
    });
  });

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
    for (const r of [row({}), row({ qt_text: "<REDACTED>", plan_text: "<REDACTED>", stat_text: "<REDACTED>" }), row({ plan_text: "  " })]) {
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

describe("query_text lookup", () => {
  const sql = readFileSync(new URL("../queries/query_text.sql", import.meta.url), "utf8");

  it("reads query_texts first, scoped to the caller's projects, and system.lakebase text only when not redacted", () => {
    expect(sql).toContain("{catalog}.lakebase_health.query_texts");
    expect(sql).toMatch(/q\.project_id = :project_id AND array_contains\(split\(:project_ids, ','\), q\.project_id\)/);
    expect(sql).toMatch(/q\.captured_at >= current_timestamp\(\) - INTERVAL 7 DAYS/);
    expect(sql).toContain("AS qt_text");
    expect(sql.match(/NOT LIKE '<REDACTED%'/g)).toHaveLength(3);
  });
});
