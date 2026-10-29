import { describe, expect, it } from "vitest";
import { MASKED, maskRows } from "../lib/mask";

describe("maskRows", () => {
  const rows = [{
    check_id: "plan_flip", query_text: "SELECT * FROM secrets WHERE ssn = '1'",
    evidence: JSON.stringify({ queryid: "42", query_text: "SELECT ssn", nested: { statement: "UPDATE t" } }),
    statement: "DELETE FROM t", measure: 3,
  }];

  it("masks top-level and evidence-nested text for non-PII users", () => {
    const [r] = maskRows(rows, false);
    expect(r.query_text).toBe(MASKED);
    expect(r.statement).toBe(MASKED);
    const ev = JSON.parse(String(r.evidence));
    expect(ev.query_text).toBe(MASKED);
    expect(ev.nested.statement).toBe(MASKED);
    expect(ev.queryid).toBe("42");
    expect(r.measure).toBe(3);
  });

  it("leaves rows unchanged for PII members and keeps nulls null", () => {
    expect(maskRows(rows, true)).toEqual(rows);
    expect(maskRows([{ query_text: null }], false)[0].query_text).toBeNull();
  });

  it("does not throw on non-JSON evidence", () => {
    expect(maskRows([{ evidence: "not json" }], false)[0].evidence).toBe("not json");
  });
});
