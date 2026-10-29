export const MASKED = "[masked]";
export const TEXT_FIELDS: ReadonlySet<string> = new Set([
  "query_text",
  "plan_json",
  "parameters",
  "statement",
  "internalquery",
  "query",
]);

function maskValue(key: string, v: unknown): unknown {
  if (TEXT_FIELDS.has(key)) return v === null || v === undefined ? v : MASKED;
  if (Array.isArray(v)) return v.map((x) => maskValue("", x));
  if (typeof v === "object" && v !== null) {
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, maskValue(k, x)]));
  }
  return v;
}

/** Hide statement text from users outside the PII group, including text nested in evidence JSON. */
export function maskRows<T extends Record<string, unknown>>(rows: T[], pii: boolean): T[] {
  if (pii) return rows;
  return rows.map((row) => {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      if (k === "evidence" && typeof v === "string") {
        try {
          out[k] = JSON.stringify(maskValue("", JSON.parse(v)));
        } catch {
          out[k] = v;
        }
      } else {
        out[k] = maskValue(k, v);
      }
    }
    return out as T;
  });
}
