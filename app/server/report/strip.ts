import type { Report } from "../../client/src/report/types";
import { maskRows } from "../lib/mask";

/**
 * The report as exported: no query text, statement text in evidence or session samples, whatever the exporter's
 * PII membership. Query-grouped rows (which only carry text through the in-app label lookup) are dropped.
 */
export function strip(report: Report): Report {
  const findings = maskRows(report.findings.map((f) => ({ ...f, query_text: null })), false);
  return { ...report, findings, queryGroups: [] };
}
