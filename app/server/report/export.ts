import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ReportDocument } from "../../client/src/report/Document";
import type { Report } from "../../client/src/report/types";
import { strip } from "./strip";

/** The theme tokens and the report styles, inlined into every export (read once; the app ships its sources). */
export function exportCss(root = process.cwd()): string {
  const css = ["client/src/theme/tokens.css", "client/src/report/report.css"]
    .map((f) => readFileSync(join(root, f), "utf8").replace(/\/\*[\s\S]*?\*\//g, ""))
    .join("\n");
  if (/@import|url\(|<\/style/i.test(css)) throw new Error("report CSS must not import, load URLs or close the style tag");
  return css;
}

let cached: string | undefined;
const css = () => (cached ??= exportCss());

const slug = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60) || "x";

/** lakebase-health-<project>-<branch>-<UTC yyyymmddThhmmZ>.html */
export function exportFilename(report: Report, now: Date): string {
  const t = now.toISOString().replace(/[-:]/g, "").slice(0, 13);
  return `lakebase-health-${slug(report.project.name)}-${slug(report.branch.name)}-${t}Z.html`;
}

/** The downloadable report: query text stripped, one self-contained HTML document with inline CSS and SVG. */
export function renderExport(report: Report, styles: string = css()): string {
  return `<!DOCTYPE html>${renderToStaticMarkup(createElement(ReportDocument, { report: strip(report), css: styles }))}`;
}
