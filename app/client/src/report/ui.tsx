import type { ReactNode } from "react";
import type { Row, Section } from "./types";
import { fmtUtc, text } from "./format";
import type { Tone } from "../components/tone";

/**
 * Report primitives. Plain elements and rp-* classes (report.css) only, no AppKit or router imports: the server
 * renders these same components to static HTML for the download.
 */
export type Mode = "app" | "export";
export type { Tone };

/**
 * The app's two schemes (components/tone.ts) as plain report.css classes, on the same --lbh-sev-* tokens, so the
 * page and its download look alike: rp-tone-* is the tinted status scheme (TONE_CLASSES); rp-sev-* the bold
 * severity scheme (SEVERITY_CLASSES), rp-row-* its row edge and tint (ROW_CLASSES / ITEM_CLASSES), rp-tile-* the
 * tinted KPI tile (TILE_CLASSES).
 */
export const rowClass = (tone: Tone | undefined) => (tone && tone !== "neutral" ? `rp-row-${tone}` : undefined);

/** A status badge (area state, fix state): tinted. */
export function Badge({ tone, children }: { tone: Tone; children: ReactNode }) {
  return <span className={`rp-badge rp-tone-${tone}`}>{children}</span>;
}

/** A severity or priority badge: solid critical and warning, outlined info, with its mark; upper-cased in CSS. */
export function SevBadge({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span className={`rp-badge rp-sev rp-sev-${tone}`}>
      <span aria-hidden className="rp-sev-mark" />
      {children}
    </span>
  );
}

export type TileSpec = { label: string; value: ReactNode; hint?: ReactNode; tone?: Tone };

/** The row of KPI tiles every tab starts with. */
export function Tiles({ tiles }: { tiles: TileSpec[] }) {
  return (
    <div className="rp-tiles">
      {tiles.map((t) => (
        <div key={t.label} className={`rp-tile${t.tone && t.tone !== "neutral" ? ` rp-tile-${t.tone}` : ""}`}>
          <div className="rp-tile-label">{t.label}</div>
          <div className="rp-tile-value">{t.value}</div>
          {t.hint ? <div className="rp-tile-hint">{t.hint}</div> : null}
        </div>
      ))}
    </div>
  );
}

/** A card: a title, a one-line plain-language explanation, then the content (or why it is missing). */
export function Card({
  title,
  explain,
  section,
  mode,
  children,
}: {
  title: string;
  explain: string;
  section?: Section<unknown>;
  mode: Mode;
  children: ReactNode;
}) {
  const failed = section?.status === "error";
  return (
    <section className="rp-card">
      <header className="rp-card-head">
        <h3 className="rp-card-title">{title}</h3>
        {section?.asOf ? (
          <span className={`rp-asof${section.stale ? " rp-stale" : ""}`}>
            {section.stale ? "stale: " : "as of "}
            {fmtUtc(section.asOf)}
          </span>
        ) : null}
      </header>
      <p className="rp-explain">{explain}</p>
      {failed ? (
        <div className="rp-error" role="alert">
          {mode === "export" ? "Section unavailable: its data could not be loaded when this report was exported." : section?.error}
        </div>
      ) : (
        children
      )}
    </section>
  );
}

export type Column = {
  key: string;
  label: string;
  numeric?: boolean;
  format?: (v: Row[string], row: Row) => ReactNode;
};

export function Table({ columns, rows, empty, limit, rowTone }: { columns: Column[]; rows: Row[]; empty: string; limit?: number; rowTone?: (r: Row) => Tone }) {
  if (rows.length === 0) return <p className="rp-empty">{empty}</p>;
  const shown = limit ? rows.slice(0, limit) : rows;
  return (
    <div className="rp-table-wrap">
      <table className="rp-table">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} className={c.numeric ? "rp-num" : undefined}>
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((r, i) => (
            <tr key={i} className={rowTone ? rowClass(rowTone(r)) : undefined}>
              {columns.map((c) => (
                <td key={c.key} className={c.numeric ? "rp-num" : undefined}>
                  {c.format ? c.format(r[c.key], r) : text(r[c.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {limit && rows.length > limit ? <p className="rp-more">Showing {limit} of {rows.length}.</p> : null}
    </div>
  );
}

export function Code({ children }: { children: string }) {
  return <pre className="rp-code">{children}</pre>;
}

export function Note({ children }: { children: ReactNode }) {
  return <p className="rp-note">{children}</p>;
}
