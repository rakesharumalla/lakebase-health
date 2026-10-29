import type { SeriesPoint } from "./types";

/**
 * A small dependency-free SVG line chart, rendered identically in the app and (server-side) in the HTML export.
 * Gaps (null values) break the line. Colours are the theme's chart tokens.
 */
export type Line = { label: string; points: SeriesPoint[]; color: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 };

const W = 640;
const H = 180;
const PAD = { left: 56, right: 12, top: 10, bottom: 24 };

const hhmm = (t: string) => new Date(t).toISOString().slice(11, 16);

export function LineChart({ lines, format, title }: { lines: Line[]; format: (v: number) => string; title: string }) {
  const pts = lines.flatMap((l) => l.points.filter((p) => p.v !== null));
  if (pts.length === 0) return <p className="rp-empty">No data in the last 24 hours.</p>;
  const times = pts.map((p) => new Date(p.t).getTime());
  const t0 = Math.min(...times);
  const t1 = Math.max(...times);
  const vals = pts.map((p) => p.v as number);
  const lo = Math.min(0, ...vals);
  const hi = Math.max(...vals) === lo ? lo + 1 : Math.max(...vals);
  const x = (t: string) => PAD.left + ((new Date(t).getTime() - t0) / Math.max(t1 - t0, 1)) * (W - PAD.left - PAD.right);
  const y = (v: number) => PAD.top + (1 - (v - lo) / (hi - lo)) * (H - PAD.top - PAD.bottom);
  const segments = (points: SeriesPoint[]) => {
    const out: string[][] = [];
    let cur: string[] = [];
    for (const p of points) {
      if (p.v === null) {
        if (cur.length) out.push(cur);
        cur = [];
      } else cur.push(`${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`);
    }
    if (cur.length) out.push(cur);
    return out;
  };
  const grid = [lo, (lo + hi) / 2, hi];
  return (
    <figure className="rp-chart">
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={title} preserveAspectRatio="none">
        <title>{title}</title>
        {grid.map((g) => (
          <g key={g}>
            <line x1={PAD.left} x2={W - PAD.right} y1={y(g)} y2={y(g)} className="rp-grid" />
            <text x={PAD.left - 6} y={y(g) + 4} textAnchor="end" className="rp-axis">
              {format(g)}
            </text>
          </g>
        ))}
        <text x={PAD.left} y={H - 6} className="rp-axis">
          {hhmm(new Date(t0).toISOString())} UTC
        </text>
        <text x={W - PAD.right} y={H - 6} textAnchor="end" className="rp-axis">
          {hhmm(new Date(t1).toISOString())} UTC
        </text>
        {lines.map((l) =>
          segments(l.points).map((s, i) =>
            s.length === 1 ? (
              <circle key={`${l.label}-${i}`} cx={s[0].split(",")[0]} cy={s[0].split(",")[1]} r={2} className={`rp-line rp-c${l.color}`} />
            ) : (
              <polyline key={`${l.label}-${i}`} points={s.join(" ")} className={`rp-line rp-c${l.color}`} fill="none" />
            ),
          ),
        )}
      </svg>
      {lines.length > 1 ? (
        <figcaption className="rp-legend">
          {lines.map((l) => (
            <span key={l.label} className="rp-legend-item">
              <span className={`rp-swatch rp-c${l.color}`} aria-hidden />
              {l.label}
            </span>
          ))}
        </figcaption>
      ) : null}
    </figure>
  );
}
