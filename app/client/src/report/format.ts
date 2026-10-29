/** Formatting shared by the report page and its HTML export (no locale or time-zone surprises in exports). */
export const num = (v: unknown): number | null => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

export const DASH = "—";

export function fmtNum(v: unknown, digits = 0): string {
  const n = num(v);
  return n === null ? DASH : n.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: 0 });
}

export function fmtPct(v: unknown, digits = 1): string {
  const n = num(v);
  return n === null ? DASH : `${n.toFixed(digits)} %`;
}

const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];
export function fmtBytes(v: unknown): string {
  let n = num(v);
  if (n === null) return DASH;
  let i = 0;
  while (Math.abs(n) >= 1024 && i < UNITS.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 || Math.abs(n) >= 100 ? 0 : 1)} ${UNITS[i]}`;
}

export function fmtDuration(seconds: unknown): string {
  const s = num(seconds);
  if (s === null) return DASH;
  if (s < 60) return `${Math.round(s)} s`;
  if (s < 3600) return `${Math.round(s / 60)} min`;
  if (s < 86400) return `${(s / 3600).toFixed(1)} h`;
  return `${(s / 86400).toFixed(1)} d`;
}

export function fmtMs(v: unknown): string {
  const n = num(v);
  if (n === null) return DASH;
  return n < 10 ? `${n.toFixed(2)} ms` : n < 1000 ? `${Math.round(n)} ms` : `${(n / 1000).toFixed(1)} s`;
}

/** UTC, minute precision: "2026-10-29 12:00 UTC". */
export function fmtUtc(v: unknown): string {
  if (typeof v !== "string" && typeof v !== "number") return DASH;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return DASH;
  return `${d.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}

export const text = (v: unknown): string =>
  typeof v === "string" ? v : typeof v === "number" || typeof v === "boolean" ? String(v) : DASH;
