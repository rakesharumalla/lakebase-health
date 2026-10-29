export const MAX_WINDOW_MS = 7 * 24 * 3600 * 1000;
export const DEFAULT_WINDOW_MS = 6 * 3600 * 1000;

const FINGERPRINT = /^[0-9a-f]{32}$/;
const ENDPOINT = /^ep-[a-z0-9-]{1,61}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

const match =
  (re: RegExp) =>
  (s: unknown): string | null =>
    typeof s === "string" && re.test(s) ? s : null;
export const parseFingerprint = match(FINGERPRINT);
export const parseEndpointId = match(ENDPOINT);
export const parseProjectId = match(UUID);
export const parseActionId = match(FINGERPRINT);

function parseDate(s: unknown): Date | null | undefined {
  if (s === undefined || s === "") return undefined;
  if (typeof s !== "string" || !/^\d{4}-\d{2}-\d{2}T/.test(s)) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function parseWindow(from: unknown, to: unknown, now: Date): { from: Date; to: Date } | { error: string } {
  const f = parseDate(from);
  const t = parseDate(to);
  if (f === null) return { error: "invalid from" };
  if (t === null) return { error: "invalid to" };
  let end = t ?? now;
  if (end.getTime() > now.getTime()) end = now;
  let start = f ?? new Date(end.getTime() - DEFAULT_WINDOW_MS);
  if (start.getTime() >= end.getTime()) return { error: "from must be before to" };
  const floor = now.getTime() - MAX_WINDOW_MS;
  if (end.getTime() <= floor) return { error: "window is older than 7 days" };
  if (start.getTime() < floor) start = new Date(floor);
  if (end.getTime() - start.getTime() > MAX_WINDOW_MS) start = new Date(end.getTime() - MAX_WINDOW_MS);
  return { from: start, to: end };
}
