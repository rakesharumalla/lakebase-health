export type Row = Record<string, string | number | boolean | null>;
export type ApiError = { status: number; message: string };
export type Meta = {
  lastSuccessAt: string | null;
  lastStatus: string | null;
  stale: boolean;
  openFindings: number;
  pii: boolean;
  accessIncomplete?: boolean;
};

export const truthy = (v: unknown) => v === true || v === "true" || v === 1 || v === "1";
export const fmtTime = (v: unknown) =>
  typeof v === "string" || typeof v === "number" ? new Date(v).toLocaleString() : "—";
export const str = (v: unknown) =>
  typeof v === "string" || typeof v === "number" || typeof v === "boolean" ? String(v) : "";
