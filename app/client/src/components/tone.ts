/**
 * The one place severity and status colours are chosen. Tones map to the --lbh-sev-* tokens in
 * theme/tokens.css. Status tones are tinted; severity (badges, issue badges, count chips, row edges)
 * uses the bolder SEVERITY_* scheme below. Nothing here is Lava, so none of it reads as a button.
 */
export type Tone = "critical" | "warning" | "info" | "ok" | "neutral";

export const TONE_CLASSES: Record<Tone, string> = {
  critical: "bg-sev-critical-bg text-sev-critical-fg border-sev-critical-border",
  warning: "bg-sev-warning-bg text-sev-warning-fg border-sev-warning-border",
  info: "bg-sev-info-bg text-sev-info-fg border-sev-info-border",
  ok: "bg-sev-ok-bg text-sev-ok-fg border-sev-ok-border",
  neutral: "bg-sev-neutral-bg text-sev-neutral-fg border-sev-neutral-border",
};

export const DOT_CLASSES: Record<Tone, string> = {
  critical: "bg-sev-critical-fg",
  warning: "bg-sev-warning-fg",
  info: "bg-sev-info-fg",
  ok: "bg-sev-ok-fg",
  neutral: "bg-sev-neutral-fg",
};

/**
 * Severity badges: solid critical and warning, an outlined info, solid green for resolved/ok. The label
 * is always printed (colour is never the only cue).
 */
export const SEVERITY_CLASSES: Record<Tone, string> = {
  critical: "border-transparent bg-sev-critical-solid-bg text-sev-critical-solid-fg",
  warning: "border-transparent bg-sev-warning-solid-bg text-sev-warning-solid-fg",
  info: "border-sev-info-outline bg-sev-info-bg text-sev-info-fg",
  ok: "border-transparent bg-sev-ok-solid-bg text-sev-ok-solid-fg",
  neutral: TONE_CLASSES.neutral,
};

/** The mark before a severity label, in the label's colour: a filled square for critical, a dot otherwise. */
export const SEVERITY_MARK: Record<Tone, string> = {
  critical: "size-2 rounded-[1px] bg-current",
  warning: "size-2 rounded-full bg-current",
  info: "size-2 rounded-full bg-current",
  ok: "size-2 rounded-full bg-current",
  neutral: "size-2 rounded-full bg-current",
};

/**
 * Table rows (and list items) carrying a severity: a 4 px inset edge in the severity colour, plus a tint
 * (with its own hover) for critical and warning. The edge is an inset shadow so it does not shift columns.
 * ROW_CLASSES puts the edge on a row's first cell; ITEM_CLASSES on the element itself.
 */
export const ROW_CLASSES: Record<Tone, string> = {
  critical:
    "bg-sev-critical-row hover:bg-sev-critical-row-hover [&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-critical-edge)]",
  warning:
    "bg-sev-warning-row hover:bg-sev-warning-row-hover [&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-warning-edge)]",
  info: "[&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-info-edge)]",
  ok: "[&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-ok-edge)]",
  neutral: "",
};

export const ITEM_CLASSES: Record<Tone, string> = {
  critical: "bg-sev-critical-row hover:bg-sev-critical-row-hover shadow-[inset_4px_0_0_var(--lbh-sev-critical-edge)]",
  warning: "bg-sev-warning-row hover:bg-sev-warning-row-hover shadow-[inset_4px_0_0_var(--lbh-sev-warning-edge)]",
  info: "hover:bg-muted/50 shadow-[inset_4px_0_0_var(--lbh-sev-info-edge)]",
  ok: "hover:bg-muted/50 shadow-[inset_4px_0_0_var(--lbh-sev-ok-edge)]",
  neutral: "hover:bg-muted/50",
};

/** Edge only, no tint: rows that summarise several findings (the fleet table). */
export const EDGE_CLASSES: Record<Tone, string> = {
  critical: "[&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-critical-edge)]",
  warning: "[&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-warning-edge)]",
  info: "[&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-info-edge)]",
  ok: "[&>td:first-child]:shadow-[inset_4px_0_0_var(--lbh-sev-ok-edge)]",
  neutral: "",
};

/** KPI tiles for a severity count: tinted ground, number in the severity colour. */
export const TILE_CLASSES: Record<Tone, { card: string; value: string }> = {
  critical: { card: "bg-sev-critical-bg border-sev-critical-border", value: "text-sev-critical-fg" },
  warning: { card: "bg-sev-warning-bg border-sev-warning-border", value: "text-sev-warning-fg" },
  info: { card: "bg-sev-info-bg border-sev-info-border", value: "text-sev-info-fg" },
  ok: { card: "bg-sev-ok-bg border-sev-ok-border", value: "text-sev-ok-fg" },
  neutral: { card: "", value: "text-heading" },
};

export function severityTone(severity: unknown): Tone {
  if (severity === "critical") return "critical";
  if (severity === "warning") return "warning";
  if (severity === "info") return "info";
  return "neutral";
}

/** Finding status, action state, run outcome, rule result and on/off flags. */
export function statusTone(status: unknown): Tone {
  const s = typeof status === "string" ? status : "";
  if (s === "resolved" || s === "verified" || s === "applied" || s === "pass" || s === "on") return "ok";
  if (s.endsWith("failed") || s === "fail") return "critical";
  if (s === "blocked" || s === "off") return "warning";
  if (s === "new" || s === "proposed" || s === "verifying" || s === "applying" || s === "running") return "info";
  return "neutral";
}

export function riskTone(risk: unknown): Tone {
  if (risk === "high") return "critical";
  if (risk === "medium") return "warning";
  return "neutral";
}
