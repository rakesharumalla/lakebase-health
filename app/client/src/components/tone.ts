/**
 * The one place severity and status colours are chosen. Tones map to the --lbh-sev-* tokens in
 * theme/tokens.css; they are tinted (never solid Lava) so they cannot be mistaken for buttons.
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
