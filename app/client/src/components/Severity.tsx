import { Badge } from "@databricks/appkit-ui/react";
import type { ReactNode } from "react";
import { cn } from "../lib/utils";
import { DOT_CLASSES, severityTone, statusTone, TONE_CLASSES, type Tone } from "./tone";

/** Severity and status badges: every coloured status in the app renders through ToneBadge. */

export function ToneBadge({
  tone,
  children,
  dot = false,
  className,
  title,
}: {
  tone: Tone;
  children: ReactNode;
  dot?: boolean;
  className?: string;
  title?: string;
}) {
  return (
    <Badge variant="outline" title={title} className={cn("font-medium", TONE_CLASSES[tone], className)}>
      {dot && <span aria-hidden className={cn("size-1.5 rounded-full", DOT_CLASSES[tone])} />}
      {children}
    </Badge>
  );
}

export function SeverityBadge({ severity }: { severity: unknown }) {
  const label = typeof severity === "string" ? severity : "";
  return (
    <ToneBadge tone={severityTone(severity)} dot className="capitalize">
      {label}
    </ToneBadge>
  );
}

export function StatusBadge({ status, label }: { status: unknown; label?: string }) {
  const text = label ?? (typeof status === "string" ? status : "");
  return <ToneBadge tone={statusTone(status)}>{text}</ToneBadge>;
}

/** A per-severity count; zero renders as quiet text so non-zero counts stand out. */
export function CountBadge({ severity, count }: { severity: Tone; count: unknown }) {
  const n = Number(count ?? 0);
  if (!n) return <span className="font-mono tabular-nums text-muted-foreground">0</span>;
  return (
    <ToneBadge tone={severity} className="font-mono tabular-nums">
      {String(count)}
    </ToneBadge>
  );
}
