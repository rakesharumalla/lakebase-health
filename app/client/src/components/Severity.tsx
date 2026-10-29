import { Badge } from "@databricks/appkit-ui/react";
import type { ReactNode } from "react";
import type { SeverityCounts } from "../lib/findings";
import { cn } from "../lib/utils";
import { DOT_CLASSES, SEVERITY_CLASSES, SEVERITY_MARK, severityTone, statusTone, TONE_CLASSES, type Tone } from "./tone";

/** Severity and status badges: every coloured status in the app renders through ToneBadge or SeverityTag. */

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

/** A badge in the bold severity scheme (solid critical/warning, outlined info), with its mark. */
export function SeverityTag({
  tone,
  children,
  mark = true,
  className,
  title,
}: {
  tone: Tone;
  children: ReactNode;
  mark?: boolean;
  className?: string;
  title?: string;
}) {
  return (
    <Badge variant="outline" title={title} className={cn("font-semibold", SEVERITY_CLASSES[tone], className)}>
      {mark && <span aria-hidden className={SEVERITY_MARK[tone]} />}
      {children}
    </Badge>
  );
}

/** "CRITICAL", "WARNING", "INFO": upper-cased in CSS so the text (and screen readers) keep the plain word. */
export function SeverityBadge({ severity }: { severity: unknown }) {
  const label = typeof severity === "string" ? severity : "";
  return (
    <SeverityTag tone={severityTone(severity)} className="uppercase tracking-wide">
      {label}
    </SeverityTag>
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
    <SeverityTag tone={severity} mark={false} className="font-mono tabular-nums" title={`${String(count)} ${severity}`}>
      {String(count)}
    </SeverityTag>
  );
}

/** "■ 8 critical ● 23 warning ● 13 info" for a section header; zero counts are left out. */
export function SeverityChips({ counts, className }: { counts: SeverityCounts; className?: string }) {
  const items = (["critical", "warning", "info"] as const).filter((k) => counts[k] > 0);
  if (items.length === 0) return null;
  return (
    <span className={cn("inline-flex flex-wrap items-center gap-1.5", className)}>
      {items.map((k) => (
        <SeverityTag key={k} tone={k} className="tabular-nums">
          {counts[k]} {k}
        </SeverityTag>
      ))}
    </span>
  );
}

/** A section header's "N open" and, when the whole list is loaded, its per-severity chips. */
export function SectionCounts({ open, counts }: { open: number; counts: SeverityCounts | null }) {
  return (
    <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
      <span>{open} open</span>
      {counts && <SeverityChips counts={counts} />}
    </span>
  );
}
