import {
  Breadcrumb,
  Button,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
  Skeleton,
  Table,
} from "@databricks/appkit-ui/react";
import { Inbox } from "lucide-react";
import { Fragment, type ComponentProps, type ReactNode } from "react";
import { Link, NavLink } from "react-router";
import { cn } from "../lib/utils";
import { TILE_CLASSES, type Tone } from "./tone";

/** Presentation primitives shared by the views. Colours come from theme/tokens.css only. */

/** Right-aligned, tabular, monospace: for every numeric table cell. */
export const NUM = "text-right font-mono tabular-nums";

export function TextLink({ className, ...props }: ComponentProps<typeof Link>) {
  return (
    <Link
      className={cn(
        "font-medium text-link underline decoration-lava/60 decoration-1 underline-offset-4 transition-colors hover:decoration-coral hover:decoration-2",
        className,
      )}
      {...props}
    />
  );
}

/** Monospace identifier, truncated with the full value in a tooltip. */
export function MonoId({ value, className, max = "max-w-[22ch]" }: { value: string; className?: string; max?: string }) {
  return (
    <span title={value} className={cn("inline-block truncate align-bottom font-mono text-[0.8125rem]", max, className)}>
      {value}
    </span>
  );
}

export function CodeBlock({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <pre
      className={cn(
        "overflow-x-auto whitespace-pre-wrap break-words rounded-md border bg-code px-3 py-2 font-mono text-[0.8125rem] leading-relaxed",
        className,
      )}
    >
      {children}
    </pre>
  );
}

export function InlineCode({ children }: { children: ReactNode }) {
  return <code className="rounded bg-code px-1 py-0.5 font-mono text-[0.8125rem]">{children}</code>;
}

export type Crumb = { label: string; to?: string };

export function PageHeader({
  crumbs = [],
  title,
  meta,
  actions,
  children,
}: {
  crumbs?: Crumb[];
  title: ReactNode;
  meta?: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div className="space-y-3">
      {crumbs.length > 0 && (
        <Breadcrumb>
          <BreadcrumbList>
            {crumbs.map((c, i) => (
              <Fragment key={c.to ?? c.label}>
                {i > 0 && <BreadcrumbSeparator />}
                <BreadcrumbItem>
                  {c.to ? (
                    <BreadcrumbLink asChild>
                      <Link to={c.to}>{c.label}</Link>
                    </BreadcrumbLink>
                  ) : (
                    <BreadcrumbPage className="max-w-[40ch] truncate">{c.label}</BreadcrumbPage>
                  )}
                </BreadcrumbItem>
              </Fragment>
            ))}
          </BreadcrumbList>
        </Breadcrumb>
      )}
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 space-y-1">
          <h1 className="text-2xl font-semibold leading-tight">{title}</h1>
          {meta && <div className="text-sm text-muted-foreground">{meta}</div>}
        </div>
        {actions && <div className="flex shrink-0 flex-wrap gap-2">{actions}</div>}
      </div>
      {children}
    </div>
  );
}

/** Sub-navigation under a page header (e.g. a project's findings vs its fixes). */
export function SubNav({ items }: { items: { label: string; to: string; end?: boolean }[] }) {
  return (
    <nav aria-label="Section" className="flex gap-6 border-b">
      {items.map((it) => (
        <NavLink
          key={it.to}
          to={it.to}
          end={it.end}
          className={({ isActive }) =>
            cn(
              "-mb-px border-b-2 px-0.5 pb-2 text-sm font-medium transition-colors",
              isActive
                ? "border-lava text-heading"
                : "border-transparent text-muted-foreground hover:border-coral hover:text-foreground",
            )
          }
        >
          {it.label}
        </NavLink>
      ))}
    </nav>
  );
}

export function KpiTile({
  label,
  value,
  hint,
  accent,
  tone,
}: {
  label: string;
  value: ReactNode;
  hint?: ReactNode;
  /** Colour class for the left accent bar, e.g. "bg-sev-critical-fg". */
  accent?: string;
  /** A severity tile: tinted ground and a number in the severity colour (theme/tokens.css). */
  tone?: Tone;
}) {
  const t = tone ? TILE_CLASSES[tone] : undefined;
  return (
    <Card className={cn("relative gap-1 overflow-hidden py-4 shadow-none", t?.card)}>
      {accent && <span aria-hidden className={cn("absolute inset-y-0 left-0 w-1", accent)} />}
      <div className={cn("px-5 text-xs font-medium uppercase tracking-wide", t ? "text-foreground" : "text-muted-foreground")}>
        {label}
      </div>
      <div className={cn("px-5 text-[1.75rem] font-semibold leading-tight tabular-nums tracking-tight", t?.value ?? "text-heading")}>
        {value}
      </div>
      {hint && <div className={cn("px-5 text-xs", t ? "text-foreground" : "text-muted-foreground")}>{hint}</div>}
    </Card>
  );
}

export function KpiRow({ children }: { children: ReactNode }) {
  return <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">{children}</div>;
}

/** A titled card section; the title is an h2 so views keep their heading outline. */
export function SectionCard({
  title,
  description,
  action,
  children,
  className,
  contentClassName,
}: {
  title: ReactNode;
  description?: ReactNode;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
  contentClassName?: string;
}) {
  return (
    <Card className={cn("gap-4 py-5 shadow-none", className)}>
      <CardHeader className="gap-1 px-5">
        <CardTitle>
          <h2 className="text-base font-semibold leading-snug">{title}</h2>
        </CardTitle>
        {description && <CardDescription>{description}</CardDescription>}
        {action && <div className="col-start-2 row-span-2 row-start-1 self-start justify-self-end">{action}</div>}
      </CardHeader>
      <CardContent className={cn("px-5", contentClassName)}>{children}</CardContent>
    </Card>
  );
}

/** AppKit Table with the app's header and row styling. Rows get AppKit's hover tint. */
export function DataTable({ className, ...props }: ComponentProps<typeof Table>) {
  return (
    <Table
      className={cn(
        "[&_td]:px-3 [&_td]:py-2 [&_th]:h-9 [&_th]:px-3 [&_th]:text-xs [&_th]:font-medium [&_th]:text-muted-foreground [&_thead_tr]:bg-muted/60 [&_thead_tr]:hover:bg-muted/60",
        className,
      )}
      {...props}
    />
  );
}

export function LoadingState({ label = "Loading", rows = 4 }: { label?: string; rows?: number }) {
  return (
    <div role="status" aria-label={label} className="space-y-4">
      <span className="sr-only">{label}…</span>
      <Skeleton className="h-7 w-64" />
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {[0, 1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-20" />
        ))}
      </div>
      <div className="space-y-2 rounded-xl border bg-card p-5">
        {Array.from({ length: rows }, (_, i) => (
          <Skeleton key={i} className="h-5 w-full" />
        ))}
      </div>
    </div>
  );
}

export function InlineLoading({ label = "Loading" }: { label?: string }) {
  return (
    <div role="status" aria-label={label} className="space-y-2">
      <span className="sr-only">{label}…</span>
      <Skeleton className="h-5 w-full" />
      <Skeleton className="h-5 w-4/5" />
      <Skeleton className="h-5 w-3/5" />
    </div>
  );
}

export function EmptyState({
  title,
  children,
  action,
  icon,
  className,
}: {
  title: string;
  children?: ReactNode;
  action?: ReactNode;
  icon?: ReactNode;
  className?: string;
}) {
  return (
    <Empty className={cn("border border-dashed bg-card", className)}>
      <EmptyHeader>
        <EmptyMedia variant="icon">{icon ?? <Inbox />}</EmptyMedia>
        <EmptyTitle className="text-heading">{title}</EmptyTitle>
        {children && <EmptyDescription>{children}</EmptyDescription>}
      </EmptyHeader>
      {action && <EmptyContent>{action}</EmptyContent>}
    </Empty>
  );
}

/** "Show all N" under a list cut to its top rows; expands in place (collapsed by default). */
export function ShowAllButton({
  total,
  shown,
  open,
  loading = false,
  onToggle,
  className,
}: {
  total: number;
  shown: number;
  open: boolean;
  loading?: boolean;
  onToggle: () => void;
  className?: string;
}) {
  if (!open && total <= shown) return null;
  return (
    <div className={cn("flex items-center gap-3 px-3 py-2", className)}>
      <Button variant="outline" size="sm" onClick={onToggle} disabled={loading} aria-expanded={open}>
        {open ? "Show top 10" : `Show all ${total}`}
      </Button>
      {loading ? <span className="text-xs text-muted-foreground">Loading…</span> : null}
      {!open ? <span className="text-xs text-muted-foreground">Showing {shown} of {total}</span> : null}
    </div>
  );
}

/** "Showing the first N of M": a list the server cut at its SQL LIMIT (totals still come from SQL counts). */
export function TruncatedNote({ shown, count, limit, className }: { shown: number; count: number; limit: number; className?: string }) {
  return (
    <p role="note" className={cn("px-3 py-2 text-xs text-muted-foreground", className)}>
      Showing the first {shown} of {count}: the list stops at {limit} rows.
    </p>
  );
}
