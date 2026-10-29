import { Button, Popover, PopoverContent, PopoverTrigger } from "@databricks/appkit-ui/react";
import { Link } from "react-router";
import type { GroupRow, Issue, QueryLabel } from "../lib/findings";
import { cn } from "../lib/utils";
import { severityTone, TONE_CLASSES } from "./tone";
import { CodeBlock } from "./ui";

const NOTES: Partial<Record<QueryLabel["kind"], string>> = {
  skeleton: "Literals are hidden: you are not in databricks_pii_access and do not own this project.",
};

/**
 * A query shown by its text: one truncated DM Mono line (click or hover for the full text) over the queryid in
 * small secondary mono. Without text (not loaded, none captured, or redacted) it reads "query <id>".
 */
export function QueryText({ queryid, label, className }: { queryid: string; label?: QueryLabel; className?: string }) {
  const text = label?.text ?? null;
  if (!text) {
    return <span className={cn("font-mono text-[0.8125rem]", className)}>{label?.label ?? `query ${queryid}`}</span>;
  }
  const note = [NOTES[label?.kind ?? "none"], label?.truncated ? "The captured text was truncated." : undefined].filter(Boolean);
  return (
    <div className={cn("min-w-0", className)}>
      <Popover>
        <PopoverTrigger asChild>
          <button
            type="button"
            title={text}
            aria-label={`Query ${queryid}: ${text}`}
            className="block w-full max-w-full cursor-pointer truncate rounded-sm text-left font-mono text-[0.8125rem] text-foreground hover:text-link focus-visible:outline-2 focus-visible:outline-ring"
          >
            {label?.label}
          </button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-[min(44rem,90vw)] space-y-2">
          <div className="flex items-center justify-between gap-2">
            <span className="font-mono text-xs text-muted-foreground">queryid {queryid}</span>
            <Button variant="ghost" size="sm" onClick={() => void navigator.clipboard?.writeText(text)}>
              Copy
            </Button>
          </div>
          <CodeBlock className="max-h-80 overflow-y-auto">{text}</CodeBlock>
          {note.length > 0 && <p className="text-xs text-muted-foreground">{note.join(" ")}</p>}
        </PopoverContent>
      </Popover>
      <div className="truncate font-mono text-[0.6875rem] leading-4 text-muted-foreground">{queryid}</div>
    </div>
  );
}

/** One severity-toned badge per check on a grouped query row, each linking to that check's finding. */
export function IssueBadges({ issues, className }: { issues: Issue[]; className?: string }) {
  return (
    <span className={cn("flex flex-wrap gap-1", className)}>
      {issues.map((i) => (
        <Link
          key={i.fingerprint}
          to={`/findings/${i.fingerprint}`}
          title={`${i.title} (${i.severity})`}
          className={cn(
            "inline-flex items-center rounded-md border px-1.5 py-0.5 text-xs font-medium whitespace-nowrap transition-colors hover:underline focus-visible:outline-2 focus-visible:outline-ring",
            TONE_CLASSES[severityTone(i.severity)],
          )}
        >
          {i.label}
        </Link>
      ))}
    </span>
  );
}

/** Main cell of a grouped row: the query (text + badges) or the finding title as before. */
export function GroupTitle({ row, labels }: { row: GroupRow; labels: Map<string, QueryLabel> }) {
  if (!row.queryid) return null;
  return (
    <div className="min-w-0 space-y-1">
      <QueryText queryid={row.queryid} label={labels.get(`${row.endpoint_id}:${row.queryid}`)} />
      <IssueBadges issues={row.issues} />
    </div>
  );
}
