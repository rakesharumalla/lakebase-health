import { InlineCode } from "./ui";

/** Mirror of server/lib/approvals.ts hasRollback: empty or comment-only text is never a runnable rollback. */
function hasRollback(sql: unknown): sql is string {
  const s = typeof sql === "string" ? sql.trim() : "";
  return s !== "" && !s.startsWith("--") && !s.startsWith("/*");
}

/** Why there is nothing to roll back: the engine's rollback_note, else the text of a legacy comment-only rollback. */
function noteText(sql: unknown, note: unknown): string | null {
  const raw = typeof note === "string" && note.trim() ? note : typeof sql === "string" ? sql : "";
  const text = raw.trim().replace(/^--\s*/, "").replace(/^\/\*\s*|\s*\*\/$/g, "").replace(/;?\s*nothing to roll back\.?$/i, "");
  const out = text.replace(/^not needed:\s*/i, "").trim();
  return out || null;
}

/** "Rollback: <SQL>" when there is a statement to run; "Rollback: not needed — <why>" as plain text otherwise. */
export function RollbackLine({ sql, note }: { sql: unknown; note: unknown }) {
  if (hasRollback(sql)) {
    return (
      <p className="text-sm text-muted-foreground">
        Rollback: <InlineCode>{sql}</InlineCode>
      </p>
    );
  }
  const why = noteText(sql, note);
  return why ? <p className="text-sm text-muted-foreground">{`Rollback: not needed — ${why}`}</p> : null;
}
