import {
  Button,
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@databricks/appkit-ui/react";
import { Copy } from "lucide-react";
import { useState } from "react";
import { ApiMessage } from "../ApiMessage";
import { StatusBadge } from "../components/Severity";
import { CodeBlock, DataTable, InlineLoading, NUM } from "../components/ui";
import { wordDiff } from "../lib/diff";
import type { QueryLabel } from "../lib/findings";
import { fmtTime, str, type Row } from "../lib/format";
import { useApi } from "../lib/useApi";

export type DraftData = {
  rewritable: boolean;
  setUp: { ready: boolean; reason: string | null };
  canManage: boolean;
  canRequest: boolean;
  pending: boolean;
  refused: { reason: string; at: string } | null;
  nextAttempt: number;
  original: QueryLabel | null;
  attempts: Row[];
};

const RUNNING = new Set(["drafting", "verifying"]);
const ADVISORY = "Change it in your application code; this tool does not deploy rewrites.";
const ADOPTION: Record<string, string> = {
  adopted: "Adopted: the workload now runs the rewritten query.",
  not_adopted: "Not adopted: the rewritten query was not seen in the 7 days after verification.",
};

type SetRow = { set: number; rows_original: number | null; rows_rewrite: number | null; same_checksum: boolean;
  original_ms: number | null; rewrite_ms: number | null };
type Rule = { rule: string; passed: boolean; warning: boolean; detail: string };
type Report = { sets: SetRow[]; rules: Rule[]; plan: { removed: string[]; added: string[]; expected_nodes: string[]; found: string[] } | null };

const num = (v: unknown) => (typeof v === "number" ? v : null);
const ms = (v: number | null) => (v === null ? "—" : String(Math.round(v * 100) / 100));
const names = (v: unknown) => (Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : []);

function parseDraftReport(raw: unknown): Report | null {
  if (typeof raw !== "string") return null;
  try {
    const o = JSON.parse(raw) as Record<string, unknown>;
    const list = (v: unknown) =>
      Array.isArray(v) ? v.filter((x): x is Record<string, unknown> => typeof x === "object" && x !== null) : [];
    const p = typeof o.plan === "object" && o.plan !== null ? (o.plan as Record<string, unknown>) : null;
    return {
      sets: list(o.sets).map((s) => ({ set: Number(s.set), rows_original: num(s.rows_original), rows_rewrite: num(s.rows_rewrite),
        same_checksum: s.same_checksum === true, original_ms: num(s.original_ms), rewrite_ms: num(s.rewrite_ms) })),
      rules: list(o.rules).map((r) => ({ rule: str(r.rule), passed: r.passed === true, warning: r.warning === true, detail: str(r.detail) })),
      plan: p ? { removed: names(p.removed), added: names(p.added), expected_nodes: names(p.expected_nodes), found: names(p.found) } : null,
    };
  } catch {
    return null;
  }
}

function Diff({ before, after }: { before: string; after: string }) {
  return (
    <p className="rounded-md border bg-code px-3 py-2 font-mono text-[0.8125rem] leading-relaxed" aria-label="Diff with the original">
      {wordDiff(before, after)
        .map((p, n) => ({ ...p, id: `${n}:${p.kind}` }))
        .map((p) =>
          p.kind === "same" ? <span key={p.id}>{p.text} </span>
            : p.kind === "removed" ? <del key={p.id} className="text-sev-critical-fg">{p.text} </del>
              : <ins key={p.id} className="text-sev-ok-fg no-underline">{p.text} </ins>)}
    </p>
  );
}

function AttemptDetail({ attempt, original }: { attempt: Row; original: QueryLabel | null }) {
  const report = parseDraftReport(attempt.report);
  const sql = attempt.sql === null || attempt.sql === undefined ? null : str(attempt.sql);
  const state = str(attempt.state);
  return (
    <div className="space-y-3">
      {attempt.reason ? <p className="text-sm text-sev-critical-fg">{str(attempt.reason)}</p> : null}
      {attempt.error ? <p className="text-sm text-sev-critical-fg">{str(attempt.error)}</p> : null}
      {attempt.rationale ? <p className="text-sm">{str(attempt.rationale)}</p> : null}
      {sql !== null && (
        <>
          <div className="flex items-center justify-between gap-2">
            <h3 className="text-sm font-semibold">Rewritten query</h3>
            <Button variant="outline" size="sm" onClick={() => void navigator.clipboard?.writeText(sql)}>
              <Copy />
              Copy SQL
            </Button>
          </div>
          <CodeBlock>{sql}</CodeBlock>
          {original?.text ? <Diff before={original.text} after={sql} /> : null}
          <p className="text-sm font-medium">{ADVISORY}</p>
        </>
      )}
      {report && report.sets.length > 0 && (
        <div className="overflow-hidden rounded-md border bg-card">
          <DataTable>
            <TableHeader>
              <TableRow>
                <TableHead>Parameter set</TableHead>
                <TableHead className="text-right">Rows (original)</TableHead>
                <TableHead className="text-right">Rows (rewrite)</TableHead>
                <TableHead>Same results</TableHead>
                <TableHead className="text-right">Original (ms)</TableHead>
                <TableHead className="text-right">Rewrite (ms)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {report.sets.map((s) => (
                <TableRow key={s.set}>
                  <TableCell>set {s.set}</TableCell>
                  <TableCell className={NUM}>{s.rows_original ?? "—"}</TableCell>
                  <TableCell className={NUM}>{s.rows_rewrite ?? "—"}</TableCell>
                  <TableCell><StatusBadge status={s.same_checksum ? "pass" : "fail"} label={s.same_checksum ? "same" : "different"} /></TableCell>
                  <TableCell className={NUM}>{ms(s.original_ms)}</TableCell>
                  <TableCell className={NUM}>{ms(s.rewrite_ms)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </DataTable>
        </div>
      )}
      {report && report.rules.length > 0 && (
        <ul className="space-y-1 text-sm">
          {report.rules.map((r) => (
            <li key={r.rule} className="flex flex-wrap items-center gap-2">
              <StatusBadge status={r.passed ? (r.warning ? "blocked" : "pass") : "fail"} label={r.passed ? (r.warning ? "warning" : "pass") : "fail"} />
              <span className="font-mono text-[0.8125rem]">{r.rule}</span>
              <span>{r.detail}</span>
            </li>
          ))}
        </ul>
      )}
      {report?.plan && (
        <p className="text-sm">
          Plan: removed {report.plan.removed.join(", ") || "nothing"}; added {report.plan.added.join(", ") || "nothing"}.
          Expected {report.plan.expected_nodes.join(", ") || "no named plan node"}
          {report.plan.found.length ? `; found ${report.plan.found.join(", ")}.` : "; not found in the new plan."}
        </p>
      )}
      {ADOPTION[state] ? <p className="text-sm font-medium">{ADOPTION[state]}</p> : null}
    </div>
  );
}

export function DraftCard({ data, onRequest, busy = false }: { data: DraftData; onRequest: () => void; busy?: boolean }) {
  const [selected, setSelected] = useState<string | null>(null);
  if (!data.rewritable) return null;
  const latest = data.attempts[0];
  const shown = data.attempts.find((a) => str(a.draft_id) === selected) ?? latest;
  const running = data.pending || (latest !== undefined && RUNNING.has(str(latest.state)));
  return (
    <Card className="gap-4 border-l-4 border-l-coral py-5 shadow-sm">
      <CardHeader className="px-5">
        <CardTitle className="flex flex-wrap items-center gap-2">
          <h2 className="text-base font-semibold">Query rewrite (advisory)</h2>
          {latest ? <StatusBadge status={latest.state} /> : null}
          {data.pending ? <StatusBadge status="running" label="requested" /> : null}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4 px-5">
        {!data.setUp.ready ? (
          <p className="text-sm">
            <span className="font-medium">LLM drafting not set up.</span> {data.setUp.reason}
          </p>
        ) : !latest ? (
          <p className="text-sm text-muted-foreground">
            A model drafts an equivalent query from the query shape, plan and table definitions (no values), and the
            fixer verifies it on a branch: same results, faster.
          </p>
        ) : null}
        {data.refused ? <p className="text-sm text-muted-foreground">The last request was not run: {data.refused.reason}.</p> : null}
        {shown ? <AttemptDetail attempt={shown} original={data.original} /> : null}
        {data.attempts.length > 1 && (
          <div className="space-y-1">
            <h3 className="text-sm font-semibold">Attempts</h3>
            <ul className="divide-y text-sm">
              {data.attempts.map((a) => (
                <li key={str(a.draft_id)} className="flex flex-wrap items-center gap-2 py-1.5">
                  <Button variant="ghost" size="sm" aria-pressed={a === shown} onClick={() => setSelected(str(a.draft_id))}>
                    Attempt {str(a.attempt)}
                  </Button>
                  <StatusBadge status={a.state} />
                  <span className="text-muted-foreground">{str(a.requested_by)} · {fmtTime(a.requested_at)}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </CardContent>
      {data.setUp.ready && (
        <CardFooter className="flex flex-wrap justify-end gap-2 border-t px-5 pt-4 [.border-t]:pt-4">
          {data.canManage ? (
            <Button disabled={busy || running || !data.canRequest} onClick={onRequest}>
              {running ? "Drafting…" : latest ? "Retry" : "Draft a rewrite"}
            </Button>
          ) : (
            <p className="text-sm text-muted-foreground">Only the project owner or a manager can draft a rewrite.</p>
          )}
        </CardFooter>
      )}
    </Card>
  );
}

/** Loads the rewrite card of a finding and records draft requests; reloads after each one. */
export function DraftPanel({ fingerprint }: { fingerprint: string }) {
  const [version, setVersion] = useState(0);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const r = useApi<DraftData>(`/api/findings/${fingerprint}/drafts?v=${version}`);
  const request = () => {
    setBusy(true);
    setNotice(null);
    fetch(`/api/findings/${fingerprint}/drafts`, { method: "POST" })
      .then(async (res) => {
        const body = (await res.json().catch(() => ({}))) as { error?: string; message?: string };
        setNotice(res.ok ? (body.message ?? "Request recorded; the fixer is drafting.") : (body.error ?? res.statusText));
      })
      .catch((e: unknown) => setNotice(String(e)))
      .finally(() => {
        setBusy(false);
        setVersion((v) => v + 1);
      });
  };
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <InlineLoading label="Loading rewrite" />;
  if (!r.data.rewritable) return null;
  return (
    <div className="space-y-2">
      <DraftCard data={r.data} onRequest={request} busy={busy} />
      {notice && (
        <p role="status" className="rounded-md border bg-card px-3 py-2 text-sm">
          {notice}
        </p>
      )}
    </div>
  );
}
