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
import { useState } from "react";
import { ApiMessage } from "../ApiMessage";
import { StatusBadge, ToneBadge } from "../components/Severity";
import { riskTone } from "../components/tone";
import { QueryText } from "../components/QueryText";
import { RollbackLine } from "../components/Rollback";
import { CodeBlock, DataTable, InlineLoading, MonoId, NUM } from "../components/ui";
import type { QueryLabel } from "../lib/findings";
import { fmtTime, str, truthy, type Row } from "../lib/format";
import { useApi } from "../lib/useApi";
import { useQueryLabels } from "../lib/useQueryLabels";

export type RequestKind = "verify" | "apply" | "rollback" | "reject";
export type ActionData = {
  action: Row;
  runs: Row[];
  canManage: boolean;
  requests: RequestKind[];
  /** Blocked for missing privileges or enrolment: the blocked request can be retried once the fixer is enrolled. */
  retryAfterEnrolment?: boolean;
  /** Why verify/apply are not offered (finding not new/open, or the fix expired); null when they are. */
  notActionable?: string | null;
};

const LABELS: Record<RequestKind, string> = {
  verify: "Verify on a branch",
  apply: "Apply",
  rollback: "Roll back",
  reject: "Reject",
};

type Rule = { rule: string; passed: boolean; detail: string };
type Timing = { queryid: string; before_ms: number | null; after_ms: number | null };
type Report = { branch?: string; rules: Rule[]; queries: Timing[] };

const ms = (v: unknown) => (typeof v === "number" ? String(Math.round(v * 100) / 100) : "—");

function parseReport(raw: unknown): Report | null {
  if (typeof raw !== "string") return null;
  try {
    const p: unknown = JSON.parse(raw);
    if (typeof p !== "object" || p === null) return null;
    const o = p as Record<string, unknown>;
    const list = (v: unknown) => (Array.isArray(v) ? v.filter((x): x is Record<string, unknown> => typeof x === "object" && x !== null) : []);
    return {
      branch: typeof o.branch === "string" ? o.branch : undefined,
      rules: list(o.rules).map((r) => ({ rule: str(r.rule), passed: r.passed === true, detail: str(r.detail) })),
      queries: list(o.queries).map((q) => ({
        queryid: str(q.queryid),
        before_ms: typeof q.before_ms === "number" ? q.before_ms : null,
        after_ms: typeof q.after_ms === "number" ? q.after_ms : null,
      })),
    };
  } catch {
    return null;
  }
}

function VerificationReport({ run, endpointId, labels }: { run: Row; endpointId: string; labels?: Map<string, QueryLabel> }) {
  const report = parseReport(run.report);
  return (
    <div className="space-y-3 rounded-lg border bg-background/60 p-4">
      <div className="flex flex-wrap items-center gap-2">
        <h3 className="text-sm font-semibold">Latest branch verification</h3>
        <StatusBadge status={str(run.outcome) || "running"} />
        <span className="text-xs text-muted-foreground">
          {fmtTime(run.started_at)}
          {report?.branch ? (
            <>
              {" "}
              · branch <MonoId value={report.branch} />
            </>
          ) : null}
        </span>
      </div>
      {run.error ? <p className="text-sm text-sev-critical-fg">{str(run.error)}</p> : null}
      {report && report.rules.length > 0 && (
        <div className="overflow-hidden rounded-md border bg-card">
          <DataTable>
            <TableHeader>
              <TableRow>
                <TableHead>Rule</TableHead>
                <TableHead className="w-20">Result</TableHead>
                <TableHead>Detail</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {report.rules.map((r) => (
                <TableRow key={r.rule}>
                  <TableCell className="font-mono text-[0.8125rem]">{r.rule}</TableCell>
                  <TableCell>
                    <StatusBadge status={r.passed ? "pass" : "fail"} />
                  </TableCell>
                  <TableCell className="whitespace-normal">{r.detail}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </DataTable>
        </div>
      )}
      {report && report.queries.length > 0 && (
        <div className="overflow-hidden rounded-md border bg-card">
          <DataTable>
            <TableHeader>
              <TableRow>
                <TableHead>Query</TableHead>
                <TableHead className="text-right">Before (ms)</TableHead>
                <TableHead className="text-right">After (ms)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {report.queries.map((q) => (
                <TableRow key={q.queryid} className="align-top">
                  <TableCell className="max-w-[36rem]">
                    <QueryText queryid={q.queryid} label={labels?.get(`${endpointId}:${q.queryid}`)} />
                  </TableCell>
                  <TableCell className={NUM}>{ms(q.before_ms)}</TableCell>
                  <TableCell className={NUM}>{ms(q.after_ms)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </DataTable>
        </div>
      )}
    </div>
  );
}

/** One clear primary per card: apply, else verify. Roll back is destructive; the rest are secondary. */
function variantFor(r: RequestKind, primary: RequestKind | undefined) {
  if (r === "rollback") return "destructive" as const;
  if (r === primary) return "default" as const;
  return "outline" as const;
}

export function ActionCard({
  action,
  runs,
  canManage,
  requests,
  retryAfterEnrolment = false,
  notActionable = null,
  onRequest,
  busy = false,
  labels,
}: ActionData & { onRequest: (r: RequestKind) => void; busy?: boolean; labels?: Map<string, QueryLabel> }) {
  const verification = runs.find((r) => r.kind === "verify");
  const latest = runs[0];
  const primary = requests.includes("apply") ? "apply" : requests.includes("verify") ? "verify" : undefined;
  // Secondary actions first, primary last (rightmost), as in most approval dialogs.
  const ordered = [...requests].sort((a, b) => Number(a === primary) - Number(b === primary));
  return (
    <Card className="gap-4 border-l-4 border-l-lava py-5 shadow-sm">
      <CardHeader className="px-5">
        <CardTitle className="flex flex-wrap items-center gap-2">
          <h2 className="text-base font-semibold">Fix</h2>
          <StatusBadge status={action.state} />
          <ToneBadge tone={riskTone(action.risk)}>risk {str(action.risk)}</ToneBadge>
          <span className="text-sm font-normal text-muted-foreground">
            {truthy(action.verify_on_branch) ? "verified on a branch before apply" : "no branch check"}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 px-5">
        <CodeBlock>{str(action.sql)}</CodeBlock>
        <RollbackLine sql={action.rollback_sql} note={action.rollback_note} />
        {latest && latest !== verification && (
          <p className="text-sm text-muted-foreground">
            Last run: {str(latest.kind)} {str(latest.outcome) || "running"} · {fmtTime(latest.started_at)}
          </p>
        )}
        {verification && <VerificationReport run={verification} endpointId={str(action.endpoint_id)} labels={labels} />}
      </CardContent>
      {canManage ? (
        (requests.length > 0 || notActionable) && (
          <CardFooter className="flex flex-wrap items-center justify-end gap-2 border-t px-5 pt-4 [.border-t]:pt-4">
            {notActionable && <p className="mr-auto text-sm text-muted-foreground">{notActionable}</p>}
            {ordered.map((r) => (
              <Button key={r} variant={variantFor(r, primary)} disabled={busy} onClick={() => onRequest(r)}>
                {retryAfterEnrolment ? "Retry after enrolment" : LABELS[r]}
              </Button>
            ))}
          </CardFooter>
        )
      ) : (
        <CardFooter className="border-t px-5 pt-4 [.border-t]:pt-4">
          <p className="text-sm text-muted-foreground">Only the project owner or a manager can approve.</p>
        </CardFooter>
      )}
    </Card>
  );
}

/** Loads an action and records approval requests for it; reloads after each request. */
export function ActionPanel({ actionId }: { actionId: string }) {
  const [version, setVersion] = useState(0);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const r = useApi<ActionData>(`/api/actions/${actionId}?v=${version}`);
  const endpointId = str(r.data?.action.endpoint_id);
  const verification = r.data?.runs.find((x) => x.kind === "verify");
  const reportQueries = verification ? (parseReport(verification.report)?.queries ?? []) : [];
  const labels = useQueryLabels(
    str(r.data?.action.project_id) || null,
    endpointId ? reportQueries.map((q) => ({ endpoint_id: endpointId, queryid: q.queryid })) : [],
  );

  const request = (kind: RequestKind) => {
    setBusy(true);
    setNotice(null);
    fetch(`/api/actions/${actionId}/requests`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request: kind }),
    })
      .then(async (res) => {
        const body = (await res.json().catch(() => ({}))) as { error?: string; message?: string };
        setNotice(res.ok ? (body.message ?? "Request recorded; the fixer is starting.") : (body.error ?? res.statusText));
      })
      .catch((e: unknown) => setNotice(String(e)))
      .finally(() => {
        setBusy(false);
        setVersion((v) => v + 1);
      });
  };

  return (
    <div className="space-y-2">
      {r.error ? (
        <ApiMessage error={r.error} />
      ) : r.data ? (
        <ActionCard {...r.data} onRequest={request} busy={busy} labels={labels} />
      ) : (
        <InlineLoading label="Loading fix" />
      )}
      {notice && (
        <p role="status" className="rounded-md border bg-card px-3 py-2 text-sm">
          {notice}
        </p>
      )}
    </div>
  );
}
