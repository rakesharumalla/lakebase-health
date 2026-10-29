import { Button, buttonVariants, LineChart, TableBody, TableCell, TableRow } from "@databricks/appkit-ui/react";
import { Activity, Copy } from "lucide-react";
import type { ReactNode } from "react";
import { Link, useParams } from "react-router";
import { ApiMessage } from "../ApiMessage";
import { SectionCounts, SeverityBadge, StatusBadge, ToneBadge } from "../components/Severity";
import { ITEM_CLASSES, riskTone, severityTone } from "../components/tone";
import { GroupTitle, QueryText } from "../components/QueryText";
import { RollbackLine } from "../components/Rollback";
import { CodeBlock, DataTable, LoadingState, MonoId, PageHeader, SectionCard, ShowAllButton, TextLink, TruncatedNote } from "../components/ui";
import { queryRefs, severityCounts, type GroupRow, type Page } from "../lib/findings";
import { fmtTime, str, truthy, type Row } from "../lib/format";
import { TIMELINE_CHECKS } from "../lib/constants";
import { useApi } from "../lib/useApi";
import { useQueryLabels } from "../lib/useQueryLabels";
import { pageRows, useShowAll } from "../lib/useShowAll";
import { cn } from "../lib/utils";
import { ActionPanel } from "./ActionCard";
import { DraftPanel } from "./DraftCard";

export type FindingData = { finding: Row; history: Row[]; actions: Row[]; related: Page<GroupRow> };

/** The finding's own query: evidence.queryid, else the object of an older per-query finding. */
function ownQueryId(f: Row): string | null {
  try {
    const ev: unknown = JSON.parse(str(f.evidence) || "{}");
    const q = typeof ev === "object" && ev !== null ? (ev as Record<string, unknown>).queryid : undefined;
    if ((typeof q === "string" || typeof q === "number") && /^-?\d{1,20}$/.test(String(q))) return String(q);
  } catch {
    /* not JSON */
  }
  const m = /^(?:ep-[a-z0-9-]+\/)?(-?\d{1,20})(?:@.*)?$/.exec(str(f.object));
  return m ? m[1] : null;
}

function evidenceEntries(evidence: unknown): [string, string][] {
  if (typeof evidence !== "string") return [];
  try {
    const parsed: unknown = JSON.parse(evidence);
    if (typeof parsed !== "object" || parsed === null) return [];
    return Object.entries(parsed).map(([k, v]) => [k, typeof v === "string" ? v : JSON.stringify(v)]);
  } catch {
    return [["evidence", evidence]];
  }
}

function timelineHref(f: Row) {
  const last = new Date(str(f.last_seen) || Date.now());
  const from = new Date(last.getTime() - 3600_000).toISOString();
  const to = new Date(last.getTime() + 15 * 60_000).toISOString();
  return `/endpoints/${str(f.endpoint_id)}/timeline?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`;
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{label}</dt>
      <dd className="mt-0.5 truncate">{children}</dd>
    </div>
  );
}

/**
 * `actionCard` is the live approval card for the finding's latest action, `draftCard` the query rewrite card (both
 * rendered by the page, not in tests).
 */
export function FindingView({ data, actionCard, draftCard }: { data: FindingData; actionCard?: ReactNode; draftCard?: ReactNode }) {
  const f = data.finding;
  const action = data.actions[0];
  // No action, or one expired (e.g. outside the fixer allowlist): the SQL is for a person to run
  const byHand = ["new", "open", "suppressed"].includes(str(f.status)) && (!action || str(action.state) === "expired");
  const history = data.history.map((h) => ({ ts: str(h.ts), measure: Number(h.measure) }));
  const evidence = evidenceEntries(f.evidence);
  const fp = str(f.fingerprint);
  const endpoint = str(f.endpoint_id);
  const own = ownQueryId(f);
  const more = useShowAll<GroupRow>(() => `/api/findings/${fp}/related`, pageRows);
  const related = more.rows("related", data.related.rows);
  const labels = useQueryLabels(str(f.project_id) || null, [
    ...(own && endpoint ? [{ endpoint_id: endpoint, queryid: own }] : []),
    ...queryRefs(related),
  ]);
  return (
    <div className="max-w-5xl space-y-6">
      <PageHeader
        crumbs={[
          { label: "Projects", to: "/" },
          ...(f.project_id ? [{ label: "Project", to: `/projects/${str(f.project_id)}` }] : []),
          { label: str(f.title) },
        ]}
        title={str(f.title)}
        meta={
          <div className="flex flex-wrap items-center gap-2">
            <SeverityBadge severity={f.severity} />
            <StatusBadge status={f.status} />
            <span>
              confidence {str(f.confidence)} · {str(f.occurrences)} occurrences
            </span>
          </div>
        }
        actions={
          TIMELINE_CHECKS.has(str(f.check_id)) ? (
            <Link className={buttonVariants({ variant: "outline" })} to={timelineHref(f)}>
              <Activity />
              Open incident timeline
            </Link>
          ) : undefined
        }
      />

      <dl className="grid grid-cols-2 gap-x-6 gap-y-3 rounded-xl border bg-card px-5 py-4 lg:grid-cols-5">
        {own && endpoint ? (
          <div className="col-span-2 min-w-0 lg:col-span-5">
            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Query</dt>
            <dd className="mt-0.5">
              <QueryText queryid={own} label={labels.get(`${endpoint}:${own}`)} />
            </dd>
          </div>
        ) : null}
        <Fact label="Check">
          <MonoId value={str(f.check_id)} max="max-w-full" />
        </Fact>
        <Fact label="Endpoint">
          <MonoId value={str(f.endpoint_id)} max="max-w-full" />
        </Fact>
        <Fact label="Object">
          <MonoId value={str(f.object)} max="max-w-full" />
        </Fact>
        <Fact label="First seen">{fmtTime(f.first_seen)}</Fact>
        <Fact label="Last seen">{fmtTime(f.last_seen)}</Fact>
      </dl>

      <SectionCard title="What this means">
        <p className="max-w-prose text-[0.9375rem] leading-relaxed">{str(f.explain)}</p>
      </SectionCard>

      {history.length > 1 && (
        <SectionCard title="Measure over time">
          <LineChart data={history} xKey="ts" yKey="measure" height={220} ariaLabel="Measure over time" />
        </SectionCard>
      )}

      <SectionCard title="Evidence" contentClassName="space-y-3">
        {evidence.length > 0 && (
          <div className="overflow-hidden rounded-md border">
            <DataTable>
              <TableBody>
                {evidence.map(([k, v]) => (
                  <TableRow key={k}>
                    <TableCell className="w-56 align-top font-mono text-[0.8125rem] text-muted-foreground">{k}</TableCell>
                    <TableCell className="whitespace-normal break-all font-mono text-[0.8125rem]">{v}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </DataTable>
          </div>
        )}
        {f.query_text !== null && f.query_text !== undefined && <CodeBlock>{str(f.query_text)}</CodeBlock>}
      </SectionCard>

      {f.candidate_sql ? (
        <SectionCard
          title="Candidate fix"
          description={
            action ? (
              <span className="flex flex-wrap items-center gap-2">
                <ToneBadge tone={riskTone(action.risk)}>Risk {str(action.risk)}</ToneBadge>
                {truthy(action.verify_on_branch) ? "verify on a branch first" : "no branch check"}
              </span>
            ) : undefined
          }
          action={
            <Button variant="outline" size="sm" onClick={() => void navigator.clipboard.writeText(str(f.candidate_sql))}>
              <Copy />
              Copy SQL
            </Button>
          }
          contentClassName="space-y-2"
        >
          {byHand && (
            <p className="text-sm">
              Apply by hand: the fixer cannot run this statement, so there is no approval card. Copy it and run it
              yourself after review.
            </p>
          )}
          <CodeBlock>{str(f.candidate_sql)}</CodeBlock>
          <RollbackLine sql={f.rollback_sql} note={f.rollback_note} />
        </SectionCard>
      ) : null}

      {actionCard}

      {draftCard}

      {action && (
        <SectionCard title="Shadow mode">
          <p className="text-sm">
            An unattended policy {truthy(action.would_execute) ? "would apply" : "would not apply"} this
            {action.would_allow_level ? ` (level ${str(action.would_allow_level)})` : ""}: {str(action.reason)}
          </p>
        </SectionCard>
      )}

      {data.related.total > 0 && (
        <SectionCard
          title="Related findings on this endpoint"
          description={
            <SectionCounts open={data.related.count ?? data.related.total} counts={severityCounts(data.related, related)} />
          }
          contentClassName="space-y-1 px-0"
        >
          <ul className="divide-y border-y text-sm">
            {related.map((r) => (
              <li
                key={r.key}
                data-severity={r.severity}
                className={cn("flex items-start gap-x-3 px-5 py-2 transition-colors", ITEM_CLASSES[severityTone(r.severity)])}
              >
                <span className="shrink-0 pt-0.5">
                  <SeverityBadge severity={r.severity} />
                </span>
                {r.kind === "query" && r.queryid ? (
                  <div className="min-w-0 flex-1">
                    <GroupTitle row={r} labels={labels} />
                  </div>
                ) : (
                  <span className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
                    <TextLink to={`/findings/${r.fingerprint}`}>{r.title}</TextLink>
                    <MonoId value={r.object} className="text-muted-foreground" max="max-w-[36ch]" />
                  </span>
                )}
              </li>
            ))}
          </ul>
          <ShowAllButton
            total={data.related.total}
            shown={related.length}
            open={more.isOpen("related")}
            loading={more.isLoading("related")}
            onToggle={() => more.toggle("related")}
            className="px-5 pb-0"
          />
          {more.errorOf("related") ? <p className="px-5 text-sm text-sev-critical-fg">{more.errorOf("related")}</p> : null}
          {data.related.truncated ? (
            <TruncatedNote shown={data.related.shown ?? related.length} count={data.related.count ?? data.related.total}
                           limit={data.related.limit ?? data.related.shown ?? 0} className="px-5" />
          ) : null}
        </SectionCard>
      )}
    </div>
  );
}

export default function FindingPage() {
  const { fp } = useParams();
  const r = useApi<FindingData>(fp ? `/api/findings/${fp}` : null);
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <LoadingState label="Loading finding" />;
  const action = r.data.actions[0];
  return (
    <FindingView
      data={r.data}
      actionCard={action ? <ActionPanel actionId={str(action.action_id)} /> : undefined}
      draftCard={<DraftPanel fingerprint={str(r.data.finding.fingerprint)} />}
    />
  );
}
