import { TableBody, TableCell, TableHead, TableHeader, TableRow } from "@databricks/appkit-ui/react";
import { CircleCheck } from "lucide-react";
import { useParams } from "react-router";
import { ApiMessage } from "../ApiMessage";
import { SectionCounts, SeverityBadge, StatusBadge } from "../components/Severity";
import { ROW_CLASSES, severityTone } from "../components/tone";
import {
  DataTable,
  EmptyState,
  InlineCode,
  KpiRow,
  KpiTile,
  LoadingState,
  MonoId,
  NUM,
  PageHeader,
  SectionCard,
  ShowAllButton,
  TruncatedNote,
  SubNav,
  TextLink,
} from "../components/ui";
import { GroupTitle } from "../components/QueryText";
import { queryRefs, severityCounts, type GroupRow, type Page, type QueryLabel } from "../lib/findings";
import { fmtTime, str, truthy, type Row } from "../lib/format";
import { useQueryLabels } from "../lib/useQueryLabels";
import { pageRows, useShowAll } from "../lib/useShowAll";
import { projectNav } from "../lib/nav";
import { useApi } from "../lib/useApi";
import { cn } from "../lib/utils";

type Section = Page<GroupRow> & { category: string };
export type ProjectData = {
  project: { id: string; name: string };
  counts: { critical: number; warning: number; info: number; resolved: number };
  sections: Section[];
  resolved: Page<GroupRow>;
  computes: Row[];
  /** The findings list hit its SQL LIMIT; counts and section totals are SQL counts either way. */
  truncated?: boolean;
  limit?: number;
};

export function FindingRows({ rows, labels }: { rows: GroupRow[]; labels: Map<string, QueryLabel> }) {
  return (
    <DataTable>
      <TableHeader>
        <TableRow>
          <TableHead className="w-28">Severity</TableHead>
          <TableHead>Finding</TableHead>
          <TableHead>Object</TableHead>
          <TableHead className="hidden xl:table-cell">First seen</TableHead>
          <TableHead>Last seen</TableHead>
          <TableHead className="text-right">Occurrences</TableHead>
          <TableHead>Confidence</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((f) => (
          <TableRow
            key={f.key}
            data-severity={f.status === "resolved" ? "resolved" : f.severity}
            className={cn(
              f.kind === "query" && "align-top",
              ROW_CLASSES[f.status === "resolved" ? "ok" : severityTone(f.severity)],
            )}
          >
            <TableCell>
              <SeverityBadge severity={f.severity} />
            </TableCell>
            <TableCell className="min-w-64 max-w-[32rem] whitespace-normal">
              {f.kind === "query" && f.queryid ? (
                <GroupTitle row={f} labels={labels} />
              ) : (
                <>
                  <TextLink to={`/findings/${f.fingerprint}`}>{f.title}</TextLink>
                  {f.status === "new" && (
                    <span className="ml-2 align-middle">
                      <StatusBadge status="new" />
                    </span>
                  )}
                </>
              )}
            </TableCell>
            <TableCell>
              <MonoId value={f.kind === "query" ? str(f.endpoint_id) : f.object} max="max-w-[28ch]" />
            </TableCell>
            <TableCell className="hidden text-muted-foreground xl:table-cell">{fmtTime(f.first_seen)}</TableCell>
            <TableCell className="text-muted-foreground">{fmtTime(f.last_seen)}</TableCell>
            <TableCell className={NUM}>{str(f.occurrences)}</TableCell>
            <TableCell className="text-muted-foreground">{str(f.confidence ?? "")}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </DataTable>
  );
}

export function ProjectView({ data }: { data: ProjectData }) {
  const id = data.project.id;
  const more = useShowAll<GroupRow>(
    (section) => `/api/projects/${id}/findings?section=${encodeURIComponent(section)}`,
    pageRows,
  );
  const shown = data.sections.map((s) => ({ ...s, rows: more.rows(s.category, s.rows) }));
  const resolved = more.rows("resolved", data.resolved.rows);
  const labels = useQueryLabels(id, [...shown.flatMap((s) => queryRefs(s.rows)), ...queryRefs(resolved)]);
  const active = data.computes.filter((c) => str(c.current_state) === "ACTIVE").length;
  const cutNote = (p: Page<GroupRow>) =>
    p.truncated ? <TruncatedNote shown={p.shown ?? p.rows.length} count={p.count ?? p.total} limit={data.limit ?? p.shown ?? 0} className="border-t" /> : null;
  const footer = (key: string, total: number, count: number) => (
    <>
      <ShowAllButton
        total={total}
        shown={count}
        open={more.isOpen(key)}
        loading={more.isLoading(key)}
        onToggle={() => more.toggle(key)}
        className="border-t"
      />
      {more.errorOf(key) ? <p className="px-3 pb-2 text-sm text-sev-critical-fg">{more.errorOf(key)}</p> : null}
    </>
  );
  return (
    <div className="space-y-6">
      <PageHeader
        crumbs={[{ label: "Projects", to: "/" }, { label: data.project.name }]}
        title={data.project.name}
        meta={<MonoId value={id} max="max-w-none" />}
      >
        <SubNav items={projectNav(id)} />
      </PageHeader>

      <KpiRow>
        <KpiTile label="Critical open" value={data.counts.critical} accent="bg-sev-critical-edge" tone={data.counts.critical ? "critical" : undefined} />
        <KpiTile label="Warning open" value={data.counts.warning} accent="bg-sev-warning-edge" tone={data.counts.warning ? "warning" : undefined} />
        <KpiTile label="Info open" value={data.counts.info} accent="bg-sev-info-edge" tone={data.counts.info ? "info" : undefined} />
        <KpiTile label="Resolved, last 7 days" value={data.counts.resolved} accent="bg-sev-ok-edge" />
      </KpiRow>

      {shown.map((s) => (
        <SectionCard
          key={s.category}
          title={<span className="capitalize">{s.category}</span>}
          description={<SectionCounts open={s.count ?? s.total} counts={severityCounts(s, s.rows)} />}
          contentClassName="px-0"
        >
          <FindingRows rows={s.rows} labels={labels} />
          {footer(s.category, s.total, s.rows.length)}
          {cutNote(s)}
        </SectionCard>
      ))}
      {data.sections.length === 0 && (
        <EmptyState title="All clear" icon={<CircleCheck />}>
          No open findings.
        </EmptyState>
      )}
      {(data.resolved.count ?? data.resolved.total) > 0 && (
        <details className="group rounded-xl border bg-card">
          <summary className="cursor-pointer select-none px-5 py-4 text-sm font-medium text-heading">
            Recently resolved (7 days): {data.resolved.count ?? data.resolved.total}
          </summary>
          <div className="border-t">
            <FindingRows rows={resolved} labels={labels} />
            {footer("resolved", data.resolved.total, resolved.length)}
            {cutNote(data.resolved)}
          </div>
        </details>
      )}
      <SectionCard
        title="Computes"
        description={`${active} of ${data.computes.length} active`}
        contentClassName="px-0"
      >
        <DataTable>
          <TableHeader>
            <TableRow>
              <TableHead>Endpoint</TableHead>
              <TableHead>Branch</TableHead>
              <TableHead>Type</TableHead>
              <TableHead>State</TableHead>
              <TableHead className="text-right">CU</TableHead>
              <TableHead className="text-right">Suspend after</TableHead>
              <TableHead className="text-right">HA</TableHead>
              <TableHead className="text-right">Restore window</TableHead>
              <TableHead>Protected</TableHead>
              <TableHead>Postgres checks</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.computes.map((c) => (
              <TableRow key={str(c.endpoint_id)} className="align-top">
                <TableCell>
                  <TextLink to={`/endpoints/${c.endpoint_id}/timeline`} className="font-mono text-[0.8125rem]" title={str(c.endpoint_id)}>
                    {str(c.endpoint_id)}
                  </TextLink>
                </TableCell>
                <TableCell>{str(c.branch_name)}</TableCell>
                <TableCell>{str(c.endpoint_type).replace("ENDPOINT_TYPE_", "").toLowerCase()}</TableCell>
                <TableCell>
                  <StatusBadge
                    status={str(c.current_state) === "ACTIVE" ? "on" : "idle"}
                    label={str(c.current_state).toLowerCase()}
                  />
                </TableCell>
                <TableCell className={NUM}>{`${c.min_cu}–${c.max_cu}`}</TableCell>
                <TableCell className={NUM}>
                  {c.suspend_timeout_s ? `${Math.round(Number(c.suspend_timeout_s) / 60)} min` : "never"}
                </TableCell>
                <TableCell className={NUM}>{str(c.ha_min ?? "")}</TableCell>
                <TableCell className={NUM}>
                  {c.restore_window_s ? `${Math.round(Number(c.restore_window_s) / 86400)} d` : "—"}
                </TableCell>
                <TableCell>{truthy(c.is_protected) ? "yes" : "no"}</TableCell>
                <TableCell className="whitespace-normal">
                  {truthy(c.enrolled) ? (
                    <StatusBadge status="on" />
                  ) : (
                    <div className="max-w-md space-y-1">
                      <StatusBadge status="off" />
                      <p className="text-xs text-muted-foreground">
                        off — grant the engine role on this branch:{" "}
                        <InlineCode>GRANT pg_monitor TO &quot;&lt;engine SP application ID&gt;&quot;;</InlineCode> (see the README
                        section &quot;Enroll a project for Postgres checks&quot;)
                      </p>
                    </div>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </DataTable>
      </SectionCard>
    </div>
  );
}

export default function ProjectPage() {
  const { id } = useParams();
  const r = useApi<ProjectData>(id ? `/api/projects/${id}` : null);
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <LoadingState label="Loading project" rows={8} />;
  return <ProjectView data={r.data} />;
}
