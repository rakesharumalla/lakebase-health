import { TableBody, TableCell, TableHead, TableHeader, TableRow } from "@databricks/appkit-ui/react";
import { Wrench } from "lucide-react";
import { useParams } from "react-router";
import { ApiMessage } from "../ApiMessage";
import { StatusBadge, ToneBadge } from "../components/Severity";
import { riskTone } from "../components/tone";
import { DataTable, EmptyState, LoadingState, MonoId, PageHeader, SectionCard, ShowAllButton, SubNav, TextLink, TruncatedNote } from "../components/ui";
import { fmtTime, str, type Row } from "../lib/format";
import { useApi } from "../lib/useApi";
import { projectNav } from "../lib/nav";
import { useShowAll } from "../lib/useShowAll";

/** total is the SQL count; truncated when one state's full list stopped at the server's limit. */
type StateGroup = { state: string; total: number; rows: Row[]; truncated?: boolean };
export type ActionsData = { project: { id: string; name: string }; groups: StateGroup[]; canManage: boolean; limit?: number };

const groupRows = (body: unknown): Row[] => {
  const groups = typeof body === "object" && body !== null ? (body as { groups?: StateGroup[] }).groups : undefined;
  return Array.isArray(groups) && groups[0] ? groups[0].rows : [];
};

function ActionRows({ rows }: { rows: Row[] }) {
  return (
    <DataTable>
      <TableHeader>
        <TableRow>
          <TableHead>Finding</TableHead>
          <TableHead>Kind</TableHead>
          <TableHead>Object</TableHead>
          <TableHead>Risk</TableHead>
          <TableHead>Updated</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((a) => (
          <TableRow key={str(a.action_id)}>
            <TableCell className="min-w-64 max-w-[32rem] whitespace-normal">
              <TextLink to={`/findings/${str(a.fingerprint)}`}>{str(a.title)}</TextLink>
            </TableCell>
            <TableCell>
              <MonoId value={str(a.kind)} />
            </TableCell>
            <TableCell>
              <MonoId value={str(a.object)} max="max-w-[28ch]" />
            </TableCell>
            <TableCell>
              <ToneBadge tone={riskTone(a.risk)}>{str(a.risk)}</ToneBadge>
            </TableCell>
            <TableCell className="text-muted-foreground">{fmtTime(a.updated_at)}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </DataTable>
  );
}

export function ActionsView({ data }: { data: ActionsData }) {
  const more = useShowAll<Row>((state) => `/api/projects/${data.project.id}/actions?state=${encodeURIComponent(state)}`, groupRows);
  return (
    <div className="space-y-6">
      <PageHeader
        crumbs={[
          { label: "Projects", to: "/" },
          { label: data.project.name, to: `/projects/${data.project.id}` },
          { label: "Fixes and approvals" },
        ]}
        title={
          <>
            {data.project.name} <span className="font-normal text-muted-foreground">· fixes and approvals</span>
          </>
        }
        meta={
          data.canManage
            ? "You can approve fixes for this project from each finding's fix card."
            : "Only the project owner or a manager can approve."
        }
      >
        <SubNav items={projectNav(data.project.id)} />
      </PageHeader>
      {data.groups.length === 0 ? (
        <EmptyState title="No proposed fixes" icon={<Wrench />}>
          Fixes appear here when the health engine proposes one for a finding.
        </EmptyState>
      ) : (
        data.groups.map((g) => {
          const rows = more.rows(g.state, g.rows);
          return (
            <SectionCard
              key={g.state}
              title={
                <span className="flex items-center gap-2">
                  <StatusBadge status={g.state} label={g.state.replace(/_/g, " ")} />
                  <span className="font-normal text-muted-foreground">{g.total}</span>
                </span>
              }
              contentClassName="px-0"
            >
              <ActionRows rows={rows} />
              <ShowAllButton
                total={g.total}
                shown={rows.length}
                open={more.isOpen(g.state)}
                loading={more.isLoading(g.state)}
                onToggle={() => more.toggle(g.state)}
                className="border-t"
              />
              {more.errorOf(g.state) ? <p className="px-3 pb-2 text-sm text-sev-critical-fg">{more.errorOf(g.state)}</p> : null}
              {more.isOpen(g.state) && rows.length < g.total ? (
                <TruncatedNote shown={rows.length} count={g.total} limit={data.limit ?? rows.length} className="border-t" />
              ) : null}
            </SectionCard>
          );
        })
      )}
    </div>
  );
}

export default function ActionsPage() {
  const { id } = useParams();
  const r = useApi<ActionsData>(id ? `/api/projects/${id}/actions` : null);
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <LoadingState label="Loading fixes" />;
  return <ActionsView data={r.data} />;
}
