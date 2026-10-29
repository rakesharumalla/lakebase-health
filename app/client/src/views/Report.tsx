import {
  Button,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@databricks/appkit-ui/react";
import { Download } from "lucide-react";
import { Navigate, useNavigate, useParams } from "react-router";
import { ApiMessage } from "../ApiMessage";
import { LoadingState, PageHeader, SectionCard, TextLink } from "../components/ui";
import { queryRefs, type GroupRow } from "../lib/findings";
import { useApi } from "../lib/useApi";
import { useQueryLabels } from "../lib/useQueryLabels";
import { ReportHeader } from "../report/Document";
import { QueriesTiles, StatementsCards, TABS, TAB_BODY, WaitsCards, isTab, type TabId } from "../report/tabs";
import type { Report } from "../report/types";
import "../report/report.css";
import { ActionsView, type ActionsData } from "./Actions";
import { FindingRows } from "./Project";

function QueriesBody({ report }: { report: Report }) {
  const rows = report.queryGroups as GroupRow[];
  const labels = useQueryLabels(report.project.id, queryRefs(rows));
  return (
    <>
      <QueriesTiles report={report} />
      <SectionCard
        title="Queries with issues"
        description="One row per query with a badge per issue; the text shows only to those allowed to read it, others see the query with its values removed."
        contentClassName="px-0"
      >
        {rows.length ? <FindingRows rows={rows} labels={labels} /> : <p className="rp-empty px-5">No query findings.</p>}
      </SectionCard>
      <StatementsCards report={report} mode="app" />
    </>
  );
}

function WaitsBody({ report }: { report: Report }) {
  return (
    <>
      <WaitsCards report={report} mode="app" />
      <SectionCard title="Incident timeline" description="Waits, lock blockers, commits and events on one time axis, up to 7 days, per compute.">
        <ul className="space-y-1">
          {report.endpoints.map((e) => (
            <li key={e.id}>
              <TextLink to={`/endpoints/${e.id}/timeline`}>Open the timeline of {e.id}</TextLink> ({e.type})
            </li>
          ))}
        </ul>
      </SectionCard>
    </>
  );
}

function FixesBody({ projectId }: { projectId: string }) {
  const r = useApi<ActionsData>(`/api/projects/${projectId}/actions`);
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <LoadingState label="Loading fixes" />;
  return <ActionsView data={r.data} embedded />;
}

export function ReportView({ report, tab }: { report: Report; tab: TabId }) {
  const navigate = useNavigate();
  const base = `/projects/${report.project.id}/branches`;
  const go = (branch: string, t: string) => void navigate(`${base}/${branch}/${t}`);
  const actions = (
    <div className="mt-2 flex flex-col items-center gap-2">
      <Button asChild size="sm">
        <a href={`/api/report/${report.project.id}/${report.branch.id}.html`} download>
          <Download /> Download report
        </a>
      </Button>
    </div>
  );
  return (
    <div className="space-y-4">
      <PageHeader crumbs={[{ label: "Projects", to: "/" }, { label: report.project.name }]} title={report.project.name}>
        <div className="flex items-center gap-2 text-sm">
          <span className="text-muted-foreground">Branch</span>
          <Select value={report.branch.id} onValueChange={(b) => go(b, tab)}>
            <SelectTrigger aria-label="Branch" className="w-64">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {report.branches.map((b) => (
                <SelectItem key={b.id} value={b.id}>
                  {b.name}
                  {b.isDefault ? " (default)" : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </PageHeader>
      <div className="rp-root">
        <ReportHeader report={report} mode="app" actions={actions} />
      </div>
      <Tabs value={tab} onValueChange={(t) => go(report.branch.id, t)}>
        <TabsList className="h-auto flex-wrap">
          {TABS.map((t) => (
            <TabsTrigger key={t.id} value={t.id}>
              {t.label}
            </TabsTrigger>
          ))}
        </TabsList>
        {TABS.map((t) => {
          const Body = TAB_BODY[t.id];
          return (
            <TabsContent key={t.id} value={t.id} className="rp-root mt-4">
              {t.id === "queries" ? (
                <QueriesBody report={report} />
              ) : t.id === "waits" ? (
                <WaitsBody report={report} />
              ) : t.id === "fixes" ? (
                <FixesBody projectId={report.project.id} />
              ) : Body ? (
                <Body report={report} mode="app" />
              ) : null}
            </TabsContent>
          );
        })}
      </Tabs>
    </div>
  );
}

/** /projects/:id[/branches/:branch[/:tab]]: the default branch and the Health tab when not given. */
export default function ReportPage() {
  const { id, branch, tab } = useParams();
  const r = useApi<Report>(id ? `/api/report/${id}/${branch ?? "default"}` : null);
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <LoadingState label="Loading the health report" rows={10} />;
  if (!branch || !isTab(tab))
    return <Navigate replace to={`/projects/${r.data.project.id}/branches/${r.data.branch.id}/${isTab(tab) ? tab : "health"}`} />;
  return <ReportView report={r.data} tab={tab} />;
}
