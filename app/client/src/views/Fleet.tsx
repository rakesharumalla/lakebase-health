import {
  Button,
  Card,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@databricks/appkit-ui/react";
import { RefreshCw, ShieldQuestion } from "lucide-react";
import { ApiMessage } from "../ApiMessage";
import { CountBadge, StatusBadge } from "../components/Severity";
import { DataTable, EmptyState, KpiRow, KpiTile, LoadingState, MonoId, NUM, PageHeader, TextLink } from "../components/ui";
import { fmtTime, str, truthy, type Meta, type Row } from "../lib/format";
import { useApi } from "../lib/useApi";

type Fleet = { projects: Row[] };

async function refreshAccess() {
  await fetch("/api/access/refresh", { method: "POST" });
  window.location.reload();
}

const sum = (rows: Row[], key: string) => rows.reduce((n, r) => n + Number(r[key] ?? 0), 0);

export function FleetView({ fleet, meta }: { fleet: Fleet; meta: Meta }) {
  if (fleet.projects.length === 0) {
    return (
      <div className="space-y-6">
        <PageHeader title="Projects" />
        <EmptyState
          title="No projects to show"
          icon={<ShieldQuestion />}
          action={
            <Button onClick={() => void refreshAccess()}>
              <RefreshCw />
              Refresh access
            </Button>
          }
        >
          There are no Lakebase projects you can see. You see a project when you own it or hold a Postgres role on one of its branches; ask the project owner for a role, then use Refresh access.
        </EmptyState>
      </div>
    );
  }
  const projects = fleet.projects;
  const withCritical = projects.filter((p) => Number(p.critical) > 0).length;
  const computes = sum(projects, "computes");
  const enrolled = projects.filter((p) => truthy(p.enrolled)).length;
  return (
    <div className="space-y-6">
      <PageHeader
        title="Projects"
        meta={`${meta.openFindings} open findings across ${projects.length} projects you can see.`}
      />
      <KpiRow>
        <KpiTile label="Open findings" value={meta.openFindings.toLocaleString()} accent="bg-lava" />
        <KpiTile
          label="Projects with critical findings"
          value={`${withCritical} of ${projects.length}`}
          accent={withCritical ? "bg-sev-critical-fg" : "bg-sev-ok-fg"}
        />
        <KpiTile label="Computes active" value={`${sum(projects, "active_computes")} of ${computes}`} />
        <KpiTile label="Postgres checks on" value={`${enrolled} of ${projects.length}`} hint="projects enrolled" />
      </KpiRow>
      <Card className="gap-0 overflow-hidden py-0 shadow-none">
        <DataTable>
          <TableHeader>
            <TableRow>
              <TableHead>Project</TableHead>
              <TableHead className="text-right">Critical</TableHead>
              <TableHead className="text-right">Warning</TableHead>
              <TableHead className="text-right">Info</TableHead>
              <TableHead>Newest finding</TableHead>
              <TableHead className="text-right">Computes active</TableHead>
              <TableHead>Postgres checks</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {projects.map((p) => (
              <TableRow key={str(p.project_id)}>
                <TableCell>
                  <div className="flex flex-col">
                    <TextLink to={`/projects/${p.project_id}`}>{str(p.project_name ?? p.project_id)}</TextLink>
                    <MonoId value={str(p.project_id)} className="text-xs text-muted-foreground" max="max-w-[18ch]" />
                  </div>
                </TableCell>
                <TableCell className="text-right">
                  <CountBadge severity="critical" count={p.critical} />
                </TableCell>
                <TableCell className="text-right">
                  <CountBadge severity="warning" count={p.warning} />
                </TableCell>
                <TableCell className="text-right">
                  <CountBadge severity="info" count={p.info} />
                </TableCell>
                <TableCell className="text-muted-foreground">{fmtTime(p.newest_finding_at)}</TableCell>
                <TableCell className={NUM}>{p.computes ? `${p.active_computes ?? 0}/${p.computes}` : "—"}</TableCell>
                <TableCell>
                  <StatusBadge status={truthy(p.enrolled) ? "on" : "off"} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </DataTable>
      </Card>
    </div>
  );
}

export default function FleetPage() {
  const fleet = useApi<Fleet>("/api/fleet");
  const meta = useApi<Meta>("/api/meta");
  if (fleet.error) return <ApiMessage error={fleet.error} />;
  if (meta.error) return <ApiMessage error={meta.error} />;
  if (!fleet.data || !meta.data) return <LoadingState label="Loading projects" />;
  return <FleetView fleet={fleet.data} meta={meta.data} />;
}
