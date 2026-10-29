import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { analytics, createApp, server } from "@databricks/appkit";
import { WorkspaceClient } from "@databricks/sdk-experimental";
import { createAccess, userHttp } from "./lib/access";
import { canManage } from "./lib/approvals";
import { createHandlers, type Approvals } from "./lib/handlers";
import { createRunner, createWriter, loadQueries, statementExec } from "./lib/sql";
import { registerRoutes } from "./routes";

function required(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is not set`);
  return v;
}

createApp({
  plugins: [analytics(), server()],
  onPluginsReady(appkit) {
    const host = required("DATABRICKS_HOST");
    // Queries run as the app service principal (or the local profile in development).
    const sp = new WorkspaceClient({});
    const catalog = required("CATALOG");
    const exec = statementExec(sp, required("DATABRICKS_WAREHOUSE_ID"));
    const runner = createRunner(loadQueries(join(process.cwd(), "server", "queries"), catalog), exec);
    const http = userHttp(host.startsWith("http") ? host : `https://${host}`);
    const insert = createWriter(join(process.cwd(), "server", "writes", "insert_approval.sql"), catalog, exec);
    const fixerJobId = Number(process.env.FIXER_JOB_ID ?? "");
    const approvals: Approvals = {
      // The approver check uses the caller's own token, GET-only: project owner, or able to read the project ACL.
      async canManage(ctx, access, uid) {
        if (!ctx.token) return false;
        const name = access.projectNames[uid] ?? uid;
        const p = await http(`/api/2.0/postgres/projects/${encodeURIComponent(name)}`, ctx.token);
        const status = p.status === 200 && typeof p.json === "object" && p.json !== null ? (p.json as { status?: { owner?: unknown } }).status : undefined;
        const owner = typeof status?.owner === "string" ? status.owner : undefined;
        return canManage(http, ctx.token, { id: name, owner }, access.userName ?? "");
      },
      insert,
      // Writes and the job trigger run as the app service principal (MODIFY on approvals, CAN_MANAGE_RUN on the job).
      async trigger() {
        if (!Number.isInteger(fixerJobId) || fixerJobId <= 0) return false;
        try {
          await sp.jobs.runNow({ job_id: fixerJobId });
          return true;
        } catch (e) {
          console.warn("lakebase-health: could not start the fixer job; its schedule will pick the request up", e);
          return false;
        }
      },
      newId: () => randomUUID().replace(/-/g, ""),
    };
    const handlers = createHandlers({
      access: createAccess({ http }),
      runner,
      scheduleMinutes: Number(process.env.SCHEDULE_MINUTES ?? "60"),
      approvals,
    });
    appkit.server.extend((app) => registerRoutes(app, handlers));
  },
}).catch(console.error);
