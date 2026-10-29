import { readdirSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";
import type { sql, WorkspaceClient } from "@databricks/sdk-experimental";

export type Param = { name: string; value: string; type: "STRING" | "TIMESTAMP" };
export type Row = Record<string, unknown>;
export type Exec = (statement: string, params: Param[]) => Promise<Row[]>;

function checkCatalog(catalog: string) {
  if (!/^[A-Za-z0-9_]+$/.test(catalog)) throw new Error("invalid catalog");
}

const withCatalog = (file: string, catalog: string) => readFileSync(file, "utf8").split("{catalog}").join(catalog);

/** Load server/queries/*.sql, substituting the install catalog. */
export function loadQueries(dir: string, catalog: string): Map<string, string> {
  checkCatalog(catalog);
  const out = new Map<string, string>();
  for (const f of readdirSync(dir).filter((x) => x.endsWith(".sql"))) {
    out.set(basename(f, ".sql"), withCatalog(join(dir, f), catalog));
  }
  return out;
}

/** The app's one write (server/writes/insert_approval.sql): binds every row field as a STRING parameter. */
export function createWriter(file: string, catalog: string, exec: Exec) {
  checkCatalog(catalog);
  const text = withCatalog(file, catalog);
  return async (row: Record<string, string>): Promise<void> => {
    await exec(text, Object.entries(row).map(([name, value]) => ({ name, value, type: "STRING" as const })));
  };
}

/** Runs a named query with the caller's allowed project IDs bound by the server, never by the browser. */
export function createRunner(queries: Map<string, string>, exec: Exec) {
  return {
    async run(name: string, projectIds: string[], params: Record<string, string | Date> = {}): Promise<Row[]> {
      const text = queries.get(name);
      if (!text) throw new Error(`unknown query ${name}`);
      if (projectIds.length === 0) return [];
      const bound: Param[] = [{ name: "project_ids", value: projectIds.join(","), type: "STRING" }];
      for (const [k, v] of Object.entries(params)) {
        bound.push(
          v instanceof Date ? { name: k, value: v.toISOString(), type: "TIMESTAMP" } : { name: k, value: v, type: "STRING" },
        );
      }
      return exec(text, bound);
    },
  };
}

const PENDING = new Set(["PENDING", "RUNNING"]);

/** Statement Execution as the client's identity (the app service principal in production). */
export function statementExec(client: WorkspaceClient, warehouseId: string): Exec {
  return async (statement, params) => {
    const req: sql.ExecuteStatementRequest = {
      statement,
      warehouse_id: warehouseId,
      parameters: params,
      wait_timeout: "50s",
      on_wait_timeout: "CONTINUE",
      disposition: "INLINE",
      format: "JSON_ARRAY",
    };
    let r = await client.statementExecution.executeStatement(req);
    const started = Date.now();
    while (PENDING.has(String(r.status?.state)) && r.statement_id) {
      if (Date.now() - started > 100_000) {
        await client.statementExecution.cancelExecution({ statement_id: r.statement_id }).catch(() => undefined);
        throw new Error("query timed out");
      }
      await new Promise((res) => setTimeout(res, 1000));
      r = await client.statementExecution.getStatement({ statement_id: r.statement_id });
    }
    if (r.status?.state !== "SUCCEEDED") throw new Error(r.status?.error?.message ?? `statement ${r.status?.state}`);
    const cols = (r.manifest?.schema?.columns ?? []).map((c) => c.name ?? "");
    const data = [...(r.result?.data_array ?? [])];
    let next = r.result?.next_chunk_index;
    while (next !== undefined && next !== null && r.statement_id) {
      const chunk = await client.statementExecution.getStatementResultChunkN({ statement_id: r.statement_id, chunk_index: next });
      data.push(...(chunk.data_array ?? []));
      next = chunk.next_chunk_index;
    }
    return data.map((row) => Object.fromEntries(cols.map((c, i) => [c, row[i] ?? null])));
  };
}
