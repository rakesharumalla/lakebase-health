import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createRunner, createWriter, loadQueries, statementExec, type Exec } from "../lib/sql";

function dirWith(files: Record<string, string>) {
  const d = mkdtempSync(join(tmpdir(), "q-"));
  for (const [n, s] of Object.entries(files)) writeFileSync(join(d, n), s);
  return d;
}

describe("sql runner", () => {
  it("substitutes the catalog and binds project_ids as one string", async () => {
    const q = loadQueries(dirWith({ "fleet.sql": "SELECT * FROM {catalog}.lakebase_health.findings WHERE array_contains(split(:project_ids, ','), project_id)" }), "my_catalog");
    const seen: Parameters<Exec>[] = [];
    const runner = createRunner(q, async (...a) => { seen.push(a); return [{ n: 1 }]; });
    expect(await runner.run("fleet", ["u1", "u2"])).toEqual([{ n: 1 }]);
    expect(seen[0][0]).toContain("my_catalog.lakebase_health.findings");
    expect(seen[0][1]).toEqual([{ name: "project_ids", value: "u1,u2", type: "STRING" }]);
  });

  it("returns no rows without calling the warehouse when the allow-list is empty", async () => {
    const q = loadQueries(dirWith({ "fleet.sql": "SELECT 1 WHERE :project_ids <> ''" }), "c");
    let called = false;
    const runner = createRunner(q, async () => { called = true; return []; });
    expect(await runner.run("fleet", [])).toEqual([]);
    expect(called).toBe(false);
  });

  it("binds dates as TIMESTAMP and rejects unknown queries and bad catalogs", async () => {
    const q = loadQueries(dirWith({ "t.sql": "SELECT :from_ts, :project_ids" }), "c");
    const seen: Parameters<Exec>[] = [];
    await createRunner(q, async (...a) => { seen.push(a); return []; }).run("t", ["u1"], { from_ts: new Date("2026-10-28T00:00:00Z") });
    expect(seen[0][1]).toContainEqual({ name: "from_ts", value: "2026-10-28T00:00:00.000Z", type: "TIMESTAMP" });
    await expect(createRunner(q, async () => []).run("nope", ["u1"])).rejects.toThrow("unknown query nope");
    expect(() => loadQueries(dirWith({}), "c; DROP")).toThrow("invalid catalog");
  });
});

describe("statementExec", () => {
  it("follows result chunks so long windows are not cut off", async () => {
    const client = {
      statementExecution: {
        executeStatement: async () => ({
          statement_id: "s1", status: { state: "SUCCEEDED" },
          manifest: { schema: { columns: [{ name: "n" }] } },
          result: { data_array: [["1"], ["2"]], next_chunk_index: 1 },
        }),
        getStatement: async () => ({}),
        getStatementResultChunkN: async ({ chunk_index }: { chunk_index: number }) =>
          chunk_index === 1 ? { data_array: [["3"]], next_chunk_index: 2 } : { data_array: [["4"]] },
        cancelExecution: async () => ({}),
      },
    };
    const rows = await statementExec(client as never, "wh")("SELECT 1", []);
    expect(rows.map((r) => r.n)).toEqual(["1", "2", "3", "4"]);
  });
});

describe("sql writer", () => {
  it("substitutes the catalog and binds every row field as a STRING parameter", async () => {
    const d = dirWith({ "w.sql": "INSERT INTO {catalog}.lakebase_health.approvals (a, b) VALUES (:a, :b)" });
    const seen: Parameters<Exec>[] = [];
    const write = createWriter(join(d, "w.sql"), "cat1", async (...a) => { seen.push(a); return []; });
    await write({ a: "1", b: "" });
    expect(seen[0][0]).toBe("INSERT INTO cat1.lakebase_health.approvals (a, b) VALUES (:a, :b)");
    expect(seen[0][1]).toEqual([{ name: "a", value: "1", type: "STRING" }, { name: "b", value: "", type: "STRING" }]);
  });

  it("rejects an unsafe catalog name", () => {
    expect(() => createWriter("x.sql", "a;b", async () => [])).toThrow(/catalog/);
  });
});
