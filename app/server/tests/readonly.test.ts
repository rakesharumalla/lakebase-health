import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const root = join(__dirname, "..", "..");
const sqlFiles = (dir: string) =>
  readdirSync(join(root, dir)).filter((f) => f.endsWith(".sql")).map((f) => join(root, dir, f));
const body = (p: string) => readFileSync(p, "utf8").replace(/--.*$/gm, "").trim();
const SCOPED_TABLES =
  /\b(findings|check_results|inventory|proposed_actions|shadow_log|ledger|metrics_daily|runs|action_runs|approvals|raw_\w+)\b/i;

describe("read-only SQL", () => {
  it("every SQL file starts with SELECT or WITH and has no write keywords", () => {
    const files = [...sqlFiles("server/queries"), ...sqlFiles("config/queries")];
    expect(files.length).toBeGreaterThan(1);
    for (const f of files) {
      const sql = body(f);
      expect(sql, f).toMatch(/^(SELECT|WITH)\b/i);
      expect(sql, f).not.toMatch(/\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT)\b/i);
    }
  });

  it("analytics-plugin queries take no parameters and never touch engine tables", () => {
    for (const f of sqlFiles("config/queries")) {
      expect(body(f), f).not.toMatch(SCOPED_TABLES);
      expect(body(f), f).not.toMatch(/:[a-z_]+/i);
    }
  });

  it("open-finding filters are exactly new and open (superseded, like resolved, is not open)", () => {
    let seen = 0;
    for (const f of sqlFiles("server/queries")) {
      for (const m of body(f).matchAll(/status IN \(([^)]*)\)/gi)) {
        if (!/'(new|open)'/.test(m[1])) continue; // runs.status ('ok', ...) is not a finding status
        seen++;
        expect(m[1].replace(/\s/g, ""), f).toBe("'new','open'");
      }
      expect(body(f), f).not.toMatch(/superseded/i);
    }
    expect(seen).toBeGreaterThan(3);
  });

  it("every server query is scoped by :project_ids", () => {
    for (const f of sqlFiles("server/queries")) expect(body(f), f).toContain(":project_ids");
  });
});

describe("app writes", () => {
  it("server/writes holds only the approval insert", () => {
    const files = readdirSync(join(root, "server", "writes"));
    expect(files).toEqual(["insert_approval.sql"]);
    const sql = body(join(root, "server", "writes", "insert_approval.sql"));
    expect(sql).toMatch(/^INSERT INTO \{catalog\}\.lakebase_health\.approvals\s*\(/);
    expect(sql.split(";").filter((s) => s.trim()).length).toBe(1);
    expect(sql).not.toMatch(/\b(UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT|SELECT)\b/i);
  });
});
