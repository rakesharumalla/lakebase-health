# Lakebase Health Read-only App Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Databricks App that shows each signed-in user the Lakebase health findings, evidence, history and incident timeline for the projects they have permission on, without writing anything.

**Architecture:** AppKit app in `app/` (Node/Express server + React client), deployed as an `apps` resource in the existing bundle. The server resolves the user's visible Lakebase projects and PII-group membership with the forwarded user token, then runs project-scoped SQL from `server/queries/` as the app service principal on a SQL warehouse, injecting the allowed project IDs itself. The browser only calls the app's JSON routes.

**Tech Stack:** `@databricks/appkit` + `@databricks/appkit-ui` (versions set by `databricks apps init`), TypeScript, Express (via AppKit server plugin), React, Zod, Vitest, Playwright smoke test, Databricks Statement Execution API.

**Spec:** `docs/superpowers/specs/2026-10-28-lakebase-health-app-design.md` (engine tables: `docs/superpowers/specs/2026-10-27-lakebase-health-engine-design.md` §4, column list in `engine/src/lakebase_health/ddl.py`).

## Global Constraints

- Read-only: no INSERT/UPDATE/DELETE/MERGE/CREATE/DROP/ALTER in any SQL file in `app/`; the app SP holds only `USE CATALOG`, `USE SCHEMA`, `SELECT` on `<catalog>.lakebase_health`.
- Every query that touches project data takes `:project_ids` (comma-separated string) injected by the server, never by the browser.
- Project-scoped SQL lives in `app/server/queries/`. `app/config/queries/` holds only `engine_freshness.sql`, which reads `runs` and nothing project-scoped.
- Lakebase API calls with the user token are GET only.
- Text fields `query_text`, `plan_json`, `parameters`, `statement`, `internalquery` are returned only to members of `databricks_pii_access`; others get `"[masked]"`, applied on the server.
- Input rules: project/endpoint IDs must be in the allowed list (else 404); fingerprint `^[0-9a-f]{32}$`; timeline window ISO 8601, clamped to the last 7 days and at most 7 days long; default last 6 hours.
- Access cache: per user, 5 minutes; `POST /api/access/refresh` clears the caller's entry.
- App name `lakebase-health` (≤ 26 chars). Profile `my-profile`, warehouse `0123456789abcdef`, catalog `my_catalog` for all live steps. Never auto-select another profile.
- Do not override `@databricks/appkit` / `@databricks/appkit-ui` versions set by `databricks apps init`. Before calling any AppKit API, confirm its signature with `npx @databricks/appkit docs <section>` in `app/`; no `as unknown as T` casts (AppKit lint fails on them).
- Commit messages end with `Co-authored-by: Isaac <no-reply@databricks.com>`; never pass `--no-verify`.

## Ruling recorded in this plan

- **Custom SELECT routes instead of `config/queries/` + `useAnalyticsQuery`.** The AppKit guide says not to add custom endpoints for warehouse SELECTs. Here it must be overridden: the analytics plugin exposes every `config/queries/*.sql` at `POST /api/analytics/query/:key` with browser-supplied parameters, so a user could pass any project ID. Scoping must be injected on the server. Cost if wrong: we hand-roll result caching and warehouse-status handling that the plugin would have given us.

## Review Focus

1. A request that names a project, endpoint or fingerprint the user cannot see must return 404 with no data, including when the object exists (enumeration), and including the timeline for an endpoint whose project only appears in telemetry (not in `inventory`).
2. The Lakebase project list paginates; a user with more than one page of projects (admins see ~800) must get all of them, and a failed page must fail the request, not return a partial allow-list.
3. Masking must cover text nested inside `evidence` JSON (for example `query_text` inside evidence of plan checks), not only top-level columns, and must apply to the timeline's session samples.
4. A missing or expired user token (no `x-forwarded-access-token`, or 401 from the API) must return 401 to the browser and never fall back to the service principal's view.
5. Timeline parameters at the edges: `from` > `to`, a window longer than 7 days, `from` older than 7 days, malformed dates, and missing params must clamp or 400 deterministically, never run an unbounded scan.

Each item has a test in the task that owns the code (Tasks 2, 3, 4, 5, 7).

## File Structure

```
app/                                   # created by `databricks apps init`, then adapted
  app.yaml                             # generated; command/env
  package.json                         # generated; add zod, vitest scripts
  config/queries/engine_freshness.sql  # the only analytics-plugin query (fleet-wide engine status)
  server/server.ts                     # createApp + registerRoutes
  server/lib/validate.ts               # Zod schemas, window clamping
  server/lib/access.ts                 # allowed projects + PII membership via user token, per-user cache
  server/lib/mask.ts                   # masking of text fields, including inside evidence JSON
  server/lib/sql.ts                    # load server/queries/*.sql, run as SP via Statement Execution
  server/lib/handlers.ts               # one function per route; pure, injected deps
  server/routes.ts                     # Express wiring of handlers
  server/queries/*.sql                 # project-scoped SELECT/WITH queries
  server/tests/*.test.ts               # Vitest unit tests
  server/tests/live/queries.live.test.ts  # runs every SQL file on my-profile (opt-in)
  client/src/api.ts                    # typed fetch helpers + useApi hook
  client/src/App.tsx                   # router + layout + freshness banner
  client/src/views/Fleet.tsx
  client/src/views/Project.tsx
  client/src/views/Finding.tsx
  client/src/views/Timeline.tsx
  client/src/views/*.test.tsx          # component tests on recorded fixtures
  client/src/fixtures/*.json           # recorded healthcheck responses
  tests/smoke.spec.ts                  # Playwright smoke test (generated; selectors updated)
resources/app.yml                      # bundle apps resource
scripts/app_acceptance.py              # deployed-app acceptance (criteria 1–6)
```

---

### Task 1: Scaffold the AppKit app and add it to the bundle

**Files:**
- Create: `app/` (via `databricks apps init`), `app/config/queries/engine_freshness.sql`, `app/server/tests/readonly.test.ts`, `resources/app.yml`
- Modify: `databricks.yml` (add `warehouse_id` variable), `app/package.json` (scripts), `.gitignore`

**Interfaces:**
- Produces: `app/` builds (`npm run build`), `npm test` runs Vitest over `server/tests` and `client/src`; bundle resource key `lakebase_health_app`.

- [ ] **Step 1: Confirm the warehouse is running and scaffold**

Run:
```bash
databricks warehouses get 0123456789abcdef --profile my-profile -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['state'])"
# if not RUNNING: databricks warehouses start 0123456789abcdef --profile my-profile
cd <repo>
databricks apps init --name lakebase-health --features analytics \
  --set analytics.sql-warehouse.id=0123456789abcdef \
  --description "Read-only Lakebase health findings, evidence and incident timeline" \
  --run none --profile my-profile
mv lakebase-health app
```
Expected: `app/` contains `client/src/App.tsx`, `server/server.ts`, `config/queries/`, `tests/smoke.spec.ts`, `app.yaml`, `package.json`, and a `databricks.yml` fragment.

- [ ] **Step 2: Fold the generated bundle config into the repo bundle**

Read `app/databricks.yml` (generated). Move its `resources.apps` block into `resources/app.yml`, keeping the generated keys (`source_code_path`, `resources`, `lifecycle`) and adjusting `source_code_path` to `../app`. Then delete `app/databricks.yml`. Target shape:

```yaml
# resources/app.yml
resources:
  apps:
    lakebase_health_app:
      name: lakebase-health
      description: Read-only Lakebase health findings, evidence and incident timeline
      source_code_path: ../app
      user_api_scopes: [sql, postgres]
      resources:
        - name: sql-warehouse
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: CAN_USE
      lifecycle:
        started: true
```

Add to `databricks.yml` variables:
```yaml
  warehouse_id:
    description: SQL warehouse the app queries (the app SP gets CAN_USE)
```

Run: `databricks bundle validate --var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 --var warehouse_id=0123456789abcdef --profile my-profile`
Expected: `Validation OK!`

- [ ] **Step 3: Replace the sample query with the engine freshness query**

Delete every file in `app/config/queries/`, then create `app/config/queries/engine_freshness.sql`:
```sql
-- @param catalog STRING = my_catalog
SELECT max(ended_at) AS last_ended_at,
       max_by(status, started_at) AS last_status,
       max(CASE WHEN status IN ('ok', 'ok_with_errors') THEN ended_at END) AS last_success_at
FROM IDENTIFIER(:catalog || '.lakebase_health.runs')
```

- [ ] **Step 4: Write the failing read-only guard test**

Add Vitest: `cd app && npm install -D vitest zod@^3 && npm install zod@^3` (keep AppKit versions untouched). In `app/package.json` scripts add `"test": "vitest run"`.

```ts
// app/server/tests/readonly.test.ts
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const root = join(__dirname, "..", "..");
const sqlFiles = (dir: string) =>
  readdirSync(join(root, dir)).filter((f) => f.endsWith(".sql")).map((f) => join(root, dir, f));
const body = (p: string) => readFileSync(p, "utf8").replace(/--.*$/gm, "").trim();
const SCOPED_TABLES = /\b(findings|check_results|inventory|proposed_actions|shadow_log|ledger|metrics_daily|raw_\w+)\b/i;

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

  it("analytics-plugin queries never touch project-scoped tables", () => {
    for (const f of sqlFiles("config/queries")) expect(body(f), f).not.toMatch(SCOPED_TABLES);
  });

  it("every server query is scoped by :project_ids", () => {
    for (const f of sqlFiles("server/queries")) expect(body(f), f).toContain(":project_ids");
  });
});
```

- [ ] **Step 5: Run it to verify it fails**

Run: `cd app && npx vitest run server/tests/readonly.test.ts`
Expected: FAIL — `server/queries` does not exist (ENOENT).

- [ ] **Step 6: Create `server/queries/` with the meta query**

```sql
-- app/server/queries/meta.sql
WITH last AS (SELECT * FROM {catalog}.lakebase_health.runs ORDER BY started_at DESC LIMIT 1)
SELECT (SELECT max(ended_at) FROM {catalog}.lakebase_health.runs WHERE status IN ('ok', 'ok_with_errors')) AS last_success_at,
       last.status AS last_status, last.started_at AS last_started_at,
       (SELECT count(*) FROM {catalog}.lakebase_health.findings
         WHERE status IN ('new', 'open') AND array_contains(split(:project_ids, ','), project_id)) AS open_findings
FROM last
```
(`{catalog}` is substituted by `sql.ts` from the `CATALOG` env var; parameters stay bound.)

- [ ] **Step 7: Run tests and typegen**

Run: `cd app && npx vitest run server/tests/readonly.test.ts && npm run typegen`
Expected: 3 passed; typegen shows ✓ for `engine_freshness`.

- [ ] **Step 8: Commit**

```bash
git add app resources/app.yml databricks.yml
git commit -m "feat(app): scaffold AppKit read-only app with read-only SQL guard

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Input validation

**Files:**
- Create: `app/server/lib/validate.ts`, `app/server/tests/validate.test.ts`

**Interfaces:**
- Produces:
  - `parseFingerprint(s: unknown): string | null`
  - `parseEndpointId(s: unknown): string | null`
  - `parseProjectId(s: unknown): string | null` (UUID)
  - `parseWindow(from: unknown, to: unknown, now: Date): { from: Date; to: Date } | { error: string }`
  - `MAX_WINDOW_MS = 7 * 24 * 3600 * 1000`, `DEFAULT_WINDOW_MS = 6 * 3600 * 1000`

- [ ] **Step 1: Write the failing tests**

```ts
// app/server/tests/validate.test.ts
import { describe, expect, it } from "vitest";
import { DEFAULT_WINDOW_MS, MAX_WINDOW_MS, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "../lib/validate";

const NOW = new Date("2026-10-28T12:00:00Z");

describe("ids", () => {
  it("accepts only 32-char lowercase hex fingerprints", () => {
    expect(parseFingerprint("cc59509434e9e437c838cc12b394bafa")).toBe("cc59509434e9e437c838cc12b394bafa");
    expect(parseFingerprint("CC59509434E9E437C838CC12B394BAFA")).toBeNull();
    expect(parseFingerprint("cc59' OR 1=1 --")).toBeNull();
    expect(parseFingerprint(undefined)).toBeNull();
  });
  it("accepts Lakebase endpoint ids and project uuids", () => {
    expect(parseEndpointId("ep-sample-primary-a1b2c3d4")).toBe("ep-sample-primary-a1b2c3d4");
    expect(parseEndpointId("ep-x; DROP")).toBeNull();
    expect(parseProjectId("11111111-2222-4333-8444-555555555555")).toBe("11111111-2222-4333-8444-555555555555");
    expect(parseProjectId("healthcheck")).toBeNull();
  });
});

describe("parseWindow", () => {
  it("defaults to the last 6 hours", () => {
    const w = parseWindow(undefined, undefined, NOW);
    expect("error" in w).toBe(false);
    if (!("error" in w)) {
      expect(w.to.toISOString()).toBe(NOW.toISOString());
      expect(w.to.getTime() - w.from.getTime()).toBe(DEFAULT_WINDOW_MS);
    }
  });
  it("rejects malformed dates and from > to", () => {
    expect(parseWindow("yesterday", undefined, NOW)).toEqual({ error: "invalid from" });
    expect(parseWindow("2026-10-28T10:00:00Z", "2026-10-28T09:00:00Z", NOW)).toEqual({ error: "from must be before to" });
  });
  it("clamps to the 7-day retention and a 7-day span", () => {
    const w = parseWindow("2026-10-01T00:00:00Z", "2026-10-28T13:00:00Z", NOW);
    if ("error" in w) throw new Error(w.error);
    expect(w.to.getTime()).toBe(NOW.getTime());
    expect(w.from.getTime()).toBe(NOW.getTime() - MAX_WINDOW_MS);
  });
  it("rejects a window entirely older than retention", () => {
    expect(parseWindow("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", NOW)).toEqual({ error: "window is older than 7 days" });
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && npx vitest run server/tests/validate.test.ts`
Expected: FAIL — cannot find module `../lib/validate`.

- [ ] **Step 3: Implement**

```ts
// app/server/lib/validate.ts
export const MAX_WINDOW_MS = 7 * 24 * 3600 * 1000;
export const DEFAULT_WINDOW_MS = 6 * 3600 * 1000;

const FINGERPRINT = /^[0-9a-f]{32}$/;
const ENDPOINT = /^ep-[a-z0-9-]{1,61}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

const match = (re: RegExp) => (s: unknown): string | null => (typeof s === "string" && re.test(s) ? s : null);
export const parseFingerprint = match(FINGERPRINT);
export const parseEndpointId = match(ENDPOINT);
export const parseProjectId = match(UUID);

function parseDate(s: unknown): Date | null | undefined {
  if (s === undefined || s === "") return undefined;
  if (typeof s !== "string" || !/^\d{4}-\d{2}-\d{2}T/.test(s)) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function parseWindow(from: unknown, to: unknown, now: Date): { from: Date; to: Date } | { error: string } {
  const f = parseDate(from);
  const t = parseDate(to);
  if (f === null) return { error: "invalid from" };
  if (t === null) return { error: "invalid to" };
  let end = t ?? now;
  if (end.getTime() > now.getTime()) end = now;
  let start = f ?? new Date(end.getTime() - DEFAULT_WINDOW_MS);
  if (start.getTime() >= end.getTime()) return { error: "from must be before to" };
  const floor = now.getTime() - MAX_WINDOW_MS;
  if (end.getTime() <= floor) return { error: "window is older than 7 days" };
  if (start.getTime() < floor) start = new Date(floor);
  if (end.getTime() - start.getTime() > MAX_WINDOW_MS) start = new Date(end.getTime() - MAX_WINDOW_MS);
  return { from: start, to: end };
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd app && npx vitest run server/tests/validate.test.ts`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add app/server/lib/validate.ts app/server/tests/validate.test.ts
git commit -m "feat(app): input validation and timeline window clamping

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: Access resolution (allowed projects, PII membership, cache)

**Files:**
- Create: `app/server/lib/access.ts`, `app/server/tests/access.test.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `type Http = (path: string, token: string) => Promise<{ status: number; json: unknown }>` (GET only)
  - `class AccessError extends Error { status: 401 | 502 }`
  - `type Access = { userKey: string; projectIds: string[]; projectNames: Record<string, string>; pii: boolean }`
  - `createAccess(opts: { http: Http; ttlMs?: number; now?: () => number; piiGroup?: string }): { resolve(token: string | undefined, userKey: string | undefined): Promise<Access>; refresh(userKey: string): void }`

- [ ] **Step 1: Write the failing tests**

```ts
// app/server/tests/access.test.ts
import { describe, expect, it } from "vitest";
import { AccessError, createAccess, type Http } from "../lib/access";

function fakeHttp(pages: Record<string, unknown>, me: unknown, calls: string[] = []): Http {
  return async (path) => {
    calls.push(path);
    if (path.startsWith("/api/2.0/preview/scim/v2/Me")) return { status: 200, json: me };
    const page = pages[path];
    if (page === undefined) return { status: 500, json: { message: "boom" } };
    return { status: 200, json: page };
  };
}
const P1 = "/api/2.0/postgres/projects?page_size=100";
const ME = { groups: [{ display: "users" }] };

describe("createAccess", () => {
  it("collects every page of projects", async () => {
    const http = fakeHttp({
      [P1]: { projects: [{ uid: "u1", project_id: "a" }], next_page_token: "t2" },
      [`${P1}&page_token=t2`]: { projects: [{ uid: "u2", project_id: "b" }] },
    }, ME);
    const a = await createAccess({ http }).resolve("tok", "alice");
    expect(a.projectIds).toEqual(["u1", "u2"]);
    expect(a.projectNames).toEqual({ u1: "a", u2: "b" });
    expect(a.pii).toBe(false);
  });

  it("fails the request when any page fails (no partial allow-list)", async () => {
    const http = fakeHttp({ [P1]: { projects: [{ uid: "u1", project_id: "a" }], next_page_token: "t2" } }, ME);
    await expect(createAccess({ http }).resolve("tok", "alice")).rejects.toMatchObject({ status: 502 });
  });

  it("rejects a missing token or a 401 with status 401", async () => {
    const access = createAccess({ http: async () => ({ status: 401, json: {} }) });
    await expect(access.resolve(undefined, "alice")).rejects.toBeInstanceOf(AccessError);
    await expect(access.resolve(undefined, "alice")).rejects.toMatchObject({ status: 401 });
    await expect(access.resolve("expired", "alice")).rejects.toMatchObject({ status: 401 });
  });

  it("detects the PII group", async () => {
    const http = fakeHttp({ [P1]: { projects: [] } }, { groups: [{ display: "databricks_pii_access" }] });
    expect((await createAccess({ http }).resolve("tok", "bob")).pii).toBe(true);
  });

  it("caches per user for the TTL and refresh clears it", async () => {
    let t = 0;
    const calls: string[] = [];
    const http = fakeHttp({ [P1]: { projects: [{ uid: "u1", project_id: "a" }] } }, ME, calls);
    const access = createAccess({ http, ttlMs: 1000, now: () => t });
    await access.resolve("tok", "alice");
    await access.resolve("tok", "alice");
    expect(calls.filter((c) => c.startsWith("/api/2.0/postgres")).length).toBe(1);
    await access.resolve("tok", "carol");
    expect(calls.filter((c) => c.startsWith("/api/2.0/postgres")).length).toBe(2);
    t = 1500;
    await access.resolve("tok", "alice");
    access.refresh("alice");
    await access.resolve("tok", "alice");
    expect(calls.filter((c) => c.startsWith("/api/2.0/postgres")).length).toBe(4);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && npx vitest run server/tests/access.test.ts`
Expected: FAIL — cannot find module `../lib/access`.

- [ ] **Step 3: Implement**

```ts
// app/server/lib/access.ts
export type Http = (path: string, token: string) => Promise<{ status: number; json: unknown }>;
export type Access = { userKey: string; projectIds: string[]; projectNames: Record<string, string>; pii: boolean };

export class AccessError extends Error {
  constructor(public status: 401 | 502, message: string) {
    super(message);
  }
}

type ProjectsPage = { projects?: { uid?: string; project_id?: string }[]; next_page_token?: string };
type Me = { groups?: { display?: string }[] };

const isObj = (x: unknown): x is Record<string, unknown> => typeof x === "object" && x !== null;

/** Real GET-only HTTP using the forwarded user token. */
export function userHttp(host: string): Http {
  return async (path, token) => {
    const r = await fetch(`${host.replace(/\/$/, "")}${path}`, { method: "GET", headers: { Authorization: `Bearer ${token}` } });
    let json: unknown = null;
    try {
      json = await r.json();
    } catch {
      json = null;
    }
    return { status: r.status, json };
  };
}

export function createAccess(opts: { http: Http; ttlMs?: number; now?: () => number; piiGroup?: string }) {
  const ttl = opts.ttlMs ?? 5 * 60 * 1000;
  const now = opts.now ?? Date.now;
  const piiGroup = opts.piiGroup ?? "databricks_pii_access";
  const cache = new Map<string, { at: number; value: Access }>();

  async function get(path: string, token: string): Promise<unknown> {
    const r = await opts.http(path, token);
    if (r.status === 401 || r.status === 403) throw new AccessError(401, "user token rejected");
    if (r.status !== 200) throw new AccessError(502, `GET ${path.split("?")[0]} failed with ${r.status}`);
    return r.json;
  }

  async function load(token: string, userKey: string): Promise<Access> {
    const projectIds: string[] = [];
    const projectNames: Record<string, string> = {};
    let pageToken: string | undefined;
    do {
      const path = `/api/2.0/postgres/projects?page_size=100${pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : ""}`;
      const page = (await get(path, token)) as ProjectsPage;
      for (const p of isObj(page) && Array.isArray(page.projects) ? page.projects : []) {
        if (typeof p.uid === "string") {
          projectIds.push(p.uid);
          projectNames[p.uid] = typeof p.project_id === "string" ? p.project_id : p.uid;
        }
      }
      pageToken = isObj(page) && typeof page.next_page_token === "string" && page.next_page_token ? page.next_page_token : undefined;
    } while (pageToken);
    const me = (await get("/api/2.0/preview/scim/v2/Me", token)) as Me;
    const pii = isObj(me) && Array.isArray(me.groups) && me.groups.some((g) => g.display === piiGroup);
    return { userKey, projectIds, projectNames, pii };
  }

  return {
    async resolve(token: string | undefined, userKey: string | undefined): Promise<Access> {
      if (!token || !userKey) throw new AccessError(401, "no user token forwarded");
      const hit = cache.get(userKey);
      if (hit && now() - hit.at < ttl) return hit.value;
      const value = await load(token, userKey);
      cache.set(userKey, { at: now(), value });
      return value;
    },
    refresh(userKey: string) {
      cache.delete(userKey);
    },
  };
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd app && npx vitest run server/tests/access.test.ts`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add app/server/lib/access.ts app/server/tests/access.test.ts
git commit -m "feat(app): resolve visible projects and PII membership with the user token

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: Masking

**Files:**
- Create: `app/server/lib/mask.ts`, `app/server/tests/mask.test.ts`

**Interfaces:**
- Produces: `MASKED = "[masked]"`, `TEXT_FIELDS: ReadonlySet<string>`, `maskRows<T extends Record<string, unknown>>(rows: T[], pii: boolean): T[]`

- [ ] **Step 1: Write the failing tests**

```ts
// app/server/tests/mask.test.ts
import { describe, expect, it } from "vitest";
import { MASKED, maskRows } from "../lib/mask";

describe("maskRows", () => {
  const rows = [{
    check_id: "plan_flip", query_text: "SELECT * FROM secrets WHERE ssn = '1'",
    evidence: JSON.stringify({ queryid: "42", query_text: "SELECT ssn", nested: { statement: "UPDATE t" } }),
    statement: "DELETE FROM t", measure: 3,
  }];

  it("masks top-level and evidence-nested text for non-PII users", () => {
    const [r] = maskRows(rows, false);
    expect(r.query_text).toBe(MASKED);
    expect(r.statement).toBe(MASKED);
    const ev = JSON.parse(String(r.evidence));
    expect(ev.query_text).toBe(MASKED);
    expect(ev.nested.statement).toBe(MASKED);
    expect(ev.queryid).toBe("42");
    expect(r.measure).toBe(3);
  });

  it("leaves rows unchanged for PII members and keeps nulls null", () => {
    expect(maskRows(rows, true)).toEqual(rows);
    expect(maskRows([{ query_text: null }], false)[0].query_text).toBeNull();
  });

  it("does not throw on non-JSON evidence", () => {
    expect(maskRows([{ evidence: "not json" }], false)[0].evidence).toBe("not json");
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && npx vitest run server/tests/mask.test.ts`
Expected: FAIL — cannot find module `../lib/mask`.

- [ ] **Step 3: Implement**

```ts
// app/server/lib/mask.ts
export const MASKED = "[masked]";
export const TEXT_FIELDS: ReadonlySet<string> = new Set(["query_text", "plan_json", "parameters", "statement", "internalquery", "query"]);

function maskValue(key: string, v: unknown): unknown {
  if (TEXT_FIELDS.has(key)) return v === null || v === undefined ? v : MASKED;
  if (Array.isArray(v)) return v.map((x) => maskValue("", x));
  if (typeof v === "object" && v !== null) {
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, maskValue(k, x)]));
  }
  return v;
}

export function maskRows<T extends Record<string, unknown>>(rows: T[], pii: boolean): T[] {
  if (pii) return rows;
  return rows.map((row) => {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      if (k === "evidence" && typeof v === "string") {
        try {
          out[k] = JSON.stringify(maskValue("", JSON.parse(v)));
        } catch {
          out[k] = v;
        }
      } else {
        out[k] = maskValue(k, v);
      }
    }
    return out as T;
  });
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd app && npx vitest run server/tests/mask.test.ts`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add app/server/lib/mask.ts app/server/tests/mask.test.ts
git commit -m "feat(app): mask query and statement text for non-PII users

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: SQL runner (server queries as the app SP)

**Files:**
- Create: `app/server/lib/sql.ts`, `app/server/tests/sql.test.ts`

**Interfaces:**
- Produces:
  - `type Exec = (statement: string, params: { name: string; value: string; type: "STRING" | "TIMESTAMP" }[]) => Promise<Record<string, unknown>[]>`
  - `loadQueries(dir: string, catalog: string): Map<string, string>` (substitutes `{catalog}`; rejects catalog names not matching `^[A-Za-z0-9_]+$`)
  - `createRunner(queries: Map<string, string>, exec: Exec): { run(name: string, projectIds: string[], params?: Record<string, string | Date>): Promise<Record<string, unknown>[]> }`
  - `statementExec(client, warehouseId: string): Exec` — Statement Execution as the SP.

- [ ] **Step 1: Write the failing tests**

```ts
// app/server/tests/sql.test.ts
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createRunner, loadQueries, type Exec } from "../lib/sql";

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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && npx vitest run server/tests/sql.test.ts`
Expected: FAIL — cannot find module `../lib/sql`.

- [ ] **Step 3: Implement**

```ts
// app/server/lib/sql.ts
import { readdirSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";

export type Param = { name: string; value: string; type: "STRING" | "TIMESTAMP" };
export type Exec = (statement: string, params: Param[]) => Promise<Record<string, unknown>[]>;

export function loadQueries(dir: string, catalog: string): Map<string, string> {
  if (!/^[A-Za-z0-9_]+$/.test(catalog)) throw new Error("invalid catalog");
  const out = new Map<string, string>();
  for (const f of readdirSync(dir).filter((x) => x.endsWith(".sql"))) {
    out.set(basename(f, ".sql"), readFileSync(join(dir, f), "utf8").replaceAll("{catalog}", catalog));
  }
  return out;
}

export function createRunner(queries: Map<string, string>, exec: Exec) {
  return {
    async run(name: string, projectIds: string[], params: Record<string, string | Date> = {}) {
      const sql = queries.get(name);
      if (!sql) throw new Error(`unknown query ${name}`);
      if (projectIds.length === 0) return [];
      const bound: Param[] = [{ name: "project_ids", value: projectIds.join(","), type: "STRING" }];
      for (const [k, v] of Object.entries(params)) {
        bound.push(v instanceof Date ? { name: k, value: v.toISOString(), type: "TIMESTAMP" } : { name: k, value: v, type: "STRING" });
      }
      return exec(sql, bound);
    },
  };
}
```

Then add `statementExec`. First confirm the client type and method names in the installed AppKit:
Run: `cd app && npx @databricks/appkit docs ./docs/plugins/execution-context.md | grep -n getWorkspaceClient` and `grep -rn "executeStatement" node_modules/@databricks/*/dist/**/*.d.ts | head -3`
Expected: `getWorkspaceClient()` is exported from `@databricks/appkit`; the SDK client exposes `statementExecution.executeStatement` and `statementExecution.getStatement`. Then append:

```ts
// app/server/lib/sql.ts (continued)
type StatementClient = {
  statementExecution: {
    executeStatement(req: Record<string, unknown>): Promise<StatementResponse>;
    getStatement(req: { statement_id: string }): Promise<StatementResponse>;
  };
};
type StatementResponse = {
  statement_id?: string;
  status?: { state?: string; error?: { message?: string } };
  manifest?: { schema?: { columns?: { name: string }[] } };
  result?: { data_array?: (string | null)[][] };
};

export function statementExec(client: StatementClient, warehouseId: string): Exec {
  return async (statement, params) => {
    let r = await client.statementExecution.executeStatement({
      statement, warehouse_id: warehouseId, parameters: params, wait_timeout: "50s", on_wait_timeout: "CONTINUE",
      disposition: "INLINE", format: "JSON_ARRAY",
    });
    const started = Date.now();
    while ((r.status?.state === "PENDING" || r.status?.state === "RUNNING") && r.statement_id) {
      if (Date.now() - started > 100_000) throw new Error("query timed out");
      await new Promise((res) => setTimeout(res, 1000));
      r = await client.statementExecution.getStatement({ statement_id: r.statement_id });
    }
    if (r.status?.state !== "SUCCEEDED") throw new Error(r.status?.error?.message ?? `statement ${r.status?.state}`);
    const cols = (r.manifest?.schema?.columns ?? []).map((c) => c.name);
    return (r.result?.data_array ?? []).map((row) => Object.fromEntries(cols.map((c, i) => [c, row[i]])));
  };
}
```
If `tsc` reports that the SDK's method names or request field casing differ (for example camelCase `warehouseId`), adapt `StatementClient` and the request object to the installed `.d.ts`, and note it in the ledger as a ruling.

- [ ] **Step 4: Run to verify it passes**

Run: `cd app && npx vitest run server/tests/sql.test.ts && npx tsc --noEmit -p .`
Expected: PASS (3 tests); `tsc` clean.

- [ ] **Step 5: Commit**

```bash
git add app/server/lib/sql.ts app/server/tests/sql.test.ts
git commit -m "feat(app): server-side SQL runner with injected project scope

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: Project-scoped queries, verified on live data

**Files:**
- Create: `app/server/queries/{fleet,project_findings,project_computes,finding,finding_history,finding_actions,finding_related,endpoint_project,timeline_waits,timeline_locks,timeline_commits,timeline_events,timeline_gauges,timeline_samples}.sql`, `app/server/tests/live/queries.live.test.ts`
- Modify: `app/package.json` (script `test:live`)

**Interfaces:**
- Consumes: `loadQueries`, `createRunner`, `statementExec` (Task 5).
- Produces: query names used by Task 7 handlers: `meta`, `fleet`, `project_findings`, `project_computes`, `finding`, `finding_history`, `finding_actions`, `finding_related`, `endpoint_project`, `timeline_waits`, `timeline_locks`, `timeline_commits`, `timeline_events`, `timeline_gauges`, `timeline_samples`. Timeline queries take `:endpoint_id`, `:from_ts`, `:to_ts`; finding queries take `:fingerprint`; project queries take `:project_id`; `timeline_samples` also takes `:at_ts`.

- [ ] **Step 1: Write the live test (fails until the SQL files exist)**

```ts
// app/server/tests/live/queries.live.test.ts
// Opt-in: LIVE=1 DATABRICKS_CONFIG_PROFILE=my-profile npm run test:live
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WorkspaceClient } from "@databricks/sdk-experimental";
import { createRunner, loadQueries, statementExec } from "../../lib/sql";

const live = process.env.LIVE === "1";
const HC = "11111111-2222-4333-8444-555555555555";
const EP = "ep-sample-primary-a1b2c3d4";
const to = new Date();
const from = new Date(to.getTime() - 7 * 24 * 3600 * 1000 + 60_000);

describe.runIf(live)("queries on my-profile", () => {
  const runner = createRunner(
    loadQueries(join(__dirname, "..", "..", "queries"), "my_catalog"),
    statementExec(new WorkspaceClient({ profile: "my-profile" }) as never, "0123456789abcdef"),
  );
  const timed = async (name: string, params: Record<string, string | Date> = {}) => {
    const t = Date.now();
    const rows = await runner.run(name, [HC], params);
    return { rows, ms: Date.now() - t };
  };

  it("fleet and project queries return healthcheck rows quickly", async () => {
    for (const [name, params, col] of [
      ["meta", {}, "last_success_at"], ["fleet", {}, "project_id"],
      ["project_findings", { project_id: HC }, "fingerprint"], ["project_computes", { project_id: HC }, "endpoint_id"],
    ] as const) {
      const { rows, ms } = await timed(name, params);
      expect(rows.length, name).toBeGreaterThan(0);
      expect(Object.keys(rows[0]), name).toContain(col);
      expect(ms, name).toBeLessThan(5000);
    }
  });

  it("finding queries work for a real healthcheck fingerprint", async () => {
    const [f] = (await timed("project_findings", { project_id: HC })).rows;
    const fp = String(f.fingerprint);
    for (const name of ["finding", "finding_history", "finding_actions", "finding_related"]) {
      const { ms } = await timed(name, { fingerprint: fp });
      expect(ms, name).toBeLessThan(5000);
    }
    expect((await timed("finding", { fingerprint: fp })).rows[0].fingerprint).toBe(fp);
  });

  it("timeline queries cover 7 days within 5 s each and map the endpoint", async () => {
    expect((await timed("endpoint_project", { endpoint_id: EP })).rows[0].project_id).toBe(HC);
    for (const name of ["timeline_waits", "timeline_locks", "timeline_commits", "timeline_events", "timeline_gauges"]) {
      const { ms } = await timed(name, { endpoint_id: EP, from_ts: from, to_ts: to });
      expect(ms, name).toBeLessThan(5000);
    }
    const { rows } = await timed("timeline_locks", { endpoint_id: EP, from_ts: from, to_ts: to });
    expect(rows.some((r) => r.blocker_class === "idle_in_transaction" || r.blocker_class === "not_sampled")).toBe(true);
  });

  it("returns nothing for a project outside the allow-list", async () => {
    const rows = await runner.run("project_findings", ["00000000-0000-0000-0000-000000000000"], { project_id: HC });
    expect(rows).toEqual([]);
  });
});
```
Add script: `"test:live": "LIVE=1 vitest run server/tests/live"`. (`@databricks/sdk-experimental` is AppKit's SDK dependency; if `npm ls @databricks/sdk-experimental` shows a different SDK package, import that one instead and ledger it.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && npm run test:live`
Expected: FAIL — `unknown query fleet`.

- [ ] **Step 3: Write the SQL files**

Every file filters on `array_contains(split(:project_ids, ','), project_id)`.

```sql
-- app/server/queries/fleet.sql
WITH inv AS (
  SELECT project_id, any_value(project_name) AS project_name,
         sum(CASE WHEN current_state = 'ACTIVE' THEN 1 ELSE 0 END) AS active_computes,
         count(*) AS computes, max(CAST(enrolled AS INT)) AS enrolled
  FROM {catalog}.lakebase_health.inventory
  WHERE run_id = (SELECT max_by(run_id, captured_at) FROM {catalog}.lakebase_health.inventory)
    AND array_contains(split(:project_ids, ','), project_id)
  GROUP BY project_id
), f AS (
  SELECT project_id,
         sum(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical,
         sum(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) AS warning,
         sum(CASE WHEN severity = 'info' THEN 1 ELSE 0 END) AS info,
         max(first_seen) AS newest_finding_at
  FROM {catalog}.lakebase_health.findings
  WHERE status IN ('new', 'open') AND array_contains(split(:project_ids, ','), project_id)
  GROUP BY project_id
)
SELECT coalesce(f.project_id, inv.project_id) AS project_id, inv.project_name,
       coalesce(f.critical, 0) AS critical, coalesce(f.warning, 0) AS warning, coalesce(f.info, 0) AS info,
       f.newest_finding_at, inv.active_computes, inv.computes, inv.enrolled
FROM f FULL OUTER JOIN inv ON inv.project_id = f.project_id
ORDER BY critical DESC, warning DESC
```

```sql
-- app/server/queries/project_findings.sql
SELECT fingerprint, check_id, title, category, endpoint_id, object, status, severity, confidence, measure,
       first_seen, last_seen, occurrences, resolved_at
FROM {catalog}.lakebase_health.findings
WHERE project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
  AND (status IN ('new', 'open') OR (status = 'resolved' AND resolved_at >= current_timestamp() - INTERVAL 7 DAYS))
ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, last_seen DESC
```

```sql
-- app/server/queries/project_computes.sql
SELECT endpoint_id, endpoint_name, branch_name, endpoint_type, current_state, min_cu, max_cu, suspend_timeout_s,
       ha_min, restore_window_s, is_protected, enrolled, captured_at
FROM {catalog}.lakebase_health.inventory
WHERE run_id = (SELECT max_by(run_id, captured_at) FROM {catalog}.lakebase_health.inventory)
  AND project_id = :project_id AND array_contains(split(:project_ids, ','), project_id)
ORDER BY branch_name, endpoint_type DESC, endpoint_id
```

```sql
-- app/server/queries/finding.sql
SELECT fingerprint, check_id, title, category, project_id, branch_id, endpoint_id, object, status, severity,
       confidence, measure, first_seen, last_seen, last_checked_at, occurrences, evidence, query_text, explain,
       candidate_sql, rollback_sql, resolved_at
FROM {catalog}.lakebase_health.findings
WHERE fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), project_id)
```

```sql
-- app/server/queries/finding_history.sql
SELECT ts, measure, severity, status
FROM {catalog}.lakebase_health.check_results
WHERE fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), project_id)
  AND ts >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY ts
```

```sql
-- app/server/queries/finding_actions.sql
WITH sl AS (
  SELECT action_id, max_by(would_allow_level, evaluated_at) AS would_allow_level,
         max_by(would_execute, evaluated_at) AS would_execute, max_by(reason, evaluated_at) AS reason,
         max(evaluated_at) AS evaluated_at
  FROM {catalog}.lakebase_health.shadow_log GROUP BY action_id
)
SELECT a.action_id, a.kind, a.sql, a.rollback_sql, a.risk, a.reversible, a.verify_on_branch, a.state, a.expires_at,
       sl.would_allow_level, sl.would_execute, sl.reason, sl.evaluated_at
FROM {catalog}.lakebase_health.proposed_actions a
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
LEFT JOIN sl ON sl.action_id = a.action_id
WHERE a.fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY a.created_at DESC
LIMIT 5
```

```sql
-- app/server/queries/finding_related.sql
SELECT o.fingerprint, o.check_id, o.title, o.severity, o.object, o.last_seen
FROM {catalog}.lakebase_health.findings f
JOIN {catalog}.lakebase_health.findings o ON o.endpoint_id = f.endpoint_id AND o.fingerprint <> f.fingerprint
WHERE f.fingerprint = :fingerprint AND o.status IN ('new', 'open')
  AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY CASE o.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, o.last_seen DESC
LIMIT 20
```

```sql
-- app/server/queries/endpoint_project.sql
SELECT project_id FROM (
  SELECT project_id FROM {catalog}.lakebase_health.inventory WHERE endpoint_id = :endpoint_id
  UNION
  SELECT project_id FROM {catalog}.lakebase_health.raw_compute_gauges
  WHERE endpoint_id = :endpoint_id AND ts >= current_timestamp() - INTERVAL 7 DAYS
)
WHERE array_contains(split(:project_ids, ','), project_id)
LIMIT 1
```

```sql
-- app/server/queries/timeline_waits.sql
SELECT date_trunc('MINUTE', ts) AS minute, coalesce(wait_event_type, 'CPU') AS wait_class, count(*) / 60.0 AS avg_sessions
FROM {catalog}.lakebase_health.raw_active_session_history
WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts
  AND array_contains(split(:project_ids, ','), project_id) AND backend_state = 2
GROUP BY ALL
ORDER BY minute
```

```sql
-- app/server/queries/timeline_locks.sql
SELECT date_trunc('MINUTE', w.ts) AS minute,
       CASE WHEN b.pid IS NULL THEN 'not_sampled' WHEN b.wait_event = 'SyncRep' THEN 'commit_wait'
            WHEN b.backend_state IN (3, 4) THEN 'idle_in_transaction' ELSE 'active' END AS blocker_class,
       count(*) AS samples, max(w.query_age_ms) / 1000.0 AS max_wait_s
FROM {catalog}.lakebase_health.raw_active_session_history w
LEFT JOIN {catalog}.lakebase_health.raw_active_session_history b
  ON b.endpoint_id = w.endpoint_id AND b.ts = w.ts AND b.pid = w.blocking_pid AND b.ts >= :from_ts AND b.ts < :to_ts
WHERE w.endpoint_id = :endpoint_id AND w.ts >= :from_ts AND w.ts < :to_ts
  AND array_contains(split(:project_ids, ','), w.project_id)
  AND w.wait_event_type = 'Lock' AND w.wait_event IN ('tuple', 'transactionid')
GROUP BY ALL
ORDER BY minute
```

```sql
-- app/server/queries/timeline_commits.sql
SELECT date_trunc('MINUTE', ts) AS minute, count(*) AS samples, count(DISTINCT pid) AS sessions,
       max(query_age_ms) / 1000.0 AS max_statement_age_s
FROM {catalog}.lakebase_health.raw_active_session_history
WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts
  AND array_contains(split(:project_ids, ','), project_id) AND wait_event = 'SyncRep'
GROUP BY ALL
ORDER BY minute
```

```sql
-- app/server/queries/timeline_events.sql
SELECT ts,
       CASE WHEN sqlstate = '57014' AND message LIKE 'canceling statement due to statement timeout%' THEN 'statement_timeout'
            WHEN message RLIKE '(?i)^(SSL error: unexpected eof|could not accept SSL connection: (EOF detected|Connection reset))' THEN 'ssl_eof'
            WHEN message RLIKE '^(could not receive data from client|could not send data to client)' THEN 'connection_reset'
            WHEN message RLIKE '^(connection to client lost|unexpected EOF on client connection)' THEN 'client_lost'
            WHEN message RLIKE '^(the database system is starting up|terminating walreceiver)' THEN 'compute_restart' END AS kind,
       sqlstate, compute_mode
FROM {catalog}.lakebase_health.raw_postgres_logs
WHERE endpoint_id = :endpoint_id AND ts >= :from_ts AND ts < :to_ts
  AND array_contains(split(:project_ids, ','), project_id)
  AND sqlstate IN ('57014', '08006', '08P01', '57P01', '57P03')
QUALIFY kind IS NOT NULL
ORDER BY ts
LIMIT 5000
```

```sql
-- app/server/queries/timeline_gauges.sql
SELECT date_trunc('MINUTE', g.ts) AS minute, max(g.max_connections) AS max_connections,
       max(c.host_cpu_seconds) / 60.0 AS cpu_cores_used
FROM {catalog}.lakebase_health.raw_compute_gauges g
LEFT JOIN {catalog}.lakebase_health.raw_compute_counters c
  ON c.endpoint_id = g.endpoint_id AND date_trunc('MINUTE', c.ts) = date_trunc('MINUTE', g.ts)
 AND c.ts >= :from_ts AND c.ts < :to_ts
WHERE g.endpoint_id = :endpoint_id AND g.ts >= :from_ts AND g.ts < :to_ts
  AND array_contains(split(:project_ids, ','), g.project_id)
GROUP BY ALL
ORDER BY minute
```

```sql
-- app/server/queries/timeline_samples.sql
SELECT w.ts, w.pid, cast(w.queryid AS STRING) AS queryid, w.wait_event_type, w.wait_event, w.backend_state,
       w.blocking_pid, w.query_age_ms, w.xact_age_ms
FROM {catalog}.lakebase_health.raw_active_session_history w
WHERE w.endpoint_id = :endpoint_id AND w.ts >= :at_ts AND w.ts < :at_ts + INTERVAL 1 SECOND
  AND array_contains(split(:project_ids, ','), w.project_id)
ORDER BY w.query_age_ms DESC
LIMIT 200
```

- [ ] **Step 4: Run the live tests and fix any SQL error against the real schema**

Run: `cd app && npm run test:live && npx vitest run server/tests/readonly.test.ts`
Expected: all live tests PASS within the 5 s budget; read-only guard PASS. A column-name error means the SQL is wrong for the real schema: fix the SQL (the engine's `ddl.py` and `raw_*` columns are the authority), never the test's expectations.

- [ ] **Step 5: Commit**

```bash
git add app/server/queries app/server/tests/live app/package.json
git commit -m "feat(app): project-scoped queries for fleet, project, finding and timeline views

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: Route handlers and server wiring

**Files:**
- Create: `app/server/lib/handlers.ts`, `app/server/routes.ts`, `app/server/tests/handlers.test.ts`
- Modify: `app/server/server.ts`

**Interfaces:**
- Consumes: `createAccess`, `AccessError`, `Access` (Task 3); `maskRows` (Task 4); `createRunner` (Task 5); validators (Task 2); query names (Task 6).
- Produces (JSON routes for Tasks 8–10):
  - `GET /api/meta` → `{ lastSuccessAt: string|null, lastStatus: string|null, stale: boolean, openFindings: number, pii: boolean }`
  - `GET /api/fleet` → `{ projects: FleetRow[] }` (FleetRow = fleet.sql columns + `project_name` filled from access names)
  - `GET /api/projects/:id` → `{ project: { id, name }, findings: Row[], computes: Row[] }`
  - `GET /api/findings/:fp` → `{ finding: Row, history: Row[], actions: Row[], related: Row[] }`
  - `GET /api/endpoints/:id/timeline?from&to` → `{ endpointId, from, to, waits, locks, commits, events, gauges }`
  - `GET /api/endpoints/:id/samples?at` → `{ samples: Row[] }`
  - `POST /api/access/refresh` → `204`
  - Errors: `{ error: string }` with 400 (bad input), 401 (no/expired token), 404 (not visible or missing), 502 (upstream failure)
  - `type Ctx = { token?: string; user?: string; now: Date }`, `createHandlers(deps: { access, runner, scheduleMinutes: number })`

- [ ] **Step 1: Write the failing tests**

```ts
// app/server/tests/handlers.test.ts
import { describe, expect, it } from "vitest";
import { AccessError } from "../lib/access";
import { createHandlers, HttpError } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const OTHER = "11111111-2222-3333-4444-555555555555";
const FP = "cc59509434e9e437c838cc12b394bafa";
const NOW = new Date("2026-10-28T12:00:00Z");
const ctx = { token: "t", user: "alice", now: NOW };

function deps(opts: { pii?: boolean; rows?: Record<string, Record<string, unknown>[]>; fail?: Error } = {}) {
  const calls: { name: string; ids: string[]; params: Record<string, unknown> }[] = [];
  return {
    calls,
    access: {
      async resolve(token?: string) {
        if (opts.fail) throw opts.fail;
        if (!token) throw new AccessError(401, "no token");
        return { userKey: "alice", projectIds: [HC], projectNames: { [HC]: "healthcheck" }, pii: !!opts.pii };
      },
      refresh() {},
    },
    runner: {
      async run(name: string, ids: string[], params: Record<string, unknown> = {}) {
        calls.push({ name, ids, params });
        return opts.rows?.[name] ?? [];
      },
    },
    scheduleMinutes: 60,
  };
}

describe("handlers", () => {
  it("always passes the caller's allow-list, never a client value", async () => {
    const d = deps({ rows: { fleet: [{ project_id: HC, critical: "2" }] } });
    const out = await createHandlers(d).fleet(ctx);
    expect(d.calls[0]).toMatchObject({ name: "fleet", ids: [HC] });
    expect(out.projects[0].project_name).toBe("healthcheck");
  });

  it("returns 404 for a project outside the allow-list without querying", async () => {
    const d = deps();
    await expect(createHandlers(d).project(ctx, OTHER)).rejects.toMatchObject({ status: 404 });
    expect(d.calls).toEqual([]);
  });

  it("returns 404 for a fingerprint the user cannot see, and 400 for a malformed one", async () => {
    const h = createHandlers(deps({ rows: { finding: [] } }));
    await expect(h.finding(ctx, FP)).rejects.toMatchObject({ status: 404 });
    await expect(h.finding(ctx, "zz")).rejects.toMatchObject({ status: 400 });
  });

  it("masks finding text for non-PII users and not for PII members", async () => {
    const rows = { finding: [{ fingerprint: FP, project_id: HC, query_text: "SELECT ssn", evidence: '{"statement":"x"}' }] };
    const masked = await createHandlers(deps({ rows })).finding(ctx, FP);
    expect(masked.finding.query_text).toBe("[masked]");
    expect(JSON.parse(String(masked.finding.evidence)).statement).toBe("[masked]");
    const open = await createHandlers(deps({ rows, pii: true })).finding(ctx, FP);
    expect(open.finding.query_text).toBe("SELECT ssn");
  });

  it("checks the timeline endpoint's project, clamps the window and masks samples", async () => {
    const d = deps({ rows: { endpoint_project: [{ project_id: HC }], timeline_samples: [{ pid: "1", query_text: "q" }] } });
    const h = createHandlers(d);
    const out = await h.timeline(ctx, "ep-sample-primary-a1b2c3d4", "2026-10-01T00:00:00Z", undefined);
    expect(out.from).toBe("2026-10-21T12:00:00.000Z");
    expect(d.calls.find((c) => c.name === "timeline_locks")?.params).toMatchObject({ endpoint_id: "ep-sample-primary-a1b2c3d4" });
    await expect(h.timeline(ctx, "ep-sample-primary-a1b2c3d4", "2026-10-28T10:00:00Z", "2026-10-28T09:00:00Z")).rejects.toMatchObject({ status: 400 });
    const s = await h.samples(ctx, "ep-sample-primary-a1b2c3d4", "2026-10-28T11:00:00Z");
    expect(s.samples[0].query_text).toBe("[masked]");
  });

  it("returns 404 for an endpoint whose project is not visible", async () => {
    const h = createHandlers(deps({ rows: { endpoint_project: [] } }));
    await expect(h.timeline(ctx, "ep-other-x", undefined, undefined)).rejects.toMatchObject({ status: 404 });
  });

  it("propagates 401 without a token and never falls back", async () => {
    const h = createHandlers(deps());
    await expect(h.fleet({ now: NOW })).rejects.toMatchObject({ status: 401 });
    await expect(createHandlers(deps({ fail: new AccessError(502, "api down") })).fleet(ctx)).rejects.toMatchObject({ status: 502 });
  });

  it("flags the engine as stale after two missed schedules", async () => {
    const fresh = await createHandlers(deps({ rows: { meta: [{ last_success_at: "2026-10-28T11:00:00Z", open_findings: "3" }] } })).meta(ctx);
    expect(fresh).toMatchObject({ stale: false, openFindings: 3 });
    const stale = await createHandlers(deps({ rows: { meta: [{ last_success_at: "2026-10-28T09:30:00Z" }] } })).meta(ctx);
    expect(stale.stale).toBe(true);
    const never = await createHandlers(deps({ rows: { meta: [] } })).meta(ctx);
    expect(never).toMatchObject({ lastSuccessAt: null, stale: true });
  });

  it("HttpError carries the status", () => {
    expect(new HttpError(404, "x").status).toBe(404);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && npx vitest run server/tests/handlers.test.ts`
Expected: FAIL — cannot find module `../lib/handlers`.

- [ ] **Step 3: Implement the handlers**

```ts
// app/server/lib/handlers.ts
import { AccessError, type Access } from "./access";
import { maskRows } from "./mask";
import { parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "./validate";

type Row = Record<string, unknown>;
export type Ctx = { token?: string; user?: string; now: Date };
type Deps = {
  access: { resolve(token: string | undefined, user: string | undefined): Promise<Access>; refresh(user: string): void };
  runner: { run(name: string, ids: string[], params?: Record<string, string | Date>): Promise<Row[]> };
  scheduleMinutes: number;
};

export class HttpError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

const num = (v: unknown) => (v === null || v === undefined ? 0 : Number(v));

export function createHandlers({ access, runner, scheduleMinutes }: Deps) {
  async function who(ctx: Ctx): Promise<Access> {
    try {
      return await access.resolve(ctx.token, ctx.user);
    } catch (e) {
      if (e instanceof AccessError) throw new HttpError(e.status, e.message);
      throw e;
    }
  }

  async function endpointAccess(ctx: Ctx, rawId: string) {
    const a = await who(ctx);
    const endpointId = parseEndpointId(rawId);
    if (!endpointId) throw new HttpError(400, "invalid endpoint id");
    const [hit] = await runner.run("endpoint_project", a.projectIds, { endpoint_id: endpointId });
    if (!hit) throw new HttpError(404, "not found");
    return { a, endpointId };
  }

  return {
    async meta(ctx: Ctx) {
      const a = await who(ctx);
      const [m] = await runner.run("meta", a.projectIds);
      const last = m?.last_success_at ? new Date(String(m.last_success_at)) : null;
      const stale = !last || ctx.now.getTime() - last.getTime() > 2 * scheduleMinutes * 60_000;
      return {
        lastSuccessAt: last ? last.toISOString() : null, lastStatus: (m?.last_status as string) ?? null,
        stale, openFindings: num(m?.open_findings), pii: a.pii,
      };
    },

    async fleet(ctx: Ctx) {
      const a = await who(ctx);
      const rows = await runner.run("fleet", a.projectIds);
      return { projects: rows.map((r) => ({ ...r, project_name: a.projectNames[String(r.project_id)] ?? r.project_name ?? r.project_id })) };
    },

    async project(ctx: Ctx, rawId: string) {
      const a = await who(ctx);
      const id = parseProjectId(rawId);
      if (!id) throw new HttpError(400, "invalid project id");
      if (!a.projectIds.includes(id)) throw new HttpError(404, "not found");
      const [findings, computes] = await Promise.all([
        runner.run("project_findings", a.projectIds, { project_id: id }),
        runner.run("project_computes", a.projectIds, { project_id: id }),
      ]);
      return { project: { id, name: a.projectNames[id] ?? id }, findings, computes };
    },

    async finding(ctx: Ctx, rawFp: string) {
      const a = await who(ctx);
      const fp = parseFingerprint(rawFp);
      if (!fp) throw new HttpError(400, "invalid fingerprint");
      const [finding] = await runner.run("finding", a.projectIds, { fingerprint: fp });
      if (!finding) throw new HttpError(404, "not found");
      const [history, actions, related] = await Promise.all([
        runner.run("finding_history", a.projectIds, { fingerprint: fp }),
        runner.run("finding_actions", a.projectIds, { fingerprint: fp }),
        runner.run("finding_related", a.projectIds, { fingerprint: fp }),
      ]);
      return { finding: maskRows([finding], a.pii)[0], history, actions, related };
    },

    async timeline(ctx: Ctx, rawId: string, from: unknown, to: unknown) {
      const { a, endpointId } = await endpointAccess(ctx, rawId);
      const w = parseWindow(from, to, ctx.now);
      if ("error" in w) throw new HttpError(400, w.error);
      const p = { endpoint_id: endpointId, from_ts: w.from, to_ts: w.to };
      const [waits, locks, commits, events, gauges] = await Promise.all([
        runner.run("timeline_waits", a.projectIds, p), runner.run("timeline_locks", a.projectIds, p),
        runner.run("timeline_commits", a.projectIds, p), runner.run("timeline_events", a.projectIds, p),
        runner.run("timeline_gauges", a.projectIds, p),
      ]);
      return { endpointId, from: w.from.toISOString(), to: w.to.toISOString(), waits, locks, commits, events, gauges };
    },

    async samples(ctx: Ctx, rawId: string, at: unknown) {
      const { a, endpointId } = await endpointAccess(ctx, rawId);
      const w = parseWindow(at, undefined, ctx.now);
      if ("error" in w) throw new HttpError(400, w.error);
      const rows = await runner.run("timeline_samples", a.projectIds, { endpoint_id: endpointId, at_ts: w.from });
      return { samples: maskRows(rows, a.pii) };
    },

    refresh(ctx: Ctx) {
      if (ctx.user) access.refresh(ctx.user);
    },
  };
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd app && npx vitest run server/tests/handlers.test.ts`
Expected: PASS (9 tests).

- [ ] **Step 5: Wire routes into the AppKit server**

Confirm the server API in the installed version: `cd app && npx @databricks/appkit docs ./docs/plugins/server.md | grep -n "extend"`. Expected: `appkit.server.extend((app) => …)` inside `onPluginsReady`.

```ts
// app/server/routes.ts
import type { Express, Request, Response } from "express";
import { HttpError, type Ctx, type createHandlers } from "./lib/handlers";

type Handlers = ReturnType<typeof createHandlers>;
const ctxOf = (req: Request): Ctx => ({
  token: req.header("x-forwarded-access-token") ?? undefined,
  user: req.header("x-forwarded-user") ?? undefined,
  now: new Date(),
});

function send(res: Response, work: () => Promise<unknown>) {
  work().then((body) => res.json(body)).catch((e: unknown) => {
    const status = e instanceof HttpError ? e.status : 502;
    res.status(status).json({ error: e instanceof HttpError ? e.message : "upstream error" });
  });
}

export function registerRoutes(app: Express, h: Handlers) {
  app.get("/api/meta", (req, res) => send(res, () => h.meta(ctxOf(req))));
  app.get("/api/fleet", (req, res) => send(res, () => h.fleet(ctxOf(req))));
  app.get("/api/projects/:id", (req, res) => send(res, () => h.project(ctxOf(req), req.params.id)));
  app.get("/api/findings/:fp", (req, res) => send(res, () => h.finding(ctxOf(req), req.params.fp)));
  app.get("/api/endpoints/:id/timeline", (req, res) =>
    send(res, () => h.timeline(ctxOf(req), req.params.id, req.query.from, req.query.to)));
  app.get("/api/endpoints/:id/samples", (req, res) => send(res, () => h.samples(ctxOf(req), req.params.id, req.query.at)));
  app.post("/api/access/refresh", (req, res) => {
    h.refresh(ctxOf(req));
    res.status(204).end();
  });
}
```

Edit `app/server/server.ts` so `onPluginsReady` builds the dependencies and registers the routes (keep the generated plugin list; it must include `server()` and `analytics()`):

```ts
// app/server/server.ts (onPluginsReady body)
import { join } from "node:path";
import { analytics, createApp, getWarehouseId, getWorkspaceClient, server } from "@databricks/appkit";
import { createAccess, userHttp } from "./lib/access";
import { createHandlers } from "./lib/handlers";
import { createRunner, loadQueries, statementExec } from "./lib/sql";
import { registerRoutes } from "./routes";

await createApp({
  plugins: [server(), analytics()],
  async onPluginsReady(appkit) {
    const host = process.env.DATABRICKS_HOST ?? "";
    const catalog = process.env.CATALOG ?? "";
    const runner = createRunner(loadQueries(join(__dirname, "queries"), catalog), statementExec(getWorkspaceClient(), getWarehouseId()));
    const handlers = createHandlers({
      access: createAccess({ http: userHttp(host.startsWith("http") ? host : `https://${host}`) }),
      runner,
      scheduleMinutes: Number(process.env.SCHEDULE_MINUTES ?? "60"),
    });
    appkit.server.extend((app) => registerRoutes(app, handlers));
  },
});
```
Make sure the build copies `server/queries/*.sql` next to the compiled server (check the generated build script; if it only compiles TS, add a copy step such as `"postbuild": "cp -r server/queries dist/server/"` matching the generated output path). Add `CATALOG` and `SCHEDULE_MINUTES` to `app.yaml` `env` (`CATALOG: my_catalog` is set from the bundle in Task 11).

Run: `cd app && npx tsc --noEmit -p . && npm test && npm run build`
Expected: `tsc` clean, all unit tests pass, build succeeds and `server/queries` is present in the build output.

- [ ] **Step 6: Commit**

```bash
git add app/server
git commit -m "feat(app): scoped JSON routes with masking, 401/404 handling and stale-engine flag

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: Client shell, fleet overview and project page

**Files:**
- Create: `app/client/src/api.ts`, `app/client/src/views/Fleet.tsx`, `app/client/src/views/Project.tsx`, `app/client/src/views/Fleet.test.tsx`, `app/client/src/views/Project.test.tsx`, `app/client/src/fixtures/{meta,fleet,project}.json`
- Modify: `app/client/src/App.tsx`, `app/package.json` (devDeps `@testing-library/react`, `jsdom`)

**Interfaces:**
- Consumes: routes from Task 7.
- Produces: `useApi<T>(path: string | null): { data?: T; error?: { status: number; message: string }; loading: boolean }`; route paths `/`, `/projects/:id`, `/findings/:fp`, `/endpoints/:id/timeline`.

- [ ] **Step 1: Record fixtures from the live server**

Run the app locally (`cd app && npm run dev`, which injects your user token in development), then save JSON:
```bash
curl -s localhost:8000/api/meta > client/src/fixtures/meta.json
curl -s localhost:8000/api/fleet | python3 -c "import json,sys;d=json.load(sys.stdin);d['projects']=[p for p in d['projects'] if p['project_id']=='11111111-2222-4333-8444-555555555555'];print(json.dumps(d))" > client/src/fixtures/fleet.json
curl -s localhost:8000/api/projects/11111111-2222-4333-8444-555555555555 > client/src/fixtures/project.json
```
(Use the port printed by `npm run dev`.) Expected: three non-empty JSON files; `fleet.json` has one project.

- [ ] **Step 2: Write the failing component tests**

Before writing UI code, read the AppKit UI component list: `npx @databricks/appkit docs "appkit-ui API reference"` and use its table/badge/card components where they exist.

```tsx
// app/client/src/views/Fleet.test.tsx
// @vitest-environment jsdom
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import fleet from "../fixtures/fleet.json";
import meta from "../fixtures/meta.json";
import { FleetView } from "./Fleet";

describe("FleetView", () => {
  it("lists visible projects with severity counts and links", () => {
    render(<MemoryRouter><FleetView fleet={fleet} meta={meta} /></MemoryRouter>);
    expect(screen.getByRole("link", { name: "healthcheck" })).toHaveProperty("href", expect.stringContaining("/projects/11111111"));
    expect(screen.getByText(/critical/i)).toBeTruthy();
  });

  it("explains access when no projects are visible", () => {
    render(<MemoryRouter><FleetView fleet={{ projects: [] }} meta={meta} /></MemoryRouter>);
    expect(screen.getByText(/no Lakebase projects you can see/i)).toBeTruthy();
  });
});
```

```tsx
// app/client/src/views/Project.test.tsx
// @vitest-environment jsdom
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import project from "../fixtures/project.json";
import { ProjectView } from "./Project";

describe("ProjectView", () => {
  it("groups findings by category and shows the computes panel", () => {
    render(<MemoryRouter><ProjectView data={project} /></MemoryRouter>);
    expect(screen.getByRole("heading", { name: "healthcheck" })).toBeTruthy();
    expect(screen.getByRole("heading", { name: /indexing/i })).toBeTruthy();
    expect(screen.getByText("ep-sample-primary-a1b2c3d4")).toBeTruthy();
  });

  it("shows enrolment instructions for a compute that is not enrolled", () => {
    const data = { ...project, computes: [{ ...project.computes[0], enrolled: "false" }] };
    render(<MemoryRouter><ProjectView data={data} /></MemoryRouter>);
    expect(screen.getByText(/GRANT pg_monitor/)).toBeTruthy();
  });
});
```
Install: `npm install -D @testing-library/react jsdom` (and `react-router-dom` if the template does not include a router; check `package.json` first).

- [ ] **Step 3: Run to verify they fail**

Run: `cd app && npx vitest run client/src/views`
Expected: FAIL — cannot find `./Fleet` / `./Project`.

- [ ] **Step 4: Implement `api.ts`, the views and the router**

```ts
// app/client/src/api.ts
import { useEffect, useState } from "react";

export type ApiError = { status: number; message: string };

export function useApi<T>(path: string | null) {
  const [state, setState] = useState<{ data?: T; error?: ApiError; loading: boolean }>({ loading: path !== null });
  useEffect(() => {
    if (!path) return;
    let live = true;
    setState({ loading: true });
    fetch(path)
      .then(async (r) => {
        const body: unknown = await r.json().catch(() => ({}));
        if (!live) return;
        if (!r.ok) {
          const message = typeof body === "object" && body !== null && "error" in body ? String(body.error) : r.statusText;
          setState({ loading: false, error: { status: r.status, message } });
        } else setState({ loading: false, data: body as T });
      })
      .catch((e: unknown) => live && setState({ loading: false, error: { status: 0, message: String(e) } }));
    return () => { live = false; };
  }, [path]);
  return state;
}
```

`Fleet.tsx` exports `FleetView({ fleet, meta })` (pure, used by tests) and a default `Fleet` that calls `useApi("/api/fleet")` and `useApi("/api/meta")` and renders `FleetView`. The table columns: project (link to `/projects/:id`), critical, warning, info, newest finding, computes (`active/total`), Postgres checks (`enrolled` → "on"/"off"). Rows sorted as returned. Empty state text: "There are no Lakebase projects you can see. Ask a project owner for CAN_USE, then use Refresh access." with a button that `POST`s `/api/access/refresh` and reloads.

`Project.tsx` exports `ProjectView({ data })` and a default `Project` using `useApi(`/api/projects/${id}`)`. Findings grouped by `category` under `<h2>` headings (category text), each row: severity badge, title (link to `/findings/:fingerprint`), object, first/last seen, occurrences, confidence. Findings with `status === "resolved"` go in a collapsed `<details>` "Recently resolved (7 days)". Computes panel: one row per compute with state, `min_cu–max_cu`, suspend timeout, HA, restore window, protected, enrolled; when `enrolled` is falsy (`false` or `"false"`), show `GRANT pg_monitor TO "<engine SP application ID>";` plus a link to the README section.

`App.tsx`: router with the four routes, a header showing `meta.lastSuccessAt` ("Data as of …"), and a banner "The health engine has not completed a run in over 2 hours" when `meta.stale`. On any `error.status === 401` show "Your session could not be verified; reload the page." On 404 show "Not found or not visible to you." When `meta.lastSuccessAt` is null show the setup page text: "The health engine has not run yet. Install it with the steps in the README."

- [ ] **Step 5: Run to verify they pass**

Run: `cd app && npx vitest run client/src/views && npx tsc --noEmit -p .`
Expected: PASS (4 tests); `tsc` clean.

- [ ] **Step 6: Commit**

```bash
git add app/client app/package.json app/package-lock.json
git commit -m "feat(app): fleet overview and project page

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Finding detail

**Files:**
- Create: `app/client/src/views/Finding.tsx`, `app/client/src/views/Finding.test.tsx`, `app/client/src/fixtures/finding.json`, `app/client/src/fixtures/finding_masked.json`
- Modify: `app/client/src/App.tsx` (route already declared in Task 8; point it at the view)

**Interfaces:**
- Consumes: `GET /api/findings/:fp` (Task 7), `useApi` (Task 8).
- Produces: `FindingView({ data })`; `TIMELINE_CHECKS = new Set(["hot_row_lock","commit_quorum_wait","statement_timeouts","gin_cleanup_lock_wait","connection_drops"])`.

- [ ] **Step 1: Record fixtures**

With `npm run dev` running: pick a healthcheck `hot_row_lock` fingerprint from `client/src/fixtures/project.json` and a `top_db_time_query` fingerprint (has query text):
```bash
curl -s localhost:8000/api/findings/<hot_row_lock fingerprint> > client/src/fixtures/finding.json
python3 - <<'EOF'
import json; d=json.load(open("client/src/fixtures/finding.json")); d["finding"]["query_text"]="[masked]"
json.dump(d, open("client/src/fixtures/finding_masked.json","w"))
EOF
```
Expected: `finding.json` has `finding.check_id == "hot_row_lock"` and a non-empty `history`.

- [ ] **Step 2: Write the failing tests**

```tsx
// app/client/src/views/Finding.test.tsx
// @vitest-environment jsdom
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import finding from "../fixtures/finding.json";
import masked from "../fixtures/finding_masked.json";
import { FindingView } from "./Finding";

describe("FindingView", () => {
  it("shows explanation, evidence and the likely cause", () => {
    render(<MemoryRouter><FindingView data={finding} /></MemoryRouter>);
    expect(screen.getByRole("heading", { name: finding.finding.title })).toBeTruthy();
    expect(screen.getByText(/likely_cause/)).toBeTruthy();
  });

  it("links incident findings to the endpoint timeline around last_seen", () => {
    render(<MemoryRouter><FindingView data={finding} /></MemoryRouter>);
    const link = screen.getByRole("link", { name: /incident timeline/i });
    expect(link.getAttribute("href")).toContain(`/endpoints/${finding.finding.endpoint_id}/timeline?from=`);
  });

  it("renders masked text as masked and candidate SQL as copyable text only", () => {
    const data = { ...masked, finding: { ...masked.finding, candidate_sql: "DROP INDEX CONCURRENTLY s.i;" } };
    render(<MemoryRouter><FindingView data={data} /></MemoryRouter>);
    expect(screen.getAllByText("[masked]").length).toBeGreaterThan(0);
    expect(screen.getByText("DROP INDEX CONCURRENTLY s.i;")).toBeTruthy();
    expect(screen.getByRole("button", { name: /copy/i })).toBeTruthy();
    expect(screen.queryByRole("button", { name: /apply|approve|run/i })).toBeNull();
  });
});
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd app && npx vitest run client/src/views/Finding.test.tsx`
Expected: FAIL — cannot find `./Finding`.

- [ ] **Step 4: Implement**

`Finding.tsx` exports `FindingView({ data })` and default `Finding` (uses `useApi(`/api/findings/${fp}`)`). Sections in order:
1. `<h1>` title; severity, confidence, status, first/last seen, occurrences; project and endpoint.
2. "What this means": `finding.explain`.
3. "Measure over time": an AppKit line chart in data mode over `history` (`ts` → `measure`), with the severity of each point as the tooltip.
4. "Evidence": `JSON.parse(finding.evidence)` rendered as a key/value table (strings shown verbatim, including `[masked]`); `finding.query_text` in a `<pre>` when present.
5. "Candidate fix" when `candidate_sql` is set: `<pre>` with the SQL and rollback, risk and verify-on-branch flags from `actions[0]`, a "Copy SQL" button (`navigator.clipboard.writeText`), and the note "Approval and one-click apply arrive with the fix loop. Run this yourself only after testing it on a branch."
6. "Shadow mode": `actions[0].would_execute`, `would_allow_level`, `reason`.
7. When `TIMELINE_CHECKS.has(finding.check_id)`: link "Open incident timeline" to `/endpoints/${endpoint_id}/timeline?from=<last_seen − 1 h ISO>&to=<last_seen + 15 min ISO>`.
8. "Related findings on this endpoint": list linking to each fingerprint.

- [ ] **Step 5: Run to verify it passes**

Run: `cd app && npx vitest run client/src/views && npx tsc --noEmit -p .`
Expected: PASS (all view tests); `tsc` clean.

- [ ] **Step 6: Commit**

```bash
git add app/client
git commit -m "feat(app): finding detail with history, evidence, candidate fix text and shadow verdict

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 10: Incident timeline

**Files:**
- Create: `app/client/src/views/Timeline.tsx`, `app/client/src/views/Timeline.test.tsx`, `app/client/src/fixtures/timeline.json`, `app/client/src/fixtures/samples.json`

**Interfaces:**
- Consumes: `GET /api/endpoints/:id/timeline`, `GET /api/endpoints/:id/samples` (Task 7), `useApi` (Task 8).
- Produces: `TimelineView({ data, onPick })`, `BLOCKER_COLORS: Record<"commit_wait"|"idle_in_transaction"|"active"|"not_sampled", string>`.

- [ ] **Step 1: Record fixtures for the workload window**

```bash
FROM=$(python3 -c "import datetime as d;print((d.datetime.utcnow()-d.timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
curl -s "localhost:8000/api/endpoints/ep-sample-primary-a1b2c3d4/timeline?from=$FROM" > client/src/fixtures/timeline.json
AT=$(python3 -c "import json;d=json.load(open('client/src/fixtures/timeline.json'));print(d['locks'][0]['minute'])")
curl -s "localhost:8000/api/endpoints/ep-sample-primary-a1b2c3d4/samples?at=$AT" > client/src/fixtures/samples.json
```
Expected: `timeline.json` has non-empty `locks` and `events` containing `statement_timeout`.

- [ ] **Step 2: Write the failing tests**

```tsx
// app/client/src/views/Timeline.test.tsx
// @vitest-environment jsdom
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import samples from "../fixtures/samples.json";
import timeline from "../fixtures/timeline.json";
import { SamplesTable, TimelineView } from "./Timeline";

describe("TimelineView", () => {
  it("renders the tracks and the freshness note", () => {
    render(<TimelineView data={timeline} onPick={() => {}} />);
    for (const name of [/active sessions/i, /lock waits/i, /commit waits/i, /events/i, /cpu/i]) {
      expect(screen.getByRole("heading", { name })).toBeTruthy();
    }
    expect(screen.getByText(/up to one engine cycle behind/i)).toBeTruthy();
  });

  it("counts statement timeouts and lists blocker classes in the legend", () => {
    render(<TimelineView data={timeline} onPick={() => {}} />);
    const n = timeline.events.filter((e: { kind: string }) => e.kind === "statement_timeout").length;
    expect(screen.getByText(new RegExp(`${n} statement timeouts?`))).toBeTruthy();
    expect(screen.getByText(/idle in transaction/i)).toBeTruthy();
  });

  it("asks for the samples of a minute when a lock bucket is picked", () => {
    const onPick = vi.fn();
    render(<TimelineView data={timeline} onPick={onPick} />);
    fireEvent.click(screen.getAllByRole("button", { name: /show sessions/i })[0]);
    expect(onPick).toHaveBeenCalledWith(timeline.locks[0].minute);
  });

  it("shows sampled sessions with masked text", () => {
    render(<SamplesTable samples={[...samples.samples, { pid: "9", query_text: "[masked]" }]} />);
    expect(screen.getByText("[masked]")).toBeTruthy();
  });
});
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd app && npx vitest run client/src/views/Timeline.test.tsx`
Expected: FAIL — cannot find `./Timeline`.

- [ ] **Step 4: Implement**

`Timeline.tsx` exports `TimelineView({ data, onPick })`, `SamplesTable({ samples })`, `BLOCKER_COLORS`, and default `Timeline` (reads `:id`, `from`, `to` from the URL; window controls for 1 h / 6 h / 24 h / 7 d that rewrite the query string; calls `useApi` for the timeline and, after `onPick(minute)`, for `/api/endpoints/:id/samples?at=<minute>`). Tracks, each under an `<h2>`, all with the same x-axis domain `[data.from, data.to]`:
1. "Active sessions by wait class": stacked area from `waits` (`minute`, `wait_class`, `avg_sessions`).
2. "Lock waits by blocker": stacked bars from `locks` coloured by `BLOCKER_COLORS` (`commit_wait` "commit waiting on storage quorum", `idle_in_transaction` "idle in transaction", `active` "active writer", `not_sampled` "holder not sampled"), with a "Show sessions" button per bucket calling `onPick(minute)`.
3. "Commit waits (SyncRep)": bars from `commits` (`minute`, `sessions`, `max_statement_age_s`), with a note that long waits here are on the platform side (see `commit_quorum_wait`) and that `locks` buckets coloured `commit_wait` are writers queued behind these commits.
4. "Events": markers from `events` grouped by `kind`, with a summary line such as "3 statement timeouts, 12 connection drops".
5. "CPU and connections": lines from `gauges` (`cpu_cores_used`, `max_connections`).
Footer: "Telemetry comes from the health engine's copy and is up to one engine cycle behind."
`SamplesTable`: columns ts, pid, queryid, wait, backend state, blocker pid, query age (s), transaction age (s), and `query_text` when present.

- [ ] **Step 5: Run to verify it passes**

Run: `cd app && npm test && npx tsc --noEmit -p .`
Expected: all unit and component tests PASS; `tsc` clean.

- [ ] **Step 6: Commit**

```bash
git add app/client
git commit -m "feat(app): incident timeline with blocker-classified lock waits and session drill-down

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 11: Deploy, grant, and run acceptance

**Files:**
- Create: `scripts/app_acceptance.py`
- Modify: `resources/app.yml` (env), `app/tests/smoke.spec.ts`, `README.md` (App section)

**Interfaces:**
- Consumes: the whole app; bundle variables `catalog`, `engine_sp`, `warehouse_id`.
- Produces: deployed app `lakebase-health` on `my-profile`; acceptance output `ALL APP ACCEPTANCE CHECKS PASSED`.

- [ ] **Step 1: Pass catalog and schedule to the app**

In `resources/app.yml` under the app add:
```yaml
      config:
        env:
          - name: CATALOG
            value: ${var.catalog}
          - name: SCHEDULE_MINUTES
            value: "60"
```
(If the generated `app.yaml` already declares `env`, move these into it instead; keep one source.)

- [ ] **Step 2: Update the smoke test selectors**

```ts
// app/tests/smoke.spec.ts — replace template assertions
await expect(page.getByRole("heading", { name: /Lakebase health/i })).toBeVisible();
await expect(page.getByText(/Data as of|has not run yet/i)).toBeVisible();
```
Run: `cd app && databricks apps validate --profile my-profile`
Expected: validation passes (typecheck, lint, build, smoke).

- [ ] **Step 3: Deploy (confirm with the user first) and grant the app SP read access**

Ask the user to confirm the deploy, then:
```bash
databricks bundle deploy --var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 --var warehouse_id=0123456789abcdef --profile my-profile
databricks bundle run lakebase_health_app --var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 --var warehouse_id=0123456789abcdef --profile my-profile
APP_SP=$(databricks apps get lakebase-health --profile my-profile -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['service_principal_client_id'])")
```
Then grant (as the catalog owner, via the warehouse):
```sql
GRANT USE CATALOG ON CATALOG my_catalog TO `<APP_SP>`;
GRANT USE SCHEMA, SELECT ON SCHEMA my_catalog.lakebase_health TO `<APP_SP>`;
```
Expected: `databricks apps get lakebase-health` shows `app_status.state: RUNNING`; opening the URL as the project owner shows the fleet.

- [ ] **Step 4: Write the acceptance script**

```python
# scripts/app_acceptance.py
"""Acceptance for the read-only app (spec section 5).

Usage: engine/.venv/bin/python scripts/app_acceptance.py --profile my-profile --warehouse 0123456789abcdef \
         --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --test-sp-id <application id> --test-sp-secret-env TEST_SP_SECRET
The test SP must have CAN_USE on the app and CAN_USE on the healthcheck project only.
"""
import argparse, json, os, sys, time, urllib.request
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

def sp_token(host, client_id, secret):
    data = f"grant_type=client_credentials&scope=all-apis&client_id={client_id}&client_secret={secret}".encode()
    req = urllib.request.Request(f"{host}/oidc/v1/token", data=data, method="POST")
    return json.load(urllib.request.urlopen(req))["access_token"]

def get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.load(r), time.time() - t
    except urllib.error.HTTPError as e:
        return e.code, None, time.time() - t

def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--test-sp-id", "--test-sp-secret-env"):
        ap.add_argument(a, required=True)
    a = ap.parse_args()
    w = WorkspaceClient(profile=a.profile)
    url = w.apps.get("lakebase-health").url.rstrip("/")
    failures = []
    owner = w.config.authenticate()["Authorization"].split()[1]
    sp = sp_token(w.config.host, a.test_sp_id, os.environ[a.test_sp_secret_env])

    s, fleet, secs = get(f"{url}/api/fleet", owner)
    if s != 200 or len(fleet["projects"]) < 2: failures.append(f"1: owner fleet status {s}, {len(fleet['projects']) if fleet else 0} projects")
    s, fleet_sp, _ = get(f"{url}/api/fleet", sp)
    ids = {p["project_id"] for p in (fleet_sp or {}).get("projects", [])}
    if s != 200 or ids != {a.project_uid}: failures.append(f"1: test SP sees {ids} (status {s})")
    other = next(p["project_id"] for p in fleet["projects"] if p["project_id"] != a.project_uid)
    s, _, _ = get(f"{url}/api/projects/{other}", sp)
    if s != 404: failures.append(f"1: test SP got {s} for another project")

    s, proj, psecs = get(f"{url}/api/projects/{a.project_uid}", owner)
    r = w.statement_execution.execute_statement(warehouse_id=a.warehouse, wait_timeout="50s", statement=
        f"SELECT count(*) FROM {a.catalog}.lakebase_health.findings WHERE project_id = '{a.project_uid}' AND (status IN ('new','open') "
        "OR (status = 'resolved' AND resolved_at >= current_timestamp() - INTERVAL 7 DAYS))")
    expected = int(r.result.data_array[0][0])
    if s != 200 or len(proj["findings"]) != expected: failures.append(f"2: project page {len(proj['findings']) if proj else None} findings, table {expected}")

    ep = next(c["endpoint_id"] for c in proj["computes"] if c["endpoint_type"].endswith("READ_WRITE"))
    frm = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 24 * 3600))
    s, tl, _ = get(f"{url}/api/endpoints/{ep}/timeline?from={frm}", owner)
    if s != 200 or not tl["locks"] or not any(e["kind"] == "statement_timeout" for e in tl["events"]):
        failures.append(f"3: timeline status {s}, locks {len(tl['locks']) if tl else 0}")

    fp = next(f["fingerprint"] for f in proj["findings"] if f["check_id"] in ("top_db_time_query", "plan_flip", "query_regression"))
    s, fd, fsecs = get(f"{url}/api/findings/{fp}", sp)
    if s != 200 or fd["finding"].get("query_text") not in (None, "[masked]"): failures.append("4: query text not masked for test SP")

    app_sp = w.apps.get("lakebase-health").service_principal_client_id
    grants = w.statement_execution.execute_statement(warehouse_id=a.warehouse, wait_timeout="50s",
        statement=f"SHOW GRANTS `{app_sp}` ON SCHEMA {a.catalog}.lakebase_health")
    privs = {row[1] for row in (grants.result.data_array or [])}
    if privs - {"USE SCHEMA", "SELECT", "USE CATALOG"}: failures.append(f"5: app SP has {privs}")

    for name, t in (("fleet", secs), ("project", psecs), ("finding", fsecs)):
        if t > 3: failures.append(f"6: {name} took {t:.1f}s")

    print("\n".join(["FAILURES:"] + failures) if failures else "ALL APP ACCEPTANCE CHECKS PASSED")
    print("note: criterion 4 (unmasked for PII members) needs a databricks_pii_access member; verified by unit tests if the group is absent")
    sys.exit(1 if failures else 0)

if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Create the test SP (confirm with the user first) and run acceptance**

Ask the user to confirm creating a second service principal `lakebase-health-app-test`, then:
```bash
databricks service-principals create --display-name lakebase-health-app-test --profile my-profile -o json
# create an OAuth secret for it in the account/workspace UI or with `databricks service-principal-secrets-proxy create <id>`
databricks permissions update apps lakebase-health --json '{"access_control_list":[{"service_principal_name":"<TEST_SP_APP_ID>","permission_level":"CAN_USE"}]}' --profile my-profile
databricks permissions update database-projects healthcheck --json '{"access_control_list":[{"service_principal_name":"<TEST_SP_APP_ID>","permission_level":"CAN_USE"}]}' --profile my-profile
TEST_SP_SECRET=<secret> engine/.venv/bin/python scripts/app_acceptance.py --profile my-profile --warehouse 0123456789abcdef \
  --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 --test-sp-id <TEST_SP_APP_ID> --test-sp-secret-env TEST_SP_SECRET
```
Expected: `ALL APP ACCEPTANCE CHECKS PASSED`. If the app proxy rejects SP tokens for user-authorized routes, record that as a ruling and run criterion 1 by opening the app as a second human user with `CAN_USE` on healthcheck only.

- [ ] **Step 6: Document and commit**

Add a README section "Read-only app": what it shows, `databricks bundle deploy --var warehouse_id=…`, the app SP grants above, user authorization scopes (`sql`, `postgres`) and the masking rule.

```bash
git add resources/app.yml app/tests/smoke.spec.ts scripts/app_acceptance.py README.md
git commit -m "feat(app): deploy read-only app with the bundle; acceptance script

Co-authored-by: Isaac <no-reply@databricks.com>"
```
