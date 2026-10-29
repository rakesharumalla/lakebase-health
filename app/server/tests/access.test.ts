import { describe, expect, it } from "vitest";
import { AccessError, createAccess, type Http } from "../lib/access";

const P1 = "/api/2.0/postgres/projects?page_size=100";
type Routes = Record<string, unknown>;

function fakeHttp(routes: Routes, calls: string[] = []): Http {
  return async (path) => {
    calls.push(path);
    const body = routes[path];
    if (body === undefined) return { status: 500, json: { message: `no route ${path}` } };
    if (typeof body === "number") return { status: body, json: {} };
    return { status: 200, json: body };
  };
}

const me = (userName: string, groups: string[] = ["users"]) => ({ userName, groups: groups.map((display) => ({ display })) });
const project = (uid: string, id: string, owner = "someone@x.com") => ({ uid, project_id: id, status: { owner } });
const branches = (id: string, names: string[]) => ({
  [`/api/2.0/postgres/projects/${id}/branches?page_size=100`]: { branches: names.map((n) => ({ name: `projects/${id}/branches/${n}` })) },
});
const roles = (id: string, branch: string, list: [string, string][]) => ({
  [`/api/2.0/postgres/projects/${id}/branches/${branch}/roles?page_size=100`]: {
    roles: list.map(([postgres_role, identity_type]) => ({ status: { postgres_role, identity_type } })),
  },
});
const ME = "/api/2.0/preview/scim/v2/Me";

describe("createAccess", () => {
  it("workspace admins see every page of projects", async () => {
    const http = fakeHttp({
      [ME]: me("admin@x.com", ["admins"]),
      [P1]: { projects: [project("u1", "a")], next_page_token: "t2" },
      [`${P1}&page_token=t2`]: { projects: [project("u2", "b")] },
    });
    const a = await createAccess({ http }).resolve("tok", "admin");
    expect(a.projectIds).toEqual(["u1", "u2"]);
    expect(a.projectNames).toEqual({ u1: "a", u2: "b" });
  });

  it("others see projects they own or hold a Postgres role on (as user or via a group)", async () => {
    const http = fakeHttp({
      [ME]: me("alice@x.com", ["users", "team-pay"]),
      [P1]: { projects: [project("u1", "owned", "alice@x.com"), project("u2", "role"), project("u3", "group"), project("u4", "none")] },
      ...branches("role", ["main", "dev"]),
      ...roles("role", "main", [["bob@x.com", "USER"]]),
      ...roles("role", "dev", [["alice@x.com", "USER"]]),
      ...branches("group", ["main"]),
      ...roles("group", "main", [["team-pay", "GROUP"]]),
      ...branches("none", ["main"]),
      ...roles("none", "main", [["bob@x.com", "USER"], ["alice@x.com", "GROUP"]]),
    });
    const a = await createAccess({ http }).resolve("tok", "alice");
    expect(a.projectIds).toEqual(["u1", "u2", "u3"]);
  });

  it("a service principal matches roles by its application id", async () => {
    const http = fakeHttp({
      [ME]: me("00000000-0000-4000-8000-0000000000a1"),
      [P1]: { projects: [project("u1", "hc")] },
      ...branches("hc", ["production"]),
      ...roles("hc", "production", [["00000000-0000-4000-8000-0000000000a1", "SERVICE_PRINCIPAL"]]),
    });
    expect((await createAccess({ http }).resolve("tok", "sp")).projectIds).toEqual(["u1"]);
  });

  it("excludes a project whose roles cannot be read, without failing the request", async () => {
    const http = fakeHttp({
      [ME]: me("alice@x.com"),
      [P1]: { projects: [project("u1", "broken"), project("u2", "ok")] },
      ...branches("broken", ["main"]),
      [`/api/2.0/postgres/projects/broken/branches/main/roles?page_size=100`]: 500,
      ...branches("ok", ["main"]),
      ...roles("ok", "main", [["alice@x.com", "USER"]]),
    });
    expect((await createAccess({ http }).resolve("tok", "alice")).projectIds).toEqual(["u2"]);
  });

  it("fails the request when the project list fails (no partial allow-list)", async () => {
    const http = fakeHttp({ [ME]: me("admin@x.com", ["admins"]), [P1]: { projects: [project("u1", "a")], next_page_token: "t2" } });
    await expect(createAccess({ http }).resolve("tok", "alice")).rejects.toMatchObject({ status: 502 });
  });

  it("rejects a missing token or a 401 with status 401", async () => {
    const access = createAccess({ http: async () => ({ status: 401, json: {} }) });
    await expect(access.resolve(undefined, "alice")).rejects.toBeInstanceOf(AccessError);
    await expect(access.resolve(undefined, "alice")).rejects.toMatchObject({ status: 401 });
    await expect(access.resolve("expired", "alice")).rejects.toMatchObject({ status: 401 });
  });

  it("detects the PII group", async () => {
    const http = fakeHttp({ [ME]: me("bob@x.com", ["databricks_pii_access"]), [P1]: { projects: [] } });
    expect((await createAccess({ http }).resolve("tok", "bob")).pii).toBe(true);
  });

  it("lets everyone see text when PII masking is off (PII_MASKING=false), and only the group when it is on", async () => {
    const http = fakeHttp({ [ME]: me("carol@x.com", []), [P1]: { projects: [] } });
    expect((await createAccess({ http, piiMasking: false }).resolve("tok", "carol")).pii).toBe(true);
    expect((await createAccess({ http, piiMasking: true }).resolve("tok", "carol")).pii).toBe(false);
    const prev = process.env.PII_MASKING;
    try {
      process.env.PII_MASKING = "false";
      expect((await createAccess({ http }).resolve("tok", "carol")).pii).toBe(true);
      process.env.PII_MASKING = "true";
      expect((await createAccess({ http }).resolve("tok", "carol")).pii).toBe(false);
      // Same strict parse as the engine: only "false" (trimmed, any case) turns masking off.
      for (const off of ["False", " FALSE ", "false\n"]) {
        process.env.PII_MASKING = off;
        expect((await createAccess({ http }).resolve("tok", "carol")).pii).toBe(true);
      }
      for (const on of ["", "0", "no", "off", "yes", "falsey"]) {
        process.env.PII_MASKING = on;
        expect((await createAccess({ http }).resolve("tok", "carol")).pii).toBe(false);
      }
      delete process.env.PII_MASKING;
      expect((await createAccess({ http }).resolve("tok", "carol")).pii).toBe(false);
    } finally {
      if (prev === undefined) delete process.env.PII_MASKING;
      else process.env.PII_MASKING = prev;
    }
  });

  it("records workspace admins and the projects the caller owns (for query text visibility)", async () => {
    const http = fakeHttp({
      [ME]: me("alice@x.com"),
      [P1]: { projects: [project("u1", "mine", "alice@x.com"), project("u2", "role")] },
      ...branches("role", ["main"]),
      ...roles("role", "main", [["alice@x.com", "USER"]]),
    });
    const a = await createAccess({ http }).resolve("tok", "alice");
    expect(a.projectIds).toEqual(["u1", "u2"]);
    expect(a.owned).toEqual(["u1"]);
    expect(a.admin).toBe(false);
    const adminHttp = fakeHttp({ [ME]: me("root@x.com", ["admins"]), [P1]: { projects: [project("u1", "a")] } });
    const b = await createAccess({ http: adminHttp }).resolve("tok", "root");
    expect(b.admin).toBe(true);
    expect(b.owned).toEqual([]);
  });

  it("caches per user for the TTL and refresh clears it", async () => {
    let t = 0;
    const calls: string[] = [];
    const http = fakeHttp({ [ME]: me("admin@x.com", ["admins"]), [P1]: { projects: [project("u1", "a")] } }, calls);
    const access = createAccess({ http, ttlMs: 1000, now: () => t });
    const lists = () => calls.filter((c) => c === P1).length;
    await access.resolve("tok", "alice");
    await access.resolve("tok", "alice");
    expect(lists()).toBe(1);
    await access.resolve("tok", "carol");
    expect(lists()).toBe(2);
    t = 1500;
    await access.resolve("tok", "alice");
    access.refresh("alice");
    await access.resolve("tok", "alice");
    expect(lists()).toBe(4);
  });
});

describe("access under load and throttling", () => {
  const base = {
    [ME]: me("alice@x.com"),
    [P1]: { projects: [project("u1", "a")] },
    ...branches("a", ["main"]),
  };
  const rolesPath = "/api/2.0/postgres/projects/a/branches/main/roles?page_size=100";
  const aliceRole = { roles: [{ status: { postgres_role: "alice@x.com", identity_type: "USER" } }] };

  it("shares one load between concurrent requests for the same user", async () => {
    const calls: string[] = [];
    const access = createAccess({ http: fakeHttp({ ...base, [rolesPath]: aliceRole }, calls) });
    await Promise.all([access.resolve("tok", "alice"), access.resolve("tok", "alice")]);
    expect(calls.filter((c) => c === P1).length).toBe(1);
  });

  it("retries throttled calls before deciding", async () => {
    let n = 0;
    const http: Http = async (path) => {
      if (path === rolesPath && n++ < 2) return { status: 429, json: {} };
      return fakeHttp({ ...base, [rolesPath]: aliceRole })(path, "t");
    };
    const a = await createAccess({ http, retryDelayMs: 0 }).resolve("tok", "alice");
    expect(a.projectIds).toEqual(["u1"]);
    expect(a.incomplete).toBe(false);
  });

  it("marks a result incomplete and caches it only briefly when a project could not be checked", async () => {
    let t = 0;
    const calls: string[] = [];
    const access = createAccess({ http: fakeHttp({ ...base, [rolesPath]: 503 }, calls), now: () => t, retryDelayMs: 0, incompleteTtlMs: 1000 });
    const a = await access.resolve("tok", "alice");
    expect(a.projectIds).toEqual([]);
    expect(a.incomplete).toBe(true);
    t = 1500;
    await access.resolve("tok", "alice");
    expect(calls.filter((c) => c === P1).length).toBe(2);
  });
});
