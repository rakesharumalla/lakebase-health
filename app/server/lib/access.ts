export type Http = (path: string, token: string) => Promise<{ status: number; json: unknown }>;
export type Access = {
  userKey: string;
  /** SCIM userName of the caller (the identity recorded as approver). */
  userName?: string;
  projectIds: string[];
  projectNames: Record<string, string>;
  pii: boolean;
  /** Member of the workspace admins group. */
  admin?: boolean;
  /** Uids of the visible projects the caller owns. */
  owned?: string[];
  /** True when some projects could not be checked (throttling or upstream errors) and are hidden for now. */
  incomplete?: boolean;
};

export class AccessError extends Error {
  constructor(
    public status: 401 | 502,
    message: string,
    public upstream?: number,
  ) {
    super(message);
  }
}

const isObj = (x: unknown): x is Record<string, unknown> => typeof x === "object" && x !== null;
const str = (x: unknown): string | undefined => (typeof x === "string" && x !== "" ? x : undefined);

/** GET-only HTTP to the workspace with the forwarded user token. */
export function userHttp(host: string): Http {
  const base = host.replace(/\/$/, "");
  return async (path, token) => {
    const r = await fetch(`${base}${path}`, { method: "GET", headers: { Authorization: `Bearer ${token}` } });
    let json: unknown = null;
    try {
      json = await r.json();
    } catch {
      json = null;
    }
    return { status: r.status, json };
  };
}

/** Paths for every page of a Lakebase list call. */
async function listAll(get: (path: string) => Promise<unknown>, base: string, field: string): Promise<unknown[]> {
  const out: unknown[] = [];
  let pageToken: string | undefined;
  do {
    const page = await get(`${base}?page_size=100${pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : ""}`);
    const items: unknown[] = isObj(page) && Array.isArray(page[field]) ? page[field] : [];
    out.push(...items);
    pageToken = isObj(page) ? str(page.next_page_token) : undefined;
  } while (pageToken);
  return out;
}

async function mapLimited<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array<R>(items.length);
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}

export function createAccess(opts: {
  http: Http;
  ttlMs?: number;
  now?: () => number;
  piiGroup?: string;
  /** false: every user may see query text (no PII group check). Default from PII_MASKING (only "false", trimmed, any case = off). */
  piiMasking?: boolean;
  adminGroup?: string;
  retryDelayMs?: number;
  incompleteTtlMs?: number;
}) {
  const ttl = opts.ttlMs ?? 5 * 60 * 1000;
  const now = opts.now ?? Date.now;
  const piiGroup = opts.piiGroup ?? "databricks_pii_access";
  const piiMasking = opts.piiMasking ?? process.env.PII_MASKING?.trim().toLowerCase() !== "false";
  const adminGroup = opts.adminGroup ?? "admins";
  const retryDelay = opts.retryDelayMs ?? 250;
  const incompleteTtl = opts.incompleteTtlMs ?? 30 * 1000;
  const cache = new Map<string, { at: number; ttl: number; value: Access }>();
  const inflight = new Map<string, Promise<Access>>();

  const transient = (status: number) => status === 429 || status >= 500;

  async function get(path: string, token: string): Promise<unknown> {
    for (let attempt = 0; ; attempt++) {
      const r = await opts.http(path, token);
      if (r.status === 401) throw new AccessError(401, "user token rejected");
      if (r.status === 200) return r.json;
      if (!transient(r.status) || attempt >= 2) {
        throw new AccessError(502, `GET ${path.split("?")[0]} failed with ${r.status}`, r.status);
      }
      await new Promise((res) => setTimeout(res, retryDelay * 2 ** attempt));
    }
  }

  // The Lakebase API shows every workspace project to every workspace user, so visibility is decided by
  // ownership or a Postgres role on one of the project's branches (the identity that can read its data).
  async function hasRole(token: string, projectId: string, names: Set<string>): Promise<boolean> {
    const branches = await listAll((p) => get(p, token), `/api/2.0/postgres/projects/${encodeURIComponent(projectId)}/branches`, "branches");
    for (const b of branches) {
      const branch = isObj(b) ? str(b.name) : undefined;
      if (!branch) continue;
      const roles = await listAll((p) => get(p, token), `/api/2.0/postgres/${branch}/roles`, "roles");
      for (const r of roles) {
        const st = isObj(r) && isObj(r.status) ? r.status : {};
        const role = str(st.postgres_role);
        const type = str(st.identity_type);
        if (role && names.has(`${type === "GROUP" ? "group" : "principal"}:${role}`)) return true;
      }
    }
    return false;
  }

  async function load(token: string, userKey: string): Promise<Access> {
    const me = await get("/api/2.0/preview/scim/v2/Me", token);
    const userName = isObj(me) ? str(me.userName) : undefined;
    const groups = (isObj(me) && Array.isArray(me.groups) ? me.groups : [])
      .map((g) => (isObj(g) ? str(g.display) : undefined))
      .filter((g): g is string => !!g);
    const projects = await listAll((p) => get(p, token), "/api/2.0/postgres/projects", "projects");
    const admin = groups.includes(adminGroup);
    const names = new Set([...(userName ? [`principal:${userName}`] : []), ...groups.map((g) => `group:${g}`)]);
    let incomplete = false;
    const visible = await mapLimited(projects, 8, async (p) => {
      if (!isObj(p)) return false;
      if (admin) return true;
      const owner = isObj(p.status) ? str(p.status.owner) : undefined;
      if (owner && owner === userName) return true;
      const id = str(p.project_id);
      if (!id) return false;
      try {
        return await hasRole(token, id, names);
      } catch (e) {
        if (e instanceof AccessError && e.status === 401) throw e;
        // Permission errors hide the project; throttling or upstream failures hide it only until the next short retry.
        if (!(e instanceof AccessError) || e.upstream === undefined || transient(e.upstream)) incomplete = true;
        console.warn(`lakebase-health: hiding project ${id}; could not read its roles`, e);
        return false;
      }
    });
    const projectIds: string[] = [];
    const projectNames: Record<string, string> = {};
    const owned: string[] = [];
    projects.forEach((p, i) => {
      const uid = isObj(p) ? str(p.uid) : undefined;
      if (!uid || !visible[i]) return;
      projectIds.push(uid);
      projectNames[uid] = (isObj(p) ? str(p.project_id) : undefined) ?? uid;
      const owner = isObj(p) && isObj(p.status) ? str(p.status.owner) : undefined;
      if (owner && owner === userName) owned.push(uid);
    });
    return { userKey, userName, projectIds, projectNames, pii: !piiMasking || groups.includes(piiGroup), admin, owned, incomplete };
  }

  return {
    async resolve(token: string | undefined, userKey: string | undefined): Promise<Access> {
      if (!token || !userKey) throw new AccessError(401, "no user token forwarded");
      const hit = cache.get(userKey);
      if (hit && now() - hit.at < hit.ttl) return hit.value;
      const running = inflight.get(userKey);
      if (running) return running;
      const p = load(token, userKey)
        .then((value) => {
          cache.set(userKey, { at: now(), ttl: value.incomplete ? incompleteTtl : ttl, value });
          return value;
        })
        .finally(() => inflight.delete(userKey));
      inflight.set(userKey, p);
      return p;
    },
    refresh(userKey: string) {
      cache.delete(userKey);
    },
  };
}

/**
 * Whether the caller may read query text for a project: everyone when PII masking is off (pii is then true),
 * otherwise databricks_pii_access members, the project owner and workspace admins.
 */
export function canSeeText(a: Access, projectUid: string): boolean {
  return a.pii || !!a.admin || (a.owned ?? []).includes(projectUid);
}
