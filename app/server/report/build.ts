import type {
  Branch,
  Endpoint,
  EndpointMetrics,
  IoData,
  IoRow,
  Overview,
  Report,
  ReportFinding,
  Row,
  Section,
  Series,
  SeriesPoint,
} from "../../client/src/report/types";
import type { Access } from "../lib/access";
import { QUERY_CHECKS, groupFindings, page } from "../lib/group";
import { maskRows } from "../lib/mask";
import type { Row as SqlRow } from "../lib/sql";
import { CATALOG, type Catalog } from "./catalog";
import { runbook, type ActionRow } from "./runbook";
import { focus, score, scoreAreas, type CheckStatus } from "./score";

export type Runner = { run(name: string, ids: string[], params?: Record<string, string | Date>): Promise<SqlRow[]> };

/** The analysis job's schedule (resources/analysis_job.yml): inventory older than two intervals is stale. */
export const ANALYSIS_MINUTES = 15;

export class ReportNotFound extends Error {}

const num = (v: unknown): number | null => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const str = (v: unknown): string | null => (typeof v === "string" ? v : typeof v === "number" ? String(v) : null);
const truthy = (v: unknown) => v === true || v === "true";
const cell = (v: unknown): Row[string] =>
  v === null || v === undefined ? null : typeof v === "number" || typeof v === "boolean" || typeof v === "string" ? v : JSON.stringify(v);
const rowOf = (r: SqlRow): Row => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, cell(v)]));
const iso = (v: unknown) => {
  const s = str(v);
  if (!s) return null;
  const d = new Date(s.includes("T") || s.endsWith("Z") ? s : `${s.replace(" ", "T")}Z`);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
};

async function section<T>(work: () => Promise<SqlRow[]>, shape: (rows: SqlRow[]) => T, empty: T, now: Date, at?: string): Promise<Section<T>> {
  try {
    const rows = await work();
    const asOf = at ? iso(rows[0]?.[at]) : undefined;
    const stale = asOf ? now.getTime() - new Date(asOf).getTime() > 2 * ANALYSIS_MINUTES * 60_000 : undefined;
    return { status: "ok", data: shape(rows), ...(at ? { asOf: asOf ?? null, stale } : {}) };
  } catch (e) {
    console.error("lakebase-health report section failed", e);
    return { status: "error", data: empty, error: "This panel could not be loaded." };
  }
}

export function overviewOf(rows: SqlRow[]): Overview {
  const r = rows[0] ?? {};
  return {
    lfcHitPct: num(r.lfc_hit_pct),
    workingSetBytes: num(r.working_set_bytes),
    lfcSizeBytes: num(r.lfc_size_bytes),
    connections: num(r.connections),
    maxConnections: num(r.max_connections),
    databaseBytes: num(r.database_bytes),
    oldestXactS: num(r.oldest_xact_s),
    getpageP99Ms: num(r.getpage_p99_ms),
    tempSpillBytes: num(r.temp_spill_bytes),
    commitsPerS: num(r.commits_per_s),
    telemetryAt: iso(r.telemetry_at),
  };
}

export function seriesOf(rows: SqlRow[]): Series {
  const out: Series = {};
  for (const r of rows) {
    const m = str(r.metric);
    const t = iso(r.bucket);
    if (!m || !t) continue;
    (out[m] ??= []).push({ t, v: num(r.value) });
  }
  for (const pts of Object.values(out)) pts.sort((a, b) => a.t.localeCompare(b.t));
  return out;
}

const IO_COUNTERS = ["reads", "writes", "extends", "hits", "evictions", "reuses", "fsyncs", "read_time", "write_time"] as const;
const ioKey = (r: SqlRow) => [r.backend_type, r.object, r.context].map((v) => str(v) ?? "").join("|");

/** pg_stat_io: the change since the previous snapshot when both exist and nothing was reset, else the totals. */
export function ioOf(rows: SqlRow[]): IoData {
  const times = [...new Set(rows.map((r) => iso(r.collected_at)).filter((t): t is string => !!t))].sort().reverse();
  const at = (t: string | undefined) => rows.filter((r) => t !== undefined && iso(r.collected_at) === t);
  const latest = at(times[0]);
  const prev = new Map(at(times[1]).map((r) => [ioKey(r), r]));
  const toRow = (r: SqlRow): IoRow => ({
    backend_type: str(r.backend_type) ?? "", object: str(r.object) ?? "", context: str(r.context) ?? "",
    ...Object.fromEntries(IO_COUNTERS.map((c) => [c, num(r[c])])),
  }) as IoRow;
  const timing = latest.some((r) => num(r.read_time) !== null || num(r.write_time) !== null);
  const sameReset = latest.every((r) => {
    const p = prev.get(ioKey(r));
    return p !== undefined && iso(p.stats_reset) === iso(r.stats_reset) && IO_COUNTERS.every((c) => (num(r[c]) ?? 0) >= (num(p[c]) ?? 0));
  });
  const delta = times.length > 1 && latest.length > 0 && sameReset;
  const out = latest.map((r) => {
    const cur = toRow(r);
    if (!delta) return cur;
    const p = toRow(prev.get(ioKey(r)) as SqlRow);
    return { ...cur, ...Object.fromEntries(IO_COUNTERS.map((c) => [c, cur[c] === null ? null : (cur[c] ?? 0) - (p[c] ?? 0)])) } as IoRow;
  });
  return {
    rows: out.sort((x, y) => (y.reads ?? 0) + (y.hits ?? 0) - ((x.reads ?? 0) + (x.hits ?? 0))),
    delta,
    timing,
    windowS: delta ? (new Date(times[0]).getTime() - new Date(times[1]).getTime()) / 1000 : null,
    statsReset: iso(latest[0]?.stats_reset),
  };
}

/** p99 (ms) from the change in a cumulative histogram between two snapshots ({le: count}). */
export function histogramP99(prev: Map<number, number>, cur: Map<number, number>): number | null {
  const les = [...cur.keys()].sort((a, b) => a - b);
  if (les.length === 0) return null;
  const d = les.map((le) => ({ le, n: (cur.get(le) ?? 0) - (prev.get(le) ?? 0) }));
  if (d.some((x) => x.n < 0)) return null; // reset between the snapshots
  const total = d[d.length - 1].n;
  if (total <= 0) return null;
  const hit = d.find((x) => x.n >= 0.99 * total);
  return hit && Number.isFinite(hit.le) ? hit.le * 1000 : null;
}

export function endpointOf(rows: SqlRow[]): EndpointMetrics {
  const times = [...new Set(rows.map((r) => iso(r.collected_at)).filter((t): t is string => !!t))].sort();
  const last = times[times.length - 1];
  const latestRows = rows.filter((r) => iso(r.collected_at) === last);
  const latest: Record<string, number | null> = {};
  const sources: EndpointMetrics["sources"] = {};
  const idleClients: EndpointMetrics["idleClients"] = [];
  let serverVersion: string | null = null;
  for (const r of latestRows) {
    const m = String(r.metric);
    if (m.startsWith("source:")) sources[m.slice(7)] = { available: num(r.value) === 1, note: str(r.note) };
    else if (m === "idle_clients") {
      const d = JSON.parse(str(r.dims) ?? "{}") as Record<string, unknown>;
      idleClients.push({ role: str(d.role) ?? "", application: str(d.application) ?? "", sessions: num(r.value) ?? 0, maxIdleS: num(d.max_idle_s) });
    } else if (m === "server_version") serverVersion = str(r.note);
    else if (!m.startsWith("neon.perf.")) latest[m] = num(r.value);
  }
  const hist = new Map<string, Map<number, number>>();
  for (const r of rows) {
    if (r.metric !== "neon.perf.getpage_wait_seconds_bucket") continue;
    const t = iso(r.collected_at);
    const le = num((JSON.parse(str(r.dims) ?? "{}") as { le?: unknown }).le);
    if (!t || le === null) continue;
    (hist.get(t) ?? hist.set(t, new Map()).get(t)!).set(le, num(r.value) ?? 0);
  }
  const ht = [...hist.keys()].sort();
  const getpageP99: SeriesPoint[] = ht.slice(1).map((t, i) => ({ t, v: histogramP99(hist.get(ht[i])!, hist.get(t)!) }));
  return { latest, sources, idleClients: idleClients.sort((a, b) => b.sessions - a.sessions), getpageP99, serverVersion };
}

function findingOf(r: SqlRow, catalog: Catalog): ReportFinding {
  return {
    fingerprint: String(r.fingerprint), check_id: String(r.check_id), title: String(r.title ?? r.check_id),
    category: str(r.category) ?? "", area: catalog[String(r.check_id)]?.area ?? null, endpoint_id: str(r.endpoint_id),
    object: str(r.object) ?? "", status: String(r.status), severity: String(r.severity) as ReportFinding["severity"],
    confidence: str(r.confidence), measure: num(r.measure), first_seen: iso(r.first_seen), last_seen: iso(r.last_seen),
    evidence: str(r.evidence), explain: str(r.explain), candidate_sql: str(r.candidate_sql), queryid: str(r.queryid),
    query_text: null,
  };
}

export function branchesOf(rows: SqlRow[]): { branches: Branch[]; endpoints: Map<string, Endpoint[]> } {
  const branches = new Map<string, Branch>();
  const endpoints = new Map<string, Endpoint[]>();
  for (const r of rows) {
    const id = String(r.branch_id);
    if (!branches.has(id)) branches.set(id, { id, name: str(r.branch_name) ?? id, isDefault: truthy(r.is_default_branch) });
    (endpoints.get(id) ?? endpoints.set(id, []).get(id)!).push({
      id: String(r.endpoint_id), type: String(r.endpoint_type).endsWith("READ_WRITE") ? "primary" : "replica",
      state: str(r.current_state) ?? "", minCu: num(r.min_cu), maxCu: num(r.max_cu),
      suspendTimeoutS: num(r.suspend_timeout_s), enrolled: truthy(r.enrolled),
    });
  }
  return { branches: [...branches.values()], endpoints };
}

const hoursAgo = (note: string | null) => /reset (\d+) h ago/.exec(note ?? "")?.[1];

/**
 * The live report for one branch (rawBranch: a branch id, or "default"). Throws ReportNotFound when the branch is
 * not one of the project's. Each panel fails on its own: an error section, never a failed page.
 */
export async function buildReport(opts: {
  runner: Runner;
  access: Access;
  projectId: string;
  branch: string;
  now: Date;
  scheduleMinutes: number;
  catalog?: Catalog;
}): Promise<Report> {
  const { runner, access: a, projectId, now } = opts;
  const catalog = opts.catalog ?? CATALOG;
  const ids = a.projectIds;
  const inv = branchesOf(await runner.run("report_branches", ids, { project_id: projectId }));
  const branch = opts.branch === "default"
    ? inv.branches.find((b) => b.isDefault) ?? inv.branches[0]
    : inv.branches.find((b) => b.id === opts.branch);
  if (!branch) throw new ReportNotFound("not found");
  const endpoints = inv.endpoints.get(branch.id) ?? [];
  const primary = endpoints.find((e) => e.type === "primary") ?? null;
  const base = { project_id: projectId, branch_id: branch.id };
  const ep = { ...base, endpoint_id: primary?.id ?? "-" };
  const run = (name: string, p: Record<string, string> = ep) => () => runner.run(name, ids, p);
  const rows = (rs: SqlRow[]) => rs.map(rowOf);
  const none = <T>(empty: T): Promise<Section<T>> => Promise.resolve({ status: "ok", data: empty, asOf: null });
  const emptyOverview = overviewOf([]);
  const emptyEndpoint = endpointOf([]);
  const emptyIo: IoData = { rows: [], delta: false, timing: false, windowS: null, statsReset: null };

  const [findingsS, actionsS, checksS, metaS, overview, databases, tables, indexes, statements, settings, io, endpoint, series, waits] =
    await Promise.all([
      section(run("report_findings", base), (r) => r, [] as SqlRow[], now),
      section(run("report_actions", base), (r) => r, [] as SqlRow[], now),
      section(run("report_checks", base), (r) => r, [] as SqlRow[], now),
      section(() => runner.run("meta", ids), (r) => r, [] as SqlRow[], now),
      primary ? section(run("report_overview"), overviewOf, emptyOverview, now) : none(emptyOverview),
      primary ? section(run("report_databases"), rows, [], now) : none<Row[]>([]),
      primary ? section(run("report_tables"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_indexes"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_statements"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_settings"), rows, [], now, "collected_at") : none<Row[]>([]),
      primary ? section(run("report_io"), ioOf, emptyIo, now, "collected_at") : none(emptyIo),
      primary ? section(run("report_endpoint"), endpointOf, emptyEndpoint, now) : none(emptyEndpoint),
      primary ? section(run("report_series"), seriesOf, {} as Series, now) : none<Series>({}),
      primary ? section(run("report_waits"), rows, [], now) : none<Row[]>([]),
    ]);

  const rawFindings = maskRows(findingsS.data, false);
  const findings = rawFindings.map((r) => findingOf(r, catalog));
  const statuses: CheckStatus[] = checksS.data.map((r) => ({
    check_id: String(r.check_id), endpoint_id: str(r.endpoint_id), status: String(r.status), error: str(r.error), last_ok_at: iso(r.last_ok_at),
  }));
  const telemetryAt = overview.data.telemetryAt;
  const telemetry = !!telemetryAt && now.getTime() - new Date(telemetryAt).getTime() < 24 * 3600_000;
  const enrolled = !!primary?.enrolled;
  const unavailable = [...(findingsS.status === "error" ? ["findings" as const] : []), ...(checksS.status === "error" ? ["checks" as const] : [])];
  const scored = scoreAreas(findings, statuses, { primaryEndpoints: primary ? [primary.id] : [], telemetry, enrolled }, catalog);
  // no findings is not the same as no problems: a failed findings query leaves every area unassessed
  const areas = findingsS.status === "error"
    ? scored.map((x) => ({ ...x, state: "not_assessed" as const, score: null, open: 0, worst: null, note: "findings unavailable" }))
    : scored;
  const overall = unavailable.length ? { ...score(areas), pct: null, grade: null, unavailable: true as const } : score(areas);

  const banners: string[] = [];
  const lastRun = iso(metaS.data[0]?.last_success_at);
  if (!lastRun || now.getTime() - new Date(lastRun).getTime() > 2 * opts.scheduleMinutes * 60_000)
    banners.push("The health engine has not completed a run in over two schedule intervals; this report may be out of date.");
  if (findingsS.status === "error" || checksS.status === "error")
    banners.push("Findings could not be loaded: the scorecard and runbook are incomplete.");
  if (!primary) banners.push("This branch has no read-write compute: inventory panels and charts are empty.");
  else if (!enrolled)
    banners.push("Postgres checks are not enabled on this branch, so its Postgres areas are not assessed and the inventory is empty. Enable them with GRANT pg_monitor to the engine's role (README: Enroll a project for Postgres checks).");
  const young = statuses.find((s) => s.status === "skipped:stats_young" && s.endpoint_id === primary?.id);
  if (young) banners.push(`Counters too young: Postgres statistics were reset ${hoursAgo(young.error) ?? "less than 24"} h ago, so counter-based checks wait until they are 24 h old.`);
  if (primary && !telemetry) banners.push("No telemetry for this branch in the last 24 h: telemetry areas are not assessed.");
  if (primary && primary.state !== "ACTIVE")
    banners.push(`The compute is ${primary.state.toLowerCase() || "not running"}: panels show the last snapshot taken while it ran; nothing here wakes it.`);
  else if (enrolled && tables.status === "ok" && (tables.stale || !tables.asOf))
    banners.push("The inventory is older than two analysis runs: panels show their age; the score uses findings only.");

  const queryRows = rawFindings.filter((r) => QUERY_CHECKS.has(String(r.check_id)));
  return {
    project: { id: projectId, name: a.projectNames[projectId] ?? projectId },
    branch,
    branches: inv.branches,
    endpoints,
    primary: primary?.id ?? null,
    pgVersion: endpoint.data.serverVersion,
    dataAsOf: lastRun,
    generatedAt: now.toISOString(),
    banners,
    unavailable,
    score: overall,
    areas,
    focus: focus(areas),
    findings,
    runbook: runbook(findings, actionsS.data.map((r) => ({ action_id: String(r.action_id), fingerprint: String(r.fingerprint), state: String(r.state) })) as ActionRow[], catalog),
    sections: { overview, databases, tables, indexes, statements, settings, io, endpoint, series, waits },
    queryGroups: page(groupFindings(queryRows), true).rows,
  };
}
