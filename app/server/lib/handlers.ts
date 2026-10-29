import { AccessError, canSeeText, type Access } from "./access";
import { NEEDS_ACTIONABLE, blockRetry, hasRollback, isRequestKind, legalRequests, notActionable, sqlHash } from "./approvals";
import { draftToken, findingQueryId, requestState, setUpOf, type DraftView } from "./drafts";
import { groupFindings, isOpen, page, type GroupRow } from "./group";
import { maskRows } from "./mask";
import { parseQueryPairs, queryLabel, skeleton } from "./querytext";
import type { Row } from "./sql";
import { ReportNotFound, buildReport } from "../report/build";
import { exportFilename, renderExport } from "../report/export";
import { parseActionId, parseBranchRef, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "./validate";

export type Ctx = { token?: string; user?: string; now: Date };
type Deps = {
  access: { resolve(token: string | undefined, user: string | undefined): Promise<Access>; refresh(user: string): void };
  runner: { run(name: string, ids: string[], params?: Record<string, string | Date>): Promise<Row[]> };
  scheduleMinutes: number;
  approvals?: Approvals;
  /** The report export's audit row (server/writes/insert_audit.sql); absent means exports are off. */
  audit?: Audit;
  /** LLM query rewrites: LLM_ENABLED and LLM_ENDPOINT. Absent means not set up. */
  llm?: { enabled: boolean; endpoint: string };
};

/** Approval writes and checks; absent means the fix loop is not installed and every action is read-only. */
export type Approvals = {
  /** Whether the caller may approve changes to this project (by uid): owner or CAN_MANAGE. */
  canManage(ctx: Ctx, access: Access, projectUid: string): Promise<boolean>;
  insert(row: Record<string, string>): Promise<void>;
  /** Starts the fixer job; false when it could not be started (the fixer's schedule picks the request up). */
  trigger(): Promise<boolean | void>;
  newId(): string;
};

/** One audit_events row per report download. */
export type Audit = { insert(row: Record<string, string>): Promise<void>; newId(): string };

export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
    /** Machine-readable reason, sent as `code` next to `error`. */
    public code?: string,
  ) {
    super(message);
  }
}

const NO_PROJECTS = "-";
const num = (v: unknown) => (v === null || v === undefined ? 0 : Number(v));
const text = (v: unknown) => (typeof v === "string" ? v : null);
const sv = (v: unknown) => (typeof v === "string" || typeof v === "number" ? String(v) : "");

const SCHEDULED =
  "Request recorded. The fixer could not be started now; it will pick the request up on its 15-minute schedule.";
/** evidence.top_queries[].queryid of a finding row that carries raw evidence JSON. */
function topQueriesOf(f: Row): string {
  try {
    const ev: unknown = JSON.parse(typeof f.evidence === "string" ? f.evidence : "{}");
    const list = typeof ev === "object" && ev !== null ? (ev as Record<string, unknown>).top_queries : undefined;
    return JSON.stringify(Array.isArray(list) ? list.map((q) => (typeof q === "object" && q !== null ? sv((q as Record<string, unknown>).queryid) : "")) : []);
  } catch {
    return "[]";
  }
}
const isTrue = (v: unknown) => v === true || v === "true";
const field = (body: unknown, key: string): unknown =>
  typeof body === "object" && body !== null && key in body ? (body as Record<string, unknown>)[key] : undefined;

const SECTION = /^[a-z_]{1,40}$/;
const STATE = /^[a-z_]{1,30}$/;
/** Action states in the order the fixes page lists them: needs a decision first, finished last. */
const STATE_ORDER = [
  "proposed", "verify_failed", "verified", "apply_failed", "blocked", "verifying", "applying", "regressed",
  "rollback_failed", "applied", "confirmed", "rolled_back", "rejected", "expired",
];
const stateRank = (st: string) => {
  const i = STATE_ORDER.indexOf(st);
  return i < 0 ? STATE_ORDER.length : i;
};
const ALL_ROWS = 1000;
/** The LIMIT in queries/project_findings.sql and queries/finding_related.sql. */
const FINDINGS_LIMIT = 5000;
const RELATED_LIMIT = 2000;

/** SQL finding counts per (category, bucket): bucket is the severity of a new/open finding, or "resolved". */
type Counts = Map<string, Map<string, number>>;
function countsOf(rows: Row[]): Counts {
  const out: Counts = new Map();
  for (const r of rows) {
    const c = out.get(sv(r.category)) ?? new Map<string, number>();
    c.set(sv(r.bucket), (c.get(sv(r.bucket)) ?? 0) + num(r.n));
    out.set(sv(r.category), c);
  }
  return out;
}
const openCount = (c: Map<string, number> | undefined) =>
  [...(c ?? new Map<string, number>()).entries()].reduce((n, [b, v]) => (b === "resolved" ? n : n + v), 0);

/**
 * Open findings grouped per category (query rows collapsed) and the recently resolved ones. Categories come from
 * the SQL counts too, so one the LIMIT cut off entirely still shows (with no rows).
 */
function projectSections(findings: Row[], counts: Counts = new Map()) {
  const open = findings.filter(isOpen);
  const fromCounts = [...counts.entries()].filter(([, c]) => openCount(c) > 0).map(([k]) => k);
  const categories = [...new Set([...open.map((f) => sv(f.category)), ...fromCounts])].sort();
  const sections = new Map<string, GroupRow[]>(
    categories.map((c) => [c, groupFindings(open.filter((f) => sv(f.category) === c), open)]),
  );
  sections.set("resolved", groupFindings(findings.filter((f) => f.status === "resolved")));
  const shown = (key: string) =>
    key === "resolved" ? findings.filter((f) => f.status === "resolved").length : open.filter((f) => sv(f.category) === key).length;
  return { categories, sections, shown };
}

/** count = findings in SQL, shown = findings fetched; truncated when the LIMIT cut this list. */
function cut(count: number, shown: number, limited: boolean) {
  return { count, shown, truncated: limited && shown < count };
}

/** Related findings: total_rows is the SQL count before the LIMIT. */
function relatedCut(related: Row[]) {
  const count = related.length ? Math.max(num(related[0].total_rows), related.length) : 0;
  return { ...cut(count, related.length, related.length >= RELATED_LIMIT), limit: RELATED_LIMIT };
}

const REWRITE = "query_rewrite";
const ADVISORY = "a query rewrite is advisory: change it in your application code";

export function createHandlers({ access, runner, scheduleMinutes, approvals, llm, audit }: Deps) {
  async function who(ctx: Ctx): Promise<Access> {
    try {
      return await access.resolve(ctx.token, ctx.user);
    } catch (e) {
      if (e instanceof AccessError) throw new HttpError(e.status, e.message);
      throw e;
    }
  }

  // An endpoint is visible only if it maps (via inventory or telemetry) to one of the caller's projects.
  async function endpointAccess(ctx: Ctx, rawId: string) {
    const a = await who(ctx);
    const endpointId = parseEndpointId(rawId);
    if (!endpointId) throw new HttpError(400, "invalid endpoint id");
    const [hit] = await runner.run("endpoint_project", a.projectIds, { endpoint_id: endpointId });
    if (!hit) throw new HttpError(404, "not found");
    return { a, endpointId, projectId: String(hit.project_id) };
  }

  async function manages(ctx: Ctx, a: Access, projectUid: string) {
    return approvals ? approvals.canManage(ctx, a, projectUid) : false;
  }

  // An action is visible only through its finding's project, which must be one of the caller's projects.
  async function visibleAction(ctx: Ctx, rawId: string) {
    const a = await who(ctx);
    const id = parseActionId(rawId);
    if (!id) throw new HttpError(400, "invalid action id");
    const [action] = await runner.run("action", a.projectIds, { action_id: id });
    if (!action) throw new HttpError(404, "not found");
    return { a, id, action };
  }

  /** Shown text of a rewrite: the full SQL for callers who may read query text, else its skeleton. */
  const rewriteText = (sql: unknown, full: boolean) =>
    typeof sql === "string" ? (full ? sql : skeleton(sql)) : null;

  async function draftView(ctx: Ctx, a: Access, finding: Row): Promise<DraftView> {
    const fp = String(finding.fingerprint);
    const project = String(finding.project_id);
    const queryid = findingQueryId(finding.check_id, finding.evidence, finding.object);
    const endpoint = sv(finding.endpoint_id);
    const full = canSeeText(a, project);
    const [attempts, requests, status, text, manager] = await Promise.all([
      runner.run("finding_drafts", a.projectIds, { fingerprint: fp }),
      runner.run("draft_requests", a.projectIds, { fingerprint: fp }),
      runner.run("llm_status", a.projectIds, { endpoint: llm?.endpoint ?? "" }),
      queryid && endpoint
        ? runner.run("query_text", a.projectIds, { project_id: project, pairs: `${endpoint}:${queryid}` })
        : Promise.resolve([] as Row[]),
      manages(ctx, a, project),
    ]);
    const setUp = setUpOf(!!llm?.enabled, status[0]);
    const req = requestState(attempts, requests);
    const rewritable = queryid !== null;
    return {
      rewritable,
      setUp,
      canManage: manager,
      canRequest: rewritable && setUp.ready && manager && isOpen(finding) && !req.pending && req.latestFinal,
      pending: req.pending,
      refused: req.refused,
      nextAttempt: req.nextAttempt,
      original: queryid && endpoint ? queryLabel(text[0] ?? { endpoint_id: endpoint, queryid }, full) : null,
      attempts: attempts.map((r) => ({ ...r, sql: rewriteText(r.sql, full) })),
    };
  }

  async function visibleFinding(ctx: Ctx, rawFp: string) {
    const a = await who(ctx);
    const fp = parseFingerprint(rawFp);
    if (!fp) throw new HttpError(400, "invalid fingerprint");
    const [finding] = await runner.run("finding", a.projectIds, { fingerprint: fp });
    if (!finding) throw new HttpError(404, "not found");
    return { a, fp, finding };
  }

  // A report is visible when its project is one of the caller's; the branch must be one of that project's.
  async function reportScope(ctx: Ctx, rawId: string, rawBranch: string) {
    const a = await who(ctx);
    const id = parseProjectId(rawId);
    if (!id) throw new HttpError(400, "invalid project id");
    const branch = parseBranchRef(rawBranch);
    if (!branch) throw new HttpError(400, "invalid branch");
    if (!a.projectIds.includes(id)) throw new HttpError(404, "not found");
    try {
      return { a, id, report: await buildReport({ runner, access: a, projectId: id, branch, now: ctx.now, scheduleMinutes }) };
    } catch (e) {
      if (e instanceof ReportNotFound) throw new HttpError(404, "not found");
      throw e;
    }
  }

  return {
    async meta(ctx: Ctx) {
      const a = await who(ctx);
      // Engine run status is not project data; an empty allow-list must still see it (sentinel matches no project).
      const [m] = await runner.run("meta", a.projectIds.length ? a.projectIds : [NO_PROJECTS]);
      const raw = text(m?.last_success_at);
      const last = raw ? new Date(raw) : null;
      const stale = !last || ctx.now.getTime() - last.getTime() > 2 * scheduleMinutes * 60_000;
      return {
        lastSuccessAt: last ? last.toISOString() : null,
        lastStatus: text(m?.last_status),
        stale,
        openFindings: num(m?.open_findings),
        pii: a.pii,
        accessIncomplete: !!a.incomplete,
      };
    },

    async fleet(ctx: Ctx) {
      const a = await who(ctx);
      const rows = await runner.run("fleet", a.projectIds);
      return {
        projects: rows.map((r) => ({
          ...r,
          project_name: a.projectNames[String(r.project_id)] ?? r.project_name ?? r.project_id,
        })),
      };
    },

    async project(ctx: Ctx, rawId: string) {
      const a = await who(ctx);
      const id = parseProjectId(rawId);
      if (!id) throw new HttpError(400, "invalid project id");
      if (!a.projectIds.includes(id)) throw new HttpError(404, "not found");
      const [findings, countRows, computes] = await Promise.all([
        runner.run("project_findings", a.projectIds, { project_id: id }),
        runner.run("project_finding_counts", a.projectIds, { project_id: id }),
        runner.run("project_computes", a.projectIds, { project_id: id }),
      ]);
      const counts = countsOf(countRows);
      const { categories, sections, shown } = projectSections(findings, counts);
      const limited = findings.length >= FINDINGS_LIMIT;
      const bucket = (b: string) => [...counts.values()].reduce((n, c) => n + (c.get(b) ?? 0), 0);
      const resolvedCount = bucket("resolved");
      return {
        project: { id, name: a.projectNames[id] ?? id },
        // findings counted in SQL, never from the (possibly cut) rows
        counts: { critical: bucket("critical"), warning: bucket("warning"), info: bucket("info"), resolved: resolvedCount },
        truncated: limited,
        limit: FINDINGS_LIMIT,
        sections: categories.map((c) => ({
          category: c, ...page(sections.get(c) ?? []), ...cut(openCount(counts.get(c)), shown(c), limited),
        })),
        resolved: { ...page(sections.get("resolved") ?? []), ...cut(resolvedCount, shown("resolved"), limited) },
        computes,
      };
    },

    /** Every row of one project section (a category, or "resolved"): the "Show all N" fetch. */
    async projectFindings(ctx: Ctx, rawId: string, rawSection: unknown) {
      const a = await who(ctx);
      const id = parseProjectId(rawId);
      if (!id) throw new HttpError(400, "invalid project id");
      if (typeof rawSection !== "string" || !SECTION.test(rawSection)) throw new HttpError(400, "invalid section");
      if (!a.projectIds.includes(id)) throw new HttpError(404, "not found");
      const [findings, countRows] = await Promise.all([
        runner.run("project_findings", a.projectIds, { project_id: id }),
        runner.run("project_finding_counts", a.projectIds, { project_id: id }),
      ]);
      const counts = countsOf(countRows);
      const { sections, shown } = projectSections(findings, counts);
      const count = rawSection === "resolved"
        ? [...counts.values()].reduce((n, c) => n + (c.get("resolved") ?? 0), 0)
        : openCount(counts.get(rawSection));
      return {
        section: rawSection, ...page(sections.get(rawSection) ?? [], true),
        ...cut(count, shown(rawSection), findings.length >= FINDINGS_LIMIT), limit: FINDINGS_LIMIT,
      };
    },

    /**
     * Latest query text per (endpoint, queryid) for one project, in one batch ("ep-a:42,ep-a:7"). Full text for
     * callers who may read it (canSeeText), the normalized skeleton for everyone else, "query <id>" when none.
     */
    async queries(ctx: Ctx, rawId: string, rawIds: unknown) {
      const a = await who(ctx);
      const id = parseProjectId(rawId);
      if (!id) throw new HttpError(400, "invalid project id");
      const pairs = parseQueryPairs(rawIds);
      if (!pairs) throw new HttpError(400, "invalid ids");
      if (!a.projectIds.includes(id)) throw new HttpError(404, "not found");
      const endpoints = [...new Set(pairs.map((p) => p.endpoint_id))];
      const [known, rows] = await Promise.all([
        runner.run("project_endpoints", a.projectIds, { project_id: id, endpoint_ids: endpoints.join(",") }),
        runner.run("query_text", a.projectIds, {
          project_id: id,
          pairs: pairs.map((p) => `${p.endpoint_id}:${p.queryid}`).join(","),
        }),
      ]);
      const seen = new Set(known.map((r) => String(r.endpoint_id)));
      if (endpoints.some((e) => !seen.has(e))) throw new HttpError(404, "not found");
      const full = canSeeText(a, id);
      const byKey = new Map(rows.map((r) => [`${String(r.endpoint_id)}:${String(r.queryid)}`, r]));
      return {
        queries: pairs.map((p) => queryLabel(byKey.get(`${p.endpoint_id}:${p.queryid}`) ?? p, full)),
      };
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
      return {
        finding: maskRows([finding], canSeeText(a, String(finding.project_id)))[0],
        history,
        actions,
        related: { ...page(groupFindings(related, [{ ...finding, top_queryids: topQueriesOf(finding) }])), ...relatedCut(related) },
      };
    },

    /** Every grouped related finding: the "Show all N" fetch on the finding page. */
    async findingRelated(ctx: Ctx, rawFp: string) {
      const a = await who(ctx);
      const fp = parseFingerprint(rawFp);
      if (!fp) throw new HttpError(400, "invalid fingerprint");
      const [finding] = await runner.run("finding", a.projectIds, { fingerprint: fp });
      if (!finding) throw new HttpError(404, "not found");
      const related = await runner.run("finding_related", a.projectIds, { fingerprint: fp });
      return { ...page(groupFindings(related, [{ ...finding, top_queryids: topQueriesOf(finding) }]), true), ...relatedCut(related) };
    },

    async timeline(ctx: Ctx, rawId: string, from: unknown, to: unknown) {
      const { a, endpointId } = await endpointAccess(ctx, rawId);
      const w = parseWindow(from, to, ctx.now);
      if ("error" in w) throw new HttpError(400, w.error);
      const p = { endpoint_id: endpointId, from_ts: w.from, to_ts: w.to };
      const [waits, locks, commits, events, gauges] = await Promise.all([
        runner.run("timeline_waits", a.projectIds, p),
        runner.run("timeline_locks", a.projectIds, p),
        runner.run("timeline_commits", a.projectIds, p),
        runner.run("timeline_events", a.projectIds, p),
        runner.run("timeline_gauges", a.projectIds, p),
      ]);
      return { endpointId, from: w.from.toISOString(), to: w.to.toISOString(), waits, locks, commits, events, gauges };
    },

    async samples(ctx: Ctx, rawId: string, at: unknown) {
      const { a, endpointId, projectId } = await endpointAccess(ctx, rawId);
      const w = parseWindow(at, undefined, ctx.now);
      if ("error" in w) throw new HttpError(400, w.error);
      const rows = await runner.run("timeline_samples", a.projectIds, { endpoint_id: endpointId, at_ts: w.from });
      return { samples: maskRows(rows, canSeeText(a, projectId)) };
    },

    /** Fixes grouped by state, the top 10 of each with its total; `state` returns every fix in that state. */
    async actions(ctx: Ctx, rawId: string, rawState?: unknown) {
      const a = await who(ctx);
      const id = parseProjectId(rawId);
      if (!id) throw new HttpError(400, "invalid project id");
      const state = rawState === undefined || rawState === "" ? "" : rawState;
      if (typeof state !== "string" || (state !== "" && !STATE.test(state))) throw new HttpError(400, "invalid state");
      if (!a.projectIds.includes(id)) throw new HttpError(404, "not found");
      const [actions, canManage] = await Promise.all([
        runner.run("project_actions", a.projectIds, { project_id: id, state, row_limit: state ? String(ALL_ROWS) : "10" }),
        manages(ctx, a, id),
      ]);
      const groups = new Map<string, { state: string; total: number; rows: Row[]; truncated: boolean }>();
      for (const r of actions) {
        const st = sv(r.state);
        const g = groups.get(st) ?? { state: st, total: num(r.state_total), rows: [], truncated: false };
        g.rows.push(r);
        groups.set(st, g);
      }
      // total is the SQL count; one state's full list stops at ALL_ROWS
      for (const g of groups.values()) g.truncated = !!state && g.rows.length < g.total;
      const list = [...groups.values()].sort((x, y) => stateRank(x.state) - stateRank(y.state) || x.state.localeCompare(y.state));
      return { project: { id, name: a.projectNames[id] ?? id }, groups: list, canManage, limit: ALL_ROWS };
    },

    async action(ctx: Ctx, rawId: string) {
      const { a, id, action } = await visibleAction(ctx, rawId);
      const [runs, canManage] = await Promise.all([
        runner.run("action_runs", a.projectIds, { action_id: id }),
        manages(ctx, a, String(action.project_id)),
      ]);
      const retry = String(action.state) === "blocked" ? blockRetry(runs) : null;
      const rewrite = action.kind === REWRITE;
      const why = rewrite ? null : notActionable(action.finding_status, action.expires_at, ctx.now);
      const legal = rewrite ? []
        : legalRequests(String(action.state), isTrue(action.verify_on_branch), retry, hasRollback(action.rollback_sql));
      const requests = why ? legal.filter((r) => !NEEDS_ACTIONABLE.includes(r)) : legal;
      return {
        action: rewrite ? { ...action, sql: rewriteText(action.sql, canSeeText(a, String(action.project_id))) } : action,
        runs, canManage, retryAfterEnrolment: retry !== null && requests.length > 0, requests,
        // said only when it hides a request the action's state would allow
        notActionable: why && requests.length < legal.length ? why : null,
      };
    },

    async request(ctx: Ctx, rawId: string, body: unknown) {
      if (!approvals) throw new HttpError(404, "not found");
      const req = field(body, "request");
      const { a, id, action } = await visibleAction(ctx, rawId);
      if (!isRequestKind(req)) throw new HttpError(400, "invalid request");
      if (!(await approvals.canManage(ctx, a, String(action.project_id))))
        throw new HttpError(403, "only the project owner or a manager can approve");
      if (action.kind === REWRITE) throw new HttpError(409, ADVISORY);
      const why = NEEDS_ACTIONABLE.includes(req) ? notActionable(action.finding_status, action.expires_at, ctx.now) : null;
      if (why) throw new HttpError(409, why, "not_actionable");
      const state = String(action.state);
      const retry = state === "blocked" ? blockRetry(await runner.run("action_runs", a.projectIds, { action_id: id })) : null;
      if (!legalRequests(state, isTrue(action.verify_on_branch), retry, hasRollback(action.rollback_sql)).includes(req))
        throw new HttpError(409, `cannot ${req} an action that is ${state}`);
      const target = req === "rollback" ? action.rollback_sql : action.sql;
      const rawNote = field(body, "note");
      const approvalId = approvals.newId();
      await approvals.insert({
        approval_id: approvalId,
        action_id: id,
        fingerprint: sv(action.fingerprint),
        request: req,
        sql_hash: sqlHash(typeof target === "string" ? target : ""),
        approver: a.userName ?? ctx.user ?? "",
        note: typeof rawNote === "string" ? rawNote.slice(0, 500) : "",
      });
      const started = await approvals.trigger();
      return started === false ? { approvalId, message: SCHEDULED } : { approvalId };
    },

    /** The rewrite card of a finding: set-up, who may draft, the original query, every attempt (newest first). */
    async drafts(ctx: Ctx, rawFp: string) {
      const { a, finding } = await visibleFinding(ctx, rawFp);
      return draftView(ctx, a, finding);
    },

    /** "Draft a rewrite" / Retry: records a draft approval for the next attempt and starts the fixer. */
    async requestDraft(ctx: Ctx, rawFp: string) {
      if (!approvals) throw new HttpError(404, "not found");
      const { a, fp, finding } = await visibleFinding(ctx, rawFp);
      if (!(await approvals.canManage(ctx, a, String(finding.project_id))))
        throw new HttpError(403, "only the project owner or a manager can draft a rewrite");
      const view = await draftView(ctx, a, finding);
      if (!view.rewritable) throw new HttpError(409, "this finding has no query to rewrite");
      if (!view.setUp.ready) throw new HttpError(409, `LLM drafting not set up: ${view.setUp.reason ?? ""}`);
      if (!isOpen(finding)) throw new HttpError(409, "the finding is no longer open");
      if (!view.canRequest) throw new HttpError(409, "a draft is already running for this finding");
      const approvalId = approvals.newId();
      await approvals.insert({
        approval_id: approvalId,
        action_id: "",
        fingerprint: fp,
        request: "draft",
        sql_hash: draftToken(fp, view.nextAttempt),
        approver: a.userName ?? ctx.user ?? "",
        note: "",
      });
      const started = await approvals.trigger();
      const out = { approvalId, attempt: view.nextAttempt };
      return started === false ? { ...out, message: SCHEDULED } : out;
    },

    /** The health report of one branch ("default" = the project's default branch), for the tabbed project page. */
    async report(ctx: Ctx, rawId: string, rawBranch: string) {
      return (await reportScope(ctx, rawId, rawBranch)).report;
    },

    /**
     * "Download report": the same data, query text stripped whatever the caller's PII membership, as one HTML file.
     * The audit row is written first; no row, no file.
     */
    async reportExport(ctx: Ctx, rawId: string, rawBranch: string) {
      if (!audit) throw new HttpError(404, "not found");
      const { a, id, report } = await reportScope(ctx, rawId, rawBranch);
      const filename = exportFilename(report, ctx.now);
      const html = renderExport(report);
      const unavailable = [...(report.unavailable ?? []), ...Object.entries(report.sections).filter(([, s]) => s.status === "error").map(([k]) => k)];
      await audit.insert({
        event_id: audit.newId(),
        actor: a.userName ?? ctx.user ?? "",
        action: "report_export",
        project_id: id,
        branch_id: report.branch.id,
        detail: JSON.stringify({ filename, unavailable }),
      });
      return { filename, html };
    },

    refresh(ctx: Ctx) {
      if (ctx.user) access.refresh(ctx.user);
    },
  };
}
