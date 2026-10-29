import type { ReactNode } from "react";
import { LineChart, type Line } from "./LineChart";
import { DASH, fmtBytes, fmtDuration, fmtMs, fmtNum, fmtPct, fmtUtc, num, text } from "./format";
import type { AreaResult, Report, ReportFinding, Row, RunbookItem, SeriesPoint } from "./types";
import { severityTone } from "../components/tone";
import { Badge, Card, Code, Note, rowClass, SevBadge, Table, Tiles, type Mode, type Tone } from "./ui";

/** The project page's tabs in order; `exported` ones make up the downloaded report, in the same order. */
export const TABS = [
  { id: "health", label: "Health", exported: true },
  { id: "overview", label: "Overview", exported: true },
  { id: "tables", label: "Tables & Indexes", exported: true },
  { id: "queries", label: "Queries", exported: true },
  { id: "waits", label: "Waits & Locks", exported: false },
  { id: "vacuum", label: "Vacuum & Bloat", exported: true },
  { id: "scale-to-zero", label: "Scale to zero", exported: true },
  { id: "system", label: "System", exported: true },
  { id: "settings", label: "Settings", exported: true },
  { id: "runbook", label: "Runbook", exported: true },
  { id: "fixes", label: "Fixes", exported: false },
] as const;
export type TabId = (typeof TABS)[number]["id"];
export const isTab = (v: unknown): v is TabId => TABS.some((t) => t.id === v);

type P = { report: Report; mode: Mode };

const STATE_TONE: Record<AreaResult["state"], Tone> = { pass: "ok", warn: "warning", fail: "critical", not_assessed: "neutral" };
const STATE_LABEL: Record<AreaResult["state"], string> = { pass: "PASS", warn: "WARN", fail: "FAIL", not_assessed: "NOT ASSESSED" };
const PRIORITY_TONE: Record<string, Tone> = { HIGH: "critical", MEDIUM: "warning", LOW: "info" };
const GRADE_TONE: Record<NonNullable<Report["score"]["grade"]>, Tone> = { A: "ok", B: "ok", C: "warning", D: "critical", F: "critical" };

const of = (r: Report, checks: string[]) => r.findings.filter((f) => checks.includes(f.check_id));
const rowsOf = (fs: ReportFinding[]): Row[] =>
  fs.map((f) => ({ severity: f.severity, title: f.title, object: f.object, measure: f.measure, fingerprint: f.fingerprint }));

function findingLink(mode: Mode, fp: unknown, label: ReactNode) {
  return mode === "app" ? <a href={`/findings/${text(fp)}`}>{label}</a> : label;
}

function FindingTable({ findings, mode, empty }: { findings: ReportFinding[]; mode: Mode; empty: string }) {
  return (
    <Table
      empty={empty}
      rows={rowsOf(findings)}
      rowTone={(r) => severityTone(r.severity)}
      columns={[
        { key: "severity", label: "Severity", format: (v) => <SevBadge tone={severityTone(v)}>{text(v)}</SevBadge> },
        { key: "title", label: "Finding", format: (v, r) => findingLink(mode, r.fingerprint, text(v)) },
        { key: "object", label: "Object" },
        { key: "measure", label: "Measure", numeric: true, format: (v) => fmtNum(v, 2) },
      ]}
    />
  );
}

const ago = (s: number | null) => (s === null ? DASH : fmtDuration(s));

export function HealthTab({ report: r, mode }: P) {
  const critical = r.runbook.filter((i) => i.severity === "critical");
  const warnings = r.runbook.filter((i) => i.severity === "warning");
  const na = !!r.score.unavailable;
  return (
    <>
      <Tiles
        tiles={[
          { label: "Score", value: r.score.pct === null ? DASH : `${r.score.pct} %`, hint: na ? "Score unavailable: findings could not be loaded" : `assessed ${r.score.assessed} of ${r.score.total} areas` },
          { label: "Grade", value: r.score.grade ?? DASH, tone: r.score.grade ? GRADE_TONE[r.score.grade] : undefined },
          { label: "Critical", value: na ? DASH : critical.length, tone: na ? undefined : critical.length ? "critical" : "ok" },
          { label: "Warnings", value: na ? DASH : warnings.length, tone: na ? undefined : warnings.length ? "warning" : "ok" },
        ]}
      />
      <Card mode={mode} title="Scorecard" explain="Each area scores 5 with no open finding, 4 with only information, 3 with a warning and 1 with a critical finding; areas whose checks did not run are left out of the score.">
        <div className="rp-scorecard">
          {r.areas.map((a) => (
            <div key={a.id} className={`rp-area rp-area-${a.state}`}>
              <div className="rp-area-head">
                <Badge tone={STATE_TONE[a.state]}>{STATE_LABEL[a.state]}</Badge>
                <span className="rp-area-label">{a.label}</span>
                <span className="rp-area-score">{a.score === null ? "" : `${a.score}/5`}</span>
              </div>
              <p className="rp-area-blurb">{a.blurb}</p>
              <p className="rp-area-detail">
                {a.worst
                  ? findingLink(mode, a.worst.fingerprint, `${a.worst.title}: ${a.worst.object}${a.open > 1 ? ` (+${a.open - 1} more)` : ""}`)
                  : a.note ?? "No open findings."}
              </p>
              {a.lastAssessed ? <p className="rp-area-detail">Compute suspended at the last run: last assessed {fmtUtc(a.lastAssessed)}</p> : null}
            </div>
          ))}
        </div>
      </Card>
      <Card mode={mode} title="Where to focus" explain="The first place to look, from the lowest-scoring areas.">
        <p className="rp-focus">{r.focus}</p>
      </Card>
      <Card mode={mode} title="Critical items and warnings" explain="Open findings that need action, most severe first; the Runbook tab has the SQL.">
        <FindingTable mode={mode} empty="No critical or warning findings." findings={r.findings.filter((f) => f.severity !== "info").sort((a, b) => (a.severity === b.severity ? 0 : a.severity === "critical" ? -1 : 1))} />
      </Card>
    </>
  );
}

export function OverviewTab({ report: r, mode }: P) {
  const o = r.sections.overview.data;
  const ws = o.workingSetBytes !== null && o.lfcSizeBytes ? (100 * o.workingSetBytes) / o.lfcSizeBytes : null;
  return (
    <>
      <Tiles
        tiles={[
          { label: "LFC hit (24 h)", value: fmtPct(o.lfcHitPct, 2), tone: o.lfcHitPct !== null && o.lfcHitPct < 99 ? "warning" : undefined, hint: "share of page reads served by the compute's local cache" },
          { label: "Working set vs LFC", value: fmtPct(ws, 0), hint: `${fmtBytes(o.workingSetBytes)} of ${fmtBytes(o.lfcSizeBytes)}`, tone: ws !== null && ws > 100 ? "warning" : undefined },
          { label: "Connections", value: `${fmtNum(o.connections)} / ${fmtNum(o.maxConnections)}`, hint: "open now vs max_connections" },
          { label: "Database size", value: fmtBytes(o.databaseBytes) },
          { label: "Oldest transaction", value: ago(o.oldestXactS), hint: "age of the oldest open client transaction" },
          { label: "Getpage p99 (24 h)", value: fmtMs(o.getpageP99Ms), hint: "per page read from storage, sampled queries" },
          { label: "Temp spill (24 h)", value: fmtBytes(o.tempSpillBytes), hint: "sorts and hashes that did not fit in work_mem" },
          { label: "Commits per second", value: fmtNum(o.commitsPerS, 1), hint: "24 h average" },
        ]}
      />
      <Card mode={mode} title="Databases" explain="Size and connections per database on the read-write compute, from the last telemetry sample." section={r.sections.databases}>
        <Table
          empty="No database telemetry in the last 24 hours."
          rows={r.sections.databases.data}
          columns={[
            { key: "datname", label: "Database" },
            { key: "size_bytes", label: "Size", numeric: true, format: fmtBytes },
            { key: "backends", label: "Connections", numeric: true, format: (v) => fmtNum(v) },
            { key: "xid_age", label: "Oldest XID age", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
    </>
  );
}

const INDEX_CHECKS = ["unused_index", "duplicate_index", "redundant_index", "invalid_index", "missing_fk_index", "synced_index_review"];

export function TablesTab({ report: r, mode }: P) {
  const t = r.sections.tables.data;
  const synced = t.filter((x) => x.synced_table);
  const idxFindings = of(r, INDEX_CHECKS);
  return (
    <>
      <Tiles
        tiles={[
          { label: "Largest tables", value: t.length, hint: "up to 25 per database" },
          { label: "Their total size", value: fmtBytes(t.reduce((n, x) => n + (num(x.total_bytes) ?? 0), 0)) },
          { label: "Synced table storage", value: synced.length },
          { label: "Index findings", value: idxFindings.length, tone: idxFindings.length ? "warning" : "ok" },
        ]}
      />
      <Card mode={mode} title="Largest tables" explain="Size (data, indexes, TOAST), live and dead rows, and how the table is read: many sequential scans on a big table usually mean a missing index." section={r.sections.tables}>
        <Table
          empty="No inventory yet: it is collected on the next analysis run of an enrolled, running compute."
          rows={t}
          columns={[
            { key: "table_name", label: "Table", format: (v, x) => `${text(x.datname)}:${text(x.schema_name)}.${text(v)}` },
            { key: "total_bytes", label: "Total", numeric: true, format: fmtBytes },
            { key: "data_bytes", label: "Data", numeric: true, format: fmtBytes },
            { key: "index_bytes", label: "Indexes", numeric: true, format: fmtBytes },
            { key: "toast_bytes", label: "TOAST", numeric: true, format: fmtBytes },
            { key: "n_live_tup", label: "Live rows", numeric: true, format: (v) => fmtNum(v) },
            { key: "n_dead_tup", label: "Dead rows", numeric: true, format: (v) => fmtNum(v) },
            { key: "seq_scan", label: "Seq scans", numeric: true, format: (v) => fmtNum(v) },
            { key: "idx_scan", label: "Index scans", numeric: true, format: (v) => fmtNum(v) },
            { key: "heap_blks_read", label: "Blocks read", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
      <Card mode={mode} title="Indexes" explain="Indexes on those tables by size; an index with no scans since statistics reset costs writes and storage." section={r.sections.indexes}>
        <Table
          empty="No indexes in the inventory."
          limit={50}
          rows={r.sections.indexes.data}
          columns={[
            { key: "index_name", label: "Index", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "table_name", label: "Table" },
            { key: "method", label: "Method" },
            { key: "index_bytes", label: "Size", numeric: true, format: fmtBytes },
            { key: "idx_scan", label: "Scans", numeric: true, format: (v) => fmtNum(v) },
            { key: "is_valid", label: "Valid", format: (v) => (v === false || v === "false" ? "no" : "yes") },
            { key: "synced_table", label: "Synced table", format: (v) => (v ? text(v) : "") },
          ]}
        />
      </Card>
      <Card mode={mode} title="Unused, duplicate and redundant indexes" explain="Index findings from the checks; indexes on synced tables are listed for review only, because the sync owns them.">
        <FindingTable mode={mode} findings={idxFindings} empty="No index findings." />
      </Card>
      <Card mode={mode} title="Synced tables" explain="Storage of tables synced from Unity Catalog: a full refresh rewrites every row and index." section={r.sections.tables}>
        <Table
          empty="No synced tables among the largest tables."
          rows={synced}
          columns={[
            { key: "synced_table", label: "Synced table" },
            { key: "table_name", label: "Storage", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "total_bytes", label: "Size", numeric: true, format: fmtBytes },
            { key: "n_live_tup", label: "Rows", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
    </>
  );
}

const STATEMENT_COLUMNS = [
  { key: "queryid", label: "Query ID" },
  { key: "role", label: "Role" },
  { key: "datname", label: "Database" },
  { key: "calls", label: "Calls", numeric: true, format: (v: Row[string]) => fmtNum(v) },
  { key: "mean_ms", label: "Mean", numeric: true, format: fmtMs },
  { key: "total_ms", label: "Total", numeric: true, format: fmtMs },
  { key: "shared_blks_hit", label: "Blocks hit", numeric: true, format: (v: Row[string]) => fmtNum(v) },
  { key: "shared_blks_read", label: "Blocks read", numeric: true, format: (v: Row[string]) => fmtNum(v) },
  { key: "temp_blks_written", label: "Temp blocks", numeric: true, format: (v: Row[string]) => fmtNum(v) },
];

/** Top statements by time and by I/O (query IDs only; the app adds text for those allowed to read it). */
export function StatementsCards({ report: r, mode }: P) {
  const s = r.sections.statements.data;
  return (
    <>
      <Card mode={mode} title="Top statements by time" explain="pg_stat_statements since its last reset: where the database spends its time. Platform and engine roles are left out." section={r.sections.statements}>
        <Table empty="pg_stat_statements has no data yet, or the extension is not installed." rows={s.filter((x) => x.ranking === "time")} columns={STATEMENT_COLUMNS} />
      </Card>
      <Card mode={mode} title="Top statements by I/O" explain="The statements that read the most blocks from outside shared buffers: candidates for an index or a smaller result." section={r.sections.statements}>
        <Table empty="No statements read from storage." rows={s.filter((x) => x.ranking === "io")} columns={STATEMENT_COLUMNS} />
      </Card>
    </>
  );
}

export function QueriesTiles({ report: r }: { report: Report }) {
  const s = r.sections.statements.data.filter((x) => x.ranking === "time");
  const q = r.findings.filter((f) => f.area === "queries");
  return (
    <Tiles
      tiles={[
        { label: "Query findings", value: q.length, tone: q.length ? "warning" : "ok" },
        { label: "Statements tracked", value: s.length, hint: "top 20 by total time" },
        { label: "Busiest statement", value: fmtMs(s[0]?.total_ms), hint: s[0] ? `query ${text(s[0].queryid)}` : undefined },
        { label: "Temp blocks written", value: fmtNum(s.reduce((n, x) => n + (num(x.temp_blks_written) ?? 0), 0)) },
      ]}
    />
  );
}

export function QueriesTab({ report, mode }: P) {
  return (
    <>
      <QueriesTiles report={report} />
      <StatementsCards report={report} mode={mode} />
    </>
  );
}

/** Average active sessions per wait class over 24 h, from the per-15-minute buckets. */
export function waitSummary(rows: Row[]): Row[] {
  const by = new Map<string, number>();
  const buckets = new Set(rows.map((r) => text(r.bucket)));
  for (const r of rows) by.set(text(r.wait_class), (by.get(text(r.wait_class)) ?? 0) + (num(r.avg_active) ?? 0));
  return [...by.entries()]
    .map(([wait_class, n]) => ({ wait_class, avg_active: n / Math.max(buckets.size, 1) }))
    .sort((a, b) => b.avg_active - a.avg_active);
}

const COLORS: Line["color"][] = [1, 2, 3, 4, 5, 6, 7, 8];
function waitLines(rows: Row[]): Line[] {
  const classes = waitSummary(rows).map((r) => text(r.wait_class)).slice(0, 8);
  return classes.map((c, i) => ({
    label: c,
    color: COLORS[i],
    points: rows.filter((r) => text(r.wait_class) === c).map((r) => ({ t: text(r.bucket), v: num(r.avg_active) })),
  }));
}

export function WaitsCards({ report: r, mode }: P) {
  const summary = waitSummary(r.sections.waits.data);
  return (
    <>
      <Tiles
        tiles={[
          { label: "Active sessions (24 h avg)", value: fmtNum(summary.reduce((n, x) => n + (num(x.avg_active) ?? 0), 0), 2) },
          { label: "Top wait class", value: summary[0] ? text(summary[0].wait_class) : DASH },
          { label: "Lock findings", value: r.findings.filter((f) => f.area === "locks").length },
          { label: "Connection findings", value: r.findings.filter((f) => f.area === "connections").length },
        ]}
      />
      <Card mode={mode} title="Wait events by class (24 h)" explain="Average sessions active in each wait class, from sampled session history; CPU means running, not waiting." section={r.sections.waits}>
        <LineChart title="Active sessions by wait class" lines={waitLines(r.sections.waits.data)} format={(v) => v.toFixed(1)} />
        <Table empty="No sampled sessions in the last 24 hours." rows={summary} columns={[{ key: "wait_class", label: "Wait class" }, { key: "avg_active", label: "Average active sessions", numeric: true, format: (v) => fmtNum(v, 2) }]} />
      </Card>
      <Card mode={mode} title="Lock and session findings" explain="Blocked sessions, deadlocks, hot rows and long or idle transactions found by the checks.">
        <FindingTable mode={mode} findings={r.findings.filter((f) => f.area === "locks" || f.area === "connections")} empty="No lock or session findings." />
      </Card>
    </>
  );
}

export function VacuumTab({ report: r, mode }: P) {
  const t = r.sections.tables.data;
  const e = r.sections.endpoint.data.latest;
  const over = t.filter((x) => (num(x.n_dead_tup) ?? 0) > (num(x.vacuum_threshold) ?? Infinity));
  const xid = [...t].sort((a, b) => (num(b.xid_age) ?? 0) - (num(a.xid_age) ?? 0));
  return (
    <>
      <Tiles
        tiles={[
          { label: "Autovacuum workers", value: `${fmtNum(e.autovacuum_workers_busy)} / ${fmtNum(e.autovacuum_max_workers)}`, hint: "busy now / autovacuum_max_workers", tone: e.autovacuum_workers_busy !== undefined && e.autovacuum_workers_busy === e.autovacuum_max_workers ? "warning" : undefined },
          { label: "Tables over vacuum threshold", value: over.length, tone: over.length ? "warning" : "ok" },
          { label: "Highest table XID age", value: fmtNum(xid[0]?.xid_age), hint: "wraparound protection starts at 200 million" },
          { label: "Dead rows (largest tables)", value: fmtNum(t.reduce((n, x) => n + (num(x.n_dead_tup) ?? 0), 0)) },
        ]}
      />
      <Card mode={mode} title="Vacuum and analyze" explain="When each large table was last vacuumed and analyzed, and whether dead rows are above the point where autovacuum should start." section={r.sections.tables}>
        <Table
          empty="No inventory yet."
          rows={t}
          columns={[
            { key: "table_name", label: "Table", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "n_dead_tup", label: "Dead rows", numeric: true, format: (v) => fmtNum(v) },
            { key: "vacuum_threshold", label: "Vacuum threshold", numeric: true, format: (v) => fmtNum(v) },
            { key: "last_autovacuum", label: "Last autovacuum", format: fmtUtc },
            { key: "last_autoanalyze", label: "Last autoanalyze", format: fmtUtc },
            { key: "autovacuum_count", label: "Autovacuums", numeric: true, format: (v) => fmtNum(v) },
            { key: "autoanalyze_count", label: "Autoanalyzes", numeric: true, format: (v) => fmtNum(v) },
          ]}
        />
      </Card>
      <Card mode={mode} title="Transaction ID age" explain="How close each table is to forced anti-wraparound vacuum; a high age with old vacuums means something holds vacuum back." section={r.sections.tables}>
        <Table
          empty="No inventory yet."
          limit={10}
          rows={xid}
          columns={[
            { key: "table_name", label: "Table", format: (v, x) => `${text(x.schema_name)}.${text(v)}` },
            { key: "xid_age", label: "XID age", numeric: true, format: (v) => fmtNum(v) },
            { key: "last_vacuum", label: "Last manual vacuum", format: fmtUtc },
          ]}
        />
      </Card>
      <Card mode={mode} title="Bloat and vacuum findings" explain="Table and index bloat, dead tuples and what blocks autovacuum, from the checks.">
        <FindingTable mode={mode} findings={r.findings.filter((f) => f.area === "vacuum_xid" || f.area === "tables_bloat")} empty="No vacuum or bloat findings." />
      </Card>
    </>
  );
}

export function ScaleToZeroTab({ report: r, mode }: P) {
  const primary = r.endpoints.find((e) => e.id === r.primary);
  const idle = r.sections.endpoint.data.idleClients;
  const overrides = r.sections.tables.data.filter((x) => x.autovacuum_overrides);
  const blockers = of(r, ["suspend_blockers"]);
  return (
    <>
      <Tiles
        tiles={[
          { label: "Scale to zero", value: primary?.suspendTimeoutS ? "on" : "off", hint: primary?.suspendTimeoutS ? `suspends after ${fmtDuration(primary.suspendTimeoutS)} idle` : "the compute never suspends" },
          { label: "Idle client sessions", value: idle.reduce((n, c) => n + c.sessions, 0) },
          { label: "Suspend blockers", value: blockers.length, tone: blockers.length ? "warning" : "ok" },
          { label: "Up for", value: fmtDuration(r.sections.endpoint.data.latest.uptime_s ?? null), hint: "since Postgres last started" },
        ]}
      />
      <Card mode={mode} title="Suspend blockers" explain="Applications whose idle connections kept the compute awake for a day: set their pool's minimum idle connections to 0.">
        <FindingTable mode={mode} findings={blockers} empty="Nothing keeps the compute from suspending." />
      </Card>
      <Card mode={mode} title="Idle clients by application" explain="Client sessions open but idle at the last snapshot, per role and application; any of them prevents a suspend." section={r.sections.endpoint}>
        <Table
          empty="No idle client sessions."
          rows={idle.map((c) => ({ application: c.application, role: c.role, sessions: c.sessions, maxIdleS: c.maxIdleS }))}
          columns={[
            { key: "application", label: "Application" },
            { key: "role", label: "Role" },
            { key: "sessions", label: "Sessions", numeric: true },
            { key: "maxIdleS", label: "Longest idle", numeric: true, format: fmtDuration },
          ]}
        />
      </Card>
      <Card mode={mode} title="Autovacuum overrides" explain="Tables with their own autovacuum settings; aggressive settings can keep a compute busy." section={r.sections.tables}>
        <Table empty="No table overrides autovacuum settings." rows={overrides} columns={[{ key: "table_name", label: "Table", format: (v, x) => `${text(x.schema_name)}.${text(v)}` }, { key: "autovacuum_overrides", label: "Settings" }]} />
      </Card>
    </>
  );
}

const pts = (s: SeriesPoint[] | undefined) => s ?? [];
const last = (s: SeriesPoint[] | undefined) => [...pts(s)].reverse().find((p) => p.v !== null)?.v ?? null;

export function SystemTab({ report: r, mode }: P) {
  const s = r.sections.series.data;
  const e = r.sections.endpoint.data;
  const io = r.sections.io.data;
  const sum = (k: string) => pts(s[k]).reduce((n, p) => n + (p.v ?? 0), 0);
  const replicas = r.endpoints.some((x) => x.type === "replica");
  const perf = e.sources.neon_perf_counters;
  const sources = Object.entries(e.sources).map(([name, v]) => ({ name, available: v.available ? "yes" : "no", note: v.note ?? "" }));
  return (
    <>
      <Tiles
        tiles={[
          { label: "LFC hit (last 15 min)", value: fmtPct(last(s.lfc_hit_pct), 2) },
          { label: "Working set", value: fmtBytes(last(s.working_set_bytes)), hint: `cache ${fmtBytes(last(s.lfc_size_bytes))}` },
          { label: "Getpage p99 (last 15 min)", value: fmtMs(last(s.getpage_p99_ms)) },
          { label: "WAL (24 h)", value: fmtBytes(sum("wal_bytes")), hint: `${fmtNum(sum("wal_fpi"))} full-page images` },
        ]}
      />
      <Card mode={mode} title="LFC hit rate" explain="Share of page reads served by the compute's local cache per 15 minutes; below 99 % queries wait on storage." section={r.sections.series}>
        <LineChart title="LFC hit rate" lines={[{ label: "LFC hit %", color: 1, points: pts(s.lfc_hit_pct) }]} format={(v) => `${v.toFixed(1)} %`} />
      </Card>
      <Card mode={mode} title="Working set vs cache" explain="Pages touched in the last hour against the cache size: a working set above the cache means constant evictions." section={r.sections.series}>
        <LineChart title="Working set" lines={[{ label: "Working set (60 min)", color: 2, points: pts(s.working_set_bytes) }, { label: "Cache size", color: 3, points: pts(s.lfc_size_bytes) }]} format={fmtBytes} />
      </Card>
      <Card mode={mode} title="Getpage latency" explain="Time per page read from storage after a cache miss, average and p99 of sampled queries per 15 minutes." section={r.sections.series}>
        <LineChart title="Getpage latency" lines={[{ label: "average", color: 1, points: pts(s.getpage_avg_ms) }, { label: "p99", color: 2, points: pts(s.getpage_p99_ms) }, ...(e.getpageP99.length ? [{ label: "p99, all reads (neon_perf_counters)", color: 4 as const, points: e.getpageP99 }] : [])]} format={(v) => fmtMs(v)} />
        {perf && !perf.available ? <Note>Endpoint-level p99 over all reads: {perf.note ?? "not available"}. See the README to enable it.</Note> : null}
      </Card>
      <Card mode={mode} title="WAL and full-page images" explain="Write-ahead log written by statements per 15 minutes; spikes come from bulk loads, index builds and synced-table refreshes." section={r.sections.series}>
        <LineChart title="WAL bytes" lines={[{ label: "WAL", color: 6, points: pts(s.wal_bytes) }]} format={fmtBytes} />
        <LineChart title="Full-page images" lines={[{ label: "Full-page images", color: 5, points: pts(s.wal_fpi) }]} format={(v) => fmtNum(v)} />
      </Card>
      <Card mode={mode} title="Active sessions by wait class" explain="Average sessions active in each wait class per 15 minutes." section={r.sections.waits}>
        <LineChart title="Active sessions by wait class" lines={waitLines(r.sections.waits.data)} format={(v) => v.toFixed(1)} />
      </Card>
      {replicas ? (
        <Card mode={mode} title="Replica lag" explain="How far the read replicas are behind the primary, in bytes of WAL not yet replayed." section={r.sections.series}>
          <LineChart title="Replica lag" lines={[{ label: "lag", color: 7, points: pts(s.replica_lag_bytes) }]} format={fmtBytes} />
        </Card>
      ) : null}
      <Card mode={mode} title="I/O by backend" explain={`pg_stat_io ${io.delta ? `over the last ${fmtDuration(io.windowS)}` : "since the counters were reset"}: who reads and writes, by backend type, object and context. Counters reset on compute restart; high autovacuum or bulkread reads point at cache churn.`} section={r.sections.io}>
        <Table
          empty="pg_stat_io is not available (Postgres 16 or later), or nothing was read yet."
          rows={io.rows as unknown as Row[]}
          columns={[
            { key: "backend_type", label: "Backend" },
            { key: "object", label: "Object" },
            { key: "context", label: "Context" },
            { key: "reads", label: "Reads", numeric: true, format: (v) => fmtNum(v) },
            { key: "writes", label: "Writes", numeric: true, format: (v) => fmtNum(v) },
            { key: "hits", label: "Hits", numeric: true, format: (v) => fmtNum(v) },
            { key: "evictions", label: "Evictions", numeric: true, format: (v) => fmtNum(v) },
            { key: "extends", label: "Extends", numeric: true, format: (v) => fmtNum(v) },
            { key: "fsyncs", label: "Fsyncs", numeric: true, format: (v) => fmtNum(v) },
            { key: "read_time", label: "Read time", numeric: true, format: (v) => (io.timing ? fmtMs(v) : "timing off") },
            { key: "write_time", label: "Write time", numeric: true, format: (v) => (io.timing ? fmtMs(v) : "timing off") },
          ]}
        />
      </Card>
      <Card mode={mode} title="Endpoint sources" explain="Which Postgres-side sources the engine could read on this compute; without them the charts use telemetry only." section={r.sections.endpoint}>
        <Table empty="No snapshot from this compute yet." rows={sources} columns={[{ key: "name", label: "Source" }, { key: "available", label: "Read" }, { key: "note", label: "Note" }]} />
      </Card>
    </>
  );
}

export function SettingsTab({ report: r, mode }: P) {
  const s = r.sections.settings.data;
  return (
    <>
      <Tiles
        tiles={[
          { label: "Settings shown", value: s.length },
          { label: "Not at default", value: s.filter((x) => x.source !== "default").length },
          { label: "Changeable per session", value: s.filter((x) => x.how_to_change === "session").length },
          { label: "Support-managed", value: s.filter((x) => x.how_to_change === "support-managed").length },
        ]}
      />
      <Card mode={mode} title="Key parameters" explain="The settings that matter most for performance, plus every one changed from its default, with where it was set and how you can change it." section={r.sections.settings}>
        <Table
          empty="No settings snapshot yet."
          rows={s}
          columns={[
            { key: "name", label: "Setting" },
            { key: "setting", label: "Value", format: (v, x) => `${text(v)}${x.unit ? ` ${text(x.unit)}` : ""}` },
            { key: "source", label: "Source" },
            { key: "how_to_change", label: "How to change" },
          ]}
        />
      </Card>
    </>
  );
}

function RunbookEntry({ item, mode, n }: { item: RunbookItem; mode: Mode; n: number }) {
  return (
    <li className={["rp-runbook-item", rowClass(severityTone(item.severity))].filter(Boolean).join(" ")}>
      <div className="rp-runbook-head">
        <span className="rp-runbook-n">{n}.</span>
        <SevBadge tone={PRIORITY_TONE[item.priority]}>{item.priority}</SevBadge>
        <span className="rp-runbook-area">{item.areaLabel}</span>
        <span className="rp-runbook-title">{findingLink(mode, item.fingerprint, item.title)}</span>
      </div>
      <p className="rp-runbook-object">{item.object}</p>
      <p className="rp-runbook-why">{item.why}</p>
      {item.evidence.length ? <p className="rp-runbook-evidence">{item.evidence.join(" · ")}</p> : null}
      {item.sql ? <Code>{item.sql}</Code> : <Note>{item.note}</Note>}
      {mode === "app" && item.actionId ? (
        <p className="rp-runbook-fix">
          <a href={`/findings/${item.fingerprint}`}>Open the fix card</a> ({item.actionState?.replace(/_/g, " ")})
        </p>
      ) : null}
    </li>
  );
}

export function RunbookTab({ report: r, mode }: P) {
  const count = (p: string) => r.runbook.filter((i) => i.priority === p).length;
  const all = r.runbook.filter((i) => i.sql).map((i) => `-- ${i.priority}: ${i.title} (${i.object})\n${i.sql}`).join("\n\n");
  return (
    <>
      <Tiles
        tiles={[
          { label: "High", value: count("HIGH"), tone: count("HIGH") ? "critical" : "ok" },
          { label: "Medium", value: count("MEDIUM"), tone: count("MEDIUM") ? "warning" : "ok" },
          { label: "Low", value: count("LOW"), tone: "info" },
          { label: "With SQL", value: r.runbook.filter((i) => i.sql).length },
        ]}
      />
      <Card mode={mode} title="Actions in order" explain="One item per open finding: most severe first, then those with a fix verified on a branch, then by how far past its threshold.">
        {r.runbook.length ? (
          <ol className="rp-runbook">
            {r.runbook.map((i, n) => (
              <RunbookEntry key={i.fingerprint} item={i} mode={mode} n={n + 1} />
            ))}
          </ol>
        ) : (
          <p className="rp-empty">No open findings.</p>
        )}
      </Card>
      <Card mode={mode} title="All SQL" explain="Every item's SQL in runbook order, to review and run as one script; verify each change on a branch first.">
        {all ? <Code>{all}</Code> : <p className="rp-empty">No item has SQL.</p>}
      </Card>
    </>
  );
}

/** The tab bodies the export (and the page, for these tabs) renders. */
export const TAB_BODY: Partial<Record<TabId, (p: P) => ReactNode>> = {
  health: HealthTab,
  overview: OverviewTab,
  tables: TablesTab,
  queries: QueriesTab,
  vacuum: VacuumTab,
  "scale-to-zero": ScaleToZeroTab,
  system: SystemTab,
  settings: SettingsTab,
  runbook: RunbookTab,
};
