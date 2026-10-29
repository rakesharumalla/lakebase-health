/**
 * The health report's data, built by the server (server/report/build.ts) and rendered by the components in this
 * folder, both in the app and in the downloaded HTML. Pure types: the server imports this file too.
 */
export type AreaId =
  | "cache_memory"
  | "storage_latency"
  | "write_path"
  | "connections"
  | "locks"
  | "vacuum_xid"
  | "indexes"
  | "tables_bloat"
  | "queries"
  | "config_security"
  | "scale_to_zero"
  | "resilience";

export type Severity = "info" | "warning" | "critical";
export type AreaState = "pass" | "warn" | "fail" | "not_assessed";

export type AreaResult = {
  id: AreaId;
  label: string;
  /** One line: what the area covers. */
  blurb: string;
  state: AreaState;
  /** 5 with no open finding, 4 info, 3 warning, 1 critical; null when not assessed. */
  score: number | null;
  open: number;
  worst: { fingerprint: string; title: string; object: string; severity: Severity } | null;
  /** Why it is not assessed (e.g. "counters too young (reset 3 h ago)"). */
  note: string | null;
  /** Set when the compute was suspended at the last run: the time of the assessment the area still uses. */
  lastAssessed?: string;
};

export type Score = {
  pct: number | null;
  grade: "A" | "B" | "C" | "D" | "F" | null;
  assessed: number;
  total: number;
  /** Set when the findings or check-status query failed: no score is shown rather than a false A. */
  unavailable?: true;
};

export type Priority = "HIGH" | "MEDIUM" | "LOW";
export type RunbookItem = {
  priority: Priority;
  severity: Severity;
  fingerprint: string;
  checkId: string;
  area: AreaId | null;
  areaLabel: string;
  title: string;
  object: string;
  why: string;
  evidence: string[];
  sql: string | null;
  /** Shown instead of SQL when there is none. */
  note: string | null;
  actionId: string | null;
  actionState: string | null;
};

export type ReportFinding = {
  fingerprint: string;
  check_id: string;
  title: string;
  category: string;
  area: AreaId | null;
  endpoint_id: string | null;
  object: string;
  status: string;
  severity: Severity;
  confidence: string | null;
  measure: number | null;
  first_seen: string | null;
  last_seen: string | null;
  /** Evidence JSON, with statement text removed. */
  evidence: string | null;
  explain: string | null;
  candidate_sql: string | null;
  queryid: string | null;
  query_text?: string | null;
};

export type Endpoint = {
  id: string;
  type: "primary" | "replica";
  state: string;
  minCu: number | null;
  maxCu: number | null;
  suspendTimeoutS: number | null;
  enrolled: boolean;
};

export type Branch = { id: string; name: string; isDefault: boolean };

export type Row = Record<string, string | number | boolean | null>;

/** One panel's data: rows, or an error, with the snapshot time and whether it is stale. */
export type Section<T = Row[]> = {
  status: "ok" | "error";
  data: T;
  error?: string;
  asOf?: string | null;
  stale?: boolean;
};

export type SeriesPoint = { t: string; v: number | null };
export type Series = Record<string, SeriesPoint[]>;

export type IoRow = {
  backend_type: string;
  object: string;
  context: string;
  reads: number | null;
  writes: number | null;
  extends: number | null;
  hits: number | null;
  evictions: number | null;
  reuses: number | null;
  fsyncs: number | null;
  read_time: number | null;
  write_time: number | null;
};
export type IoData = { rows: IoRow[]; delta: boolean; timing: boolean; windowS: number | null; statsReset: string | null };

export type Overview = {
  lfcHitPct: number | null;
  workingSetBytes: number | null;
  lfcSizeBytes: number | null;
  connections: number | null;
  maxConnections: number | null;
  databaseBytes: number | null;
  oldestXactS: number | null;
  getpageP99Ms: number | null;
  tempSpillBytes: number | null;
  commitsPerS: number | null;
  telemetryAt: string | null;
};

export type EndpointMetrics = {
  /** Latest value per metric (autovacuum_workers_busy, neon.lfc.file_cache_hits, ...). */
  latest: Record<string, number | null>;
  /** source:<name> -> available, with the reason when not. */
  sources: Record<string, { available: boolean; note: string | null }>;
  idleClients: { role: string; application: string; sessions: number; maxIdleS: number | null }[];
  /** Endpoint-level getpage p99 (ms) per snapshot pair, from neon_perf_counters; [] when not readable. */
  getpageP99: SeriesPoint[];
  serverVersion: string | null;
};

export type ReportSections = {
  overview: Section<Overview>;
  databases: Section;
  tables: Section;
  indexes: Section;
  statements: Section;
  settings: Section;
  io: Section<IoData>;
  endpoint: Section<EndpointMetrics>;
  series: Section<Series>;
  waits: Section;
};

export type Report = {
  project: { id: string; name: string };
  branch: Branch;
  branches: Branch[];
  endpoints: Endpoint[];
  /** The read-write endpoint the inventory panels and charts describe. */
  primary: string | null;
  pgVersion: string | null;
  dataAsOf: string | null;
  generatedAt: string;
  banners: string[];
  /** Report-level queries that failed ("findings", "checks"); the inventory panels carry their own status. */
  unavailable?: ("findings" | "checks")[];
  score: Score;
  areas: AreaResult[];
  focus: string;
  findings: ReportFinding[];
  runbook: RunbookItem[];
  sections: ReportSections;
  /** In the app only: query findings grouped one row per query (server/lib/group.ts). Empty in exports. */
  queryGroups: unknown[];
};
