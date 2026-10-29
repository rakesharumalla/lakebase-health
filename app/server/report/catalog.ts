import type { AreaId, Severity } from "../../client/src/report/types";
import checks from "./checks.json";

/** One check as the engine defines it (scripts/gen_check_areas.py writes checks.json from the YAMLs). */
export type CheckInfo = {
  area: AreaId | null;
  source: "delta" | "postgres" | "api";
  title: string;
  thresholds: Partial<Record<Severity, number>>;
  direction: "above" | "below";
};
export type Catalog = Record<string, CheckInfo>;

export const CATALOG = checks as unknown as Catalog;

/** The 12 scorecard areas in report order, with the one-line explanation each card shows. */
export const AREAS: { id: AreaId; label: string; blurb: string }[] = [
  { id: "cache_memory", label: "Cache & memory", blurb: "Local cache hit rate, working set against the cache, CPU and CU sizing." },
  { id: "storage_latency", label: "Storage latency", blurb: "Time queries wait on the pageserver after a cache miss." },
  { id: "write_path", label: "Write path", blurb: "Write throttling, WAL bursts, commit waits and synced-table refreshes." },
  { id: "connections", label: "Connections & sessions", blurb: "Connection use, drops and sessions left idle in a transaction." },
  { id: "locks", label: "Locks & contention", blurb: "Blocked sessions, deadlocks, hot rows and lock wait share." },
  { id: "vacuum_xid", label: "Vacuum & XID", blurb: "Autovacuum health, dead tuples, transaction ID age and what holds it back." },
  { id: "indexes", label: "Indexes", blurb: "Unused, duplicate, redundant, invalid and missing indexes; sequential scans." },
  { id: "tables_bloat", label: "Tables & bloat", blurb: "Table and index bloat, tables without a primary key, sequence runway." },
  { id: "queries", label: "Queries", blurb: "The slowest and regressed queries, plan flips, spills and errors." },
  { id: "config_security", label: "Configuration & security", blurb: "Guardrail settings, drift, privileged and password roles, TLS." },
  { id: "scale_to_zero", label: "Scale to zero", blurb: "Whether idle clients keep the compute from suspending." },
  { id: "resilience", label: "Resilience", blurb: "High availability, restore window, branch protection, storage runway, replica lag." },
];

export const areaLabel = (id: AreaId | null | undefined) => AREAS.find((a) => a.id === id)?.label ?? "Engine";
export const areaOf = (checkId: string, catalog: Catalog = CATALOG): AreaId | null => catalog[checkId]?.area ?? null;
