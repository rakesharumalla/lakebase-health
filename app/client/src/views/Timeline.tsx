import {
  AreaChart,
  BarChart,
  Button,
  ButtonGroup,
  LineChart,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@databricks/appkit-ui/react";
import { useMemo, useState } from "react";
import { useParams, useSearchParams } from "react-router";
import { ApiMessage } from "../ApiMessage";
import { ToneBadge } from "../components/Severity";
import { DataTable, InlineLoading, LoadingState, MonoId, NUM, PageHeader, SectionCard } from "../components/ui";
import { BLOCKER_CLASSES, BLOCKER_COLOR_TOKENS, BLOCKER_LABELS } from "../lib/constants";
import { fmtTime, str, type Row } from "../lib/format";
import { pivot } from "../lib/pivot";
import { tokenColors } from "../lib/theme";
import { useApi } from "../lib/useApi";

export type TimelineData = {
  endpointId: string;
  from: string;
  to: string;
  waits: Row[];
  locks: Row[];
  commits: Row[];
  events: Row[];
  gauges: Row[];
};

const EVENT_LABELS: Record<string, string> = {
  statement_timeout: "statement timeout",
  ssl_eof: "SSL EOF",
  connection_reset: "connection reset",
  client_lost: "client lost",
  compute_restart: "compute restart",
};

function plural(n: number, word: string) {
  return `${n} ${word}${n === 1 ? "" : "s"}`;
}

const secs = (v: unknown) => (v ? (Number(v) / 1000).toFixed(1) : "");

export function SamplesTable({ samples }: { samples: Row[] }) {
  return (
    <DataTable>
      <TableHeader>
        <TableRow>
          <TableHead>Time</TableHead>
          <TableHead className="text-right">PID</TableHead>
          <TableHead>Query ID</TableHead>
          <TableHead>Wait</TableHead>
          <TableHead>State</TableHead>
          <TableHead className="text-right">Blocked by</TableHead>
          <TableHead className="text-right">Query age (s)</TableHead>
          <TableHead className="text-right">Transaction age (s)</TableHead>
          <TableHead>Query</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {samples.map((s) => (
          <TableRow key={`${str(s.ts)}-${str(s.pid)}`}>
            <TableCell className="text-muted-foreground">{fmtTime(s.ts)}</TableCell>
            <TableCell className={NUM}>{str(s.pid)}</TableCell>
            <TableCell>
              <MonoId value={str(s.queryid)} max="max-w-[14ch]" />
            </TableCell>
            <TableCell>{[str(s.wait_event_type), str(s.wait_event)].filter(Boolean).join(" / ")}</TableCell>
            <TableCell>{str(s.backend_state)}</TableCell>
            <TableCell className={NUM}>{str(s.blocking_pid) === "0" ? "" : str(s.blocking_pid)}</TableCell>
            <TableCell className={NUM}>{secs(s.query_age_ms)}</TableCell>
            <TableCell className={NUM}>{secs(s.xact_age_ms)}</TableCell>
            <TableCell className="max-w-[28rem]">
              <span title={str(s.query_text)} className="line-clamp-2 whitespace-normal break-all font-mono text-[0.8125rem]">
                {str(s.query_text)}
              </span>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </DataTable>
  );
}

function BlockerLegend() {
  return (
    <ul className="flex flex-wrap gap-x-5 gap-y-1 text-sm" aria-label="Blocker classes">
      {BLOCKER_CLASSES.map((c) => (
        <li key={c} className="flex items-center gap-2">
          <span aria-hidden className="size-3 rounded-sm" style={{ background: `var(${BLOCKER_COLOR_TOKENS[c]})` }} />
          {BLOCKER_LABELS[c]}
        </li>
      ))}
    </ul>
  );
}

export function TimelineView({ data, onPick }: { data: TimelineData; onPick: (minute: string) => void }) {
  const waitClasses = useMemo(() => [...new Set(data.waits.map((w) => str(w.wait_class)))].sort(), [data.waits]);
  const waits = useMemo(() => pivot(data.waits, "minute", "wait_class", "avg_sessions", waitClasses), [data.waits, waitClasses]);
  const locks = useMemo(() => pivot(data.locks, "minute", "blocker_class", "samples", [...BLOCKER_CLASSES]), [data.locks]);
  const blockerColors = useMemo(() => tokenColors(BLOCKER_CLASSES.map((c) => BLOCKER_COLOR_TOKENS[c])), []);
  const commits = data.commits.map((c) => ({ minute: str(c.minute), sessions: Number(c.sessions) }));
  const gauges = data.gauges.map((g) => ({
    minute: str(g.minute),
    cpu_cores_used: Number(g.cpu_cores_used ?? 0),
    max_connections: Number(g.max_connections ?? 0),
  }));
  const eventCounts = new Map<string, number>();
  for (const e of data.events) eventCounts.set(str(e.kind), (eventCounts.get(str(e.kind)) ?? 0) + 1);

  return (
    <div className="space-y-6">
      <p className="text-sm text-muted-foreground">
        <MonoId value={data.endpointId} max="max-w-none" /> · {fmtTime(data.from)} – {fmtTime(data.to)}
      </p>

      <SectionCard title="Active sessions by wait class" description="Average active sessions per minute, stacked.">
        <AreaChart data={waits} xKey="minute" yKey={waitClasses} stacked height={240} ariaLabel="Active sessions by wait class" />
      </SectionCard>

      <SectionCard title="Lock waits by blocker" description={<BlockerLegend />} contentClassName="space-y-3">
        <BarChart
          data={locks}
          xKey="minute"
          yKey={[...BLOCKER_CLASSES]}
          colors={blockerColors}
          showLegend={false}
          stacked
          height={240}
          ariaLabel="Lock waits by blocker"
        />
        <details className="rounded-lg border">
          <summary className="cursor-pointer select-none px-4 py-2.5 text-sm font-medium text-heading">
            Minutes with lock waits ({locks.length})
          </summary>
          <div className="max-h-96 overflow-y-auto border-t">
            <DataTable>
              <TableBody>
                {locks.map((l) => (
                  <TableRow key={str(l.minute)}>
                    <TableCell className="w-48 text-muted-foreground">{fmtTime(l.minute)}</TableCell>
                    <TableCell className="whitespace-normal">
                      {BLOCKER_CLASSES.filter((c) => Number(l[c]) > 0)
                        .map((c) => `${BLOCKER_LABELS[c]}: ${str(l[c])}`)
                        .join(", ")}
                    </TableCell>
                    <TableCell className="w-36 text-right">
                      <Button variant="link" size="sm" onClick={() => onPick(str(l.minute))}>
                        Show sessions
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </DataTable>
          </div>
        </details>
      </SectionCard>

      <div className="grid gap-6 xl:grid-cols-2">
        <SectionCard
          title="Commit waits (SyncRep)"
          description="Sessions whose commit waited for the WAL storage quorum. Long waits here are on the platform side (see commit_quorum_wait); lock waits coloured as commit waits are writers queued behind these commits."
        >
          <BarChart data={commits} xKey="minute" yKey="sessions" height={180} ariaLabel="Commit waits" />
        </SectionCard>

        <SectionCard title="CPU and connections">
          <LineChart
            data={gauges}
            xKey="minute"
            yKey={["cpu_cores_used", "max_connections"]}
            height={180}
            ariaLabel="CPU and connections"
          />
        </SectionCard>
      </div>

      <SectionCard title="Events">
        {eventCounts.size === 0 ? (
          <p className="text-sm text-muted-foreground">No statement timeouts or connection drops in this window.</p>
        ) : (
          <div className="flex flex-wrap gap-2">
            {[...eventCounts.entries()].map(([k, n]) => (
              <ToneBadge key={k} tone="warning" className="px-2.5 py-1 text-sm">
                {plural(n, EVENT_LABELS[k] ?? k)}
              </ToneBadge>
            ))}
          </div>
        )}
      </SectionCard>

      <p className="text-xs text-muted-foreground">
        Telemetry comes from the health engine&apos;s copy and is up to one engine cycle behind.
      </p>
    </div>
  );
}

const WINDOWS: [string, number][] = [
  ["1 h", 1],
  ["6 h", 6],
  ["24 h", 24],
  ["7 d", 24 * 7 - 1],
];

export default function TimelinePage() {
  const { id } = useParams();
  const [params, setParams] = useSearchParams();
  const [picked, setPicked] = useState<string | null>(null);
  const qs = params.toString();
  const t = useApi<TimelineData>(id ? `/api/endpoints/${id}/timeline${qs ? `?${qs}` : ""}` : null);
  const s = useApi<{ samples: Row[] }>(
    id && picked ? `/api/endpoints/${id}/samples?at=${encodeURIComponent(picked)}` : null,
  );
  if (t.error) return <ApiMessage error={t.error} />;
  return (
    <div className="space-y-6">
      <PageHeader
        crumbs={[{ label: "Projects", to: "/" }, { label: "Incident timeline" }]}
        title="Incident timeline"
        actions={
          <ButtonGroup aria-label="Time window">
            {WINDOWS.map(([label, hours]) => (
              <Button
                key={label}
                variant="outline"
                size="sm"
                onClick={() => setParams({ from: new Date(Date.now() - hours * 3600_000).toISOString() })}
              >
                Last {label}
              </Button>
            ))}
          </ButtonGroup>
        }
      />
      {t.data ? <TimelineView data={t.data} onPick={setPicked} /> : <LoadingState label="Loading timeline" />}
      {picked && (
        <SectionCard title={`Sessions sampled at ${fmtTime(picked)}`} contentClassName="px-0">
          {s.error ? (
            <div className="px-5">
              <ApiMessage error={s.error} />
            </div>
          ) : s.data ? (
            <div className="max-h-[32rem] overflow-y-auto border-t">
              <SamplesTable samples={s.data.samples} />
            </div>
          ) : (
            <div className="px-5">
              <InlineLoading label="Loading sessions" />
            </div>
          )}
        </SectionCard>
      )}
    </div>
  );
}
