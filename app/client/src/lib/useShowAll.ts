import { useState } from "react";

/**
 * "Show all N" for lists that arrive cut to their top rows: opening one fetches its full list from
 * `pathFor(key)` once (on demand, never with the page) and shows it in place; closing goes back to the top rows.
 */
export function useShowAll<T>(pathFor: (key: string) => string, extract: (body: unknown) => T[]) {
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [full, setFull] = useState<Record<string, T[]>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [error, setError] = useState<Record<string, string>>({});

  const toggle = (key: string) => {
    const opening = !open[key];
    setOpen((o) => ({ ...o, [key]: opening }));
    if (!opening || full[key] || loading[key]) return;
    setLoading((l) => ({ ...l, [key]: true }));
    setError((e) => ({ ...e, [key]: "" }));
    fetch(pathFor(key))
      .then(async (r) => {
        const body: unknown = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(typeof body === "object" && body && "error" in body ? String(body.error) : r.statusText);
        setFull((f) => ({ ...f, [key]: extract(body) }));
      })
      .catch((e: unknown) => {
        setError((x) => ({ ...x, [key]: e instanceof Error ? e.message : String(e) }));
        setOpen((o) => ({ ...o, [key]: false }));
      })
      .finally(() => setLoading((l) => ({ ...l, [key]: false })));
  };

  return {
    /** The rows to render: the full list once opened and loaded, else the top rows. */
    rows: (key: string, top: T[]) => (open[key] && full[key] ? full[key] : top),
    isOpen: (key: string) => !!open[key] && !!full[key],
    isLoading: (key: string) => !!loading[key],
    errorOf: (key: string) => error[key] || undefined,
    toggle,
  };
}

/** Body of a { total, rows } page response. */
export const pageRows = <T>(body: unknown): T[] =>
  typeof body === "object" && body !== null && Array.isArray((body as { rows?: unknown }).rows) ? (body as { rows: T[] }).rows : [];
