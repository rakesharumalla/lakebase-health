import { useEffect, useState } from "react";
import type { ApiError } from "./format";

type State<T> = { path: string | null; data?: T; error?: ApiError };

/** Fetches one of the app's JSON routes; errors keep their HTTP status for the 401/404 messages. */
export function useApi<T>(path: string | null) {
  const [state, setState] = useState<State<T>>({ path: null });
  useEffect(() => {
    if (!path) return;
    let live = true;
    fetch(path)
      .then(async (r) => {
        const body: unknown = await r.json().catch(() => ({}));
        if (!live) return;
        if (!r.ok) {
          const message =
            typeof body === "object" && body !== null && "error" in body ? String(body.error) : r.statusText;
          setState({ path, error: { status: r.status, message } });
        } else {
          setState({ path, data: body as T });
        }
      })
      .catch((e: unknown) => {
        if (live) setState({ path, error: { status: 0, message: String(e) } });
      });
    return () => {
      live = false;
    };
  }, [path]);
  const current = state.path === path;
  return { data: current ? state.data : undefined, error: current ? state.error : undefined, loading: !current };
}
