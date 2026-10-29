/**
 * Reads colour tokens from theme/tokens.css for chart props that need concrete colours (ECharts
 * cannot resolve var()). Returns undefined when any token is missing (e.g. in tests without CSS),
 * so callers fall back to AppKit's themed palette.
 */
export function tokenColors(names: readonly string[]): string[] | undefined {
  if (typeof window === "undefined") return undefined;
  const styles = getComputedStyle(document.documentElement);
  const values = names.map((n) => styles.getPropertyValue(n).trim());
  return values.every(Boolean) ? values : undefined;
}
