type Long = Record<string, unknown>;
type Wide = Record<string, string | number>;

/** Long rows (key, series, value) -> one row per key with a numeric column per series. */
export function pivot(rows: Long[], key: string, series: string, value: string, seriesNames: string[]): Wide[] {
  const out = new Map<string, Wide>();
  for (const r of rows) {
    const k = String(r[key]);
    let row = out.get(k);
    if (!row) {
      row = { [key]: k };
      for (const s of seriesNames) row[s] = 0;
      out.set(k, row);
    }
    const s = String(r[series]);
    if (seriesNames.includes(s)) row[s] = Number(row[s]) + Number(r[value] ?? 0);
  }
  return [...out.values()];
}
