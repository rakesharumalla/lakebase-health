import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

/** WCAG 2.x contrast for the text/background pairs the UI uses, read from tokens.css. */

const css = readFileSync(path.resolve(__dirname, "tokens.css"), "utf8");

function block(selector: RegExp): Record<string, string> {
  const out: Record<string, string> = {};
  for (const m of css.matchAll(new RegExp(`${selector.source}\\s*\\{([^}]*)\\}`, "g"))) {
    for (const d of m[1].matchAll(/(--lbh-[\w-]+)\s*:\s*([^;]+);/g)) out[d[1]] = d[2].trim();
  }
  return out;
}

// Light = every :root block; dark = light overlaid with the .dark block.
const light = block(/(?:^|\n):root/);
const dark = { ...light, ...block(/\n\.dark/) };

function resolve(tokens: Record<string, string>, name: string): string {
  let v = tokens[name];
  for (let i = 0; v?.startsWith("var("); i++) {
    if (i > 10) throw new Error(`cycle at ${name}`);
    v = tokens[v.slice(4, -1).trim()];
  }
  if (!v || !/^#[0-9a-f]{6}$/i.test(v)) throw new Error(`${name} does not resolve to a hex colour: ${v}`);
  return v;
}

const channel = (c: number) => {
  const s = c / 255;
  return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
};
const rgb = (hex: string) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
const luminance = (hex: string) => {
  const [r, g, b] = rgb(hex).map(channel);
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
export const contrast = (a: string, b: string) => {
  const [x, y] = [luminance(a), luminance(b)].sort((p, q) => q - p);
  return (x + 0.05) / (y + 0.05);
};
/** `fg` at `alpha` over `bg`, as Tailwind's bg-primary/90 renders. */
const over = (fg: string, alpha: number, bg: string) =>
  "#" +
  rgb(fg)
    .map((c, i) => Math.round(c * alpha + rgb(bg)[i] * (1 - alpha)))
    .map((c) => c.toString(16).padStart(2, "0"))
    .join("");

type Pair = { label: string; fg: string; bg: string; min: number; alpha?: number; alphaDark?: number };
const AA = 4.5;
const UI = 3; // WCAG 1.4.11 non-text contrast (focus ring, brand mark)

const pairs: Pair[] = [
  { label: "Body text on page", fg: "--lbh-text", bg: "--lbh-page", min: AA },
  { label: "Body text on card", fg: "--lbh-text", bg: "--lbh-card", min: AA },
  { label: "Heading on page", fg: "--lbh-heading", bg: "--lbh-page", min: AA },
  { label: "Muted text on page", fg: "--lbh-text-muted", bg: "--lbh-page", min: AA },
  { label: "Muted text on card", fg: "--lbh-text-muted", bg: "--lbh-card", min: AA },
  { label: "Muted text on subtle (table head, code)", fg: "--lbh-text-muted", bg: "--lbh-subtle", min: AA },
  { label: "Body text on code block", fg: "--lbh-text", bg: "--lbh-code-bg", min: AA },
  { label: "Link on card", fg: "--lbh-link", bg: "--lbh-card", min: AA },
  { label: "Header title on header", fg: "--lbh-header-text", bg: "--lbh-header-bg", min: AA },
  { label: "Header nav text on header", fg: "--lbh-header-muted", bg: "--lbh-header-bg", min: AA },
  { label: "Primary button text", fg: "--lbh-primary-text", bg: "--lbh-primary", min: AA },
  { label: "Primary button text, hover (90% over card)", fg: "--lbh-primary-text", bg: "--lbh-primary", min: AA, alpha: 0.9 },
  { label: "Destructive button text (AppKit uses 60% fill in dark)", fg: "--lbh-white", bg: "--lbh-danger", min: AA, alphaDark: 0.6 },
  { label: "Lava focus ring / accent vs card", fg: "--lbh-focus", bg: "--lbh-card", min: UI },
  { label: "Lava focus ring / accent vs page", fg: "--lbh-focus", bg: "--lbh-page", min: UI },
  { label: "Lava brand mark on header", fg: "--lbh-lava", bg: "--lbh-header-bg", min: UI },
  { label: "Critical badge", fg: "--lbh-sev-critical-fg", bg: "--lbh-sev-critical-bg", min: AA },
  { label: "Warning badge", fg: "--lbh-sev-warning-fg", bg: "--lbh-sev-warning-bg", min: AA },
  { label: "Info badge", fg: "--lbh-sev-info-fg", bg: "--lbh-sev-info-bg", min: AA },
  { label: "OK / resolved badge", fg: "--lbh-sev-ok-fg", bg: "--lbh-sev-ok-bg", min: AA },
  { label: "Neutral badge", fg: "--lbh-sev-neutral-fg", bg: "--lbh-sev-neutral-bg", min: AA },
  { label: "Critical text on card (inline errors)", fg: "--lbh-sev-critical-fg", bg: "--lbh-card", min: AA },
];

function measure(tokens: Record<string, string>, p: Pair) {
  const fg = resolve(tokens, p.fg);
  let bg = resolve(tokens, p.bg);
  const alpha = tokens === dark ? (p.alphaDark ?? p.alpha) : p.alpha;
  if (alpha) bg = over(bg, alpha, resolve(tokens, "--lbh-card"));
  return { fg, bg, ratio: contrast(fg, bg) };
}

describe("theme contrast (WCAG 2.x)", () => {
  for (const [mode, tokens] of [
    ["light", light],
    ["dark", dark],
  ] as const) {
    it.each(pairs)(`${mode}: $label`, (p) => {
      expect(measure(tokens, p).ratio).toBeGreaterThanOrEqual(p.min);
    });
  }

  it("prints the table when PRINT_CONTRAST is set", () => {
    if (!process.env.PRINT_CONTRAST) return;
    const rows = pairs.map((p) => {
      const l = measure(light, p);
      const d = measure(dark, p);
      return `| ${p.label} | ${l.fg} on ${l.bg} | ${l.ratio.toFixed(2)} | ${d.fg} on ${d.bg} | ${d.ratio.toFixed(2)} | ${p.min} |`;
    });
    const lava = resolve(light, "--lbh-lava");
    rows.push(`| (reference) White on brand Lava, not used for text | #ffffff on ${lava} | ${contrast("#ffffff", lava).toFixed(2)} | | | n/a |`);
    console.log(
      ["| Pair | Light | Ratio | Dark | Ratio | Min |", "|---|---|---|---|---|---|", ...rows].join("\n"),
    );
  });
});
