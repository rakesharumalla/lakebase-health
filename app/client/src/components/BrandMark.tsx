/** Lakebase Health mark: a Lava tile with a pulse line. Decorative; the app name carries meaning. */
export function BrandMark({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden focusable="false">
      <rect width="32" height="32" rx="7" fill="var(--lbh-lava)" />
      <path
        d="M5 17h5.5l2.5-6 4 11 3-8 1.8 3H27"
        fill="none"
        stroke="#fff"
        strokeWidth="2.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
