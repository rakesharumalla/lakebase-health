export const TIMELINE_CHECKS = new Set([
  "hot_row_lock",
  "commit_quorum_wait",
  "statement_timeouts",
  "gin_cleanup_lock_wait",
  "connection_drops",
]);

export const BLOCKER_CLASSES = ["commit_wait", "idle_in_transaction", "active", "not_sampled"] as const;
export const BLOCKER_LABELS: Record<(typeof BLOCKER_CLASSES)[number], string> = {
  commit_wait: "commit waiting on storage quorum",
  idle_in_transaction: "idle in transaction",
  active: "active writer",
  not_sampled: "holder not sampled",
};
/** CSS custom properties defined in theme/tokens.css. */
export const BLOCKER_COLOR_TOKENS: Record<(typeof BLOCKER_CLASSES)[number], string> = {
  commit_wait: "--lbh-blocker-commit-wait",
  idle_in_transaction: "--lbh-blocker-idle-in-transaction",
  active: "--lbh-blocker-active",
  not_sampled: "--lbh-blocker-not-sampled",
};
