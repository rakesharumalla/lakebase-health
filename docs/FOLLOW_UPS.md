# Follow-up items

Collected 2026-10-29. Ordered by impact within each section. "Spec" marks items with a written design.

## Suggested order

1. Setup wizard and in-app enrolment.
2. Health report (spec: `docs/superpowers/specs/2026-10-28-lakebase-health-report-design.md`), including synced-table
   awareness and the stats-reset guard.
3. Calibrate thresholds with 3–5 design-partner customers; per-project threshold overrides and suppressions in the app.
4. Alerts.
5. Postgres-only mode for accounts without `system.lakebase`; decision on provisioned Lakebase.
6. LLM query rewrites (spec: `docs/superpowers/specs/2026-10-29-lakebase-health-llm-rewrites-design.md`), then
   plain-language explanations.

## 1. Ease of use and onboarding

- **Setup wizard in the app.** A Setup page that checks each prerequisite (SPs, `servicePrincipal.user`, bundle folder
  grant, first engine run, grants script, MANAGE on person-created tables, PII group, endpoint permissions) with a
  pass/fail and the exact command or a "Fix it" button. The same checks as `scripts/doctor.sh` for CLI users.
- **In-app enrolment.** "Enable Postgres checks" and "Enable fixes" per project: show the one `create-role` command, or
  run it with the owner's token.
- **Simple mode.** Optional single identity (fixer = engine SP) for small customers, with the risk stated; keep the
  separation as the default for large ones.
- **Packaging.** Marketplace listing or a one-command installer instead of cloning the repo and editing variables.

## 2. Lakebase scenarios to handle

| Scenario | Today | To do |
|---|---|---|
| Provisioned (non-autoscaling) Lakebase | Not covered | Detect and say "not supported", or add a provisioned executor (Postgres checks would still work) |
| No `system.lakebase` access (preview) | Telemetry checks silent | Postgres-only mode; mark telemetry areas "not assessed" |
| Scale-to-zero | Suspended endpoints skipped | Show "last checked while awake"; never imply health from stale data |
| Many databases per endpoint | Instance-wide checks fixed | Cap per-database work; let the user choose databases |
| Read replicas | Lag and some checks | Label findings by endpoint role; replica-only problems (hot standby feedback holding back vacuum) |
| Synced tables (`__db_system` partitions) | Done (`synced.py`, `synced_index_review`) | Indexes on synced storage are reviewed, never dropped |
| Very large fleets (1,000+ endpoints) | Ingest 20–30 min | Incremental ingest per table, parallel Postgres executor, per-run time budget, paging in the app |
| Several workspaces or regions | One bundle per workspace | Document; later a central cross-workspace fleet view |
| Private Link / IP access lists | Untested | Document serverless-to-endpoint egress requirements |
| Azure and GCP | AWS only | Test, especially `system.lakebase` availability |
| Customer connection pools | `suspend_blockers` check | Done; extend to pools that never go idle (min idle > 0 with traffic) |
| Postgres versions | PG 17 only | Version-gate catalog queries (16/17/18 column changes) |

## 3. Edge cases

- History copied while query text was masked stays `<REDACTED>`; a PII setup mistake costs 7 days of history.
  Make the order explicit in setup and warn in the wizard.
- Counter resets after scale-to-zero restarts: apply the stats-reset guard to existing counter-based checks too.
- Platform roles: keep the exclusion list current; flag unknown `databricks_*` roles instead of counting them.
- Thresholds tuned on one test project broke at fleet scale (`row_misestimate`, `cu_sizing`): calibrate on real
  projects before general release.
- Branch limit (10) and verification compute cost: queue verifications per project and show the cost.
- Branch measurement noise: more warm-up and alternating before/after runs to raise confidence.
- Missing extensions or functions (`pgstatginindex`, `pg_hint_plan` not loadable): detect and report "not
  available" instead of an error every run. `gin_pending_list` errors on healthcheck today.
- Time zones and naive timestamps: keep everything UTC and covered by tests.

## 4. Assumptions to document or remove

- The engine SP reads all of `system.lakebase` (account-wide visibility for whoever holds that grant).
- Project visibility is scoped by ownership or a Postgres role because the Lakebase API lists every project to every
  user; role holders see that project's findings.
- Fixes assume the target tables are owned by one role the fixer can join; support mixed ownership.
- PII: `databricks_pii_access` exists or can be created; query text in a Delta copy is acceptable; offer a no-text mode
  and short retention for raw copies.
- Default thresholds suit OLTP; analytics-heavy use needs per-project overrides or suppressions.

## 5. Product additions

- Alerts: email, Slack or webhook for new critical findings; digest mode and quiet hours.
- Per-project settings in the app: suppress a check or object, override thresholds, choose branches to watch.
- Trends: score over time, findings opened vs resolved, "since last week".
- Cost view: CU-hours and storage per project next to right-sizing (304 always-on idle computes found).
- Lakebase Insights and Genie: show Insights findings alongside ours; "Ask Genie" about a finding's evidence.
- Plain-language explanations for application developers (deferred from the LLM release).
- Keep the safe defaults: approval for every fix, no query text in exports, fixer optional.

## 6. Open operational items

- Turn PII masking back on before any shared or customer deployment: add the engine SP, fixer SP and viewers to
  `databricks_pii_access` and remove the dev override (`pii_masking`).
- Leftover grants from the removed bundle-wide permissions block (engine and fixer SPs: CAN_USE on the app, CAN_VIEW
  on two jobs) were kept by decision; revoke later.
- `pg_hint_plan` hints: revisit if Lakebase allows loading the library (tested 2026-10-29: refused).
- `DESIGN.md` and `HEALTH_CHECKS.md` say pg_hint_plan is available; correct them (in the LLM rewrites plan).
- Cleanup in the test workspace: test service principal and its local secret, `hc_test_owner` role, finished worktrees.
- Deferred minors from reviews: per_database migration lost history on 9 checks (one time); `hot_row_lock` shows a
  platform blocker as `not_sampled`; a customer role literally named `databricks_writer_<n>` is excluded;
  `row_misestimate` SQL tests are string checks; project page KPI tiles count grouped rows, not findings.
