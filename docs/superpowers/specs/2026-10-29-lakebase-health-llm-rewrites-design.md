# Lakebase Health — LLM Query Rewrites (sub-project 4) Design

**Date:** 2026-10-29  **Status:** Draft for review  **Related:** `2026-10-28-lakebase-health-fix-loop-design.md` (approvals, fixer job, branch verification, ledger), `2026-10-28-lakebase-health-app-design.md` (app, access, masking), `DESIGN.md` §5 (LLM scope)

## 1. Goal and scope

For a query finding, a project owner or manager asks for a rewrite. The fixer job asks an LLM (through the
workspace AI Gateway) for an equivalent, faster query, checks it statically, and verifies it on a copy-on-write
branch: same results, faster, plan changed as described. The app shows the verified rewrite with its evidence as an
**advisory** card; the customer changes their application code. The post-apply check later reports whether the
rewritten query was adopted.

**In scope:** query rewrites for query-scoped findings (`top_db_time_query`, `query_regression`, `plan_flip`,
`storage_wait`, `disk_spill`, `sort_limit_waste`, `row_misestimate` top queries).

**Out of scope:**
- **pg_hint_plan hints.** Tested on 2026-10-29: the extension is installed but the library is not loaded, and
  `LOAD`, `ALTER ROLE/DATABASE … SET session_preload_libraries` and `pg_hint_plan.enable_hint_table` are refused for
  non-superusers. Hint comments did not change plans. Hints need a support request per endpoint, so they are left
  out until Lakebase allows them.
- Explanations, report summaries, chat (not requested for release 1).
- Applying rewrites (application code is outside the database).

### Decisions this spec builds on

| Topic | Decision |
|---|---|
| Capability | Draft query rewrites only |
| Model | `databricks-claude-opus-5-5` via the AI Gateway serving endpoint; bundle variable `llm_endpoint` |
| Trigger | On demand only: "Draft a rewrite" on a query finding |
| Attempts | One draft per finding at a time, with a Retry button once it has finished; every attempt is kept |
| Model input | Normalized query, plan, DDL, indexes, row counts, column-statistics summaries; **no parameter values or literals** |
| Apply | Rewrites are advisory (evidence and copy); nothing is applied by the fixer |
| Approach | Drafting runs in the fixer job (rejected: app calls the model in a web request; a separate drafter job and SP) |

## 2. Architecture

```
App: query finding ──"Draft a rewrite"──► approvals (request = draft) ──► fixer job (run now)
  context: normalized query, plan_json (literals stripped), DDL, indexes, row counts, pg_stats summaries
  model:   var.llm_endpoint via AI Gateway → JSON {rewrite, rationale, expected_plan_change} | {rewrite: null, reason}
  static:  one SELECT/WITH, same $n set, same tables, no new literals, safe functions only
  verify:  lbh-verify-<8> branch: equivalence on sampled parameters, speed, plan change
  result:  proposed_actions (kind = query_rewrite, origin = llm, state verified | verify_failed) → advisory card
  later:   post-check → adopted | not_adopted
```

| Unit | Responsibility |
|---|---|
| `fixer/draft/context.py` | Builds the model context from the finding, plan history and the target catalog; strips literals |
| `fixer/draft/prompt.py` + `prompts/rewrite_v1.md` | Versioned prompt template and message assembly |
| `fixer/draft/llm.py` | Calls the serving endpoint (SDK `serving_endpoints.query`), timeout, one retry on timeout/429/5xx, writes `llm_calls` |
| `fixer/draft/static.py` | Parses and checks the rewrite (sqlglot or the existing allowlist parser extended for SELECT) |
| `fixer/draft/verify.py` | Branch verification: equivalence, speed, plan rules (reuses `fixer/branch.py`, `fixer/samples.py`, `fixer/verify.py` measurement) |
| `fixer/postcheck.py` | Adds `adopted` / `not_adopted` for rewrite actions |
| App: draft card | Button, states, diff, evidence, Retry, attempt history |

## 3. States, data and ledger

**Draft states (per attempt):** `drafting` → `draft_failed` | `verifying` → `verified` | `verify_failed`; a verified
rewrite later gets `adopted` or `not_adopted`. Retry is allowed when the latest attempt is final; while one is running
it is disabled.

| Table | Grain | Key columns |
|---|---|---|
| `drafts` | attempt | `draft_id`, `finding_fingerprint`, `attempt`, `action_id` (nullable), `endpoint`, `prompt_version`, `context_hash`, `rationale`, `expected_plan_change`, `state`, `reason`, `requested_by`, `requested_at`, `ended_at` |
| `llm_calls` | model call | `call_id`, `draft_id`, `endpoint`, `prompt_hash`, `response_hash`, `input_tokens`, `output_tokens`, `latency_ms`, `status`, `request_id` — **no prompt or response text** |
| `proposed_actions` | (existing) | gains `origin` (`check` / `llm`) and `draft_id`; kind `query_rewrite` |
| `approvals` | (existing) | request gains `draft` |

The rewrite SQL is stored on the action (masked like other query text). Ledger events: `draft_requested`,
`draft_started`, `drafted`, `draft_failed`, `adopted`, `not_adopted`, plus the existing verify events, in the same
hash chain.

## 4. Model call

- **Prompt (system):** PostgreSQL 17 tuning assistant; return JSON only; the rewrite must return exactly the same rows
  and columns in the same order (same ORDER BY); use only the given tables; keep every `$n` parameter; preserve NULL,
  duplicate and LIMIT semantics; if there is no safe rewrite return `{"rewrite": null, "reason": "..."}`.
- **User content:** finding (check, measure, evidence numbers), normalized query, worst recent `plan_json` with
  literals stripped, DDL and indexes of the involved tables, row counts, `pg_stats` summaries (`n_distinct`,
  `null_frac`, `correlation`; no most-common values or histograms).
- **Settings:** temperature 0, max output 2,000 tokens, timeout 120 s; one retry after 30 s on timeout, 429 or 5xx.
- **No values leave the database:** sampled parameters are used only on the branch inside the job.

## 5. Static checks and verification

**Static (before any branch):** parses as one SELECT or WITH; reads only tables the original reads; same set of `$n`
parameters; no literals beyond the original skeleton's; same number of output columns; no functions outside a safe
list; no data-modifying CTEs, `pg_sleep`, `SET` or comments with hints. Failure → `draft_failed` with the reason.

**Branch verification** (`lbh-verify-<8>`, TTL 4 h, deleted afterwards):

| Rule | Passes when |
|---|---|
| Equivalence | For each of up to 3 sampled parameter sets, run both in a read-only transaction: same row count and same checksum (order-sensitive when the original has ORDER BY, else order-insensitive) |
| Speed | Median of 3 warm runs per set; rewrite ≥ 1.2× faster on the median across sets; no set > 10 % and > 1 ms slower |
| Plan | Plan node changes reported; `expected_plan_change` found in the new plan (mismatch is a warning, not a failure) |

## 6. Error handling

| Case | Result |
|---|---|
| Model timeout, 429, 5xx | One retry after 30 s, then `draft_failed: model_unavailable` |
| Invalid JSON | `draft_failed: invalid_response` |
| `{"rewrite": null}` | `draft_failed: no_safe_rewrite` with the model's reason |
| Static check fails | `draft_failed: <check>` |
| No sampled parameters or redacted text | `draft_failed: no_samples` |
| Branch unavailable / missing privileges | Existing handling (`verify_failed: branch_unavailable`, `blocked`) |
| Drafting not set up (endpoint missing, no `CAN_QUERY`, `llm_enabled` false) | Button hidden; card says "LLM drafting not set up" |

## 7. App, access, setup

- **Button and card:** "Draft a rewrite" for the project owner or managers (same check as approvals). The card shows
  state, rationale, rewritten SQL with copy, a diff with the original, equivalence results per parameter set,
  before/after timings, plan node changes, expected-vs-actual plan change, the advisory note ("change it in your
  application code; this tool does not deploy rewrites"), adoption outcome, Retry and attempt history.
- **Query text** follows the existing masking rule; exports never include it.
- **Setup:** the fixer SP gets `CAN_QUERY` on `var.llm_endpoint` (granted by `fixer_grants.sh`); `var.llm_enabled`
  (default `true`) turns the feature off. The app SP gets nothing new.

## 8. Testing and acceptance

- **Engine (pytest, fake model client):** context building has no values (assert on the exact payload); JSON parsing;
  static checks (extra statement, new literal, other table, data-modifying CTE, hint comment); checksum equivalence
  (ordered and unordered); speed and plan rules with the noise floor; draft state machine with Retry; ledger events;
  model errors (timeout, 429, invalid JSON, null rewrite).
- **App (Vitest):** button visibility (manager, role holder, not set up); card per state; Retry and history.

**Acceptance (healthcheck on my-profile):**
1. A planted inefficient query (correlated subquery on `hc_test.orders`) gets a draft that passes static checks and
   verifies on a branch: same results, ≥ 1.2× faster, plan changed as described.
2. An unsafe or non-equivalent rewrite (fixed bad response through a test switch) ends `draft_failed` or
   `verify_failed` with the reason.
3. Retry creates a second attempt and the card shows both.
4. `llm_calls` holds hashes and token counts only; the model payload contains none of the sampled parameter values.
5. No `lbh-verify-*` branch remains; the ledger chain verifies; a non-manager gets 403 on draft.
6. After the workload switches to the rewritten query, the post-check marks the rewrite `adopted`.

## 9. Delivery and open items

Branch `feature/llm-rewrites` from `feature/fix-loop` after its final review and merge.

- Hints, if Lakebase allows loading `pg_hint_plan` (would use the existing apply/rollback path with `hint_plan.hints`).
- Explanations and report summaries with the same gateway and logging.
- `DESIGN.md` and `HEALTH_CHECKS.md` say pg_hint_plan is available; correct them with the finding above.
