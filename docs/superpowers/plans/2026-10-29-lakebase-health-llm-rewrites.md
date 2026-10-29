# Lakebase Health LLM Query Rewrites Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** For a query finding, a project owner or manager clicks "Draft a rewrite"; the fixer job asks `databricks-claude-opus-5-5` for an equivalent faster query (no values sent), checks it statically, verifies it on a copy-on-write branch (same results, ≥ 1.2× faster, plan change reported), and the app shows it as an advisory card with Retry and history; the engine later marks it `adopted` or `not_adopted`.

**Architecture:** The app inserts an `approvals` row with `request = 'draft'` (keyed by the finding's fingerprint, with an attempt token in `sql_hash`) and triggers the fixer job. The fixer consumes it, writes a `drafts` row, builds a literal-free context (normalized query, stripped plan, catalog), calls the serving endpoint through the SDK (one retry after 30 s), runs sqlglot-based static checks, creates a `proposed_actions` row (`kind = query_rewrite`, `origin = llm`, `action_id = draft_id`), and verifies it on `lbh-verify-<8>` with checksum equivalence, the fixer's speed noise floor and a plan-node comparison. Ledger events chain on the draft id. The engine's post-check matches the rewrite's query id or normalized text in `pg_stat_statements`.

**Tech Stack:** Python 3.10+ (`lakebase_health`, psycopg 3, databricks-sdk ≥ 0.81, **sqlglot 30.x**, Spark SQL on serverless), pytest; app: AppKit/TypeScript/React, Vitest (happy-dom); Databricks Asset Bundles.

**Spec:** `docs/superpowers/specs/2026-10-29-lakebase-health-llm-rewrites-design.md` (builds on `2026-10-28-lakebase-health-fix-loop-design.md` and `2026-10-28-lakebase-health-app-design.md`).

**Delivery:** branch `feature/llm-rewrites`, cut from `feature/fix-loop` **after** the fix loop's final review and merge. The executor creates the worktree (superpowers:using-git-worktrees); all paths below are relative to that worktree. Line numbers are not given for existing files because `feature/fix-loop` was still moving when this plan was written; every edit quotes the exact text it replaces.

## Global Constraints

- Capability: query rewrites only, for findings of `top_db_time_query`, `query_regression`, `plan_flip`, `storage_wait`, `disk_spill`, `sort_limit_waste`, `row_misestimate` (its first `evidence.top_queries` entry). No pg_hint_plan hints, explanations, summaries or chat.
- Model: `var.llm_endpoint` (default `databricks-claude-opus-5-5`, an `llm/v1/chat` serving endpoint), called with `w.serving_endpoints.query(name=..., messages=[ChatMessage(...)], temperature=0, max_tokens=2000)`; timeout 120 s; one retry after 30 s on timeout, 429 or 5xx, then `draft_failed: model_unavailable`.
- On demand only; one draft per finding at a time; Retry once the latest attempt is final (`draft_failed`, `verified`, `verify_failed`, `adopted`, `not_adopted`); every attempt is a `drafts` row.
- **No parameter values or literals in the model payload:** query literals become `$k`, plan and catalog literals become `?`, `pg_stats` gives only `n_distinct`, `null_frac`, `correlation`; sampled parameter values are used only on the branch. Tests assert on the exact payload.
- `llm_calls` stores hashes, token counts, latency, status and request id — never prompt or response text. Verification reports store counts, checksum verdicts, timings, plan node names and query ids — never query text or values. Ledger payloads for rewrites carry hashes, not SQL.
- Static checks before any branch: one SELECT/WITH; only the original's tables; same `$n` set; no literals except `0` and `1`; same output column count (unless a star); functions in the safe list or the original's (never `pg_sleep`, `set_config`, `random`, …); no data-modifying CTE, `FOR UPDATE`, `SELECT INTO`, `SET`, or comments (hint comments included).
- Branch verification: `lbh-verify-<draft_id[:8]>`, TTL 4 h, deleted afterwards. Equivalence on up to 3 sampled parameter sets in read-only transactions (row count + checksum; order-sensitive iff the original has a top-level ORDER BY). Speed: median of 3 warm runs per set; rewrite ≥ 1.2× faster on the median across sets; no set both > 1.10× and > 1 ms slower (the fixer's `SLOWDOWN`/`MIN_INCREASE_MS` noise rule). Plan: node changes reported; `expected_plan_change` not found is a warning, never a failure.
- Rewrites are advisory: nothing applies them. Fix requests (`verify`/`apply`/`rollback`/`reject`) on a `query_rewrite` action are refused by the app (409) and consumed as `stale:advisory` by the fixer.
- Access: "Draft a rewrite" and Retry for the project owner or managers (the approvals `canManage` check, re-checked on POST → 403). Rewrite SQL follows the query-text masking rule (UC mask on `proposed_actions.sql` for `origin = 'llm'` rows, `canSeeText` in the app); exports never include it.
- Setup: the fixer SP gets `MODIFY` on `drafts`, `llm_calls`, `llm_status` and `CAN_QUERY` on the endpoint (`scripts/fixer_grants.sh`); `var.llm_enabled` (default `"true"`) turns the feature off; the app SP gets nothing new. Not set up (disabled, endpoint missing/not READY, no permission) → button hidden, card says "LLM drafting not set up".
- The acceptance test switch (`--llm-test-response unsafe|non_equivalent`) is a fixer CLI flag only, never in the bundle YAML (a test asserts it).
- Profile `my-profile`, catalog `my_catalog`, warehouse `0123456789abcdef`, test project `healthcheck` (uid `11111111-2222-4333-8444-555555555555`). Commit messages end with a blank line and `Co-authored-by: Isaac <no-reply@databricks.com>`; never `--no-verify`.

## Decisions recorded in this plan

- **sqlglot, not the allowlist parser.** `fixer/allowlist.py` is a regex matcher for four DDL shapes; the static checks need table references (minus CTE names), function names, data-modifying CTEs, locking clauses, output column counts and top-level ORDER BY, which a hand parser would get wrong on subqueries, LATERAL and comma joins. `sqlglot` 30.x is pure Python with no dependencies, is served by the configured index (`pypi-proxy.dev.databricks.com`, 30.24.0 checked 2026-10-29) and by PyPI, and serverless jobs install the wheel's dependencies from PyPI like `psycopg`/`pyyaml` today. Pinned `>=30.24,<31` because the safe-function list uses sqlglot's normalized names (`date_trunc` → `timestamp_trunc`, `random` → `rand`). Its Postgres **tokenizer** also does literal/parameter normalization. The allowlist stays the only gate for DDL. Cost if wrong (package missing on a serverless index): the jobs fail to start; the fallback is vendoring the wheel next to the engine wheel in `resources/*_job.yml`.
- **Literals are parameters in the model's view.** `normalize()` replaces each literal with `$k` (k after the largest `$n`, like `pg_stat_statements`), the model must keep every `$n`, and `denormalize()` puts the literal tokens back before the branch run and before storing the action SQL.
- **`action_id = draft_id`.** The rewrite's action reuses the draft's 32-hex id, so draft events and verify events share one ledger chain (`last_hashes` accepts it) and the app's action id validation holds.
- **Draft approvals carry the finding, not an action.** `approvals` gains `fingerprint`; a draft approval has `action_id` NULL (the existing `pending()` inner join ignores it) and `sql_hash = sha256("draft:<fingerprint>:<attempt>")`. A double click produces two approvals for the same attempt; the second is consumed as `stale:attempt_changed`.
- **Setup status is recorded by the fixer.** The app cannot check the fixer SP's endpoint permission, so each sweep with pending drafts probes `serving_endpoints.get` (`state.ready`, `permission_level`) and merges a row into `llm_status`; the app reads it with `LLM_ENABLED`/`LLM_ENDPOINT` env.
- **Adoption** = the rewrite's query id (from `EXPLAIN (VERBOSE)` on the branch) or its normalized-text hash seen ≥ 20 calls in `raw_pg_stat_statements_counters` since verification; `not_adopted` after 7 days without.

## Review Focus

1. **A rewrite action reached through the old fix paths.** Someone POSTs `{"request": "apply"}` to `/api/actions/<rewrite id>/requests`, or an approval row for it reaches the fixer: expect 409 from the app and `stale:advisory` in the fixer, never `parse()` → `blocked` or SQL executed on the target. Tests: Task 8 (`test_fix_requests_on_a_rewrite_action_are_refused_as_advisory`), Task 11 (`refuses fix requests on a rewrite`).
2. **Engine runs after a rewrite exists.** `record.persist` reads every action; a verified rewrite would count as a fixer state and block the check's own fix, and merging it back as an `Action` would wipe `origin`/`draft_id`. Expect the engine to ignore `origin = 'llm'` rows. Test: Task 3 (`test_an_llm_rewrite_neither_blocks_the_check_fix_nor_is_merged_by_the_engine`).
3. **Values leaking through side channels:** plan JSON (`Index Cond` with custom-plan constants, `Query Text`), partial-index and CHECK definitions, evidence strings, error messages. Expect `?`, dropped keys, numbers-only evidence, errors by type. Tests: Task 4 exact payload; Task 6 `test_checksum_errors_are_recorded_by_type_only`; Task 8 report assertions.
4. **The job dying mid-draft** (during the 2-minute model call or on the branch): expect the attempt to become final (`draft_failed`/`verify_failed`, reason `interrupted`) on a later sweep so Retry is offered again, and the approval never drafted twice. Test: Task 8 (`test_an_interrupted_draft_is_recovered_so_retry_is_offered`).
5. **The finding page for a non-manager or a masked user:** expect no button, the role message, and the rewrite and original shown as skeletons, not full text. Tests: Task 11 (`masks the rewrite for users who may not read query text`), Task 12 (`role holder sees no button`).

---

## File Structure

```
engine/pyproject.toml                         # modify: sqlglot dependency, prompts package data
engine/src/lakebase_health/
  config.py                                   # modify: llm_endpoint, llm_enabled, llm_test_response
  ddl.py                                      # modify: drafts, llm_calls, llm_status; origin/draft_id; approvals.fingerprint; mask_llm_text
  record.py                                   # modify: the engine plans only origin = 'check' actions
  fixer/pg.py                                 # modify: read-only "draft_context" session kind
  fixer/queue.py                              # modify: fix requests on query_rewrite actions are stale:advisory
  fixer/postcheck.py                          # modify: run_rewrite_postcheck (adopted | not_adopted)
  fixer/draft/__init__.py                     # create (empty)
  fixer/draft/sqltext.py                      # tokenizer helpers: params, literals, normalize/denormalize, strip_literals, text_key
  fixer/draft/static.py                       # sqlglot static checks -> Shape | StaticError
  fixer/draft/states.py                       # REWRITE_CHECKS, RUNNING, FINAL, draft_token, finding_queryid
  fixer/draft/context.py                      # load_history, load_catalog, build_context (no values)
  fixer/draft/prompt.py + prompts/rewrite_v1.md   # versioned prompt, messages, hashes, parse_response
  fixer/draft/llm.py                          # ModelClient (retry, llm_calls rows), sdk_query, probe, canned test replies
  fixer/draft/verify.py                       # checksum, plan_shape, judge (equivalence, speed, plan)
  fixer/draft/store.py                        # pending_drafts, staleness, draft/action/status rows, stuck_drafts
  jobs/fix.py                                 # modify: _handle_draft, _verify_rewrite, recovery, CLI flags
  jobs/run.py                                 # modify: call run_rewrite_postcheck
engine/tests/unit/test_sqltext.py, test_static.py, test_draft_context.py, test_draft_prompt.py, test_draft_llm.py,
  test_draft_verify.py, test_draft_store.py, test_draft_job.py   # create
engine/tests/unit/test_ddl.py, test_record.py, test_fix_job.py, test_postcheck.py, test_run.py,
  test_bundle.py, test_fixer_grants.py                          # modify
databricks.yml, resources/{fixer,engine,analysis}_job.yml, resources/app.yml, scripts/fixer_grants.sh
app/server/writes/insert_approval.sql         # modify: fingerprint column
app/server/queries/finding_drafts.sql, draft_requests.sql, llm_status.sql   # create
app/server/queries/finding_actions.sql, project_actions.sql                 # modify: check actions only
app/server/lib/drafts.ts                      # mirror of states.py + the draft view logic
app/server/lib/handlers.ts, routes.ts, server.ts   # modify
app/server/tests/drafts.test.ts               # create; approvals.test.ts modify
app/client/src/lib/diff.ts (+ diff.test.ts)   # word diff
app/client/src/views/DraftCard.tsx (+ DraftCard.test.tsx), Finding.tsx   # card and embedding
scripts/workload.py, scripts/seed_test_workload.sql   # planted correlated subquery; --mode rewrite
engine/src/lakebase_health/checks/telemetry/plan_flip.yaml, docs/CHECKS.md, notebooks/lakebase_health_check.py, README.md
DESIGN.md, HEALTH_CHECKS.md                   # pg_hint_plan corrections
scripts/llm_acceptance.py                     # create: the 6 acceptance criteria
```

---
### Task 1: sqlglot dependency and query-text helpers

**Files:**
- Modify: `engine/pyproject.toml`
- Create: `engine/src/lakebase_health/fixer/draft/__init__.py` (empty), `engine/src/lakebase_health/fixer/draft/sqltext.py`
- Test: `engine/tests/unit/test_sqltext.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `params(sql) -> set[int]`; `literals(sql) -> list[str]`; `has_comment(sql) -> bool`; `normalize(sql) -> tuple[str, dict[int, str]]` (each literal → `$k`, k after the largest `$n`); `denormalize(sql, values: dict[int, str]) -> str`; `strip_literals(text) -> str` (literals → `?`, untokenizable → `"?"`); `text_key(sql) -> str` (sha256 of normalized, whitespace-collapsed, lower-cased text); `SqlTextError(ValueError)`; `tokens(sql)`.

- [ ] **Step 1: Add the dependency and install it**

`engine/pyproject.toml` (apply this change):

```diff
--- a/engine/pyproject.toml
+++ b/engine/pyproject.toml
@@ -6,7 +6,7 @@ build-backend = "setuptools.build_meta"
 name = "lakebase_health"
 version = "0.1.0"
 requires-python = ">=3.10"
-dependencies = ["pyyaml>=6", "psycopg[binary]>=3.1", "databricks-sdk>=0.81.0"]
+dependencies = ["pyyaml>=6", "psycopg[binary]>=3.1", "databricks-sdk>=0.81.0", "sqlglot>=30.24,<31"]
 
 [project.scripts]
 main = "lakebase_health.jobs.run:main"
@@ -17,7 +17,7 @@ fix = "lakebase_health.jobs.fix:main"
 where = ["src"]
 
 [tool.setuptools.package-data]
-lakebase_health = ["checks/*.yaml", "checks/*/*.yaml"]
+lakebase_health = ["checks/*.yaml", "checks/*/*.yaml", "fixer/draft/prompts/*.md"]
 
 [tool.pytest.ini_options]
 testpaths = ["tests/unit"]
```

Run: `cd engine && .venv/bin/pip install "sqlglot>=30.24,<31" && .venv/bin/python -c "import sqlglot; print(sqlglot.__version__)"`
Expected: a 30.x version. (If `.venv` is missing: `python3 -m venv .venv && .venv/bin/pip install -e . pytest`.)

- [ ] **Step 2: Write the failing test**

`engine/tests/unit/test_sqltext.py` (new file, complete):

```python
import pytest

from lakebase_health.fixer.draft.sqltext import (SqlTextError, denormalize, has_comment, literals, normalize, params,
                                                  strip_literals, text_key)

Q = "SELECT * FROM hc_test.orders o WHERE o.status = 'paid' AND o.id BETWEEN $1 AND $2 AND o.amount > 10.5 LIMIT 10"


def test_parameters_and_literals_are_told_apart():
    assert params(Q) == {1, 2} and params("SELECT $10, $2") == {10, 2}
    assert literals(Q) == ["'paid'", "10.5", "10"]
    assert literals("SELECT x FROM t WHERE a = $1") == []


def test_normalize_turns_every_literal_into_a_new_parameter_and_back():
    text, values = normalize(Q)
    assert text == ("SELECT * FROM hc_test.orders o WHERE o.status = $3 AND o.id BETWEEN $1 AND $2 "
                    "AND o.amount > $4 LIMIT $5")
    assert values == {3: "'paid'", 4: "10.5", 5: "10"}
    assert literals(text) == [] and denormalize(text, values) == Q


def test_every_literal_form_is_normalized():
    text, values = normalize("SELECT E'a\\'b', $$q$$, x'1F', B'101', interval '1 day', -5, 1e3")
    assert text == "SELECT $1, $2, $3, $4, interval $5, -$6, $7"
    assert len(values) == 7


def test_denormalize_only_replaces_literal_placeholders():
    assert denormalize("SELECT $1 WHERE s = $3 OR s = $3", {3: "'x'"}) == "SELECT $1 WHERE s = 'x' OR s = 'x'"


def test_strip_literals_for_plans_and_catalog_text():
    assert strip_literals("((status = 'paid'::text) AND (id >= $1) AND (amount > 10.5))") == \
        "((status = ?::text) AND (id >= $1) AND (amount > ?))"
    assert strip_literals("CREATE INDEX x ON s.t USING btree (a) WHERE (s = 'x'::text)") == \
        "CREATE INDEX x ON s.t USING btree (a) WHERE (s = ?::text)"
    assert strip_literals("'unterminated") == "?"


def test_comments_are_found_outside_strings_only():
    assert has_comment("SELECT 1 /*+ SeqScan(t) */") and has_comment("SELECT 1 -- x\n")
    assert not has_comment("SELECT '/* not a comment */'")


def test_text_key_ignores_literal_values_whitespace_and_case():
    assert text_key("SELECT a FROM t WHERE x = 5") == text_key("select  a from t\nwhere x = $1")
    assert text_key("SELECT a FROM t WHERE x = 5") != text_key("SELECT b FROM t WHERE x = 5")


def test_untokenizable_text_raises():
    with pytest.raises(SqlTextError):
        params("SELECT 'unterminated")
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_sqltext.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'lakebase_health.fixer.draft'`.

- [ ] **Step 4: Implement**

Create the empty `engine/src/lakebase_health/fixer/draft/__init__.py`, then:

`engine/src/lakebase_health/fixer/draft/sqltext.py` (new file, complete):

```python
"""Token-level helpers for query text: parameters, literals and their placeholders (sqlglot's Postgres tokenizer)."""
import hashlib

from sqlglot.dialects.postgres import Postgres
from sqlglot.errors import TokenError
from sqlglot.tokens import TokenType

LITERAL_TOKENS = {TokenType.STRING, TokenType.NUMBER, TokenType.BYTE_STRING, TokenType.HEREDOC_STRING,
                  TokenType.HEX_STRING, TokenType.BIT_STRING, TokenType.NATIONAL_STRING, TokenType.RAW_STRING,
                  TokenType.UNICODE_STRING}


class SqlTextError(ValueError):
    pass


def tokens(sql: str) -> list:
    try:
        return Postgres().tokenize(sql)
    except TokenError as e:
        raise SqlTextError(f"cannot tokenize: {e}") from e


def _scan(sql: str):
    """Yields (kind, start, end_inclusive, text): kind 'param' ($n, text = n), 'literal' or 'other'."""
    toks = tokens(sql)
    i = 0
    while i < len(toks):
        t = toks[i]
        nxt = toks[i + 1] if i + 1 < len(toks) else None
        if (t.token_type == TokenType.PARAMETER and nxt is not None and nxt.token_type == TokenType.NUMBER
                and nxt.start == t.end + 1 and nxt.text.isdigit()):
            yield "param", t.start, nxt.end, nxt.text
            i += 2
            continue
        yield ("literal" if t.token_type in LITERAL_TOKENS else "other"), t.start, t.end, sql[t.start:t.end + 1]
        i += 1


def params(sql: str) -> set:
    """The $n placeholders of a statement, as ints."""
    return {int(text) for kind, _, _, text in _scan(sql) if kind == "param"}


def literals(sql: str) -> list:
    return [text for kind, _, _, text in _scan(sql) if kind == "literal"]


def has_comment(sql: str) -> bool:
    return any(t.comments for t in tokens(sql))


def normalize(sql: str) -> tuple:
    """Every literal becomes a new $k (k after the largest $n), like pg_stat_statements. Returns (text, {k: literal})."""
    spans = list(_scan(sql))
    k = max([int(t) for kind, _, _, t in spans if kind == "param"], default=0)
    out, pos, values = [], 0, {}
    for kind, start, end, text in spans:
        if kind != "literal":
            continue
        k += 1
        out.append(sql[pos:start])
        out.append(f"${k}")
        values[k] = text
        pos = end + 1
    out.append(sql[pos:])
    return "".join(out), values


def denormalize(sql: str, values: dict) -> str:
    """Puts the original literal tokens back in place of their $k placeholders."""
    out, pos = [], 0
    for kind, start, end, text in _scan(sql):
        if kind == "param" and int(text) in values:
            out.append(sql[pos:start])
            out.append(values[int(text)])
            pos = end + 1
    out.append(sql[pos:])
    return "".join(out)


def strip_literals(text: str) -> str:
    """Literals become ?: for plan expressions and catalog definitions sent to the model. Untokenizable text -> ?."""
    try:
        spans = list(_scan(text))
    except SqlTextError:
        return "?"
    out, pos = [], 0
    for kind, start, end, _ in spans:
        if kind == "literal":
            out.append(text[pos:start])
            out.append("?")
            pos = end + 1
    out.append(text[pos:])
    return "".join(out)


def text_key(sql: str) -> str:
    """Identity of a statement's shape: normalized (literals as $k), whitespace collapsed, lower-cased."""
    norm, _ = normalize(sql)
    return hashlib.sha256(" ".join(norm.split()).lower().encode()).hexdigest()
```

Notes: sqlglot's Postgres tokenizer yields `$1` as a `PARAMETER` token followed by an adjacent `NUMBER`; `E'..'` is `BYTE_STRING`, `$$..$$` is `HEREDOC_STRING`. Token `end` is inclusive.

- [ ] **Step 5: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_sqltext.py -v && .venv/bin/python -m pytest -q`
Expected: 8 passed; the whole suite still passes.

- [ ] **Step 6: Commit**

```bash
git add engine/pyproject.toml engine/src/lakebase_health/fixer/draft/__init__.py engine/src/lakebase_health/fixer/draft/sqltext.py engine/tests/unit/test_sqltext.py
git commit -m "feat(draft): sqlglot dependency; parameter and literal helpers for query text

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Static checks of a rewrite

**Files:**
- Create: `engine/src/lakebase_health/fixer/draft/static.py`
- Test: `engine/tests/unit/test_static.py`

**Interfaces:**
- Consumes: `sqltext.params`, `literals`, `has_comment`, `SqlTextError` (Task 1).
- Produces: `Shape(tables: frozenset[tuple[str, str]], functions: frozenset[str], columns: int | None, ordered: bool)`; `shape(sql) -> Shape` (raises `StaticError` unless one SELECT/WITH without DML, locks or INTO); `check(original_normalized, rewrite) -> Shape` (raises `StaticError(check, detail)` with `check` in `comment`, `parse`, `one_statement`, `select_only`, `data_modifying_cte`, `locking`, `select_into`, `parameters`, `new_literal`, `other_table`, `unsafe_function`, `column_count`); constants `SAFE_FUNCTIONS`, `DENIED_FUNCTIONS`, `ALLOWED_LITERALS = {"0", "1"}`.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_static.py` (new file, complete):

```python
import pytest

from lakebase_health.fixer.draft.static import StaticError, check, shape

ORIGINAL = ("SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
            "AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
            "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) ORDER BY o.id")
GOOD = ("WITH a AS (SELECT status, avg(amount) AS avg_amount FROM hc_test.orders WHERE id BETWEEN $1 AND $2 "
        "GROUP BY status) SELECT o.id, o.status, o.amount FROM hc_test.orders o JOIN a ON a.status = o.status "
        "WHERE o.id BETWEEN $1 AND $2 AND o.amount > a.avg_amount ORDER BY o.id")
WINDOW = ("SELECT id, status, amount FROM (SELECT o.id, o.status, o.amount, avg(o.amount) OVER "
          "(PARTITION BY o.status) AS a FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2) s "
          "WHERE amount > a ORDER BY id")


def test_shape_of_the_original():
    s = shape(ORIGINAL)
    assert s.tables == {("hc_test", "orders")} and s.functions == {"avg"} and s.columns == 3 and s.ordered
    assert shape("SELECT * FROM hc_test.orders WHERE id = $1").columns is None  # a star: count unknown


@pytest.mark.parametrize("rewrite", [GOOD, WINDOW,
                                     "SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 "
                                     "AND $2 AND EXISTS (SELECT 1 FROM hc_test.orders o2 WHERE o2.id = o.id) ORDER BY o.id"])
def test_equivalent_shapes_pass(rewrite):
    assert check(ORIGINAL, rewrite).ordered


@pytest.mark.parametrize("name,rewrite,failed", [
    ("extra statement", GOOD + "; SELECT 1", "one_statement"),
    ("new literal", GOOD.replace("ORDER BY o.id", "AND o.amount > 5 ORDER BY o.id"), "new_literal"),
    ("other table", GOOD.replace("JOIN a ON", "JOIN hc_test.customers c ON c.id = o.customer_id JOIN a ON"),
     "other_table"),
    ("data-modifying CTE", "WITH d AS (DELETE FROM hc_test.orders WHERE id = $1 RETURNING 1) "
                           "SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
                           "ORDER BY o.id", "data_modifying_cte"),
    ("hint comment", "/*+ SeqScan(o) */ " + GOOD, "comment"),
    ("line comment", GOOD + " -- fast", "comment"),
    ("pg_sleep", GOOD.replace("o.amount FROM hc_test.orders o JOIN", "o.amount + pg_sleep(1) FROM hc_test.orders o JOIN"),
     "unsafe_function"),
    ("volatile function", GOOD.replace("ORDER BY o.id", "ORDER BY random()"), "unsafe_function"),
    ("SET", "SET work_mem = $1 || $2", "select_only"),
    ("UPDATE", "UPDATE hc_test.orders SET amount = $1 WHERE id = $2", "select_only"),
    ("a parameter dropped", GOOD.replace("$2", "$1"), "parameters"),
    ("a parameter added", GOOD.replace("ORDER BY o.id", "AND o.id <> $3 ORDER BY o.id"), "parameters"),
    ("fewer columns", GOOD.replace("o.id, o.status, o.amount FROM hc_test.orders o JOIN",
                                   "o.id, o.status FROM hc_test.orders o JOIN"), "column_count"),
    ("FOR UPDATE", GOOD + " FOR UPDATE", "locking"),
    ("SELECT INTO", "SELECT o.id, o.status, o.amount INTO hc_test.copy FROM hc_test.orders o "
                    "WHERE o.id BETWEEN $1 AND $2 ORDER BY o.id", "select_into"),
    ("garbage", "SELEC FROM (", "parse"),
])
def test_unsafe_rewrites_fail_with_the_check_name(name, rewrite, failed):
    with pytest.raises(StaticError) as e:
        check(ORIGINAL, rewrite)
    assert e.value.check == failed, (name, e.value)


def test_functions_the_original_calls_are_allowed():
    original = "SELECT md5(note) FROM hc_test.orders WHERE id = $1"
    check(original, "SELECT md5(o.note) AS h FROM hc_test.orders o WHERE o.id = $1")
    with pytest.raises(StaticError, match="md5"):
        check("SELECT note FROM hc_test.orders WHERE id = $1", "SELECT md5(note) FROM hc_test.orders WHERE id = $1")
    with pytest.raises(StaticError, match="pg_sleep"):
        check("SELECT pg_sleep($1)", "SELECT pg_sleep($1)")  # denied even when the original calls it
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_static.py -v`
Expected: FAIL with `ModuleNotFoundError ... static`.

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/fixer/draft/static.py` (new file, complete):

```python
"""Static checks of a model's rewrite against the normalized original, before any branch exists."""
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .sqltext import SqlTextError, has_comment, literals, params

# sqlglot's names for the functions a rewrite may call (it normalizes some: date_trunc -> timestamp_trunc,
# string_agg -> group_concat, bool_and -> logical_and). Functions the original calls are allowed as well.
SAFE_FUNCTIONS = frozenset({
    "count", "sum", "avg", "min", "max", "coalesce", "nullif", "greatest", "least", "cast", "lower", "upper",
    "length", "abs", "round", "floor", "ceil", "trim", "substring", "concat", "extract", "timestamp_trunc",
    "row_number", "rank", "dense_rank", "lag", "lead", "first_value", "last_value", "ntile", "percent_rank",
    "cume_dist", "array_agg", "array", "group_concat", "logical_and", "logical_or", "case", "if",
})
DENIED_FUNCTIONS = frozenset({
    "pg_sleep", "pg_sleep_for", "pg_sleep_until", "set_config", "current_setting", "pg_read_file",
    "pg_read_binary_file", "pg_ls_dir", "lo_import", "lo_export", "dblink", "dblink_exec", "nextval", "setval",
    "pg_terminate_backend", "pg_cancel_backend", "pg_advisory_lock", "pg_advisory_xact_lock", "pg_notify",
    "txid_current", "pg_reload_conf", "pg_switch_wal", "rand", "random",
})
# Literals a rewrite may add: EXISTS (SELECT 1), LIMIT 1, coalesce(x, 0).
ALLOWED_LITERALS = frozenset({"0", "1"})
_STRUCTURAL = (exp.Binary, exp.Connector, exp.Unary, exp.Predicate)


@dataclass(frozen=True)
class Shape:
    tables: frozenset      # (schema, name), lower-cased; schema "" when unqualified
    functions: frozenset
    columns: int | None    # None when the select list has a star
    ordered: bool          # top-level ORDER BY


class StaticError(ValueError):
    def __init__(self, check: str, detail: str):
        super().__init__(f"{check}: {detail}")
        self.check, self.detail = check, detail


def _function_names(tree) -> set:
    out = set()
    for f in tree.find_all(exp.Func):
        if isinstance(f, _STRUCTURAL):
            continue
        if isinstance(f, exp.Anonymous):
            out.add(f.name.lower())
        else:
            names = f.sql_names()
            out.add((names[0] if names else f.key).lower())
    return out


def shape(sql: str) -> Shape:
    """Parses one SELECT/WITH statement; raises StaticError otherwise."""
    try:
        trees = [t for t in sqlglot.parse(sql, read="postgres") if t is not None]
    except (ParseError, SqlTextError) as e:
        raise StaticError("parse", str(e).splitlines()[0][:200]) from e
    if len(trees) != 1:
        raise StaticError("one_statement", f"{len(trees)} statements")
    tree = trees[0]
    if not isinstance(tree, (exp.Select, exp.SetOperation)):
        raise StaticError("select_only", f"{type(tree).__name__} is not a SELECT")
    if any(True for _ in tree.find_all(exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        raise StaticError("data_modifying_cte", "INSERT, UPDATE, DELETE or MERGE inside the query")
    for s in tree.find_all(exp.Select):
        if s.args.get("locks"):
            raise StaticError("locking", "FOR UPDATE/SHARE is not allowed")
        if s.args.get("into"):
            raise StaticError("select_into", "SELECT INTO creates a table")
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    tables = frozenset((t.db.lower(), t.name.lower()) for t in tree.find_all(exp.Table)
                       if t.name and not (not t.db and t.name.lower() in ctes))
    selects = tree.selects
    star = any(isinstance(s, exp.Star) or (isinstance(s, exp.Column) and isinstance(s.this, exp.Star)) for s in selects)
    return Shape(tables, frozenset(_function_names(tree)), None if star else len(selects),
                 tree.args.get("order") is not None)


def check(original: str, rewrite: str) -> Shape:
    """`original` is the normalized original (literals as $k). Returns the rewrite's shape or raises StaticError."""
    try:
        if has_comment(rewrite):
            raise StaticError("comment", "comments (and hint comments) are not allowed")
        rp, op, lits = params(rewrite), params(original), literals(rewrite)
    except SqlTextError as e:
        raise StaticError("parse", str(e)[:200]) from e
    o, r = shape(original), shape(rewrite)
    if rp != op:
        raise StaticError("parameters", f"uses {sorted(rp)}, the original uses {sorted(op)}")
    new = sorted({lit for lit in lits if lit not in ALLOWED_LITERALS})
    if new:
        raise StaticError("new_literal", f"{len(new)} literal(s) not in the original")
    extra = sorted(".".join(p for p in t if p) for t in r.tables - o.tables)
    if extra:
        raise StaticError("other_table", f"reads {', '.join(extra)}, which the original does not")
    denied = sorted(r.functions & DENIED_FUNCTIONS)
    unsafe = sorted(r.functions - SAFE_FUNCTIONS - o.functions - DENIED_FUNCTIONS)
    if denied or unsafe:
        raise StaticError("unsafe_function", ", ".join(denied + unsafe))
    if o.columns is not None and r.columns is not None and o.columns != r.columns:
        raise StaticError("column_count", f"{r.columns} output columns, the original has {o.columns}")
    return r
```

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_static.py -v`
Expected: 21 passed.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/draft/static.py engine/tests/unit/test_static.py
git commit -m "feat(draft): static checks for model rewrites (one SELECT, same tables and parameters, safe functions)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: Tables, columns, masks and config; the engine ignores rewrite actions

**Files:**
- Modify: `engine/src/lakebase_health/ddl.py`, `engine/src/lakebase_health/config.py`, `engine/src/lakebase_health/record.py`
- Test: `engine/tests/unit/test_ddl.py`, `engine/tests/unit/test_record.py`

**Interfaces:**
- Produces: tables `drafts(draft_id, finding_fingerprint, attempt INT, action_id, approval_id, endpoint, prompt_version, context_hash, rationale, expected_plan_change, state, reason, requested_by, requested_at, updated_at, ended_at)`, `llm_calls(call_id, draft_id, endpoint, prompt_hash, response_hash, input_tokens, output_tokens, latency_ms, status, request_id, called_at)`, `llm_status(endpoint, enabled, ready, reason, checked_at)`; columns `proposed_actions.origin`, `proposed_actions.draft_id`, `approvals.fingerprint` (also added to existing installs via `ADDED_COLUMNS`); function `<schema>.mask_llm_text(v, origin)` masking `proposed_actions.sql` only on `origin = 'llm'` rows; `EngineConfig.llm_endpoint = "databricks-claude-opus-5-5"`, `llm_enabled = True`, `llm_test_response = ""`. `persist()` plans only actions whose `origin` is NULL or `'check'`.

- [ ] **Step 1: Write the failing tests**

`engine/tests/unit/test_ddl.py` (apply this change):

```diff
--- a/engine/tests/unit/test_ddl.py
+++ b/engine/tests/unit/test_ddl.py
@@ -6,7 +6,7 @@ from tests.fakes import FakeSql
 
 CFG = EngineConfig(catalog="cat")
 TABLES = ("runs", "ingest_watermarks", "inventory", "check_results", "findings", "proposed_actions",
-          "shadow_log", "ledger", "approvals", "action_runs", "metrics_daily")
+          "shadow_log", "ledger", "approvals", "action_runs", "metrics_daily", "drafts", "llm_calls", "llm_status")
 
 
 def test_ensure_schema_creates_everything_idempotently():
@@ -25,7 +25,7 @@ def test_columns_added_later_are_added_to_existing_tables_once():
     ensure_schema(sql, CFG)
     assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.findings ADD COLUMNS \(rollback_note STRING\)$")
     assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ADD COLUMNS \(rollback_note STRING\)$")
-    current = [{"col_name": c} for c in ("fingerprint", "rollback_sql", "rollback_note")]
+    current = [{"col_name": c} for c in ("fingerprint", "rollback_sql", "rollback_note", "origin", "draft_id")]
     sql = FakeSql(responses=[(r"SHOW COLUMNS IN", current)])
     ensure_schema(sql, CFG)
     assert not sql.ran(r"ADD COLUMNS")
@@ -82,3 +82,28 @@ def test_dropping_masks_fails_loudly_on_permission_errors(err):
     sql = FakeSql(responses=[(r"DROP MASK", RuntimeError(err))], existing_tables={"cat.lakebase_health.raw_plan_history"})
     with pytest.raises(RuntimeError, match="PERMISSION_DENIED|INSUFFICIENT_PRIVILEGES"):
         ensure_masks(sql, EngineConfig(catalog="cat", pii_masking=False))
+
+
+def test_rewrite_columns_and_tables_are_added_to_existing_installs():
+    sql = FakeSql(responses=[(r"SHOW COLUMNS IN cat\.lakebase_health\.proposed_actions$", [{"col_name": "rollback_note"}]),
+                             (r"SHOW COLUMNS IN cat\.lakebase_health\.approvals$", [{"col_name": "note"}])])
+    ensure_schema(sql, CFG)
+    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ADD COLUMNS \(origin STRING\)$")
+    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ADD COLUMNS \(draft_id STRING\)$")
+    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.approvals ADD COLUMNS \(fingerprint STRING\)$")
+    [calls] = sql.ran(r"TABLE IF NOT EXISTS cat\.lakebase_health\.llm_calls ")
+    assert "prompt_hash STRING" in calls and "response_hash STRING" in calls
+    assert "prompt STRING" not in calls and "response STRING" not in calls and "content" not in calls
+
+
+def test_rewrite_sql_is_masked_only_on_llm_rows():
+    sql = FakeSql(existing_tables={"cat.lakebase_health.proposed_actions"})
+    ensure_schema(sql, CFG)
+    [fn] = sql.ran(r"FUNCTION IF NOT EXISTS cat\.lakebase_health\.mask_llm_text")
+    assert "coalesce(origin, 'check') <> 'llm'" in fn
+    ensure_masks(sql, CFG)
+    assert sql.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ALTER COLUMN sql SET MASK "
+                   r"cat\.lakebase_health\.mask_llm_text USING COLUMNS \(origin\)$")
+    off = FakeSql(existing_tables={"cat.lakebase_health.proposed_actions"})
+    ensure_masks(off, EngineConfig(catalog="cat", pii_masking=False))
+    assert off.ran(r"^ALTER TABLE cat\.lakebase_health\.proposed_actions ALTER COLUMN sql DROP MASK$")
```

`engine/tests/unit/test_record.py` (apply this change):

```diff
--- a/engine/tests/unit/test_record.py
+++ b/engine/tests/unit/test_record.py
@@ -88,3 +88,17 @@ def test_sequence_metric_and_check_results_retention():
 def test_load_last_ok():
     sql = FakeSql(responses=[(r"max\(ts\) AS last_ok", [{"check_id": "c1", "last_ok": NOW}])])
     assert load_last_ok(sql, CFG) == {"c1": NOW}
+
+
+def test_an_llm_rewrite_neither_blocks_the_check_fix_nor_is_merged_by_the_engine():
+    fp = fingerprint("c1", "p", "e", "o")
+    rewrite = {"action_id": "d" * 32, "fingerprint": fp, "check_id": "c1", "kind": "query_rewrite", "sql": "SELECT 1",
+               "rollback_sql": None, "risk": "low", "reversible": True, "verify_on_branch": True, "state": "verified",
+               "created_at": NOW, "updated_at": NOW, "expires_at": NOW + timedelta(days=7), "rollback_note": "x",
+               "origin": "llm", "draft_id": "d" * 32}
+    sql = FakeSql()
+    sql.tables[T("proposed_actions")] = [dict(rewrite)]
+    counts = persist(sql, CFG, "r1", NOW, [outcome()], SPECS, [ep()])
+    assert counts["actions_proposed"] == 1  # a verified rewrite is not a fixer state of the check's fix
+    rows = sql.tables[T("proposed_actions")]
+    assert rows[0] == rewrite and rows[1]["kind"] == "ddl" and rows[1]["state"] == "proposed"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_ddl.py tests/unit/test_record.py -v`
Expected: FAIL — no `drafts` table, no `mask_llm_text`, and `actions_proposed == 0` (the verified rewrite counts as a fixer state of the finding).

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/ddl.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/ddl.py
+++ b/engine/src/lakebase_health/ddl.py
@@ -20,22 +20,32 @@ TABLE_DDL = {
     "proposed_actions": "action_id STRING, fingerprint STRING, check_id STRING, kind STRING, sql STRING, "
                         "rollback_sql STRING, risk STRING, reversible BOOLEAN, verify_on_branch BOOLEAN, "
                         "state STRING, created_at TIMESTAMP, updated_at TIMESTAMP, expires_at TIMESTAMP, "
-                        "rollback_note STRING",
+                        "rollback_note STRING, origin STRING, draft_id STRING",
     "shadow_log": "action_id STRING, evaluated_at TIMESTAMP, run_id STRING, would_allow_level STRING, "
                   "would_execute BOOLEAN, reason STRING, outcome_score DOUBLE",
     "ledger": "event_id STRING, action_id STRING, event_type STRING, actor STRING, payload STRING, ts TIMESTAMP, "
               "prev_hash STRING, hash STRING",
     "approvals": "approval_id STRING, action_id STRING, request STRING, sql_hash STRING, approver STRING, "
-                 "requested_at TIMESTAMP, note STRING",
+                 "requested_at TIMESTAMP, note STRING, fingerprint STRING",
     "action_runs": "run_id STRING, approval_id STRING, action_id STRING, kind STRING, started_at TIMESTAMP, "
                    "ended_at TIMESTAMP, outcome STRING, report STRING, branch STRING, error STRING",
     "metrics_daily": "day DATE, project_id STRING, endpoint_id STRING, metric STRING, value DOUBLE",
+    # LLM query rewrites: one row per attempt, one per model call (hashes and token counts only), endpoint set-up
+    "drafts": "draft_id STRING, finding_fingerprint STRING, attempt INT, action_id STRING, approval_id STRING, "
+              "endpoint STRING, prompt_version STRING, context_hash STRING, rationale STRING, "
+              "expected_plan_change STRING, state STRING, reason STRING, requested_by STRING, "
+              "requested_at TIMESTAMP, updated_at TIMESTAMP, ended_at TIMESTAMP",
+    "llm_calls": "call_id STRING, draft_id STRING, endpoint STRING, prompt_hash STRING, response_hash STRING, "
+                 "input_tokens BIGINT, output_tokens BIGINT, latency_ms DOUBLE, status STRING, request_id STRING, "
+                 "called_at TIMESTAMP",
+    "llm_status": "endpoint STRING, enabled BOOLEAN, ready BOOLEAN, reason STRING, checked_at TIMESTAMP",
 }
 
 # Columns added after the first release: CREATE TABLE IF NOT EXISTS leaves existing tables as they are
 ADDED_COLUMNS = {
     "findings": ("rollback_note STRING",),
-    "proposed_actions": ("rollback_note STRING",),
+    "proposed_actions": ("rollback_note STRING", "origin STRING", "draft_id STRING"),
+    "approvals": ("fingerprint STRING",),
 }
 
 MASKED_COLUMNS = {
@@ -46,6 +56,8 @@ MASKED_COLUMNS = {
     "findings": ("query_text",),
     "check_results": ("query_text",),
 }
+# Masked only on rows whose origin is 'llm': a rewrite carries the original query's literals; check fixes are DDL.
+ORIGIN_MASKED_COLUMNS = {"proposed_actions": ("sql",)}
 
 
 def ensure_schema(sql, cfg) -> None:
@@ -62,21 +74,26 @@ def ensure_schema(sql, cfg) -> None:
     sql.execute(
         f"CREATE FUNCTION IF NOT EXISTS {cfg.table('mask_text')}(v STRING) RETURNS STRING "
         "RETURN CASE WHEN is_account_group_member('databricks_pii_access') THEN v ELSE '<REDACTED>' END")
+    sql.execute(
+        f"CREATE FUNCTION IF NOT EXISTS {cfg.table('mask_llm_text')}(v STRING, origin STRING) RETURNS STRING "
+        "RETURN CASE WHEN coalesce(origin, 'check') <> 'llm' OR is_account_group_member('databricks_pii_access') "
+        "THEN v ELSE '<REDACTED>' END")
 
 
 def ensure_masks(sql, cfg) -> None:
-    """Sets the mask_text column masks, or with cfg.pii_masking off removes them (idempotent either way)."""
-    fn = cfg.table("mask_text")
-    for table, cols in MASKED_COLUMNS.items():
+    """Sets the column masks, or with cfg.pii_masking off removes them (idempotent either way)."""
+    masks = [(t, c, cfg.table("mask_text")) for t, cols in MASKED_COLUMNS.items() for c in cols]
+    masks += [(t, c, f"{cfg.table('mask_llm_text')} USING COLUMNS (origin)")
+              for t, cols in ORIGIN_MASKED_COLUMNS.items() for c in cols]
+    for table, col, fn in masks:
         full = cfg.table(table)
         if not sql.table_exists(full):
             continue
-        for col in cols:
-            if cfg.pii_masking:
-                sql.execute(f"ALTER TABLE {full} ALTER COLUMN {col} SET MASK {fn}")
-                continue
-            try:
-                sql.execute(f"ALTER TABLE {full} ALTER COLUMN {col} DROP MASK")
-            except Exception as e:  # tolerated only when the column has no mask (already dropped or never set)
-                if any(m in str(e).upper() for m in ("PERMISSION", "PRIVILEGE", "NOT AUTHORIZED")):
-                    raise  # masks would silently stay while the app shows text to everyone
+        if cfg.pii_masking:
+            sql.execute(f"ALTER TABLE {full} ALTER COLUMN {col} SET MASK {fn}")
+            continue
+        try:
+            sql.execute(f"ALTER TABLE {full} ALTER COLUMN {col} DROP MASK")
+        except Exception as e:  # tolerated only when the column has no mask (already dropped or never set)
+            if any(m in str(e).upper() for m in ("PERMISSION", "PRIVILEGE", "NOT AUTHORIZED")):
+                raise  # masks would silently stay while the app shows text to everyone
```

`engine/src/lakebase_health/config.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/config.py
+++ b/engine/src/lakebase_health/config.py
@@ -24,6 +24,9 @@ class EngineConfig:
     pii_masking: bool = True  # False: drop the mask_text column masks so every reader sees query text
     service_roles: tuple = ()  # engine/fixer service principal roles whose queries the query checks ignore
     ingest_interval_minutes: int = 60  # the ingest schedule; analysis-only runs call a source stale after 2x this
+    llm_endpoint: str = "databricks-claude-opus-5-5"  # serving endpoint that drafts query rewrites
+    llm_enabled: bool = True  # False: the fixer drafts nothing and the app hides "Draft a rewrite"
+    llm_test_response: str = ""  # acceptance only (--llm-test-response): a fixed bad reply instead of the model
 
     def table(self, name: str) -> str:
         return f"{self.catalog}.{self.schema}.{name}"
```

`engine/src/lakebase_health/record.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/record.py
+++ b/engine/src/lakebase_health/record.py
@@ -86,7 +86,9 @@ def persist(sql, cfg, run_id, now, outcomes, specs, inventory) -> dict:
     changed = apply_run(existing, outcomes, all_specs, now, cfg.resolve_after_clean_runs, superseded)
     sql.merge_rows(cfg.table("findings"), [f.to_row() for f in changed], ["fingerprint"])
 
-    all_actions = {r["action_id"]: Action.from_row(r) for r in sql.query(f"SELECT * FROM {cfg.table('proposed_actions')}")}
+    # LLM rewrites belong to the fixer (states, adoption); the engine plans only check actions
+    all_actions = {r["action_id"]: Action.from_row(r) for r in sql.query(f"SELECT * FROM {cfg.table('proposed_actions')}")
+                   if (r.get("origin") or "check") == "check"}
     actions, events = plan_actions(changed, all_actions, all_specs, now)
     sql.merge_rows(cfg.table("proposed_actions"), [a.to_row() for a in actions], ["action_id"])
     active = [a for a in actions if a.state == "proposed"]
```

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/ddl.py engine/src/lakebase_health/config.py engine/src/lakebase_health/record.py engine/tests/unit/test_ddl.py engine/tests/unit/test_record.py
git commit -m "feat(engine): drafts, llm_calls and llm_status tables; rewrite SQL masked; the engine plans check actions only

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: Model context, versioned prompt and reply parsing

**Files:**
- Create: `engine/src/lakebase_health/fixer/draft/states.py`, `engine/src/lakebase_health/fixer/draft/context.py`, `engine/src/lakebase_health/fixer/draft/prompt.py`, `engine/src/lakebase_health/fixer/draft/prompts/rewrite_v1.md`
- Modify: `engine/src/lakebase_health/fixer/pg.py` (read-only `draft_context` session kind)
- Test: `engine/tests/unit/test_draft_context.py`, `engine/tests/unit/test_draft_prompt.py`

**Interfaces:**
- Consumes: `sqltext.normalize`, `params`, `strip_literals` (Task 1); `fixer.samples.parse_params` (existing); `EngineConfig.raw()`.
- Produces:
  - `states.REWRITE_CHECKS` (7 check ids), `RUNNING = ("drafting", "verifying")`, `FINAL = ("draft_failed", "verified", "verify_failed", "adopted", "not_adopted")`, `ADOPTION_WINDOW = timedelta(days=7)`, `draft_token(fingerprint, attempt) -> str` (sha256 of `"draft:<fp>:<attempt>"`), `finding_queryid(check_id, evidence, obj) -> str | None`.
  - `context.NoSamples(Exception)`; `load_history(sql, cfg, finding: dict) -> {"queryid", "query", "param_sets": list[list[str | None]], "plan": dict | None}` (`[[]]` when the query has no `$n`); `load_catalog(conn, tables: set[tuple[str, str]]) -> list[dict]`; `build_context(finding, normalized_query, ordered: bool, plan, tables) -> dict`; `strip_plan(node)`.
  - `prompt.PROMPT_VERSION = "rewrite_v1"`, `SYSTEM: str`, `messages(context) -> list[{"role", "content"}]`, `prompt_hash(msgs) -> str`, `context_hash(context) -> str`, `parse_response(text) -> {"ok": True, "rewrite", "rationale", "expected_plan_change"} | {"ok": False, "reason": "invalid_response" | "no_safe_rewrite", "detail": str}`.
  - `pg.FIXER_SETUP("draft_context")` = search path, `default_transaction_read_only = on`, `lock_timeout = '1s'`, `statement_timeout = '30s'`.

- [ ] **Step 1: Write the failing tests**

The payload test pins the exact user message and checks that none of the sampled values, the query's literal (`'cancelled'`), the CHECK and partial-index literals or evidence strings reach it.

`engine/tests/unit/test_draft_context.py` (new file, complete):

```python
import json
from types import SimpleNamespace

import pytest

from lakebase_health.config import EngineConfig
from lakebase_health.fixer.draft import prompt
from lakebase_health.fixer.draft.context import NoSamples, build_context, load_catalog, load_history
from lakebase_health.fixer.draft.sqltext import normalize
from lakebase_health.fixer.draft.states import draft_token, finding_queryid
from lakebase_health.fixer.pg import FIXER_SETUP
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
QUERY = ("SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
         "AND o.status <> 'cancelled' AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
         "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) ORDER BY o.id")
PLAN = json.dumps([{"Plan": {"Node Type": "Index Scan", "Relation Name": "orders", "Alias": "o",
                             "Index Name": "orders_pkey", "Index Cond": "((o.id >= 1600004111) AND (o.id <= $2))",
                             "Filter": "((o.status <> 'cancelled'::text) AND (o.amount > (SubPlan 1)))",
                             "Plan Rows": 9, "Actual Rows": 1803,
                             "Plans": [{"Node Type": "Aggregate", "Strategy": "Plain", "Parent Relationship": "SubPlan",
                                        "Subplan Name": "SubPlan 1", "Plan Rows": 1}]},
                    "Query Text": "SELECT ... 1600004111 'cancelled'", "Execution Time": 512.5}])
ROWS = [
    {"query_text": "<REDACTED>", "parameters": "<REDACTED>", "plan_json": "<REDACTED>", "duration_ms": 9000.0},
    {"query_text": QUERY, "parameters": "$1 = '1600004111', $2 = '1600006111'", "plan_json": PLAN, "duration_ms": 512.5},
    {"query_text": QUERY, "parameters": "$1 = '1600004111', $2 = '1600006111'", "plan_json": None, "duration_ms": 400.0},
    {"query_text": QUERY, "parameters": "$1 = '1600200000', $2 = '1600202000'", "plan_json": None, "duration_ms": 1.0},
]
FINDING = {"fingerprint": "f" * 32, "check_id": "top_db_time_query", "project_id": "p1", "branch_id": "br1",
           "endpoint_id": "ep-a", "object": "42", "status": "open", "measure": 62.5,
           "evidence": json.dumps({"queryid": "42", "calls": 400, "share_pct": 62.5, "query_text": "SELECT 'x'",
                                   "regressed": True})}


class Conn:
    """Catalog answers keyed by the first words of each statement."""
    ANSWERS = {
        "SELECT n.nspname": [("hc_test", "orders", 900000)],
        "SELECT a.attname": [("id", "integer", True), ("status", "text", True), ("amount", "numeric(12,2)", True)],
        "SELECT pg_get_constraintdef": [("PRIMARY KEY (id)",), ("CHECK ((status <> 'void'::text))",)],
        "SELECT pg_get_indexdef": [("CREATE UNIQUE INDEX orders_pkey ON hc_test.orders USING btree (id)",),
                                   ("CREATE INDEX orders_part ON hc_test.orders USING btree (id) WHERE (status = 'paid'::text)",)],
        "SELECT attname, n_distinct": [("status", 5.0, 0.0, 0.21)],
    }

    def __init__(self):
        self.ran = []

    def execute(self, stmt, args=None):
        self.ran.append((stmt, args))
        rows = next(v for k, v in self.ANSWERS.items() if stmt.startswith(k))
        return SimpleNamespace(fetchone=lambda: rows[0] if rows else None, fetchall=lambda: rows)


def history():
    return load_history(FakeSql([(r"raw_plan_history", ROWS)]), CFG, FINDING)


def test_history_takes_readable_text_distinct_parameter_sets_and_the_slowest_readable_plan():
    h = history()
    assert h["queryid"] == "42" and h["query"] == QUERY
    assert h["param_sets"] == [["1600004111", "1600006111"], ["1600200000", "1600202000"]]
    assert h["plan"]["Plan"]["Index Cond"] == "((o.id >= ?) AND (o.id <= $2))"
    assert "Query Text" not in h["plan"] and "Execution Time" not in h["plan"]


def test_history_without_text_or_values_is_no_samples():
    with pytest.raises(NoSamples, match="redacted"):
        load_history(FakeSql([(r"raw_plan_history", ROWS[:1])]), CFG, FINDING)
    no_values = [{**r, "parameters": None} for r in ROWS[1:]]
    with pytest.raises(NoSamples, match="2 parameters"):
        load_history(FakeSql([(r"raw_plan_history", no_values)]), CFG, FINDING)
    no_params = [{**ROWS[1], "query_text": "SELECT count(*) FROM hc_test.orders", "parameters": None}]
    assert load_history(FakeSql([(r"raw_plan_history", no_params)]), CFG, FINDING)["param_sets"] == [[]]


def test_the_model_payload_is_exactly_this_and_holds_no_values():
    h = history()
    norm, literals = normalize(h["query"])
    tables = load_catalog(Conn(), {("hc_test", "orders")})
    msgs = prompt.messages(build_context(FINDING, norm, True, h["plan"], tables))
    assert [m["role"] for m in msgs] == ["system", "user"] and msgs[0]["content"] == prompt.SYSTEM
    assert json.loads(msgs[1]["content"]) == {
        "finding": {"check_id": "top_db_time_query", "measure": 62.5, "evidence": {"calls": 400, "share_pct": 62.5}},
        "query": ("SELECT o.id, o.status, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 "
                  "AND o.status <> $3 AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
                  "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) ORDER BY o.id"),
        "parameters": ["$1", "$2", "$3"],
        "ordered": True,
        "plan": {"Plan": {"Node Type": "Index Scan", "Relation Name": "orders", "Alias": "o",
                          "Index Name": "orders_pkey", "Index Cond": "((o.id >= ?) AND (o.id <= $2))",
                          "Filter": "((o.status <> ?::text) AND (o.amount > (SubPlan ?)))",
                          "Plan Rows": 9, "Actual Rows": 1803,
                          "Plans": [{"Node Type": "Aggregate", "Strategy": "Plain", "Parent Relationship": "SubPlan",
                                     "Subplan Name": "SubPlan 1", "Plan Rows": 1}]}},
        "tables": [{"name": "hc_test.orders", "row_estimate": 900000,
                    "columns": [{"name": "id", "type": "integer", "not_null": True},
                                {"name": "status", "type": "text", "not_null": True},
                                {"name": "amount", "type": "numeric(12,2)", "not_null": True}],
                    "constraints": ["PRIMARY KEY (id)", "CHECK ((status <> ?::text))"],
                    "indexes": ["CREATE UNIQUE INDEX orders_pkey ON hc_test.orders USING btree (id)",
                                "CREATE INDEX orders_part ON hc_test.orders USING btree (id) WHERE (status = ?::text)"],
                    "column_stats": [{"column": "status", "n_distinct": 5.0, "null_frac": 0.0, "correlation": 0.21}]}],
    }
    payload = json.dumps(msgs)
    for value in ("1600004111", "1600006111", "1600200000", "cancelled", "void", "paid", "SELECT 'x'"):
        assert value not in payload, value
    assert literals == {3: "'cancelled'"}


def test_catalog_reads_resolve_names_through_the_search_path_and_never_read_values():
    conn = Conn()
    load_catalog(conn, {("", "orders")})
    assert all(args in (("orders",), ("hc_test", "orders")) for _, args in conn.ran)
    assert not any("most_common" in s or "histogram" in s for s, _ in conn.ran)


def test_catalog_session_is_read_only_and_short():
    setup = FIXER_SETUP("draft_context")
    assert "SET default_transaction_read_only = on" in setup and "SET statement_timeout = '30s'" in setup


def test_queryid_of_each_rewritable_check_and_the_attempt_token():
    assert finding_queryid("top_db_time_query", '{"queryid": "42"}', "42") == "42"
    assert finding_queryid("disk_spill", "{}", "-17@app") == "-17"
    assert finding_queryid("row_misestimate", '{"top_queries": [{"queryid": "7"}, {"queryid": "8"}]}', "ep-a") == "7"
    assert finding_queryid("missing_fk_index", '{"queryid": "42"}', "x") is None
    assert finding_queryid("plan_flip", "not json", "abc") is None
    assert draft_token("f" * 32, 2) != draft_token("f" * 32, 3)
```

`engine/tests/unit/test_draft_prompt.py` (new file, complete):

````python
from lakebase_health.fixer.draft.prompt import PROMPT_VERSION, SYSTEM, context_hash, messages, parse_response, prompt_hash


def test_the_versioned_system_prompt_states_the_rules():
    assert PROMPT_VERSION == "rewrite_v1"
    for rule in ("PostgreSQL 17", "JSON only", "same rows and columns, in the same order", "Keep every parameter",
                 "Read only the tables the original reads", '{"rewrite": null, "reason": "<why>"}', "LIMIT"):
        assert rule in SYSTEM, rule


def test_hashes_are_stable_and_cover_the_whole_payload():
    a, b = messages({"query": "SELECT 1"}), messages({"query": "SELECT 2"})
    assert prompt_hash(a) == prompt_hash(messages({"query": "SELECT 1"})) != prompt_hash(b)
    assert context_hash({"a": 1, "b": 2}) == context_hash({"b": 2, "a": 1})


def test_reply_parsing():
    ok = parse_response('{"rewrite": "SELECT 1;", "rationale": "r", "expected_plan_change": "Hash Join"}')
    assert ok == {"ok": True, "rewrite": "SELECT 1", "rationale": "r", "expected_plan_change": "Hash Join"}
    assert parse_response('```json\n{"rewrite": "SELECT 1"}\n```')["ok"]
    assert parse_response('{"rewrite": null, "reason": "already optimal"}') == \
        {"ok": False, "reason": "no_safe_rewrite", "detail": "already optimal"}
    for bad in ('Sure! {"rewrite": "x"}', "[1]", '{"rewrite": 5}', '{"rationale": "r"}', "", None):
        assert parse_response(bad)["reason"] == "invalid_response", bad
````

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_context.py tests/unit/test_draft_prompt.py -v`
Expected: FAIL with `ModuleNotFoundError` (`states`, `context`, `prompt`).

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/fixer/draft/states.py` (new file, complete):

```python
"""Draft states, the checks whose findings can be rewritten, and the attempt token an approval covers."""
import hashlib
import json
import re
from datetime import timedelta

REWRITE_CHECKS = ("top_db_time_query", "query_regression", "plan_flip", "storage_wait", "disk_spill",
                  "sort_limit_waste", "row_misestimate")
RUNNING = ("drafting", "verifying")
FINAL = ("draft_failed", "verified", "verify_failed", "adopted", "not_adopted")
ADOPTION_WINDOW = timedelta(days=7)  # a verified rewrite not seen in the workload by then is not_adopted
_QUERYID = re.compile(r"-?\d{1,20}")


def draft_token(fingerprint: str, attempt: int) -> str:
    """What a draft approval's sql_hash holds: the attempt it asks for (app/server/lib/drafts.ts draftToken)."""
    return hashlib.sha256(f"draft:{fingerprint}:{attempt}".encode()).hexdigest()


def finding_queryid(check_id, evidence, obj) -> str | None:
    """The query a rewrite is drafted for: evidence.queryid (row_misestimate: its top query), else the object."""
    if check_id not in REWRITE_CHECKS:
        return None
    try:
        ev = json.loads(evidence) if isinstance(evidence, str) else (evidence or {})
    except ValueError:
        ev = {}
    ev = ev if isinstance(ev, dict) else {}
    if check_id == "row_misestimate":
        top = ev.get("top_queries") if isinstance(ev.get("top_queries"), list) else []
        q = top[0].get("queryid") if top and isinstance(top[0], dict) else None
    else:
        q = ev.get("queryid", str(obj or "").split("@")[0])
    q = "" if q is None else str(q)
    return q if _QUERYID.fullmatch(q) else None
```

`engine/src/lakebase_health/fixer/draft/context.py` (new file, complete):

```python
"""The model's input: the normalized query, its worst recent plan and the catalog of the tables it reads.
Literal values never enter it: the query's literals become $k, plan and catalog literals become ?."""
import json

from .. import samples
from .sqltext import params, strip_literals
from .states import finding_queryid

HISTORY_DAYS = 7
MAX_SETS = 3
# Plan keys whose values are names, not expressions: kept as they are.
NAME_KEYS = frozenset({"Node Type", "Relation Name", "Schema", "Alias", "Index Name", "Join Type", "Strategy",
                       "Parent Relationship", "Subplan Name", "Scan Direction", "Sort Method", "Sort Space Type",
                       "Partial Mode", "Operation", "CTE Name", "Function Name", "Command"})


class NoSamples(Exception):
    """No readable query text or no usable parameter values: nothing to draft from or verify with."""


def _redacted(v) -> bool:
    return v is None or str(v).startswith("<REDACTED")


def _num(v):
    return None if v is None else float(v)


def strip_plan(node):
    if isinstance(node, dict):
        return {k: (v if k in NAME_KEYS and isinstance(v, str) else strip_plan(v)) for k, v in node.items()}
    if isinstance(node, list):
        return [strip_plan(v) for v in node]
    if isinstance(node, str):
        return strip_literals(node)
    return node


def _plan(raw):
    try:
        doc = json.loads(raw)
    except (TypeError, ValueError):
        return None
    doc = doc[0] if isinstance(doc, list) and doc else doc
    return {"Plan": strip_plan(doc["Plan"])} if isinstance(doc, dict) and isinstance(doc.get("Plan"), dict) else None


def load_history(sql, cfg, finding) -> dict:
    """Query text, up to 3 distinct sampled parameter sets (newest first) and the slowest plan of the last 7 days."""
    qid = finding_queryid(finding.get("check_id"), finding.get("evidence"), finding.get("object"))
    rows = sql.query(f"""
        SELECT query_text, parameters, plan_json, duration_ms FROM {cfg.raw('plan_history')}
        WHERE project_id = :p AND endpoint_id = :e AND queryid = CAST(:q AS BIGINT)
          AND ts >= current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS AND NOT coalesce(truncated, false)
        ORDER BY ts DESC LIMIT 200""", {"p": finding["project_id"], "e": finding["endpoint_id"], "q": qid})
    texts = [r["query_text"] for r in rows if not _redacted(r["query_text"]) and r["query_text"].strip()]
    if not texts:
        raise NoSamples(f"{len(rows)} sampled plans in {HISTORY_DAYS} days, none with readable query text"
                        + (" (redacted: add the fixer SP to databricks_pii_access)" if rows else ""))
    query = texts[0].strip().rstrip(";").strip()
    need = max(params(query), default=0)
    sets = []
    for r in rows:
        p = None if _redacted(r["parameters"]) else samples.parse_params(r["parameters"])
        if p is not None and len(p) == need and p not in sets:
            sets.append(p)
        if len(sets) == MAX_SETS:
            break
    if need and not sets:
        raise NoSamples(f"the query has {need} parameters but no sampled plan has usable values")
    plans = [r for r in rows if not _redacted(r["plan_json"])]
    worst = max(plans, key=lambda r: float(r.get("duration_ms") or 0), default=None)
    return {"queryid": qid, "query": query, "param_sets": sets or [[]],
            "plan": _plan(worst["plan_json"]) if worst else None}


def load_catalog(conn, tables) -> list:
    """Columns, constraints, indexes, row estimate and pg_stats summaries (no most-common values or histograms)."""
    out = []
    for schema, name in sorted(tables):
        rel = f"{schema}.{name}" if schema else name
        row = conn.execute("SELECT n.nspname, c.relname, c.reltuples::bigint FROM pg_class c "
                           "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass(%s)", (rel,)).fetchone()
        if not row:
            out.append({"name": rel, "missing": True})
            continue
        nsp, relname, tuples = row
        cols = conn.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull FROM pg_attribute a "
                            "WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped "
                            "ORDER BY a.attnum", (rel,)).fetchall()
        cons = conn.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = to_regclass(%s) "
                            "ORDER BY conname", (rel,)).fetchall()
        idx = conn.execute("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = to_regclass(%s) "
                           "ORDER BY indexrelid", (rel,)).fetchall()
        stats = conn.execute("SELECT attname, n_distinct, null_frac, correlation FROM pg_stats "
                             "WHERE schemaname = %s AND tablename = %s ORDER BY attname", (nsp, relname)).fetchall()
        out.append({"name": f"{nsp}.{relname}", "row_estimate": int(tuples),
                    "columns": [{"name": c[0], "type": c[1], "not_null": bool(c[2])} for c in cols],
                    "constraints": [strip_literals(c[0]) for c in cons],
                    "indexes": [strip_literals(i[0]) for i in idx],
                    "column_stats": [{"column": s[0], "n_distinct": _num(s[1]), "null_frac": _num(s[2]),
                                      "correlation": _num(s[3])} for s in stats]})
    return out


def _numbers(evidence) -> dict:
    try:
        ev = json.loads(evidence) if isinstance(evidence, str) else (evidence or {})
    except ValueError:
        return {}
    return {k: v for k, v in sorted(ev.items()) if isinstance(v, (int, float)) and not isinstance(v, bool)} \
        if isinstance(ev, dict) else {}


def build_context(finding, normalized_query, ordered, plan, tables) -> dict:
    """The user message: finding numbers, the normalized query, the stripped plan and the tables. No values."""
    return {"finding": {"check_id": finding["check_id"], "measure": _num(finding.get("measure")),
                        "evidence": _numbers(finding.get("evidence"))},
            "query": normalized_query, "parameters": [f"${n}" for n in sorted(params(normalized_query))],
            "ordered": ordered, "plan": plan, "tables": tables}
```

`engine/src/lakebase_health/fixer/draft/prompts/rewrite_v1.md` (new file, complete):

```markdown
You are a PostgreSQL 17 query tuning assistant. You receive one slow query from a production database, the
health finding that flagged it, its slowest recent execution plan, and the tables it reads: columns,
constraints, indexes, row estimates and column statistics. Literal values have been removed: in the query
they are numbered parameters, in the plan and the table definitions they are "?".

Propose one rewritten query that returns exactly the same rows and columns, in the same order, for every
possible parameter value, and runs faster. Rules:
- One SELECT statement (WITH is allowed). No other statements, no comments, no hints, no SET.
- Read only the tables the original reads, with the same schema-qualified names.
- Keep every parameter of the original ($1, $2, ...) and add no new ones. Add no literal values other than 0
  and 1.
- Keep the same output columns, in the same order, with the same types.
- Preserve NULL handling, duplicate rows, DISTINCT, LIMIT and OFFSET. If "ordered" is true, the original has
  ORDER BY: keep an ORDER BY that produces the same order.
- Use only built-in, deterministic functions; nothing volatile or with side effects.
- Do not suggest indexes, settings or schema changes: only the query text may change.

Reply with JSON only, no prose and no code fence:
{"rewrite": "<the rewritten SQL>", "rationale": "<why it is faster, at most two sentences>",
 "expected_plan_change": "<the plan change you expect, with PostgreSQL node names such as SubPlan, Hash Join, HashAggregate, Seq Scan>"}
If no rewrite is both safe and faster, reply:
{"rewrite": null, "reason": "<why>"}
```

`engine/src/lakebase_health/fixer/draft/prompt.py` (new file, complete):

````python
"""The versioned rewrite prompt, the message list sent to the model, and parsing of its reply."""
import hashlib
import json
import re
from importlib.resources import files

PROMPT_VERSION = "rewrite_v1"
SYSTEM = files("lakebase_health.fixer.draft").joinpath("prompts", f"{PROMPT_VERSION}.md").read_text()
_FENCE = re.compile(r"^```(?:json)?\s*\n(.*)\n```$", re.S)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def messages(context: dict) -> list:
    return [{"role": "system", "content": SYSTEM},
            {"role": "user", "content": json.dumps(context, sort_keys=True, indent=1)}]


def prompt_hash(msgs: list) -> str:
    return _sha(json.dumps(msgs, sort_keys=True))


def context_hash(context: dict) -> str:
    return _sha(json.dumps(context, sort_keys=True))


def parse_response(text) -> dict:
    """{"ok": True, rewrite, rationale, expected_plan_change} or {"ok": False, "reason": invalid_response |
    no_safe_rewrite, "detail": str}. One surrounding ```json fence is tolerated; anything else must be JSON."""
    raw = (text or "").strip()
    m = _FENCE.match(raw)
    if m:
        raw = m.group(1).strip()
    try:
        doc = json.loads(raw)
    except ValueError:
        return {"ok": False, "reason": "invalid_response", "detail": "not JSON"}
    if not isinstance(doc, dict) or "rewrite" not in doc:
        return {"ok": False, "reason": "invalid_response", "detail": "no rewrite field"}
    if doc["rewrite"] is None:
        reason = doc.get("reason")
        return {"ok": False, "reason": "no_safe_rewrite", "detail": reason[:500] if isinstance(reason, str) else ""}
    if not isinstance(doc["rewrite"], str) or not doc["rewrite"].strip():
        return {"ok": False, "reason": "invalid_response", "detail": "rewrite is not text"}
    text_of = lambda k: doc.get(k)[:2000] if isinstance(doc.get(k), str) else ""
    return {"ok": True, "rewrite": doc["rewrite"].strip().rstrip(";").strip(), "rationale": text_of("rationale"),
            "expected_plan_change": text_of("expected_plan_change")}
````

`engine/src/lakebase_health/fixer/pg.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/fixer/pg.py
+++ b/engine/src/lakebase_health/fixer/pg.py
@@ -5,9 +5,13 @@ from ..executors.postgres import SEARCH_PATH_SQL
 from .allowlist import IDENT, parse
 
 LONG = {"create_index", "drop_index"}
+READ_ONLY = {"draft_context"}  # catalog reads for a rewrite's model context, on the target branch
 
 
 def FIXER_SETUP(kind: str) -> tuple:
+    if kind in READ_ONLY:
+        return (SEARCH_PATH_SQL, "SET default_transaction_read_only = on", "SET lock_timeout = '1s'",
+                "SET statement_timeout = '30s'")
     return (SEARCH_PATH_SQL, "SET lock_timeout = '5s'",
             f"SET statement_timeout = '{'2h' if kind in LONG else '10min'}'")
```

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_context.py tests/unit/test_draft_prompt.py tests/unit/test_fixer_pg.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/draft/states.py engine/src/lakebase_health/fixer/draft/context.py engine/src/lakebase_health/fixer/draft/prompt.py engine/src/lakebase_health/fixer/draft/prompts/rewrite_v1.md engine/src/lakebase_health/fixer/pg.py engine/tests/unit/test_draft_context.py engine/tests/unit/test_draft_prompt.py
git commit -m "feat(draft): literal-free model context, rewrite_v1 prompt and reply parsing

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: Model client, endpoint probe and the acceptance test switch

**Files:**
- Create: `engine/src/lakebase_health/fixer/draft/llm.py`
- Test: `engine/tests/unit/test_draft_llm.py`

**Interfaces:**
- Consumes: `static.check` (in the test only).
- Produces: `TIMEOUT_S = 120`, `RETRY_AFTER_S = 30`, `MAX_TOKENS = 2000`, `TEST_RESPONSES = ("unsafe", "non_equivalent")`; `ModelError(reason: "model_unavailable" | "not_set_up", status: str)`; `classify(e) -> (status, transient, not_set_up)`; `sdk_query(w) -> query(endpoint, msgs)`; `canned(name, msgs) -> str`; `ModelClient(query, endpoint, sleep=time.sleep, clock=time.monotonic, test_response="")` with `complete(msgs, draft_id, record) -> str`, where `record(row)` receives each `llm_calls` row (`called_at` is left `None` for the caller to fill); `probe(w, cfg, now) -> dict` (an `llm_status` row).

SDK facts checked in the engine venv (databricks-sdk 0.157): `ServingEndpointsAPI.query(self, name, *, messages: List[ChatMessage], max_tokens, temperature, ...) -> QueryEndpointResponse` with `choices[].message.content`, `usage.prompt_tokens` / `completion_tokens` and `id`; `ChatMessage(content, role: ChatMessageRole)`; `serving_endpoints.get(name) -> ServingEndpointDetailed` with `state.ready` (`READY` / `NOT_READY`) and `permission_level` (the caller's level). In the test workspace the hosted `databricks-claude-opus-5-5` endpoint is READY, has no `id` and no permissions object.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_draft_llm.py` (new file, complete):

```python
import json
from types import SimpleNamespace as NS

import pytest
from databricks.sdk.errors import InternalError, NotFound, PermissionDenied, TooManyRequests

from lakebase_health.fixer.draft.llm import RETRY_AFTER_S, ModelClient, ModelError, canned, probe, sdk_query
from lakebase_health.fixer.draft.static import StaticError, check

QUERY = "SELECT a FROM s.t WHERE x = $1 ORDER BY a"
MSGS = [{"role": "system", "content": "rules"},
        {"role": "user", "content": json.dumps({"query": QUERY, "tables": [{"name": "s.t"}]})}]


def reply(text):
    return NS(choices=[NS(message=NS(content=text))], usage=NS(prompt_tokens=812, completion_tokens=95), id="req-1")


def run(*outcomes):
    seq, rows, slept = iter(outcomes), [], []

    def query(endpoint, msgs):
        assert endpoint == "ep" and msgs is MSGS
        x = next(seq)
        if isinstance(x, BaseException):
            raise x
        return x
    client = ModelClient(query, "ep", sleep=slept.append, clock=iter(range(0, 1000, 2)).__next__)
    try:
        out = client.complete(MSGS, "d1", rows.append)
    except ModelError as e:
        out = e
    return out, rows, slept


def test_one_call_one_row_with_hashes_and_tokens_only():
    out, rows, slept = run(reply('{"rewrite": null, "reason": "no"}'))
    assert out == '{"rewrite": null, "reason": "no"}' and slept == []
    [row] = rows
    assert row["status"] == "ok" and row["input_tokens"] == 812 and row["output_tokens"] == 95
    assert row["request_id"] == "req-1" and row["draft_id"] == "d1" and row["endpoint"] == "ep"
    assert len(row["prompt_hash"]) == 64 and len(row["response_hash"]) == 64 and row["latency_ms"] == 2000
    assert "rewrite" not in json.dumps(row) and QUERY not in json.dumps(row)


@pytest.mark.parametrize("first,status", [(TooManyRequests("slow down"), "429"), (InternalError("boom"), "5xx"),
                                          (TimeoutError("read timed out"), "timeout")])
def test_timeout_429_and_5xx_retry_once_after_30_s(first, status):
    out, rows, slept = run(first, reply("{}"))
    assert out == "{}" and slept == [RETRY_AFTER_S] and [r["status"] for r in rows] == [status, "ok"]
    out, rows, slept = run(first, first)
    assert isinstance(out, ModelError) and out.reason == "model_unavailable" and slept == [RETRY_AFTER_S]
    assert [r["status"] for r in rows] == [status, status] and rows[0]["response_hash"] is None


def test_permission_and_missing_endpoint_mean_not_set_up_without_retry():
    for e, status in ((PermissionDenied("no"), "403"), (NotFound("gone"), "404")):
        out, rows, slept = run(e)
        assert out.reason == "not_set_up" and out.status == status and slept == [] and len(rows) == 1


def test_other_errors_are_model_unavailable_without_retry():
    out, rows, slept = run(ValueError("bad request"))
    assert out.reason == "model_unavailable" and out.status == "error:ValueError" and slept == []


def test_sdk_query_sends_temperature_0_and_2000_tokens():
    seen = {}
    w = NS(serving_endpoints=NS(query=lambda **kw: seen.update(kw) or reply("{}")))
    sdk_query(w)("ep", MSGS)
    assert seen["name"] == "ep" and seen["temperature"] == 0 and seen["max_tokens"] == 2000
    assert [(m.role.value, m.content) for m in seen["messages"]] == [("system", "rules"), ("user", MSGS[1]["content"])]


def test_the_test_switch_never_calls_the_endpoint_and_is_caught_downstream():
    rows = []
    unsafe = ModelClient(None, "ep", test_response="unsafe").complete(MSGS, "d1", rows.append)
    assert rows[0]["status"] == "test:unsafe" and rows[0]["input_tokens"] == 0
    with pytest.raises(StaticError) as e:
        check(QUERY, json.loads(unsafe)["rewrite"])
    assert e.value.check == "data_modifying_cte"
    wrong = json.loads(canned("non_equivalent", MSGS))["rewrite"]
    assert check(QUERY, wrong).columns is None  # passes static checks; the branch's equivalence rule fails it
    with pytest.raises(ValueError):
        canned("other", MSGS)


def endpoint(ready="READY", level="CAN_QUERY"):
    return NS(state=NS(ready=NS(value=ready)), permission_level=None if level is None else NS(value=level))


@pytest.mark.parametrize("get,ready,reason", [
    (lambda n: endpoint(), True, None),
    (lambda n: endpoint(level=None), True, None),
    (lambda n: endpoint(ready="NOT_READY"), False, "serving endpoint ep is NOT_READY"),
    (lambda n: endpoint(level="CAN_VIEW"), False, "the fixer SP has CAN_VIEW on ep; it needs CAN_QUERY"),
    (lambda n: (_ for _ in ()).throw(NotFound("x")), False, "serving endpoint ep not found"),
    (lambda n: (_ for _ in ()).throw(PermissionDenied("x")), False, "the fixer SP may not use ep (needs CAN_QUERY)"),
])
def test_probe_reports_why_drafting_is_not_set_up(get, ready, reason):
    row = probe(NS(serving_endpoints=NS(get=get)), NS(llm_endpoint="ep", llm_enabled=True), "now")
    assert (row["ready"], row["reason"], row["checked_at"], row["endpoint"]) == (ready, reason, "now", "ep")


def test_probe_when_turned_off_does_not_call_the_endpoint():
    row = probe(None, NS(llm_endpoint="ep", llm_enabled=False), "now")
    assert row["ready"] is False and row["enabled"] is False and "llm_enabled" in row["reason"]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_llm.py -v`
Expected: FAIL with `ModuleNotFoundError ... llm`.

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/fixer/draft/llm.py` (new file, complete):

```python
"""The model call: one retry after 30 s on timeout, 429 or 5xx; every HTTP call is one llm_calls row with hashes
and token counts only. Also the endpoint set-up probe and the acceptance test switch."""
import hashlib
import json
import time
import uuid

TIMEOUT_S = 120
RETRY_AFTER_S = 30
MAX_TOKENS = 2000
TEST_RESPONSES = ("unsafe", "non_equivalent")


class ModelError(Exception):
    def __init__(self, reason: str, status: str):
        super().__init__(f"{reason} ({status})")
        self.reason, self.status = reason, status


def _sha(text) -> str | None:
    return None if text is None else hashlib.sha256(text.encode()).hexdigest()


def classify(e) -> tuple:
    """(status, transient, not_set_up) for an exception raised by the serving call."""
    from databricks.sdk import errors as E
    if isinstance(e, (E.PermissionDenied, E.Unauthenticated)):
        return "403", False, True
    if isinstance(e, E.NotFound):
        return "404", False, True
    if isinstance(e, (E.TooManyRequests, E.ResourceExhausted, E.RequestLimitExceeded)):
        return "429", True, False
    if isinstance(e, (E.InternalError, E.TemporarilyUnavailable, E.DeadlineExceeded, E.Unknown)):
        return "5xx", True, False
    if isinstance(e, TimeoutError) or "Timeout" in type(e).__name__:
        return "timeout", True, False
    return f"error:{type(e).__name__}", False, False


def _reply(resp) -> tuple:
    choice = (resp.choices or [None])[0]
    content = getattr(getattr(choice, "message", None), "content", None)
    if isinstance(content, list):  # content blocks
        content = "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in content)
    usage = resp.usage
    return (content or "", getattr(usage, "prompt_tokens", None), getattr(usage, "completion_tokens", None),
            getattr(resp, "id", None))


def sdk_query(w):
    """The serving call through the SDK. `w` should carry http_timeout_seconds=TIMEOUT_S (see jobs/fix.py)."""
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

    def query(endpoint, msgs):
        return w.serving_endpoints.query(name=endpoint, temperature=0, max_tokens=MAX_TOKENS,
                                         messages=[ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"])
                                                   for m in msgs])
    return query


def canned(name: str, msgs: list) -> str:
    """Acceptance test switch (--llm-test-response): a fixed bad reply built from the normalized query."""
    ctx = json.loads(msgs[-1]["content"])
    q = ctx["query"]
    if name == "unsafe":
        table = ctx["tables"][0]["name"] if ctx.get("tables") else "pg_catalog.pg_class"
        cte = f"lbh_gone AS (DELETE FROM {table} RETURNING 1)"
        rewrite = f"WITH {cte}, {q.lstrip()[4:].lstrip()}" if q.lstrip().lower().startswith("with") else f"WITH {cte} {q}"
    elif name == "non_equivalent":
        rewrite = f"SELECT * FROM ({q}) AS lbh_a EXCEPT ALL SELECT * FROM ({q}) AS lbh_b"
    else:
        raise ValueError(f"unknown test response {name!r}; use one of {TEST_RESPONSES}")
    return json.dumps({"rewrite": rewrite, "rationale": f"test response {name}", "expected_plan_change": "SetOp"})


class ModelClient:
    def __init__(self, query, endpoint: str, sleep=time.sleep, clock=time.monotonic, test_response: str = ""):
        self.query, self.endpoint, self.sleep, self.clock, self.test_response = query, endpoint, sleep, clock, test_response

    def _row(self, draft_id, p_hash, content, tokens_in, tokens_out, ms, status, request_id):
        return {"call_id": uuid.uuid4().hex, "draft_id": draft_id, "endpoint": self.endpoint, "prompt_hash": p_hash,
                "response_hash": _sha(content), "input_tokens": tokens_in, "output_tokens": tokens_out,
                "latency_ms": ms, "status": status, "request_id": request_id, "called_at": None}

    def complete(self, msgs: list, draft_id: str, record) -> str:
        """The reply text. `record(row)` stores each llm_calls row (called_at is filled by the caller)."""
        p_hash = hashlib.sha256(json.dumps(msgs, sort_keys=True).encode()).hexdigest()
        if self.test_response:
            content = canned(self.test_response, msgs)
            record(self._row(draft_id, p_hash, content, 0, 0, 0.0, f"test:{self.test_response}", None))
            return content
        for attempt in (1, 2):
            start = self.clock()
            try:
                resp = self.query(self.endpoint, msgs)
            except Exception as e:
                status, transient, not_set_up = classify(e)
                record(self._row(draft_id, p_hash, None, None, None, (self.clock() - start) * 1000, status, None))
                if not_set_up:
                    raise ModelError("not_set_up", status) from e
                if transient and attempt == 1:
                    self.sleep(RETRY_AFTER_S)
                    continue
                raise ModelError("model_unavailable", status) from e
            content, tin, tout, rid = _reply(resp)
            record(self._row(draft_id, p_hash, content, tin, tout, (self.clock() - start) * 1000, "ok", rid))
            return content
        raise AssertionError("unreachable")


def probe(w, cfg, now) -> dict:
    """The llm_status row: whether drafting can run (enabled, endpoint present and READY, fixer may query it)."""
    from databricks.sdk import errors as E
    row = {"endpoint": cfg.llm_endpoint, "enabled": cfg.llm_enabled, "ready": False, "reason": None, "checked_at": now}
    if not cfg.llm_enabled:
        return {**row, "reason": "turned off (llm_enabled is false)"}
    try:
        ep = w.serving_endpoints.get(cfg.llm_endpoint)
    except E.NotFound:
        return {**row, "reason": f"serving endpoint {cfg.llm_endpoint} not found"}
    except (E.PermissionDenied, E.Unauthenticated):
        return {**row, "reason": f"the fixer SP may not use {cfg.llm_endpoint} (needs CAN_QUERY)"}
    except Exception as e:
        return {**row, "reason": f"could not check {cfg.llm_endpoint}: {type(e).__name__}"}
    ready = getattr(getattr(ep.state, "ready", None), "value", getattr(ep.state, "ready", None))
    if ready != "READY":
        return {**row, "reason": f"serving endpoint {cfg.llm_endpoint} is {ready or 'not ready'}"}
    level = getattr(ep.permission_level, "value", ep.permission_level)
    if level not in (None, "CAN_QUERY", "CAN_MANAGE"):
        return {**row, "reason": f"the fixer SP has {level} on {cfg.llm_endpoint}; it needs CAN_QUERY"}
    return {**row, "ready": True}
```

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_llm.py -v`
Expected: 15 passed.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/draft/llm.py engine/tests/unit/test_draft_llm.py
git commit -m "feat(draft): serving-endpoint client with one retry, hash-only call rows, set-up probe, test switch

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: Branch verification rules for a rewrite

**Files:**
- Create: `engine/src/lakebase_health/fixer/draft/verify.py`
- Test: `engine/tests/unit/test_draft_verify.py`

**Interfaces:**
- Consumes: `fixer.verify.SPEEDUP` (1.2), `SLOWDOWN` (1.10), `MIN_INCREASE_MS` (1.0), `_walk` (existing).
- Produces: `checksum(conn, q, values, ordered) -> {"rows": int, "checksum": str}` or `{"rows": None, "checksum": None, "error": <type name>}` (read-only transaction, `PREPARE lbh_eq`, always cleaned up); `plan_shape(conn, q, values) -> {"nodes": list[str], "queryid": str | None}` (`EXPLAIN (VERBOSE, FORMAT JSON)`, not executed; aggregates labelled `HashAggregate` / `GroupAggregate`; `SubPlan` / `InitPlan` from `Parent Relationship`); `mentioned(text) -> list[str]`; `judge(sets, ordered, before, after, expected) -> {"passed": bool, "rules": [equivalence, speed, plan], "plan": dict}`, where each set is `{"set": i, "original": <checksum>, "rewrite": <checksum>, "original_ms", "rewrite_ms"}`.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_draft_verify.py` (new file, complete):

```python
from types import SimpleNamespace

from lakebase_health.fixer.draft.verify import checksum, judge, mentioned, plan_shape

B = {"nodes": ["Aggregate", "Index Scan", "SubPlan"]}
A = {"nodes": ["Hash Join", "HashAggregate", "Index Scan"]}


def s(i, before_ms, after_ms, original=(10, "c1"), rewrite=(10, "c1")):
    return {"set": i, "original": {"rows": original[0], "checksum": original[1]},
            "rewrite": {"rows": rewrite[0], "checksum": rewrite[1]}, "original_ms": before_ms, "rewrite_ms": after_ms}


def rule(r, name):
    return next(x for x in r["rules"] if x["rule"] == name)


def test_verified_when_equivalent_faster_and_the_plan_changed_as_described():
    r = judge([s(1, 500, 2), s(2, 480, 2.5), s(3, 0.5, 1.2)], True, B, A,
              "The SubPlan becomes a Hash Join over a HashAggregate")
    assert r["passed"], r
    assert rule(r, "equivalence")["detail"] == "3 of 3 parameter sets: same rows in order"
    assert rule(r, "speed")["detail"].startswith("median 480 -> 2 ms, 240.00x faster")
    assert r["plan"]["removed"] == ["Aggregate", "SubPlan"] and r["plan"]["added"] == ["Hash Join", "HashAggregate"]
    assert rule(r, "plan")["warning"] is False


def test_equivalence_is_order_sensitive_only_for_ordered_queries():
    r = judge([s(1, 10, 5, rewrite=(9, "x")), s(2, 10, 5, rewrite=(10, "x"))], True, B, A, "Hash Join")
    assert not r["passed"]
    assert rule(r, "equivalence")["detail"] == "set 1: 10 rows vs 9; set 2: same row count, different rows or order"
    unordered = judge([s(1, 10, 5)], False, B, A, "Hash Join")
    assert rule(unordered, "equivalence")["detail"] == "1 of 1 parameter sets: same rows in any order"


def test_a_failing_query_fails_equivalence():
    bad = s(1, 10, None)
    bad["rewrite"] = {"rows": None, "checksum": None, "error": "UndefinedColumn"}
    r = judge([bad], True, B, A, "Hash Join")
    assert rule(r, "equivalence")["detail"] == "set 1: rewrite failed (UndefinedColumn)"
    assert not rule(r, "speed")["passed"]


def test_speed_needs_1_2x_on_the_median_and_no_set_slower_past_the_noise_floor():
    assert not rule(judge([s(1, 10, 8.4)], True, B, A, ""), "speed")["passed"]  # 1.19x
    assert rule(judge([s(1, 10, 8.3)], True, B, A, ""), "speed")["passed"]
    slow = judge([s(1, 100, 10), s(2, 100, 10), s(3, 10, 12)], True, B, A, "")
    assert not rule(slow, "speed")["passed"] and "slower: set 3 10 -> 12 ms" in rule(slow, "speed")["detail"]
    noise = judge([s(1, 100, 10), s(2, 100, 10), s(3, 0.5, 1.2)], True, B, A, "")  # 2.4x but +0.7 ms
    assert rule(noise, "speed")["passed"]
    edge = judge([s(1, 100, 10), s(2, 100, 10), s(3, 20, 22)], True, B, A, "")  # exactly 1.10x is not > 1.10
    assert rule(edge, "speed")["passed"]


def test_an_unexpected_plan_change_is_a_warning_not_a_failure():
    r = judge([s(1, 10, 5)], True, B, B, "Hash Join")
    p = rule(r, "plan")
    assert r["passed"] and p["passed"] and p["warning"]
    assert p["detail"] == ("removed nothing; added nothing; expected Hash Join "
                           "(warning: the expected change is not in the new plan)")


def test_node_names_are_read_longest_first():
    assert mentioned("Hash Join instead of a Nested Loop, plus a Hash node") == ["Hash", "Hash Join", "Nested Loop"]
    assert mentioned("HashAggregate replaces the SubPlan") == ["HashAggregate", "SubPlan"]
    assert mentioned(None) == []


class Conn:
    def __init__(self, row=None, plan=None, fail_on=None):
        self.log, self.row, self.plan, self.fail_on = [], row, plan, fail_on

    def execute(self, stmt):
        self.log.append(stmt)
        if self.fail_on and self.fail_on in stmt:
            raise RuntimeError("relation does not exist: 4111")
        if stmt.startswith("EXPLAIN"):
            return SimpleNamespace(fetchone=lambda: [[self.plan]])
        return SimpleNamespace(fetchone=lambda: self.row)


def literal(monkeypatch):
    from psycopg import sql as pgsql
    monkeypatch.setattr(pgsql.Literal, "as_string", lambda self, c: "'" + str(self._obj) + "'")


def test_checksum_runs_read_only_with_the_sampled_values_and_cleans_up(monkeypatch):
    literal(monkeypatch)
    conn = Conn(row=(1803, "ab12"))
    assert checksum(conn, "SELECT a FROM t WHERE x = $1 ORDER BY a", ["7"], True) == {"rows": 1803, "checksum": "ab12"}
    assert conn.log[0] == "BEGIN READ ONLY" and conn.log[-2:] == ["ROLLBACK", "DEALLOCATE ALL"]
    assert "string_agg(h, ',' ORDER BY n)" in conn.log[1] and "row_number() OVER ()" in conn.log[1]
    assert conn.log[2] == "EXECUTE lbh_eq('7')"
    unordered = Conn(row=(3, "123"))
    checksum(unordered, "SELECT a FROM t", [], False)
    assert "sum(('x' || substr(md5(t::text), 1, 15))::bit(60)::bigint)" in unordered.log[1]
    assert unordered.log[2] == "EXECUTE lbh_eq"


def test_checksum_errors_are_recorded_by_type_only(monkeypatch):
    literal(monkeypatch)
    conn = Conn(fail_on="EXECUTE")
    assert checksum(conn, "SELECT 1", [], True) == {"rows": None, "checksum": None, "error": "RuntimeError"}
    assert conn.log[-2:] == ["ROLLBACK", "DEALLOCATE ALL"]


def test_plan_shape_labels_nodes_and_reads_the_query_identifier(monkeypatch):
    literal(monkeypatch)
    plan = {"Query Identifier": -4711, "Plan": {"Node Type": "Index Scan", "Plans": [
        {"Node Type": "Aggregate", "Strategy": "Plain", "Parent Relationship": "SubPlan"},
        {"Node Type": "Aggregate", "Strategy": "Hashed", "Parent Relationship": "InitPlan"}]}}
    conn = Conn(plan=plan)
    assert plan_shape(conn, "SELECT 1", []) == {"nodes": ["Aggregate", "HashAggregate", "Index Scan", "InitPlan",
                                                          "SubPlan"], "queryid": "-4711"}
    assert conn.log[2] == "EXPLAIN (VERBOSE, FORMAT JSON) EXECUTE lbh_pl" and conn.log[0] == "BEGIN READ ONLY"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_verify.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/fixer/draft/verify.py` (new file, complete):

```python
"""Branch verification of a rewrite: result equivalence by checksum, speed with the fixer's noise floor, and the
plan change (reported; a mismatch with the model's expectation is a warning, never a failure)."""
import statistics

from ..verify import MIN_INCREASE_MS, SLOWDOWN, SPEEDUP, _walk

# PostgreSQL plan node labels, as EXPLAIN (FORMAT JSON) names them; aggregates by strategy; sub/init plans.
NODE_NAMES = ("Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Bitmap Index Scan", "Tid Scan",
              "Subquery Scan", "Function Scan", "Values Scan", "CTE Scan", "WorkTable Scan", "Nested Loop",
              "Merge Join", "Hash Join", "Hash", "Materialize", "Memoize", "Sort", "Incremental Sort",
              "HashAggregate", "GroupAggregate", "MixedAggregate", "Aggregate", "WindowAgg", "Unique", "SetOp",
              "Append", "Merge Append", "Gather", "Gather Merge", "Limit", "Result", "SubPlan", "InitPlan")
_AGG = {"Hashed": "HashAggregate", "Sorted": "GroupAggregate", "Mixed": "MixedAggregate"}


def _args(conn, values) -> str:
    from psycopg import sql as pgsql
    return ", ".join(pgsql.Literal(v).as_string(conn) for v in values)


def _execute(name, conn, values) -> str:
    return f"EXECUTE {name}({_args(conn, values)})" if values else f"EXECUTE {name}"


def _read_only(conn, name, stmt, values, fn):
    """PREPAREs `stmt` as `name` in a read-only transaction, runs fn(cursor-producing EXECUTE text), always cleans up."""
    conn.execute("BEGIN READ ONLY")
    try:
        conn.execute(f"PREPARE {name} AS {stmt}")
        return fn(_execute(name, conn, values))
    finally:
        try:
            conn.execute("ROLLBACK")
        finally:
            conn.execute("DEALLOCATE ALL")


def checksum(conn, q, values, ordered) -> dict:
    """Row count and checksum of q's result: in result order when `ordered`, else as a multiset."""
    if ordered:
        stmt = ("SELECT count(*), md5(coalesce(string_agg(h, ',' ORDER BY n), '')) FROM "
                f"(SELECT md5(t::text) AS h, row_number() OVER () AS n FROM ({q}) AS t) AS s")
    else:
        stmt = ("SELECT count(*), coalesce(sum(('x' || substr(md5(t::text), 1, 15))::bit(60)::bigint), 0)::text "
                f"FROM ({q}) AS t")
    try:
        row = _read_only(conn, "lbh_eq", stmt, values, lambda ex: conn.execute(ex).fetchone())
        return {"rows": int(row[0]), "checksum": str(row[1])}
    except Exception as e:  # by type only: messages can carry values
        return {"rows": None, "checksum": None, "error": type(e).__name__}


def plan_shape(conn, q, values) -> dict:
    """Node labels of q's plan (EXPLAIN VERBOSE, not executed) and its query identifier when Postgres computes one."""
    try:
        doc = _read_only(conn, "lbh_pl", q, values,
                         lambda ex: conn.execute(f"EXPLAIN (VERBOSE, FORMAT JSON) {ex}").fetchone()[0][0])
    except Exception as e:
        return {"nodes": [], "queryid": None, "error": type(e).__name__}
    nodes = set()
    for n in _walk(doc["Plan"]):
        t = n["Node Type"]
        nodes.add(_AGG.get(n.get("Strategy"), t) if t == "Aggregate" else t)
        if n.get("Parent Relationship") in ("SubPlan", "InitPlan"):
            nodes.add(n["Parent Relationship"])
    qid = doc.get("Query Identifier")
    return {"nodes": sorted(nodes), "queryid": None if qid is None else str(qid)}


def mentioned(text) -> list:
    """Plan node labels named in free text, longest first so "Hash Join" is not also read as "Hash"."""
    t, found = (text or "").lower(), []
    for name in sorted(NODE_NAMES, key=len, reverse=True):
        if name.lower() in t:
            found.append(name)
            t = t.replace(name.lower(), " ")
    return sorted(found)


def _equivalence(sets, ordered) -> dict:
    bad = []
    for s in sets:
        o, r = s["original"], s["rewrite"]
        if o.get("error") or r.get("error"):
            bad.append(f"set {s['set']}: {'original' if o.get('error') else 'rewrite'} failed "
                       f"({o.get('error') or r.get('error')})")
        elif o["rows"] != r["rows"]:
            bad.append(f"set {s['set']}: {o['rows']} rows vs {r['rows']}")
        elif o["checksum"] != r["checksum"]:
            bad.append(f"set {s['set']}: same row count, different {'rows or order' if ordered else 'rows'}")
    how = "in order" if ordered else "in any order"
    return {"rule": "equivalence", "passed": bool(sets) and not bad,
            "detail": "; ".join(bad) if bad else f"{len(sets)} of {len(sets)} parameter sets: same rows {how}"}


def _speed(sets) -> dict:
    """Median across sets >= 1.2x faster; no set both > 10 % and > 1 ms slower (the fixer's noise floor)."""
    pairs = [(s["original_ms"], s["rewrite_ms"]) for s in sets]
    if not pairs or any(o is None or r is None for o, r in pairs):
        return {"rule": "speed", "passed": False, "detail": "a parameter set could not be timed"}
    b, a = statistics.median(o for o, _ in pairs), statistics.median(r for _, r in pairs)
    speedup = b / a if a > 0 else float("inf")
    slower = [f"set {s['set']} {o:g} -> {r:g} ms" for s, (o, r) in zip(sets, pairs)
              if r > o * SLOWDOWN and r - o > MIN_INCREASE_MS]
    detail = f"median {b:g} -> {a:g} ms, {speedup:.2f}x faster (needs {SPEEDUP}x)"
    if slower:
        detail += f"; slower: {'; '.join(slower)}"
    return {"rule": "speed", "passed": speedup >= SPEEDUP and not slower, "detail": detail}


def _plan(before, after, expected) -> tuple:
    b, a = set(before.get("nodes") or []), set(after.get("nodes") or [])
    removed, added = sorted(b - a), sorted(a - b)
    want = mentioned(expected)
    found = [n for n in want if n in added or n in removed]
    warning = not found
    detail = (f"removed {', '.join(removed) or 'nothing'}; added {', '.join(added) or 'nothing'}; "
              f"expected {', '.join(want) or 'no named plan node'}")
    if warning:
        detail += " (warning: the expected change is not in the new plan)"
    if before.get("error") or after.get("error"):
        detail += f"; EXPLAIN failed: {before.get('error') or after.get('error')}"
    plan = {"original_nodes": sorted(b), "rewrite_nodes": sorted(a), "removed": removed, "added": added,
            "expected_nodes": want, "found": found}
    return {"rule": "plan", "passed": True, "warning": warning, "detail": detail}, plan


def judge(sets, ordered, before, after, expected) -> dict:
    plan_rule, plan = _plan(before, after, expected)
    rules = [_equivalence(sets, ordered), _speed(sets), plan_rule]
    return {"passed": all(r["passed"] for r in rules), "rules": rules, "plan": plan}
```

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_verify.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/draft/verify.py engine/tests/unit/test_draft_verify.py
git commit -m "feat(draft): checksum equivalence, speed rule with the fixer noise floor, plan-node comparison

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: Draft queue, staleness and rows

**Files:**
- Create: `engine/src/lakebase_health/fixer/draft/store.py`
- Test: `engine/tests/unit/test_draft_store.py`

**Interfaces:**
- Consumes: `states.RUNNING`, `ADOPTION_WINDOW`, `draft_token`, `finding_queryid` (Task 4).
- Produces: `DraftWork(approval: dict, finding: dict, last_attempt: int, last_state: str | None)`; `pending_drafts(sql, cfg) -> list[DraftWork]`; `staleness(work, ready: bool) -> str | None` (`not_set_up`, `finding_resolved`, `finding_superseded`, `not_rewritable`, `in_progress`, `attempt_changed`); `new_draft(work, cfg, now) -> dict` (a full `drafts` row in state `drafting`); `rewrite_action(draft, finding, sql_text, now) -> dict` (a full `proposed_actions` row: `action_id = draft_id`, state `verifying`, `origin = "llm"`); `save_draft`, `save_action`, `write_status` (merge on the key); `record_llm_call` (insert); `stuck_drafts(sql, cfg, now) -> list[dict]`; `STUCK_AFTER` (4 h 15 min); `ADVISORY_NOTE`.

Rows are written whole with `merge_rows` (Spark `MERGE … UPDATE SET *`), so the job keeps each row in memory and merges it again after every state change; no partial write can null a column.

- [ ] **Step 1: Write the failing test**

`engine/tests/unit/test_draft_store.py` (new file, complete):

```python
from datetime import datetime, timedelta, timezone

from lakebase_health.config import EngineConfig
from lakebase_health.fixer.draft.states import draft_token
from lakebase_health.fixer.draft.store import (DraftWork, new_draft, pending_drafts, rewrite_action, staleness,
                                               stuck_drafts)
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
NOW = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
FP = "f" * 32


def work(attempt=1, last=0, last_state=None, status="open", check="top_db_time_query"):
    return DraftWork({"approval_id": "ap1", "fingerprint": FP, "sql_hash": draft_token(FP, attempt),
                      "approver": "owner@x", "requested_at": NOW},
                     {"fingerprint": FP, "status": status, "check_id": check, "project_id": "p1", "branch_id": "b1",
                      "endpoint_id": "ep-a", "object": "42", "evidence": '{"queryid": "42"}', "measure": 60.0},
                     last, last_state)


def test_pending_drafts_reads_unconsumed_draft_approvals_with_the_latest_attempt():
    row = {"approval_id": "ap1", "fingerprint": FP, "sql_hash": "h", "approver": "owner@x", "requested_at": NOW,
           "finding_status": "open", "check_id": "plan_flip", "project_id": "p1", "branch_id": "b1",
           "endpoint_id": "ep-a", "object": "42", "evidence": "{}", "measure": 3.0, "last_attempt": 2,
           "last_state": "verify_failed"}
    sql = FakeSql([(r"FROM cat\.lakebase_health\.approvals ap", [row])])
    [w] = pending_drafts(sql, CFG)
    assert w.approval["approval_id"] == "ap1" and w.finding["status"] == "open" and w.finding["check_id"] == "plan_flip"
    assert (w.last_attempt, w.last_state) == (2, "verify_failed")
    stmt = sql.statements[0][0]
    assert "ap.request = 'draft'" in stmt and "FROM cat.lakebase_health.action_runs WHERE approval_id IS NOT NULL" in stmt
    assert "max_by(state, attempt)" in stmt


def test_staleness_rules():
    assert staleness(work(), True) is None
    assert staleness(work(attempt=3, last=2, last_state="verified"), True) is None  # Retry after a final attempt
    assert staleness(work(), False) == "not_set_up"
    assert staleness(work(status="resolved"), True) == "finding_resolved"
    assert staleness(work(status="superseded"), True) == "finding_superseded"
    assert staleness(work(status=None), True) == "finding_resolved"  # the finding row is gone
    assert staleness(work(check="missing_fk_index"), True) == "not_rewritable"
    assert staleness(work(attempt=2, last=1, last_state="verifying"), True) == "in_progress"
    assert staleness(work(attempt=1, last=1, last_state="draft_failed"), True) == "attempt_changed"  # double click


def test_new_draft_and_its_advisory_action_share_the_id():
    d = new_draft(work(attempt=3, last=2, last_state="verified"), CFG, NOW)
    assert d["attempt"] == 3 and d["state"] == "drafting" and d["endpoint"] == "databricks-claude-opus-5-5"
    assert d["requested_by"] == "owner@x" and d["approval_id"] == "ap1" and len(d["draft_id"]) == 32
    a = rewrite_action(d, work().finding, "SELECT 1", NOW)
    assert a["action_id"] == a["draft_id"] == d["draft_id"] and a["origin"] == "llm" and a["kind"] == "query_rewrite"
    assert a["state"] == "verifying" and a["rollback_sql"] is None and "application code" in a["rollback_note"]
    assert a["expires_at"] == NOW + timedelta(days=7)


def test_stuck_drafts_are_the_running_ones_older_than_the_branch_ttl():
    rows = [{"draft_id": "old", "state": "drafting", "updated_at": (NOW - timedelta(hours=5)).replace(tzinfo=None)},
            {"draft_id": "new", "state": "verifying", "updated_at": NOW - timedelta(hours=1)}]
    sql = FakeSql([(r"FROM cat\.lakebase_health\.drafts WHERE state IN \('drafting', 'verifying'\)", rows)])
    assert [r["draft_id"] for r in stuck_drafts(sql, CFG, NOW)] == ["old"]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_store.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/fixer/draft/store.py` (new file, complete):

```python
"""Draft approvals, draft and model-call rows, the rewrite action and the endpoint status, in the Delta tables."""
import uuid
from dataclasses import dataclass
from datetime import timedelta, timezone

from .states import ADOPTION_WINDOW, RUNNING, draft_token, finding_queryid

STUCK_AFTER = timedelta(hours=4, minutes=15)  # the verification branch's TTL (4 h) plus a margin
ADVISORY_NOTE = "Advisory: change the query in your application code; this tool does not deploy rewrites."
_APPROVAL = ("approval_id", "fingerprint", "sql_hash", "approver", "requested_at")
_FINDING = ("check_id", "project_id", "branch_id", "endpoint_id", "object", "evidence", "measure")


@dataclass
class DraftWork:
    approval: dict
    finding: dict          # fingerprint, status and the columns in _FINDING
    last_attempt: int      # 0 before the first attempt
    last_state: str | None


def _utc(dt):
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def pending_drafts(sql, cfg) -> list:
    """Draft approvals no run has consumed yet, oldest first, with the finding and its latest attempt."""
    rows = sql.query(f"""
        SELECT ap.approval_id, ap.fingerprint, ap.sql_hash, ap.approver, ap.requested_at,
               f.status AS finding_status, f.check_id, f.project_id, f.branch_id, f.endpoint_id, f.object,
               f.evidence, f.measure, coalesce(d.last_attempt, 0) AS last_attempt, d.last_state
        FROM {cfg.table('approvals')} ap
        LEFT JOIN {cfg.table('findings')} f ON f.fingerprint = ap.fingerprint
        LEFT JOIN (SELECT finding_fingerprint, max(attempt) AS last_attempt, max_by(state, attempt) AS last_state
                   FROM {cfg.table('drafts')} GROUP BY finding_fingerprint) d ON d.finding_fingerprint = ap.fingerprint
        WHERE ap.request = 'draft'
          AND ap.approval_id NOT IN (SELECT approval_id FROM {cfg.table('action_runs')} WHERE approval_id IS NOT NULL)
        ORDER BY ap.requested_at""")
    return [DraftWork({k: r.get(k) for k in _APPROVAL},
                      {"fingerprint": r.get("fingerprint"), "status": r.get("finding_status"),
                       **{k: r.get(k) for k in _FINDING}},
                      int(r.get("last_attempt") or 0), r.get("last_state")) for r in rows]


def staleness(work: DraftWork, ready: bool) -> str | None:
    """Why a draft approval is consumed without drafting, or None."""
    f = work.finding
    if not ready:
        return "not_set_up"
    if f.get("status") not in ("new", "open"):
        return "finding_superseded" if f.get("status") == "superseded" else "finding_resolved"
    if finding_queryid(f.get("check_id"), f.get("evidence"), f.get("object")) is None:
        return "not_rewritable"
    if work.last_state in RUNNING:
        return "in_progress"
    if work.approval.get("sql_hash") != draft_token(f["fingerprint"], work.last_attempt + 1):
        return "attempt_changed"  # e.g. a second click: the attempt it asked for already exists
    return None


def new_draft(work: DraftWork, cfg, now) -> dict:
    return {"draft_id": uuid.uuid4().hex, "finding_fingerprint": work.finding["fingerprint"],
            "attempt": work.last_attempt + 1, "action_id": None, "approval_id": work.approval["approval_id"],
            "endpoint": cfg.llm_endpoint, "prompt_version": None, "context_hash": None, "rationale": None,
            "expected_plan_change": None, "state": "drafting", "reason": None,
            "requested_by": work.approval.get("approver"), "requested_at": work.approval.get("requested_at"),
            "updated_at": now, "ended_at": None}


def rewrite_action(draft: dict, finding: dict, sql_text: str, now) -> dict:
    """The advisory action: action_id is the draft id, so draft and verification events share one ledger chain."""
    return {"action_id": draft["draft_id"], "fingerprint": finding["fingerprint"], "check_id": finding["check_id"],
            "kind": "query_rewrite", "sql": sql_text, "rollback_sql": None, "risk": "low", "reversible": True,
            "verify_on_branch": True, "state": "verifying", "created_at": now, "updated_at": now,
            "expires_at": now + ADOPTION_WINDOW, "rollback_note": ADVISORY_NOTE, "origin": "llm",
            "draft_id": draft["draft_id"]}


def save_draft(sql, cfg, row: dict) -> None:
    sql.merge_rows(cfg.table("drafts"), [row], ["draft_id"])


def save_action(sql, cfg, row: dict) -> None:
    sql.merge_rows(cfg.table("proposed_actions"), [row], ["action_id"])


def record_llm_call(sql, cfg, row: dict) -> None:
    sql.insert_rows(cfg.table("llm_calls"), [row])


def write_status(sql, cfg, row: dict) -> None:
    sql.merge_rows(cfg.table("llm_status"), [row], ["endpoint"])


def stuck_drafts(sql, cfg, now) -> list:
    rows = sql.query(f"SELECT * FROM {cfg.table('drafts')} WHERE state IN ('drafting', 'verifying')")
    return [r for r in rows if r.get("updated_at") and now - _utc(r["updated_at"]) > STUCK_AFTER]
```

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_store.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/draft/store.py engine/tests/unit/test_draft_store.py
git commit -m "feat(draft): pending draft approvals, attempt-token staleness, draft and rewrite-action rows

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: Drafting in the fixer job

**Files:**
- Modify: `engine/src/lakebase_health/jobs/fix.py`, `engine/src/lakebase_health/fixer/queue.py`
- Test: `engine/tests/unit/test_draft_job.py` (create), `engine/tests/unit/test_fix_job.py` (two new deps keys)

**Interfaces:**
- Consumes: everything from Tasks 1–7; existing `queue.record_run`, `queue.events`, `queue.sql_hash`, `_target`, `_denied_sqlstate`, `_bump`, `FixerPg.session`, `branchmod.verification_branch`, `verify.measure`.
- Produces: `default_deps(sql, w, user, cfg=None, llm_w=None)` gains keys `pending_drafts`, `stuck_drafts`, `probe`, `history`, `catalog`, `model`, `checksum`, `plan_shape`; `run_fixer(sql, w, cfg, user, deps=None, now=None, llm_w=None) -> dict` (counts gain `verified`, `verify_failed`, `draft_failed`, `stale`, `recovered:draft_failed`, `recovered:verify_failed` for drafts); CLI flags `--llm-endpoint`, `--llm-enabled`, `--llm-test-response {"",unsafe,non_equivalent}`; `queue.staleness()` returns `"advisory"` for any request on a `query_rewrite` action.
- Draft run rows in `action_runs`: `kind = "draft"`, outcome `claimed` (approval consumed, `action_id = draft_id`) or `stale:<reason>` (`action_id` NULL); verification row `kind = "verify"`, `approval_id` NULL, `action_id = draft_id`, report with `branch`, `ordered`, `original_queryid`, `rewrite_queryid`, `rewrite_key`, `sets`, `rules`, `plan`.
- Ledger chain per draft id: `draft_requested`, `draft_started`, then `draft_failed`, or `drafted`, `verify_started`, `verified` | `verify_failed`; one `events()` call per draft.

Sweep order: stuck actions, stuck drafts, fix approvals (existing), then draft approvals. The endpoint is probed (and `llm_status` merged) only when draft approvals are pending. Drafts share the existing one-project-per-sweep `busy` set.

- [ ] **Step 1: Write the failing tests**

Add the two keys to `deps()` in `engine/tests/unit/test_fix_job.py` (the existing tests pass a complete deps dict):

`engine/tests/unit/test_fix_job.py` (apply this change):

```diff
--- a/engine/tests/unit/test_fix_job.py
+++ b/engine/tests/unit/test_fix_job.py
@@ -69,6 +69,8 @@ def deps(rec, judged=True, branch_error=None, execute=None):
         "judge": lambda kind, ix, before, after: {"passed": judged, "rules": []},
         "execute": do_execute,
         "recover": recover,
+        "stuck_drafts": lambda sql, cfg, now: [],
+        "pending_drafts": lambda sql, cfg: [],
     }
```

`engine/tests/unit/test_draft_job.py` (new file, complete):

```python
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from lakebase_health import ledger
from lakebase_health.config import EngineConfig
from lakebase_health.fixer.draft.context import NoSamples
from lakebase_health.fixer.draft.llm import ModelError
from lakebase_health.fixer.draft.states import draft_token
from lakebase_health.fixer.draft.store import DraftWork
from lakebase_health.fixer.queue import Work, sql_hash, staleness
from lakebase_health.jobs import fix
from tests.fakes import FakeSql

CFG = EngineConfig(catalog="cat")
T = CFG.table
NOW = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
FP = "f" * 32
ORIGINAL = ("SELECT o.id, o.amount FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 AND o.status <> 'void' "
            "AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 WHERE o2.id BETWEEN $1 AND $2 "
            "AND o2.status = o.status) ORDER BY o.id")
REWRITE = ("WITH a AS (SELECT status, avg(amount) AS m FROM hc_test.orders WHERE id BETWEEN $1 AND $2 GROUP BY status) "
           "SELECT o.id, o.amount FROM hc_test.orders o JOIN a ON a.status = o.status WHERE o.id BETWEEN $1 AND $2 "
           "AND o.status <> $3 AND o.amount > a.m ORDER BY o.id")
GOOD = json.dumps({"rewrite": REWRITE, "rationale": "Aggregates once per status instead of once per row.",
                   "expected_plan_change": "SubPlan replaced by a Hash Join over a HashAggregate"})
SETS = [["1600004111", "1600006111"], ["1600200000", "1600202000"]]


def work(attempt=1, last=0, last_state=None, project="p1", apid="ap1"):
    return DraftWork({"approval_id": apid, "fingerprint": FP, "sql_hash": draft_token(FP, attempt),
                      "approver": "owner@x", "requested_at": NOW - timedelta(minutes=1)},
                     {"fingerprint": FP, "status": "open", "check_id": "top_db_time_query", "project_id": project,
                      "branch_id": "br1", "endpoint_id": "ep-a", "object": "42", "evidence": '{"queryid": "42"}',
                      "measure": 60.0}, last, last_state)


class Model:
    def __init__(self, reply=GOOD, error=None):
        self.reply, self.error, self.sent = reply, error, []

    def complete(self, msgs, draft_id, record):
        self.sent.append(msgs)
        record({"call_id": "c1", "draft_id": draft_id, "endpoint": "ep", "prompt_hash": "p" * 64,
                "response_hash": None if self.error else "r" * 64, "input_tokens": 900, "output_tokens": 80,
                "latency_ms": 1500.0, "status": "429" if self.error else "ok", "request_id": "req", "called_at": None})
        if self.error:
            raise self.error
        return self.reply


def deps(items, model=None, rewrite_rows=None, branch_error=None, history_error=None, ready=True, fixes=()):
    seen = {"branch_closed": 0, "sessions": [], "checksums": []}

    @contextmanager
    def branch(w, project, source, did):
        if branch_error:
            raise branch_error
        try:
            yield {"branch": f"{project}/branches/lbh-verify-{did[:8]}", "endpoint": "e-branch", "host": "h"}
        finally:
            seen["branch_closed"] += 1

    @contextmanager
    def session(endpoint, host, db, kind):
        seen["sessions"].append((endpoint, kind))
        yield object()

    def history(sql, cfg, finding):
        if history_error:
            raise history_error
        return {"queryid": "42", "query": ORIGINAL, "param_sets": SETS, "plan": {"Plan": {"Node Type": "Index Scan"}}}

    def checksum(conn, q, values, ordered):
        seen["checksums"].append((q, tuple(values), ordered))
        rows = (rewrite_rows or {}).get(tuple(values), 1803) if q != ORIGINAL else 1803
        return {"rows": rows, "checksum": f"c{rows}"}

    return seen, {
        "pending": lambda sql, cfg: list(fixes), "stuck": lambda sql, cfg, now: [],
        "pending_drafts": lambda sql, cfg: list(items), "stuck_drafts": fix.dstore.stuck_drafts,
        "probe": lambda w, cfg, now: {"endpoint": cfg.llm_endpoint, "enabled": True, "ready": ready,
                                      "reason": None if ready else "turned off", "checked_at": now},
        "record_run": fix.queue.record_run, "events": fix.queue.events,
        "target": lambda w, a: {"project": "projects/x", "branch": "projects/x/branches/production",
                                "endpoint": "projects/x/branches/production/endpoints/primary", "host": "h",
                                "database": "db"},
        "branch": branch, "session": session, "history": history,
        "catalog": lambda conn, tables: [{"name": "hc_test.orders", "row_estimate": 900000}],
        "model": model or Model(), "checksum": checksum,
        "measure": lambda conn, q, params: {"ms": 512.0 if q == ORIGINAL else 2.0, "nodes": [], "indexes": []},
        "plan_shape": lambda conn, q, values: ({"nodes": ["Index Scan", "SubPlan"], "queryid": "42"} if q == ORIGINAL
                                               else {"nodes": ["Hash Join", "HashAggregate"], "queryid": "-77"}),
    }


def run(items, **kw):
    sql = FakeSql()
    seen, d = deps(items, **kw)
    out = fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    return sql, seen, d, out


def drafts(sql):
    return sorted(sql.tables.get(T("drafts"), []), key=lambda r: r["attempt"])


def test_a_verified_rewrite_end_to_end():
    sql, seen, d, out = run([work()])
    assert out == {"verified": 1}
    [draft] = drafts(sql)
    did = draft["draft_id"]
    assert draft["state"] == "verified" and draft["attempt"] == 1 and draft["reason"] is None
    assert draft["action_id"] == did and draft["prompt_version"] == "rewrite_v1" and len(draft["context_hash"]) == 64
    assert draft["rationale"].startswith("Aggregates once") and "Hash Join" in draft["expected_plan_change"]
    [action] = sql.tables[T("proposed_actions")]
    assert action["action_id"] == did and action["origin"] == "llm" and action["state"] == "verified"
    assert action["sql"] == REWRITE.replace("$3", "'void'")  # the original's literal put back, ready to copy
    claimed, verify_run = sql.tables[T("action_runs")]
    assert (claimed["approval_id"], claimed["kind"], claimed["outcome"]) == ("ap1", "draft", "claimed")
    assert (verify_run["approval_id"], verify_run["kind"], verify_run["outcome"]) == (None, "verify", "verified")
    report = json.loads(verify_run["report"])
    assert [r["rule"] for r in report["rules"]] == ["equivalence", "speed", "plan"] and report["ordered"] is True
    assert report["sets"][0] == {"set": 1, "rows_original": 1803, "rows_rewrite": 1803, "same_checksum": True,
                                 "original_ms": 512.0, "rewrite_ms": 2.0}
    assert report["rewrite_queryid"] == "-77" and report["original_queryid"] == "42" and len(report["rewrite_key"]) == 64
    assert report["branch"] == f"projects/x/branches/lbh-verify-{did[:8]}" and seen["branch_closed"] == 1
    for text in ("1600004111", "void", "hc_test.orders", "SELECT"):
        assert text not in verify_run["report"], text
    events = sql.tables[T("ledger")]
    assert [e["event_type"] for e in events] == ["draft_requested", "draft_started", "drafted", "verify_started",
                                                "verified"]
    assert {e["action_id"] for e in events} == {did} and ledger.verify(events)
    [call] = sql.tables[T("llm_calls")]
    assert call["draft_id"] == did and call["called_at"] == NOW
    payload = json.dumps(d["model"].sent)
    assert not any(v in payload for v in ("1600004111", "1600006111", "1600200000", "'void'"))
    assert seen["sessions"] == [("projects/x/branches/production/endpoints/primary", "draft_context"),
                                ("e-branch", "query_rewrite")]
    assert sql.tables[T("llm_status")][0]["ready"] is True


def test_retry_after_a_final_attempt_creates_the_next_attempt_and_keeps_the_first():
    sql = FakeSql()
    seen, d = deps([work()], model=Model(reply="not json"))
    fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    d["pending_drafts"] = lambda s, c: [work(attempt=2, last=1, last_state="draft_failed", apid="ap2")]
    d["model"] = Model()
    fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW + timedelta(minutes=5))
    first, second = drafts(sql)
    assert (first["attempt"], first["state"], first["reason"]) == (1, "draft_failed", "invalid_response: not JSON")
    assert (second["attempt"], second["state"]) == (2, "verified")
    assert first["draft_id"] != second["draft_id"] and len(sql.tables[T("proposed_actions")]) == 1


def test_requests_that_cannot_draft_are_consumed_with_the_reason():
    for item, ready, reason in ((work(attempt=2, last=1, last_state="verifying"), True, "in_progress"),
                                (work(attempt=1, last=1, last_state="verified"), True, "attempt_changed"),
                                (work(), False, "not_set_up")):
        sql, seen, d, out = run([item], ready=ready)
        [r] = sql.tables[T("action_runs")]
        assert (r["approval_id"], r["action_id"], r["outcome"]) == ("ap1", None, f"stale:{reason}")
        assert T("drafts") not in sql.tables and T("ledger") not in sql.tables and d["model"].sent == []
        assert out == {"stale": 1}


def failed(**kw):
    sql, seen, d, out = run([work()], **kw)
    [draft] = drafts(sql)
    events = [e["event_type"] for e in sql.tables[T("ledger")]]
    return draft, events, sql, out


def test_model_failures_end_draft_failed_with_the_reason():
    draft, events, sql, out = failed(model=Model(error=ModelError("model_unavailable", "429")))
    assert (draft["state"], draft["reason"]) == ("draft_failed", "model_unavailable") and out == {"draft_failed": 1}
    assert events == ["draft_requested", "draft_started", "draft_failed"]
    assert sql.tables[T("llm_calls")][0]["status"] == "429" and T("proposed_actions") not in sql.tables
    assert failed(model=Model(reply='{"rewrite": null, "reason": "already optimal"}'))[0]["reason"] == \
        "no_safe_rewrite: already optimal"
    assert failed(model=Model(error=ModelError("not_set_up", "403")))[0]["reason"] == "not_set_up"


def test_static_check_failures_never_reach_a_branch():
    bad = json.dumps({"rewrite": f"WITH d AS (DELETE FROM hc_test.orders RETURNING 1) {ORIGINAL.replace(chr(39) + 'void' + chr(39), '$3')}",
                      "rationale": "x", "expected_plan_change": "x"})
    draft, events, sql, out = failed(model=Model(reply=bad))
    assert draft["state"] == "draft_failed" and draft["reason"].startswith("data_modifying_cte")
    assert T("proposed_actions") not in sql.tables


def test_no_samples_fails_before_the_model_is_called():
    draft, events, sql, out = failed(history_error=NoSamples("the query has 2 parameters but no sampled values"))
    assert draft["reason"] == "no_samples: the query has 2 parameters but no sampled values"
    assert T("llm_calls") not in sql.tables


def test_a_non_equivalent_rewrite_is_verify_failed_with_the_rule():
    draft, events, sql, out = failed(rewrite_rows={tuple(SETS[1]): 1700})
    assert draft["state"] == "verify_failed" and draft["reason"] == "equivalence: set 2: 1803 rows vs 1700"
    assert sql.tables[T("proposed_actions")][0]["state"] == "verify_failed"
    assert events[-1] == "verify_failed" and out == {"verify_failed": 1}


def test_branch_unavailable_is_verify_failed():
    draft, events, sql, out = failed(branch_error=RuntimeError("branch limit"))
    assert draft["state"] == "verify_failed" and draft["reason"] == "branch_unavailable: RuntimeError: branch limit"


def test_one_branch_per_project_per_sweep_across_fixes_and_drafts():
    fixw = Work({"approval_id": "apx", "action_id": "a" * 32, "request": "reject", "sql_hash": "", "approver": "o"},
                {"action_id": "a" * 32, "state": "proposed", "sql": "x", "project_id": "p1", "kind": "ddl"}, "open")
    sql = FakeSql()
    seen, d = deps([work(project="p1"), work(project="p2", apid="ap2")], fixes=[fixw])
    d.update(claim=lambda *a: True, set_state=lambda *a: None)
    out = fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    assert out == {"rejected": 1, "verified": 1}
    assert [r["approval_id"] for r in sql.tables[T("action_runs")] if r["kind"] == "draft"] == ["ap2"]


def test_an_interrupted_draft_is_recovered_so_retry_is_offered():
    stuck = {"draft_id": "d" * 32, "finding_fingerprint": FP, "attempt": 1, "state": "drafting",
             "updated_at": NOW - timedelta(hours=5)}
    sql = FakeSql([(r"FROM cat\.lakebase_health\.drafts WHERE state IN", [stuck])])
    seen, d = deps([])
    out = fix.run_fixer(sql, None, CFG, "fixer", deps=d, now=NOW)
    [row] = sql.tables[T("drafts")]
    assert (row["state"], row["reason"]) == ("draft_failed", "interrupted: the fixer job stopped")
    assert out == {"recovered:draft_failed": 1}
    assert [e["event_type"] for e in sql.tables[T("ledger")]] == ["draft_failed"]


def test_fix_requests_on_a_rewrite_action_are_refused_as_advisory():
    w = Work({"approval_id": "ap", "action_id": "d" * 32, "request": "apply", "sql_hash": sql_hash("SELECT 1"),
              "approver": "o"}, {"action_id": "d" * 32, "state": "verified", "sql": "SELECT 1", "kind": "query_rewrite",
                                 "verify_on_branch": True, "expires_at": NOW + timedelta(days=1)}, "open")
    assert staleness(w, NOW) == "advisory"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_draft_job.py tests/unit/test_fix_job.py -v`
Expected: test_draft_job FAILs (`fix.dstore` missing, no drafting); test_fix_job still passes.

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/fixer/queue.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/fixer/queue.py
+++ b/engine/src/lakebase_health/fixer/queue.py
@@ -47,6 +47,8 @@ def pending(sql, cfg) -> list:
 
 def staleness(w: Work, now) -> str | None:
     a, ap = w.action, w.approval
+    if a.get("kind") == "query_rewrite":
+        return "advisory"  # a rewrite is changed in application code; the fixer never runs its SQL
     target = a.get("rollback_sql") if ap["request"] == "rollback" else a.get("sql")
     if ap["request"] == "rollback" and not runnable(target):
         return "no_rollback"  # nothing to undo, or a stored rollback outside the allowlist: never executed
```

`engine/src/lakebase_health/jobs/fix.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/jobs/fix.py
+++ b/engine/src/lakebase_health/jobs/fix.py
@@ -9,6 +9,13 @@ from ..fixer import apply as applymod
 from ..fixer import branch as branchmod
 from ..fixer import queue, samples, verify
 from ..fixer.allowlist import NotAllowed, parse
+from ..fixer.draft import context as dcontext
+from ..fixer.draft import llm as dllm
+from ..fixer.draft import prompt as dprompt
+from ..fixer.draft import static as dstatic
+from ..fixer.draft import store as dstore
+from ..fixer.draft import verify as dverify
+from ..fixer.draft.sqltext import denormalize, normalize, text_key
 from ..fixer.pg import CREDENTIAL_DENIED, FixerPg
 from ..fixer.states import NOT_ALLOWED, PRIVILEGES, START
 
@@ -44,13 +51,18 @@ def _target(w, action):
     return {"project": proj.name, "branch": branch.name, "endpoint": ep.name, "host": ep.status.hosts.host, "database": db}
 
 
-def default_deps(sql, w, user):
+def default_deps(sql, w, user, cfg=None, llm_w=None):
     pg = FixerPg(w, user)
+    cfg = cfg or EngineConfig(catalog="unused")
+    model = dllm.ModelClient(dllm.sdk_query(llm_w or w), cfg.llm_endpoint, test_response=cfg.llm_test_response)
     return {"pending": queue.pending, "stuck": _stuck, "claim": queue.claim, "set_state": queue.set_state,
             "record_run": queue.record_run, "events": queue.events, "branch": branchmod.verification_branch,
             "session": pg.session, "target": _target, "samples": samples.affected_queries,
             "no_samples": samples.no_samples_reason, "measure": verify.measure,
-            "judge": verify.judge, "execute": applymod.execute, "recover": applymod.recover}
+            "judge": verify.judge, "execute": applymod.execute, "recover": applymod.recover,
+            "pending_drafts": dstore.pending_drafts, "stuck_drafts": dstore.stuck_drafts, "probe": dllm.probe,
+            "history": dcontext.load_history, "catalog": dcontext.load_catalog, "model": model,
+            "checksum": dverify.checksum, "plan_shape": dverify.plan_shape}
 
 
 def _run_row(work, kind, now):
@@ -225,9 +237,160 @@ def _recover(d, sql, w, cfg, row, now):
     return outcome
 
 
-def run_fixer(sql, w, cfg, user, deps=None, now=None) -> dict:
+def _draft_event(did, event_type, payload):
+    return {"action_id": did, "event_type": event_type, "payload": payload}
+
+
+def _first_failure(report) -> str:
+    bad = next((r for r in report.get("rules", []) if not r.get("passed")), None)
+    return f"{bad['rule']}: {bad['detail']}"[:1000] if bad else "verification failed"
+
+
+def _verify_rewrite(d, w, tgt, did, original, rewrite, hist, ordered, expected):
+    """Returns (passed, report, error). The report holds counts, checksums' verdicts, timings, plan node names and
+    query ids only, never query text or parameter values."""
+    report = {"branch": None, "ordered": ordered, "original_queryid": hist["queryid"], "sets": [], "rules": []}
+    sets, entered = [], False
+    try:
+        with d["branch"](w, tgt["project"], tgt["branch"], did) as b:
+            entered = True
+            report["branch"] = b["branch"]
+            with d["session"](b["endpoint"], b["host"], tgt["database"], "query_rewrite") as conn:
+                for i, values in enumerate(hist["param_sets"], 1):
+                    o = d["checksum"](conn, original, values, ordered)
+                    r = d["checksum"](conn, rewrite, values, ordered)
+                    om, rm = d["measure"](conn, original, [values]), d["measure"](conn, rewrite, [values])
+                    sets.append({"set": i, "original": o, "rewrite": r, "original_ms": om.get("ms"),
+                                 "rewrite_ms": rm.get("ms")})
+                before = d["plan_shape"](conn, original, hist["param_sets"][0])
+                after = d["plan_shape"](conn, rewrite, hist["param_sets"][0])
+    except Exception as e:
+        if entered or _denied_sqlstate(e):
+            raise
+        return False, report, f"branch_unavailable: {type(e).__name__}: {e}"[:1000]
+    verdict = dverify.judge(sets, ordered, before, after, expected)
+    report.update(rules=verdict["rules"], plan=verdict["plan"], rewrite_queryid=after.get("queryid"),
+                  rewrite_key=text_key(rewrite),
+                  sets=[{"set": x["set"], "rows_original": x["original"].get("rows"),
+                         "rows_rewrite": x["rewrite"].get("rows"),
+                         "same_checksum": x["original"].get("checksum") is not None
+                         and x["original"].get("checksum") == x["rewrite"].get("checksum"),
+                         "original_ms": x["original_ms"], "rewrite_ms": x["rewrite_ms"]} for x in sets])
+    return verdict["passed"], report, None
+
+
+def _handle_draft(d, sql, w, cfg, work, now, counts, ready):
+    """One draft approval: consume it, draft with the model, check statically, verify on a branch. Every path
+    ends the attempt in a final state and writes its ledger events in one events() call."""
+    ap, f = work.approval, work.finding
+    stale = dstore.staleness(work, ready)
+    if stale:  # no draft row and no ledger chain exist for a refused request: the run row records why
+        d["record_run"](sql, cfg, {"run_id": uuid.uuid4().hex, "approval_id": ap["approval_id"], "action_id": None,
+                                   "kind": "draft", "started_at": now, "ended_at": now, "outcome": f"stale:{stale}",
+                                   "report": None, "branch": None, "error": None})
+        return _bump(counts, "stale")
+    draft = dstore.new_draft(work, cfg, now)
+    did = draft["draft_id"]
+    # Consume the approval before anything runs, so a crash can never draft it twice.
+    d["record_run"](sql, cfg, {"run_id": uuid.uuid4().hex, "approval_id": ap["approval_id"], "action_id": did,
+                               "kind": "draft", "started_at": now, "ended_at": now, "outcome": "claimed",
+                               "report": None, "branch": None, "error": None})
+    dstore.save_draft(sql, cfg, draft)
+    events = [_draft_event(did, "draft_requested", {"approval_id": ap["approval_id"], "requested_by": ap.get("approver"),
+                                                     "requested_at": ap.get("requested_at"), "attempt": draft["attempt"],
+                                                     "fingerprint": f["fingerprint"]}),
+              _draft_event(did, "draft_started", {"attempt": draft["attempt"], "endpoint": cfg.llm_endpoint})]
+
+    def finish(state, reason=None):
+        draft.update(state=state, reason=None if reason is None else str(reason)[:1000], updated_at=now, ended_at=now)
+        dstore.save_draft(sql, cfg, draft)
+        events.append(_draft_event(did, state, {"reason": draft["reason"]}))
+        d["events"](sql, cfg, events, now)
+        _bump(counts, state)
+
+    try:
+        hist = d["history"](sql, cfg, f)
+        norm, literal_values = normalize(hist["query"])
+        original = dstatic.shape(norm)
+        tgt = d["target"](w, {"project_id": f["project_id"], "branch_id": f["branch_id"]})
+        with d["session"](tgt["endpoint"], tgt["host"], tgt["database"], "draft_context") as conn:
+            tables = d["catalog"](conn, original.tables)
+        context = dcontext.build_context(f, norm, original.ordered, hist["plan"], tables)
+        draft.update(prompt_version=dprompt.PROMPT_VERSION, context_hash=dprompt.context_hash(context))
+        content = d["model"].complete(dprompt.messages(context), did,
+                                      lambda row: dstore.record_llm_call(sql, cfg, {**row, "called_at": now}))
+    except dcontext.NoSamples as e:
+        return finish("draft_failed", f"no_samples: {e}")
+    except dstatic.StaticError as e:
+        return finish("draft_failed", f"not_rewritable: {e.detail}")  # the original is not one SELECT
+    except dllm.ModelError as e:
+        return finish("draft_failed", e.reason)
+    except Exception as e:
+        denied = _denied_sqlstate(e)
+        return finish("draft_failed", f"privileges: fixer not enrolled or missing privileges ({denied})" if denied
+                      else f"error: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}")
+    parsed = dprompt.parse_response(content)
+    if not parsed["ok"]:
+        return finish("draft_failed", f"{parsed['reason']}: {parsed['detail']}" if parsed["detail"] else parsed["reason"])
+    try:
+        dstatic.check(norm, parsed["rewrite"])
+    except dstatic.StaticError as e:
+        return finish("draft_failed", f"{e.check}: {e.detail}")
+    rewrite_sql = denormalize(parsed["rewrite"], literal_values)
+    draft.update(state="verifying", action_id=did, rationale=parsed["rationale"],
+                 expected_plan_change=parsed["expected_plan_change"], updated_at=now)
+    dstore.save_draft(sql, cfg, draft)
+    action = dstore.rewrite_action(draft, f, rewrite_sql, now)
+    dstore.save_action(sql, cfg, action)
+    events += [_draft_event(did, "drafted", {"attempt": draft["attempt"], "prompt_version": draft["prompt_version"],
+                                              "context_hash": draft["context_hash"], "sql_hash": queue.sql_hash(rewrite_sql)}),
+               _draft_event(did, "verify_started", {"approval_id": ap["approval_id"], "approver": ap.get("approver")})]
+    run = {"run_id": uuid.uuid4().hex, "approval_id": None, "action_id": did, "kind": "verify", "started_at": now,
+           "ended_at": None, "outcome": None, "report": None, "branch": None, "error": None}
+    try:
+        passed, report, error = _verify_rewrite(d, w, tgt, did, hist["query"], rewrite_sql, hist, original.ordered,
+                                                parsed["expected_plan_change"])
+    except Exception as e:
+        denied = _denied_sqlstate(e)
+        passed, report = False, {"branch": None, "rules": []}
+        error = (f"fixer not enrolled or missing privileges on this project: {denied}" if denied
+                 else f"{type(e).__name__}: {e}")[:1000]
+    outcome = "verified" if passed else "verify_failed"
+    run.update(outcome=outcome, ended_at=now, report=json.dumps(report, default=str), branch=report.get("branch"),
+               error=error)
+    d["record_run"](sql, cfg, run)
+    action.update(state=outcome, updated_at=now)
+    dstore.save_action(sql, cfg, action)
+    draft.update(state=outcome, reason=None if passed else (error or _first_failure(report)), updated_at=now,
+                 ended_at=now)
+    dstore.save_draft(sql, cfg, draft)
+    events.append(_draft_event(did, outcome, {"run_id": run["run_id"], "error": error}))
+    d["events"](sql, cfg, events, now)
+    _bump(counts, outcome)
+
+
+def _recover_draft(d, sql, cfg, row, now) -> str:
+    """A draft left drafting or verifying by a job that died: final, so Retry is offered again."""
+    state = "draft_failed" if row["state"] == "drafting" else "verify_failed"
+    dstore.save_draft(sql, cfg, {**row, "state": state, "reason": "interrupted: the fixer job stopped",
+                                 "updated_at": now, "ended_at": now})
+    d["events"](sql, cfg, [_draft_event(row["draft_id"], state, {"recovered": True})], now)
+    return state
+
+
+def _llm_ready(d, sql, w, cfg, now) -> bool:
+    try:
+        row = d["probe"](w, cfg, now)
+        dstore.write_status(sql, cfg, row)
+        return bool(row["ready"])
+    except Exception:
+        log.exception("could not record the model endpoint status; drafting is skipped this sweep")
+        return False
+
+
+def run_fixer(sql, w, cfg, user, deps=None, now=None, llm_w=None) -> dict:
     now = now or datetime.now(timezone.utc)
-    d = {**(default_deps(sql, w, user) if deps is None else {}), **(deps or {})}
+    d = {**(default_deps(sql, w, user, cfg, llm_w) if deps is None else {}), **(deps or {})}
     counts: dict = {}
     for row in d["stuck"](sql, cfg, now):
         try:
@@ -235,6 +398,12 @@ def run_fixer(sql, w, cfg, user, deps=None, now=None) -> dict:
         except Exception:
             log.exception("recovery of action %s failed; it stays %s for the next sweep", row["action_id"], row["state"])
             _bump(counts, "recover_error")
+    for row in d["stuck_drafts"](sql, cfg, now):
+        try:
+            _bump(counts, f"recovered:{_recover_draft(d, sql, cfg, row, now)}")
+        except Exception:
+            log.exception("recovery of draft %s failed", row["draft_id"])
+            _bump(counts, "recover_error")
     busy = set()
     for work in d["pending"](sql, cfg):
         project = work.action.get("project_id")
@@ -242,6 +411,14 @@ def run_fixer(sql, w, cfg, user, deps=None, now=None) -> dict:
             continue
         busy.add(project)
         _handle(d, sql, w, cfg, work, now, counts)
+    drafts = d["pending_drafts"](sql, cfg)
+    ready = _llm_ready(d, sql, w, cfg, now) if drafts else False
+    for work in drafts:  # one branch per project per sweep: a draft waits behind that project's fix
+        project = work.finding.get("project_id")
+        if project in busy:
+            continue
+        busy.add(project)
+        _handle_draft(d, sql, w, cfg, work, now, counts, ready)
     return counts
 
 
@@ -250,11 +427,20 @@ def main(argv=None) -> None:
     ap.add_argument("--catalog", required=True)
     ap.add_argument("--schema", default="lakebase_health")
     ap.add_argument("--pii-masking", default="true")
+    ap.add_argument("--llm-endpoint", default="databricks-claude-opus-5-5")
+    ap.add_argument("--llm-enabled", default="true")
+    # Acceptance only, never set in the bundle: a fixed bad model reply (unsafe | non_equivalent).
+    ap.add_argument("--llm-test-response", default="", choices=("", *dllm.TEST_RESPONSES))
     a = ap.parse_args(argv)
     from databricks.sdk import WorkspaceClient
+    from databricks.sdk.config import Config
     from pyspark.sql import SparkSession
 
     from ..sql import SparkSql
-    cfg = EngineConfig(catalog=a.catalog, schema=a.schema, pii_masking=pii_masking_enabled(a.pii_masking))
+    cfg = EngineConfig(catalog=a.catalog, schema=a.schema, pii_masking=pii_masking_enabled(a.pii_masking),
+                       llm_endpoint=a.llm_endpoint, llm_enabled=a.llm_enabled.strip().lower() != "false",
+                       llm_test_response=a.llm_test_response)
     sql, w = SparkSql(SparkSession.builder.getOrCreate()), WorkspaceClient()
-    print(json.dumps(run_fixer(sql, w, cfg, w.current_user.me().user_name)))
+    # The model call gets its own client: a 120 s read timeout, and no SDK retries (the drafter retries once itself).
+    llm_w = WorkspaceClient(config=Config(http_timeout_seconds=dllm.TIMEOUT_S, retry_timeout_seconds=1))
+    print(json.dumps(run_fixer(sql, w, cfg, w.current_user.me().user_name, llm_w=llm_w)))
```

Notes: `_verify_rewrite` calls `d["measure"](conn, q, [values])` (one set at a time, the fixer's default 3 warm repetitions), so per-set medians feed the speed rule. A `StaticError` from `shape()` of the original (e.g. the finding's query is an `UPDATE`) ends `draft_failed: not_rewritable: …` before the model is called.

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest -q`
Expected: all pass (11 new in test_draft_job).

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/jobs/fix.py engine/src/lakebase_health/fixer/queue.py engine/tests/unit/test_draft_job.py engine/tests/unit/test_fix_job.py
git commit -m "feat(fixer): draft rewrites on request: context, model, static checks, branch verification, Retry

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Adoption post-check in the engine run

**Files:**
- Modify: `engine/src/lakebase_health/fixer/postcheck.py`, `engine/src/lakebase_health/jobs/run.py`
- Test: `engine/tests/unit/test_postcheck.py`, `engine/tests/unit/test_run.py`

**Interfaces:**
- Consumes: `sqltext.text_key`, `SqlTextError` (Task 1); `states.ADOPTION_WINDOW` (Task 4); the verify report keys `rewrite_queryid`, `rewrite_key` (Task 8).
- Produces: `ADOPTED_MIN_CALLS = 20`; `run_rewrite_postcheck(sql, cfg, now, errors=None) -> list[dict]` (each `{..., "outcome": "adopted" | "not_adopted", "calls", ...}`); it moves the action `verified` → outcome with a compare-and-set UPDATE, sets `drafts.state`, writes a ledger event (actor `engine`) and an `action_runs` `postcheck` row. `run_engine` counts `rewrites_adopted`; a failure is an error string, not a failed run.

- [ ] **Step 1: Write the failing tests**

`engine/tests/unit/test_postcheck.py` (apply this change):

```diff
--- a/engine/tests/unit/test_postcheck.py
+++ b/engine/tests/unit/test_postcheck.py
@@ -99,3 +99,52 @@ def test_the_run_in_progress_counts_as_the_second_run():
     assert res["action_id"] == "a1"
     due_sql = second_run.ran(r"FROM \S*proposed_actions a JOIN")[0]
     assert "r.status IN ('ok', 'ok_with_errors')" in due_sql and "AS runs_after" in due_sql
+
+
+# --- LLM rewrites: adopted | not_adopted ---
+from datetime import timedelta  # noqa: E402
+
+from lakebase_health import ledger  # noqa: E402
+from lakebase_health.fixer.draft.sqltext import text_key  # noqa: E402
+from lakebase_health.fixer.postcheck import run_rewrite_postcheck  # noqa: E402
+
+REWRITE = "SELECT o.id FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 AND o.status <> 'void' ORDER BY o.id"
+VERIFIED = {"action_id": "d" * 32, "verified_at": NOW - timedelta(hours=2), "endpoint_id": "ep-a",
+            "rewrite_queryid": "-77", "rewrite_key": text_key(REWRITE)}
+
+
+def rewrite_sql(due, seen, upd=UPD_OK):
+    return FakeSql(responses=[(r"a\.origin = 'llm'", due), upd, (r"pg_stat_statements_counters", seen)])
+
+
+def test_a_rewrite_seen_by_query_id_is_adopted():
+    sql = rewrite_sql([VERIFIED], [{"queryid": "-77", "query": "<REDACTED>", "calls": 25},
+                                   {"queryid": "42", "query": "SELECT old", "calls": 900}])
+    [res] = run_rewrite_postcheck(sql, EngineConfig(catalog="cat"), NOW)
+    assert res["outcome"] == "adopted" and res["calls"] == 25
+    assert sql.ran(r"UPDATE cat\.lakebase_health\.drafts SET state = :s")
+    assert [a["s"] for s, a in sql.statements if "drafts SET state" in s] == ["adopted"]
+    [ev] = sql.tables[T + "ledger"]
+    assert ev["event_type"] == "adopted" and ev["action_id"] == "d" * 32 and ledger.verify([ev])
+    assert sql.tables[T + "action_runs"][0]["kind"] == "postcheck"
+
+
+def test_a_rewrite_seen_by_its_normalized_text_is_adopted():
+    sent = "select o.id from hc_test.orders o where o.id between $1 and $2 and o.status <> $3 order by o.id"
+    sql = rewrite_sql([{**VERIFIED, "rewrite_queryid": None}], [{"queryid": "5", "query": sent, "calls": 30}])
+    assert run_rewrite_postcheck(sql, EngineConfig(catalog="cat"), NOW)[0]["outcome"] == "adopted"
+
+
+def test_too_few_calls_wait_and_a_week_without_is_not_adopted():
+    few = rewrite_sql([VERIFIED], [{"queryid": "-77", "query": None, "calls": 5}])
+    assert run_rewrite_postcheck(few, EngineConfig(catalog="cat"), NOW) == [] and not few.ran(r"^UPDATE")
+    old = rewrite_sql([{**VERIFIED, "verified_at": NOW - timedelta(days=7, hours=1)}], [])
+    [res] = run_rewrite_postcheck(old, EngineConfig(catalog="cat"), NOW)
+    assert res["outcome"] == "not_adopted" and res["calls"] == 0
+
+
+def test_a_lost_state_race_writes_nothing_else():
+    sql = rewrite_sql([VERIFIED], [{"queryid": "-77", "query": None, "calls": 25}],
+                      upd=(r"^\s*UPDATE \S*proposed_actions", [{"num_affected_rows": 0}]))
+    assert run_rewrite_postcheck(sql, EngineConfig(catalog="cat"), NOW) == []
+    assert not sql.ran(r"drafts SET state") and T + "ledger" not in sql.tables
```

`engine/tests/unit/test_run.py` (apply this change):

```diff
--- a/engine/tests/unit/test_run.py
+++ b/engine/tests/unit/test_run.py
@@ -169,3 +169,19 @@ def test_fix_regressed_finding_resolves_once_its_action_is_rolled_back(tmp_path,
     assert by_fp["fr1"]["status"] == "resolved" and by_fp["fr1"]["resolved_at"] == NOW
     assert by_fp["fr2"]["status"] == "open"
     assert run["status"] == "ok" and '"fixes_rolled_back": 1' in run["counts"]
+
+
+def test_rewrite_postcheck_runs_in_every_engine_run_and_its_failure_is_not_fatal(tmp_path, monkeypatch):
+    write_check(tmp_path)
+    monkeypatch.setattr("lakebase_health.jobs.run.run_rewrite_postcheck",
+                        lambda sql, cfg, now, errors=None: [{"outcome": "adopted"}, {"outcome": "not_adopted"}])
+    sql = FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])])
+    run = run_engine(sql, fake_w(), EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
+    assert '"rewrites_adopted": 1' in run["counts"]
+
+    def boom(sql, cfg, now, errors=None):
+        raise RuntimeError("no drafts table")
+    monkeypatch.setattr("lakebase_health.jobs.run.run_rewrite_postcheck", boom)
+    run = run_engine(FakeSql(responses=[(r"SELECT max\(ts\) AS max_ts", [{"max_ts": NOW, "n": 1}])]), fake_w(),
+                     EngineConfig(catalog="cat"), connect=lambda **k: None, now=NOW, checks_dir=tmp_path)
+    assert run["status"] == "ok_with_errors" and "rewrite postcheck: RuntimeError" in run["errors"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_postcheck.py tests/unit/test_run.py -v`
Expected: FAIL with `ImportError: cannot import name 'run_rewrite_postcheck'`.

- [ ] **Step 3: Implement**

`engine/src/lakebase_health/fixer/postcheck.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/fixer/postcheck.py
+++ b/engine/src/lakebase_health/fixer/postcheck.py
@@ -1,9 +1,13 @@
 import json
 import uuid
+from datetime import timezone
 
 from ..ledger import chain, last_hashes
+from .draft.sqltext import SqlTextError, text_key
+from .draft.states import ADOPTION_WINDOW
 
 MIN_CALLS = 50
+ADOPTED_MIN_CALLS = 20  # calls of the rewritten query since verification that count as the workload switching
 POSTCHECK_RUNS = 2  # engine runs (this one included) after the apply before the outcome is decided
 
 
@@ -71,3 +75,65 @@ def run_postcheck(sql, cfg, now, errors=None) -> list:
             if errors is not None:
                 errors.append(f"postcheck {d['action_id']}: {type(e).__name__}: {e}")
     return out
+
+
+def _utc(dt):
+    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)
+
+
+def _matches(row, queryid, key) -> bool:
+    if queryid and str(row.get("queryid")) == str(queryid):
+        return True
+    q = row.get("query")
+    if not key or not q or str(q).startswith("<REDACTED"):
+        return False
+    try:
+        return text_key(q) == key
+    except SqlTextError:
+        return False
+
+
+def run_rewrite_postcheck(sql, cfg, now, errors=None) -> list:
+    """A verified rewrite is adopted once the endpoint has run it (its query id from the branch, or the same
+    normalized text) ADOPTED_MIN_CALLS times since verification; not_adopted after ADOPTION_WINDOW without."""
+    due = sql.query(f"""
+        SELECT a.action_id, a.updated_at AS verified_at, f.endpoint_id,
+               get_json_object(r.report, '$.rewrite_queryid') AS rewrite_queryid,
+               get_json_object(r.report, '$.rewrite_key') AS rewrite_key
+        FROM {cfg.table('proposed_actions')} a JOIN {cfg.table('findings')} f ON f.fingerprint = a.fingerprint
+        LEFT JOIN (SELECT action_id, max_by(report, started_at) AS report FROM {cfg.table('action_runs')}
+                   WHERE kind = 'verify' AND outcome = 'verified' GROUP BY action_id) r ON r.action_id = a.action_id
+        WHERE a.state = 'verified' AND a.origin = 'llm'""")
+    out = []
+    for d in due:
+        try:
+            verified_at = _utc(d["verified_at"])
+            seen = sql.query(f"""
+                SELECT CAST(queryid AS STRING) AS queryid, max(query) AS query, sum(calls) AS calls
+                FROM {cfg.raw('pg_stat_statements_counters')}
+                WHERE endpoint_id = :ep AND ts >= :t GROUP BY queryid""", {"ep": d["endpoint_id"], "t": verified_at})
+            hits = [r for r in seen if _matches(r, d.get("rewrite_queryid"), d.get("rewrite_key"))]
+            calls = sum(int(r.get("calls") or 0) for r in hits)
+            if calls >= ADOPTED_MIN_CALLS:
+                outcome, report = "adopted", {"calls": calls, "queryids": sorted(str(r["queryid"]) for r in hits)}
+            elif now - verified_at >= ADOPTION_WINDOW:
+                outcome, report = "not_adopted", {"calls": calls, "days": ADOPTION_WINDOW.days}
+            else:
+                continue
+            won = sql.query(f"UPDATE {cfg.table('proposed_actions')} SET state = :s, updated_at = :now "
+                            "WHERE action_id = :id AND state = 'verified'", {"s": outcome, "now": now, "id": d["action_id"]})
+            if not (won and int(won[0].get("num_affected_rows") or 0) == 1):
+                continue
+            sql.execute(f"UPDATE {cfg.table('drafts')} SET state = :s, updated_at = :now WHERE draft_id = :id",
+                        {"s": outcome, "now": now, "id": d["action_id"]})
+            ev = [{"action_id": d["action_id"], "event_type": outcome, "payload": report}]
+            sql.insert_rows(cfg.table("ledger"), chain(last_hashes(sql, cfg, [d["action_id"]]), ev, now))
+            sql.insert_rows(cfg.table("action_runs"), [{"run_id": uuid.uuid4().hex, "approval_id": None,
+                                                        "action_id": d["action_id"], "kind": "postcheck",
+                                                        "started_at": now, "ended_at": now, "outcome": outcome,
+                                                        "report": json.dumps(report), "branch": None, "error": None}])
+            out.append({**d, "outcome": outcome, **report})
+        except Exception as e:
+            if errors is not None:
+                errors.append(f"rewrite postcheck {d['action_id']}: {type(e).__name__}: {e}")
+    return out
```

`engine/src/lakebase_health/jobs/run.py` (apply this change):

```diff
--- a/engine/src/lakebase_health/jobs/run.py
+++ b/engine/src/lakebase_health/jobs/run.py
@@ -15,7 +15,7 @@ from ..executors.delta import DeltaExecutor
 from ..executors.postgres import PostgresExecutor
 from ..ingest import ingest_all
 from ..inventory import build_inventory
-from ..fixer.postcheck import run_postcheck
+from ..fixer.postcheck import run_postcheck, run_rewrite_postcheck
 from ..record import load_last_ok, persist, persist_regressions, resolve_rolled_back
 from ..runner import Runner
 
@@ -103,6 +103,10 @@ def run_engine(sql, w, cfg, connect=None, now=None, checks_dir=None, skip_ingest
         counts["fixes_rolled_back"] = resolve_rolled_back(sql, cfg, now)
     except Exception as e:
         errors.append(f"rolled back fixes: {type(e).__name__}: {e}")
+    try:
+        counts["rewrites_adopted"] = sum(r["outcome"] == "adopted" for r in run_rewrite_postcheck(sql, cfg, now, errors))
+    except Exception as e:
+        errors.append(f"rewrite postcheck: {type(e).__name__}: {e}")
     steps["postcheck"] = time.monotonic() - t
 
     run.update(ended_at=now if injected_now else datetime.now(timezone.utc),
```

- [ ] **Step 4: Run the tests**

Run: `cd engine && .venv/bin/python -m pytest -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add engine/src/lakebase_health/fixer/postcheck.py engine/src/lakebase_health/jobs/run.py engine/tests/unit/test_postcheck.py engine/tests/unit/test_run.py
git commit -m "feat(engine): post-check marks verified rewrites adopted or not_adopted

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 10: Bundle variables, job parameters and grants

**Files:**
- Modify: `databricks.yml`, `resources/fixer_job.yml`, `resources/engine_job.yml`, `resources/analysis_job.yml`, `resources/app.yml`, `scripts/fixer_grants.sh`
- Test: `engine/tests/unit/test_bundle.py`, `engine/tests/unit/test_fixer_grants.py`

**Interfaces:**
- Produces: variables `llm_endpoint` (default `databricks-claude-opus-5-5`) and `llm_enabled` (default `"true"`); fixer parameters `--llm-endpoint ${var.llm_endpoint} --llm-enabled ${var.llm_enabled}` (never `--llm-test-response`); `sqlglot>=30.24,<31` in all three job environments; app env `LLM_ENABLED`, `LLM_ENDPOINT`; `fixer_grants.sh` grants `MODIFY` on `drafts`, `llm_calls`, `llm_status` and, when the endpoint has an `id`, `CAN_QUERY` through `databricks serving-endpoints update-permissions <id>` (env `LLM_ENDPOINT`, `LLM_ENABLED`).

CLI facts checked: `databricks serving-endpoints update-permissions SERVING_ENDPOINT_ID --json '{"access_control_list": [...]}'` takes the endpoint **id**; `databricks serving-endpoints get databricks-claude-opus-5-5 -p my-profile` returns no `id` (`GET /permissions/serving-endpoints/` has no API for it), so the script notes it and grants nothing.

- [ ] **Step 1: Write the failing tests**

`engine/tests/unit/test_bundle.py` (apply this change):

```diff
--- a/engine/tests/unit/test_bundle.py
+++ b/engine/tests/unit/test_bundle.py
@@ -21,3 +21,31 @@ def test_ingest_interval_is_one_variable_for_both_jobs_and_the_app():
         assert params[i + 1] == "${var.ingest_interval_minutes}", f
     env = {e["name"]: e for e in _load("resources/app.yml")["resources"]["apps"]["lakebase_health_app"]["config"]["env"]}
     assert env["SCHEDULE_MINUTES"]["value"] == "${var.ingest_interval_minutes}"
+
+
+def _wheel_task(f, key):
+    return _load(f)["resources"]["jobs"][key]["tasks"][0]["python_wheel_task"]
+
+
+def test_llm_variables_reach_the_fixer_and_the_app_and_the_test_switch_never_does():
+    variables = _load("databricks.yml")["variables"]
+    assert variables["llm_endpoint"]["default"] == "databricks-claude-opus-5-5"
+    assert variables["llm_enabled"]["default"] == "true"
+    params = _wheel_task("resources/fixer_job.yml", "lakebase_health_fixer")["parameters"]
+    assert params[params.index("--llm-endpoint") + 1] == "${var.llm_endpoint}"
+    assert params[params.index("--llm-enabled") + 1] == "${var.llm_enabled}"
+    for f in ("databricks.yml", "resources/fixer_job.yml", "resources/engine_job.yml", "resources/analysis_job.yml",
+              "resources/app.yml"):
+        assert "llm-test-response" not in (REPO / f).read_text() and "llm_test_response" not in (REPO / f).read_text()
+    env = {e["name"]: e for e in _load("resources/app.yml")["resources"]["apps"]["lakebase_health_app"]["config"]["env"]}
+    assert env["LLM_ENABLED"]["value"] == "${var.llm_enabled}" and env["LLM_ENDPOINT"]["value"] == "${var.llm_endpoint}"
+
+
+def test_every_job_installs_sqlglot_like_the_wheel_requires():
+    pyproject = (REPO / "engine" / "pyproject.toml").read_text()
+    assert '"sqlglot>=30.24,<31"' in pyproject and '"fixer/draft/prompts/*.md"' in pyproject
+    for f, key in (("resources/engine_job.yml", "lakebase_health_engine"),
+                   ("resources/analysis_job.yml", "lakebase_health_analysis"),
+                   ("resources/fixer_job.yml", "lakebase_health_fixer")):
+        deps = _load(f)["resources"]["jobs"][key]["environments"][0]["spec"]["dependencies"]
+        assert "sqlglot>=30.24,<31" in deps, f
```

`engine/tests/unit/test_fixer_grants.py` (apply this change):

```diff
--- a/engine/tests/unit/test_fixer_grants.py
+++ b/engine/tests/unit/test_fixer_grants.py
@@ -27,16 +27,20 @@ elif args[:2] == ["bundle", "validate"]:
     print(json.dumps({"workspace": {"root_path": f"/Workspace/bundle/{target}"}}))
 elif args[:2] == ["workspace", "get-status"]:
     print(json.dumps({"object_id": 42}))
+elif args[:2] == ["serving-endpoints", "get"]:
+    ep_id = os.environ.get("FAKE_EP_ID", "")
+    print(json.dumps({"name": args[2], **({"id": ep_id} if ep_id else {})}))
 '''
 
 
-def _run(tmp_path, target):
+def _run(tmp_path, target, **extra):
     cli = tmp_path / "databricks"
     cli.write_text(FAKE_CLI)
     cli.chmod(cli.stat().st_mode | stat.S_IEXEC)
     log = tmp_path / "log.jsonl"
     env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "CLI_LOG": str(log), "PROFILE": "p",
            "CATALOG": "cat", "ENGINE_SP": UUID, "FIXER_SP": UUID, "APP_SP": UUID, "WAREHOUSE": "abc123"}
+    env.update(extra)
     if target:
         env["DATABRICKS_BUNDLE_TARGET"] = target
     out = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, cwd=REPO)
@@ -53,3 +57,35 @@ def test_fixer_job_comes_from_the_bundle_summary_of_the_target(tmp_path):
         assert summary[summary.index("-t") + 1] == (target or "dev")
         assert not any(c[:2] == ["jobs", "list"] for c in calls)
         (tmp_path / "log.jsonl").unlink()
+
+
+def _statements(calls):
+    return [json.loads(c[c.index("--json") + 1])["statement"] for c in calls if c[:2] == ["api", "post"]]
+
+
+def test_the_fixer_may_write_the_draft_tables(tmp_path):
+    _, calls = _run(tmp_path, None)
+    stmts = _statements(calls)
+    for t in ("drafts", "llm_calls", "llm_status"):
+        assert f"GRANT MODIFY ON TABLE cat.lakebase_health.{t} TO `{UUID}`" in stmts, t
+
+
+def test_the_fixer_gets_can_query_on_a_custom_endpoint(tmp_path):
+    stdout, calls = _run(tmp_path, None, FAKE_EP_ID="ep-123", LLM_ENDPOINT="my-model")
+    [get] = [c for c in calls if c[:2] == ["serving-endpoints", "get"]]
+    assert get[2] == "my-model"
+    [upd] = [c for c in calls if c[:2] == ["serving-endpoints", "update-permissions"]]
+    assert upd[2] == "ep-123"
+    acl = json.loads(upd[upd.index("--json") + 1])["access_control_list"]
+    assert acl == [{"service_principal_name": UUID, "permission_level": "CAN_QUERY"}]
+    assert "CAN_QUERY on my-model" in stdout
+
+
+def test_a_foundation_model_endpoint_needs_no_grant_and_llm_off_skips_it(tmp_path):
+    stdout, calls = _run(tmp_path, None)
+    assert [c[2] for c in calls if c[:2] == ["serving-endpoints", "get"]] == ["databricks-claude-opus-5-5"]
+    assert not any(c[:2] == ["serving-endpoints", "update-permissions"] for c in calls)
+    assert "foundation model endpoint" in stdout
+    (tmp_path / "log.jsonl").unlink()
+    stdout, calls = _run(tmp_path, None, LLM_ENABLED="false")
+    assert not any(c[0] == "serving-endpoints" for c in calls) and "no model endpoint grant" in stdout
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd engine && .venv/bin/python -m pytest tests/unit/test_bundle.py tests/unit/test_fixer_grants.py -v`
Expected: FAIL (`KeyError: 'llm_endpoint'`; no MODIFY on `drafts`; no `serving-endpoints` calls).

- [ ] **Step 3: Implement**

`databricks.yml` (apply this change):

```diff
--- a/databricks.yml
+++ b/databricks.yml
@@ -35,6 +35,12 @@ variables:
   engine_enabled:
     description: Set to "false" to stop all engine work
     default: "true"
+  llm_endpoint:
+    description: Serving endpoint (llm/v1/chat) that drafts query rewrites; the fixer SP needs CAN_QUERY on it
+    default: databricks-claude-opus-5-5
+  llm_enabled:
+    description: Set to "false" to turn LLM query rewrites off (the app hides "Draft a rewrite")
+    default: "true"
 
 artifacts:
   engine:
```

`resources/fixer_job.yml` (apply this change):

```diff
--- a/resources/fixer_job.yml
+++ b/resources/fixer_job.yml
@@ -14,7 +14,8 @@ resources:
           python_wheel_task:
             package_name: lakebase_health
             entry_point: fix
-            parameters: ["--catalog", "${var.catalog}", "--schema", "${var.schema}", "--pii-masking", "${var.pii_masking}"]
+            parameters: ["--catalog", "${var.catalog}", "--schema", "${var.schema}", "--pii-masking", "${var.pii_masking}",
+                         "--llm-endpoint", "${var.llm_endpoint}", "--llm-enabled", "${var.llm_enabled}"]
       environments:
         - environment_key: engine
           spec:
@@ -24,3 +25,4 @@ resources:
               - psycopg[binary]>=3.1
               - databricks-sdk>=0.81.0
               - pyyaml>=6
+              - sqlglot>=30.24,<31
```

`resources/engine_job.yml` (apply this change):

```diff
--- a/resources/engine_job.yml
+++ b/resources/engine_job.yml
@@ -27,3 +27,4 @@ resources:
               - psycopg[binary]>=3.1
               - databricks-sdk>=0.81.0
               - pyyaml>=6
+              - sqlglot>=30.24,<31
```

`resources/analysis_job.yml` (apply this change):

```diff
--- a/resources/analysis_job.yml
+++ b/resources/analysis_job.yml
@@ -29,3 +29,4 @@ resources:
               - psycopg[binary]>=3.1
               - databricks-sdk>=0.81.0
               - pyyaml>=6
+              - sqlglot>=30.24,<31
```

`resources/app.yml` (apply this change):

```diff
--- a/resources/app.yml
+++ b/resources/app.yml
@@ -23,5 +23,9 @@ resources:
             value: ${var.pii_masking}
           - name: FIXER_JOB_ID
             value: ${resources.jobs.lakebase_health_fixer.id}
+          - name: LLM_ENABLED  # "false" hides "Draft a rewrite"; the fixer records the endpoint's status in llm_status
+            value: ${var.llm_enabled}
+          - name: LLM_ENDPOINT
+            value: ${var.llm_endpoint}
       lifecycle:
         started: true
```

`scripts/fixer_grants.sh` (apply this change):

```diff
--- a/scripts/fixer_grants.sh
+++ b/scripts/fixer_grants.sh
@@ -31,7 +31,7 @@ q() {
 
 q "GRANT USE CATALOG ON CATALOG $CATALOG TO \`$FIXER_SP\`"
 q "GRANT USE SCHEMA, SELECT ON SCHEMA $CATALOG.lakebase_health TO \`$FIXER_SP\`"
-for t in proposed_actions action_runs ledger shadow_log; do
+for t in proposed_actions action_runs ledger shadow_log drafts llm_calls llm_status; do
   q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.$t TO \`$FIXER_SP\`"
 done
 q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.approvals TO \`$APP_SP\`"
@@ -53,4 +53,23 @@ DIR_ID=$(databricks workspace get-status "$ROOT" --profile "$PROFILE" -o json |
 databricks permissions update directories "$DIR_ID" --profile "$PROFILE" \
   --json "{\"access_control_list\":[{\"service_principal_name\":\"$ENGINE_SP\",\"permission_level\":\"CAN_READ\"},{\"service_principal_name\":\"$FIXER_SP\",\"permission_level\":\"CAN_READ\"}]}" >/dev/null \
   || { echo "FAILED: folder grant on $ROOT" >&2; exit 1; }
+# LLM query rewrites: the fixer SP queries the model endpoint. A Databricks-hosted foundation model endpoint has
+# no permissions object (no id): workspace users can query it unless an admin restricted it.
+LLM_ENDPOINT="${LLM_ENDPOINT:-databricks-claude-opus-5-5}"
+if [[ "${LLM_ENABLED:-true}" == "false" ]]; then
+  echo "LLM_ENABLED=false: no model endpoint grant"
+else
+  EP_ID=$(databricks serving-endpoints get "$LLM_ENDPOINT" --profile "$PROFILE" -o json \
+    | python3 -c 'import json,sys;print(json.load(sys.stdin).get("id") or "")') \
+    || { echo "FAILED: serving endpoint $LLM_ENDPOINT not found" >&2; exit 1; }
+  if [[ -n "$EP_ID" ]]; then
+    databricks serving-endpoints update-permissions "$EP_ID" --profile "$PROFILE" \
+      --json "{\"access_control_list\":[{\"service_principal_name\":\"$FIXER_SP\",\"permission_level\":\"CAN_QUERY\"}]}" >/dev/null \
+      || { echo "FAILED: CAN_QUERY on $LLM_ENDPOINT" >&2; exit 1; }
+    echo "fixer SP has CAN_QUERY on $LLM_ENDPOINT"
+  else
+    echo "note: $LLM_ENDPOINT is a Databricks-hosted foundation model endpoint without a permissions object;" \
+      "the fixer SP can query it unless an admin restricted foundation models"
+  fi
+fi
 echo "grants done (target $TARGET, job $JOB, folder $ROOT)"
```

- [ ] **Step 4: Run the tests and validate the bundle**

Run: `cd engine && .venv/bin/python -m pytest -q && bash -n ../scripts/fixer_grants.sh`
Expected: all pass.
Run: `databricks bundle validate --var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 --var warehouse_id=0123456789abcdef --var fixer_sp=<fixer app id> --profile my-profile`
Expected: `Validation OK!`
Run: `cd engine && .venv/bin/python -m pip wheel . --no-deps -w /tmp/lbh_wheel && unzip -l /tmp/lbh_wheel/lakebase_health-*.whl | grep rewrite_v1.md`
Expected: `lakebase_health/fixer/draft/prompts/rewrite_v1.md` is in the wheel.

- [ ] **Step 5: Commit**

```bash
git add databricks.yml resources/fixer_job.yml resources/engine_job.yml resources/analysis_job.yml resources/app.yml scripts/fixer_grants.sh engine/tests/unit/test_bundle.py engine/tests/unit/test_fixer_grants.py
git commit -m "feat(bundle): llm_endpoint and llm_enabled; sqlglot in the job environments; fixer grants for drafting

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 11: App server — draft view, draft requests, advisory rewrite actions

**Files:**
- Create: `app/server/lib/drafts.ts`, `app/server/queries/finding_drafts.sql`, `app/server/queries/draft_requests.sql`, `app/server/queries/llm_status.sql`, `app/server/tests/drafts.test.ts`
- Modify: `app/server/writes/insert_approval.sql`, `app/server/queries/finding_actions.sql`, `app/server/queries/project_actions.sql`, `app/server/lib/handlers.ts`, `app/server/routes.ts`, `app/server/server.ts`

**Interfaces:**
- Consumes: tables and columns from Task 3; `draft_token`/state names from Task 4 (mirrored, and a test reads `states.py` to keep them equal); existing `canSeeText`, `queryLabel`, `skeleton`, `isOpen`, `approvals.canManage/insert/trigger/newId`.
- Produces:
  - `drafts.ts`: `REWRITE_CHECKS`, `RUNNING`, `FINAL`, `draftToken(fp, attempt)`, `findingQueryId(checkId, evidence, object)`, `setUpOf(enabled, statusRow) -> SetUp`, `requestState(attempts, requests) -> {pending, latestFinal, nextAttempt, refused}`, `type DraftView`.
  - `GET /api/findings/:fp/drafts` → `DraftView = {rewritable, setUp: {ready, reason}, canManage, canRequest, pending, refused: {reason, at} | null, nextAttempt, original: QueryLabel | null, attempts: Row[]}` (attempts newest first; each with `draft_id, attempt, state, reason, rationale, expected_plan_change, requested_by, requested_at, ended_at, endpoint, prompt_version, sql, report, error, adoption_report`; `sql` is the skeleton unless `canSeeText`).
  - `POST /api/findings/:fp/drafts` → 202 `{approvalId, attempt, message?}`; 403 for non-managers; 409 not rewritable / not set up / finding not open / a draft pending or running; 404 when the fix loop is not installed or the finding is hidden.
  - `Deps.llm?: {enabled: boolean, endpoint: string}` (from `LLM_ENABLED` / `LLM_ENDPOINT`).
  - `GET /api/actions/:id` on a `query_rewrite` action returns `requests: []` and masked `sql`; `POST /api/actions/:id/requests` on it → 409. Every approval insert binds `fingerprint`. The Fixes page and the finding's fix card list `origin = 'check'` actions only.

- [ ] **Step 1: Write the failing test**

`app/server/tests/drafts.test.ts` (new file, complete):

```ts
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { Access } from "../lib/access";
import { draftToken, findingQueryId, FINAL, requestState, REWRITE_CHECKS, RUNNING, setUpOf } from "../lib/drafts";
import { createHandlers } from "../lib/handlers";

const HC = "11111111-2222-4333-8444-555555555555";
const FP = "f".repeat(32);
const AID = "d".repeat(32);
const REWRITE = "SELECT o.id FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 AND o.status <> 'void' ORDER BY o.id";
const ctx = { token: "t", user: "me@x.com", now: new Date("2026-10-29T12:00:00Z") };
const finding = { fingerprint: FP, project_id: HC, check_id: "top_db_time_query", endpoint_id: "ep-a", object: "42",
                  status: "open", evidence: '{"queryid": "42"}' };
const attempt = (n: number, state: string, o: Record<string, unknown> = {}) => ({
  draft_id: `${n}`.repeat(32), attempt: String(n), state, reason: null, requested_at: `2026-10-29T1${n}:00:00Z`,
  sql: REWRITE, report: null, ...o,
});

function setup(o: {
  manager?: boolean; pii?: boolean; ready?: boolean; enabled?: boolean; attempts?: Record<string, unknown>[];
  requests?: Record<string, unknown>[]; finding?: Record<string, unknown> | null; started?: boolean;
} = {}) {
  const writes: Record<string, string>[] = [];
  const calls: { name: string; params: Record<string, unknown> }[] = [];
  const access: Access = { userKey: "me@x.com", userName: "me@x.com", projectIds: [HC],
                           projectNames: { [HC]: "healthcheck" }, pii: !!o.pii };
  const h = createHandlers({
    access: { async resolve() { return access; }, refresh() {} },
    runner: {
      async run(name, _ids, params = {}) {
        calls.push({ name, params });
        if (name === "finding") return o.finding === null ? [] : [o.finding ?? finding];
        if (name === "finding_drafts") return o.attempts ?? [];
        if (name === "draft_requests") return o.requests ?? [];
        if (name === "llm_status") return o.ready === undefined || o.ready ? [{ endpoint: "m", ready: "true" }]
          : [{ endpoint: "m", ready: "false", reason: "serving endpoint m is NOT_READY" }];
        if (name === "query_text") return [{ endpoint_id: "ep-a", queryid: "42", plan_text: "SELECT x FROM t WHERE a = 'secret'" }];
        if (name === "action") return [{ action_id: AID, fingerprint: FP, project_id: HC, kind: "query_rewrite",
                                         state: "verified", sql: REWRITE, verify_on_branch: "true" }];
        return [];
      },
    },
    scheduleMinutes: 60,
    approvals: {
      canManage: async () => o.manager ?? true,
      insert: async (row) => { writes.push(row); },
      trigger: async () => o.started,
      newId: () => "ap-9",
    },
    llm: { enabled: o.enabled ?? true, endpoint: "m" },
  });
  return { h, writes, calls };
}

describe("draft helpers mirror the engine", () => {
  it("lists the same rewritable checks and states as fixer/draft/states.py", () => {
    const py = readFileSync(join(__dirname, "..", "..", "..", "engine", "src", "lakebase_health", "fixer", "draft",
                                 "states.py"), "utf8");
    const tuple = (name: string) => [...(new RegExp(`${name} = \\(([^)]*)\\)`, "s").exec(py)?.[1] ?? "")
      .matchAll(/"([a-z_]+)"/g)].map((m) => m[1]);
    expect(tuple("REWRITE_CHECKS")).toEqual(REWRITE_CHECKS);
    expect(tuple("RUNNING")).toEqual(RUNNING);
    expect(tuple("FINAL")).toEqual(FINAL);
    expect(draftToken(FP, 2)).toBe(createHash("sha256").update(`draft:${FP}:2`).digest("hex"));
  });

  it("finds the query of each rewritable check", () => {
    expect(findingQueryId("top_db_time_query", '{"queryid": "42"}', "42")).toBe("42");
    expect(findingQueryId("disk_spill", "{}", "-17@app")).toBe("-17");
    expect(findingQueryId("row_misestimate", '{"top_queries": [{"queryid": "7"}]}', "ep-a")).toBe("7");
    expect(findingQueryId("missing_fk_index", '{"queryid": "42"}', "x")).toBeNull();
  });

  it("explains why drafting is not set up", () => {
    expect(setUpOf(false, { ready: "true" })).toMatchObject({ ready: false, reason: expect.stringMatching(/turned off/) });
    expect(setUpOf(true, undefined)).toMatchObject({ ready: false, reason: expect.stringMatching(/not checked/) });
    expect(setUpOf(true, { ready: "false", reason: "no CAN_QUERY" })).toEqual({ ready: false, reason: "no CAN_QUERY" });
    expect(setUpOf(true, { ready: true })).toEqual({ ready: true, reason: null });
  });

  it("derives pending, final, the next attempt and the last refusal", () => {
    const s = requestState([attempt(2, "verify_failed"), attempt(1, "verified")],
                           [{ requested_at: "2026-10-29T13:00:00Z", outcome: "stale:attempt_changed" },
                            { requested_at: "2026-10-29T12:00:00Z", outcome: "claimed" }]);
    expect(s).toEqual({ pending: false, latestFinal: true, nextAttempt: 3,
                        refused: { reason: "attempt_changed", at: "2026-10-29T13:00:00Z" } });
    expect(requestState([attempt(1, "verifying")], [{ outcome: null }])).toMatchObject({ pending: true, latestFinal: false });
    expect(requestState([], [])).toEqual({ pending: false, latestFinal: true, nextAttempt: 1, refused: null });
  });
});

describe("drafts handler", () => {
  it("returns the card data for a manager, newest attempt first, with the original query", async () => {
    const view = await setup({ pii: true, attempts: [attempt(2, "verified"), attempt(1, "draft_failed")] }).h.drafts(ctx, FP);
    expect(view).toMatchObject({ rewritable: true, setUp: { ready: true }, canManage: true, canRequest: true,
                                 pending: false, nextAttempt: 3 });
    expect(view.original).toMatchObject({ queryid: "42", kind: "full", text: "SELECT x FROM t WHERE a = 'secret'" });
    expect(view.attempts.map((a) => a.attempt)).toEqual(["2", "1"]);
    expect(view.attempts[0].sql).toBe(REWRITE);
  });

  it("masks the rewrite for users who may not read query text", async () => {
    const view = await setup({ attempts: [attempt(1, "verified")] }).h.drafts(ctx, FP);
    expect(view.attempts[0].sql).toBe("SELECT o.id FROM hc_test.orders o WHERE o.id BETWEEN $1 AND $2 AND o.status <> ? ORDER BY o.id");
    expect(view.original).toMatchObject({ kind: "skeleton", text: "SELECT x FROM t WHERE a = ?" });
  });

  it("offers no request while one is pending or running, to non-managers, or when not set up", async () => {
    expect((await setup({ requests: [{ outcome: null }] }).h.drafts(ctx, FP)).canRequest).toBe(false);
    expect((await setup({ attempts: [attempt(1, "drafting")] }).h.drafts(ctx, FP)).canRequest).toBe(false);
    expect((await setup({ manager: false }).h.drafts(ctx, FP))).toMatchObject({ canRequest: false, canManage: false });
    expect((await setup({ ready: false }).h.drafts(ctx, FP))).toMatchObject({
      canRequest: false, setUp: { ready: false, reason: "serving endpoint m is NOT_READY" } });
    expect((await setup({ enabled: false }).h.drafts(ctx, FP)).setUp.ready).toBe(false);
    const other = await setup({ finding: { ...finding, check_id: "missing_fk_index" } }).h.drafts(ctx, FP);
    expect(other).toMatchObject({ rewritable: false, canRequest: false, original: null });
  });

  it("404s a hidden finding and 400s a malformed fingerprint", async () => {
    await expect(setup({ finding: null }).h.drafts(ctx, FP)).rejects.toMatchObject({ status: 404 });
    await expect(setup().h.drafts(ctx, "nope")).rejects.toMatchObject({ status: 400 });
  });
});

describe("draft requests", () => {
  it("records a draft approval for the next attempt and starts the fixer", async () => {
    const s = setup({ attempts: [attempt(1, "verify_failed")] });
    expect(await s.h.requestDraft(ctx, FP)).toEqual({ approvalId: "ap-9", attempt: 2 });
    expect(s.writes).toEqual([{ approval_id: "ap-9", action_id: "", fingerprint: FP, request: "draft",
                                sql_hash: draftToken(FP, 2), approver: "me@x.com", note: "" }]);
  });

  it("says the schedule will pick it up when the job could not start", async () => {
    expect(await setup({ started: false }).h.requestDraft(ctx, FP)).toMatchObject({ message: expect.stringMatching(/15-minute/) });
  });

  it("refuses non-managers with 403 and impossible requests with 409, writing nothing", async () => {
    const cases: [Parameters<typeof setup>[0], number][] = [
      [{ manager: false }, 403], [{ ready: false }, 409], [{ requests: [{ outcome: null }] }, 409],
      [{ attempts: [attempt(1, "verifying")] }, 409], [{ finding: { ...finding, status: "resolved" } }, 409],
      [{ finding: { ...finding, check_id: "missing_fk_index" } }, 409],
    ];
    for (const [o, status] of cases) {
      const s = setup(o);
      await expect(s.h.requestDraft(ctx, FP)).rejects.toMatchObject({ status });
      expect(s.writes).toHaveLength(0);
    }
  });
});

describe("rewrite actions through the fix routes", () => {
  it("refuses fix requests on a rewrite and offers none", async () => {
    const s = setup({ pii: true });
    expect(await s.h.action(ctx, AID)).toMatchObject({ requests: [] });
    await expect(s.h.request(ctx, AID, { request: "verify" })).rejects.toMatchObject({ status: 409 });
    expect(s.writes).toHaveLength(0);
  });

  it("masks a rewrite's SQL on the action route too", async () => {
    expect((await setup().h.action(ctx, AID)).action.sql).toContain("o.status <> ?");
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd app && npx vitest run server/tests/drafts.test.ts`
Expected: FAIL — `Cannot find module '../lib/drafts'`.

- [ ] **Step 3: Implement**

`app/server/lib/drafts.ts` (new file, complete):

```ts
import { createHash } from "node:crypto";
import type { QueryLabel } from "./querytext";
import type { Row } from "./sql";

// Mirror of engine/src/lakebase_health/fixer/draft/states.py.
export const REWRITE_CHECKS = [
  "top_db_time_query", "query_regression", "plan_flip", "storage_wait", "disk_spill", "sort_limit_waste",
  "row_misestimate",
];
export const RUNNING = ["drafting", "verifying"];
export const FINAL = ["draft_failed", "verified", "verify_failed", "adopted", "not_adopted"];

/** What a draft approval's sql_hash holds: the attempt it asks for (states.draft_token). */
export const draftToken = (fingerprint: string, attempt: number) =>
  createHash("sha256").update(`draft:${fingerprint}:${attempt}`).digest("hex");

const QUERYID = /^-?\d{1,20}$/;
const isObj = (x: unknown): x is Record<string, unknown> => typeof x === "object" && x !== null && !Array.isArray(x);
const sv = (v: unknown) => (typeof v === "string" || typeof v === "number" ? String(v) : "");

/** The query a rewrite is drafted for (states.finding_queryid): evidence.queryid, row_misestimate's top query, the object. */
export function findingQueryId(checkId: unknown, evidence: unknown, object: unknown): string | null {
  if (typeof checkId !== "string" || !REWRITE_CHECKS.includes(checkId)) return null;
  let ev: unknown = {};
  try {
    ev = typeof evidence === "string" ? JSON.parse(evidence) : (evidence ?? {});
  } catch {
    ev = {};
  }
  const e = isObj(ev) ? ev : {};
  let q: unknown;
  if (checkId === "row_misestimate") {
    const top = Array.isArray(e.top_queries) ? e.top_queries : [];
    q = isObj(top[0]) ? top[0].queryid : undefined;
  } else {
    q = "queryid" in e ? e.queryid : sv(object).split("@")[0];
  }
  const s = sv(q);
  return QUERYID.test(s) ? s : null;
}

export type SetUp = { ready: boolean; reason: string | null };

/** Whether drafting can run: LLM_ENABLED, and the fixer's last probe of the endpoint (llm_status). */
export function setUpOf(enabled: boolean, status: Row | undefined): SetUp {
  if (!enabled) return { ready: false, reason: "LLM drafting is turned off (llm_enabled is false)." };
  if (!status) return { ready: false, reason: "The fixer has not checked the model endpoint yet." };
  if (status.ready !== true && status.ready !== "true")
    return { ready: false, reason: sv(status.reason) || "The model endpoint is not ready." };
  return { ready: true, reason: null };
}

export type RequestState = {
  pending: boolean;
  latestFinal: boolean;
  nextAttempt: number;
  refused: { reason: string; at: string } | null;
};

/**
 * From the attempts (newest first) and the draft approvals (newest first, with the outcome of the run that consumed
 * them): an unconsumed request is pending; the newest refused request after the latest attempt says why.
 */
export function requestState(attempts: Row[], requests: Row[]): RequestState {
  const latest = attempts[0];
  const since = latest ? sv(latest.requested_at) : "";
  const refusedRow = requests.find((r) => sv(r.outcome).startsWith("stale:") && sv(r.requested_at) > since);
  return {
    pending: requests.some((r) => r.outcome === null || r.outcome === undefined),
    latestFinal: !latest || FINAL.includes(sv(latest.state)),
    nextAttempt: (latest ? Number(latest.attempt) : 0) + 1,
    refused: refusedRow ? { reason: sv(refusedRow.outcome).slice("stale:".length), at: sv(refusedRow.requested_at) } : null,
  };
}

export type DraftView = {
  rewritable: boolean;
  setUp: SetUp;
  canManage: boolean;
  canRequest: boolean;
  pending: boolean;
  refused: { reason: string; at: string } | null;
  nextAttempt: number;
  original: QueryLabel | null;
  attempts: Row[];
};
```

`app/server/queries/finding_drafts.sql` (new file, complete):

```sql
WITH v AS (
  SELECT action_id, max_by(report, started_at) AS report, max_by(error, started_at) AS error
  FROM {catalog}.lakebase_health.action_runs
  WHERE kind = 'verify' AND outcome <> 'claimed'
  GROUP BY action_id
), adoption AS (
  SELECT action_id, max_by(report, started_at) AS adoption_report
  FROM {catalog}.lakebase_health.action_runs
  WHERE kind = 'postcheck'
  GROUP BY action_id
)
SELECT d.draft_id, d.attempt, d.state, d.reason, d.rationale, d.expected_plan_change, d.requested_by,
       d.requested_at, d.ended_at, d.endpoint, d.prompt_version, a.sql, v.report, v.error, ad.adoption_report
FROM {catalog}.lakebase_health.drafts d
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = d.finding_fingerprint
LEFT JOIN {catalog}.lakebase_health.proposed_actions a ON a.action_id = d.action_id
LEFT JOIN v ON v.action_id = d.action_id
LEFT JOIN adoption ad ON ad.action_id = d.action_id
WHERE d.finding_fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY d.attempt DESC
LIMIT 20
```

`app/server/queries/draft_requests.sql` (new file, complete):

```sql
SELECT ap.approval_id, ap.requested_at, ap.approver, r.outcome
FROM {catalog}.lakebase_health.approvals ap
JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = ap.fingerprint
LEFT JOIN {catalog}.lakebase_health.action_runs r ON r.approval_id = ap.approval_id
WHERE ap.fingerprint = :fingerprint AND ap.request = 'draft' AND array_contains(split(:project_ids, ','), f.project_id)
ORDER BY ap.requested_at DESC
LIMIT 20
```

`app/server/queries/llm_status.sql` (new file, complete):

```sql
-- Not project data; :project_ids keeps every server query scoped (an empty allow-list never reaches here).
SELECT endpoint, enabled, ready, reason, checked_at
FROM {catalog}.lakebase_health.llm_status
WHERE endpoint = :endpoint AND size(split(:project_ids, ',')) > 0
```

`app/server/writes/insert_approval.sql` (apply this change):

```diff
--- a/app/server/writes/insert_approval.sql
+++ b/app/server/writes/insert_approval.sql
@@ -1,2 +1,4 @@
-INSERT INTO {catalog}.lakebase_health.approvals (approval_id, action_id, request, sql_hash, approver, requested_at, note)
-VALUES (:approval_id, :action_id, :request, :sql_hash, :approver, current_timestamp(), :note)
+INSERT INTO {catalog}.lakebase_health.approvals (approval_id, action_id, fingerprint, request, sql_hash, approver,
+                                                  requested_at, note)
+VALUES (:approval_id, NULLIF(:action_id, ''), NULLIF(:fingerprint, ''), :request, :sql_hash, :approver,
+        current_timestamp(), :note)
```

`app/server/queries/finding_actions.sql` (apply this change):

```diff
--- a/app/server/queries/finding_actions.sql
+++ b/app/server/queries/finding_actions.sql
@@ -10,6 +10,7 @@ SELECT a.action_id, a.kind, a.sql, a.rollback_sql, a.rollback_note, a.risk, a.re
 FROM {catalog}.lakebase_health.proposed_actions a
 JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
 LEFT JOIN sl ON sl.action_id = a.action_id
-WHERE a.fingerprint = :fingerprint AND array_contains(split(:project_ids, ','), f.project_id)
+WHERE a.fingerprint = :fingerprint AND coalesce(a.origin, 'check') = 'check'
+  AND array_contains(split(:project_ids, ','), f.project_id)
 ORDER BY a.created_at DESC
 LIMIT 5
```

`app/server/queries/project_actions.sql` (apply this change):

```diff
--- a/app/server/queries/project_actions.sql
+++ b/app/server/queries/project_actions.sql
@@ -4,6 +4,7 @@ SELECT a.action_id, a.fingerprint, a.check_id, a.kind, a.risk, a.reversible, a.v
 FROM {catalog}.lakebase_health.proposed_actions a
 JOIN {catalog}.lakebase_health.findings f ON f.fingerprint = a.fingerprint
 WHERE f.project_id = :project_id AND array_contains(split(:project_ids, ','), f.project_id)
+  AND coalesce(a.origin, 'check') = 'check'
   AND (:state = '' OR a.state = :state)
 QUALIFY row_number() OVER (
   PARTITION BY a.state
```

`app/server/lib/handlers.ts` (apply this change):

```diff
--- a/app/server/lib/handlers.ts
+++ b/app/server/lib/handlers.ts
@@ -1,8 +1,9 @@
 import { AccessError, canSeeText, type Access } from "./access";
 import { blockRetry, hasRollback, isRequestKind, legalRequests, sqlHash } from "./approvals";
+import { draftToken, findingQueryId, requestState, setUpOf, type DraftView } from "./drafts";
 import { groupFindings, isOpen, page, type GroupRow } from "./group";
 import { maskRows } from "./mask";
-import { parseQueryPairs, queryLabel } from "./querytext";
+import { parseQueryPairs, queryLabel, skeleton } from "./querytext";
 import type { Row } from "./sql";
 import { parseActionId, parseEndpointId, parseFingerprint, parseProjectId, parseWindow } from "./validate";
 
@@ -12,6 +13,8 @@ type Deps = {
   runner: { run(name: string, ids: string[], params?: Record<string, string | Date>): Promise<Row[]> };
   scheduleMinutes: number;
   approvals?: Approvals;
+  /** LLM query rewrites: LLM_ENABLED and LLM_ENDPOINT. Absent means not set up. */
+  llm?: { enabled: boolean; endpoint: string };
 };
 
 /** Approval writes and checks; absent means the fix loop is not installed and every action is read-only. */
@@ -78,7 +81,10 @@ function projectSections(findings: Row[]) {
   return { categories, sections };
 }
 
-export function createHandlers({ access, runner, scheduleMinutes, approvals }: Deps) {
+const REWRITE = "query_rewrite";
+const ADVISORY = "a query rewrite is advisory: change it in your application code";
+
+export function createHandlers({ access, runner, scheduleMinutes, approvals, llm }: Deps) {
   async function who(ctx: Ctx): Promise<Access> {
     try {
       return await access.resolve(ctx.token, ctx.user);
@@ -112,6 +118,50 @@ export function createHandlers({ access, runner, scheduleMinutes, approvals }: D
     return { a, id, action };
   }
 
+  /** Shown text of a rewrite: the full SQL for callers who may read query text, else its skeleton. */
+  const rewriteText = (sql: unknown, full: boolean) =>
+    typeof sql === "string" ? (full ? sql : skeleton(sql)) : null;
+
+  async function draftView(ctx: Ctx, a: Access, finding: Row): Promise<DraftView> {
+    const fp = String(finding.fingerprint);
+    const project = String(finding.project_id);
+    const queryid = findingQueryId(finding.check_id, finding.evidence, finding.object);
+    const endpoint = sv(finding.endpoint_id);
+    const full = canSeeText(a, project);
+    const [attempts, requests, status, text, manager] = await Promise.all([
+      runner.run("finding_drafts", a.projectIds, { fingerprint: fp }),
+      runner.run("draft_requests", a.projectIds, { fingerprint: fp }),
+      runner.run("llm_status", a.projectIds, { endpoint: llm?.endpoint ?? "" }),
+      queryid && endpoint
+        ? runner.run("query_text", a.projectIds, { project_id: project, pairs: `${endpoint}:${queryid}` })
+        : Promise.resolve([] as Row[]),
+      manages(ctx, a, project),
+    ]);
+    const setUp = setUpOf(!!llm?.enabled, status[0]);
+    const req = requestState(attempts, requests);
+    const rewritable = queryid !== null;
+    return {
+      rewritable,
+      setUp,
+      canManage: manager,
+      canRequest: rewritable && setUp.ready && manager && isOpen(finding) && !req.pending && req.latestFinal,
+      pending: req.pending,
+      refused: req.refused,
+      nextAttempt: req.nextAttempt,
+      original: queryid && endpoint ? queryLabel(text[0] ?? { endpoint_id: endpoint, queryid }, full) : null,
+      attempts: attempts.map((r) => ({ ...r, sql: rewriteText(r.sql, full) })),
+    };
+  }
+
+  async function visibleFinding(ctx: Ctx, rawFp: string) {
+    const a = await who(ctx);
+    const fp = parseFingerprint(rawFp);
+    if (!fp) throw new HttpError(400, "invalid fingerprint");
+    const [finding] = await runner.run("finding", a.projectIds, { fingerprint: fp });
+    if (!finding) throw new HttpError(404, "not found");
+    return { a, fp, finding };
+  }
+
   return {
     async meta(ctx: Ctx) {
       const a = await who(ctx);
@@ -285,9 +335,12 @@ export function createHandlers({ access, runner, scheduleMinutes, approvals }: D
         manages(ctx, a, String(action.project_id)),
       ]);
       const retry = String(action.state) === "blocked" ? blockRetry(runs) : null;
+      const rewrite = action.kind === REWRITE;
       return {
-        action, runs, canManage, retryAfterEnrolment: retry !== null,
-        requests: legalRequests(String(action.state), isTrue(action.verify_on_branch), retry, hasRollback(action.rollback_sql)),
+        action: rewrite ? { ...action, sql: rewriteText(action.sql, canSeeText(a, String(action.project_id))) } : action,
+        runs, canManage, retryAfterEnrolment: retry !== null,
+        requests: rewrite ? []
+          : legalRequests(String(action.state), isTrue(action.verify_on_branch), retry, hasRollback(action.rollback_sql)),
       };
     },
 
@@ -298,6 +351,7 @@ export function createHandlers({ access, runner, scheduleMinutes, approvals }: D
       if (!isRequestKind(req)) throw new HttpError(400, "invalid request");
       if (!(await approvals.canManage(ctx, a, String(action.project_id))))
         throw new HttpError(403, "only the project owner or a manager can approve");
+      if (action.kind === REWRITE) throw new HttpError(409, ADVISORY);
       const state = String(action.state);
       const retry = state === "blocked" ? blockRetry(await runner.run("action_runs", a.projectIds, { action_id: id })) : null;
       if (!legalRequests(state, isTrue(action.verify_on_branch), retry, hasRollback(action.rollback_sql)).includes(req))
@@ -308,6 +362,7 @@ export function createHandlers({ access, runner, scheduleMinutes, approvals }: D
       await approvals.insert({
         approval_id: approvalId,
         action_id: id,
+        fingerprint: sv(action.fingerprint),
         request: req,
         sql_hash: sqlHash(typeof target === "string" ? target : ""),
         approver: a.userName ?? ctx.user ?? "",
@@ -317,6 +372,38 @@ export function createHandlers({ access, runner, scheduleMinutes, approvals }: D
       return started === false ? { approvalId, message: SCHEDULED } : { approvalId };
     },
 
+    /** The rewrite card of a finding: set-up, who may draft, the original query, every attempt (newest first). */
+    async drafts(ctx: Ctx, rawFp: string) {
+      const { a, finding } = await visibleFinding(ctx, rawFp);
+      return draftView(ctx, a, finding);
+    },
+
+    /** "Draft a rewrite" / Retry: records a draft approval for the next attempt and starts the fixer. */
+    async requestDraft(ctx: Ctx, rawFp: string) {
+      if (!approvals) throw new HttpError(404, "not found");
+      const { a, fp, finding } = await visibleFinding(ctx, rawFp);
+      if (!(await approvals.canManage(ctx, a, String(finding.project_id))))
+        throw new HttpError(403, "only the project owner or a manager can draft a rewrite");
+      const view = await draftView(ctx, a, finding);
+      if (!view.rewritable) throw new HttpError(409, "this finding has no query to rewrite");
+      if (!view.setUp.ready) throw new HttpError(409, `LLM drafting not set up: ${view.setUp.reason ?? ""}`);
+      if (!isOpen(finding)) throw new HttpError(409, "the finding is no longer open");
+      if (!view.canRequest) throw new HttpError(409, "a draft is already running for this finding");
+      const approvalId = approvals.newId();
+      await approvals.insert({
+        approval_id: approvalId,
+        action_id: "",
+        fingerprint: fp,
+        request: "draft",
+        sql_hash: draftToken(fp, view.nextAttempt),
+        approver: a.userName ?? ctx.user ?? "",
+        note: "",
+      });
+      const started = await approvals.trigger();
+      const out = { approvalId, attempt: view.nextAttempt };
+      return started === false ? { ...out, message: SCHEDULED } : out;
+    },
+
     refresh(ctx: Ctx) {
       if (ctx.user) access.refresh(ctx.user);
     },
```

`app/server/routes.ts` (apply this change):

```diff
--- a/app/server/routes.ts
+++ b/app/server/routes.ts
@@ -43,6 +43,8 @@ export function registerRoutes(app: express.Application, h: Handlers) {
     send(res, () => h.request(ctxOf(req), param(req.params.id), req.body as unknown), 202),
   );
   app.get("/api/findings/:fp", (req, res) => send(res, () => h.finding(ctxOf(req), param(req.params.fp))));
+  app.get("/api/findings/:fp/drafts", (req, res) => send(res, () => h.drafts(ctxOf(req), param(req.params.fp))));
+  app.post("/api/findings/:fp/drafts", (req, res) => send(res, () => h.requestDraft(ctxOf(req), param(req.params.fp)), 202));
   app.get("/api/findings/:fp/related", (req, res) => send(res, () => h.findingRelated(ctxOf(req), param(req.params.fp))));
   app.get("/api/endpoints/:id/timeline", (req, res) =>
     send(res, () => h.timeline(ctxOf(req), param(req.params.id), req.query.from, req.query.to)),
```

`app/server/server.ts` (apply this change):

```diff
--- a/app/server/server.ts
+++ b/app/server/server.ts
@@ -55,6 +55,10 @@ createApp({
       runner,
       scheduleMinutes: Number(process.env.SCHEDULE_MINUTES ?? "60"),
       approvals,
+      llm: {
+        enabled: process.env.LLM_ENABLED?.trim().toLowerCase() !== "false",
+        endpoint: process.env.LLM_ENDPOINT ?? "databricks-claude-opus-5-5",
+      },
     });
     appkit.server.extend((app) => registerRoutes(app, handlers));
   },
```

`server/tests/readonly.test.ts` needs no change: the new queries start with SELECT/WITH, contain `:project_ids`, and `server/writes` still holds only `insert_approval.sql`.

- [ ] **Step 4: Run the tests, types and lint**

Run: `cd app && npx vitest run && npx tsc -b tsconfig.server.json && npx eslint server`
Expected: all pass, no lint errors.

- [ ] **Step 5: Commit**

```bash
git add app/server
git commit -m "feat(app): draft view and draft requests per finding; rewrite actions are advisory and masked

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 12: App client — the rewrite card on the finding page

**Files:**
- Create: `app/client/src/lib/diff.ts`, `app/client/src/lib/diff.test.ts`, `app/client/src/views/DraftCard.tsx`, `app/client/src/views/DraftCard.test.tsx`
- Modify: `app/client/src/views/Finding.tsx`

**Interfaces:**
- Consumes: `GET`/`POST /api/findings/:fp/drafts` and `DraftView` (Task 11); the verify report keys from Task 8 (`sets`, `rules`, `plan`); existing `StatusBadge`, `CodeBlock`, `DataTable`, `NUM`, `InlineLoading`, `ApiMessage`, `useApi`, theme tokens (`border-l-coral`, `text-sev-ok-fg`, `text-sev-critical-fg`, `bg-code`).
- Produces: `wordDiff(before, after) -> DiffPart[]` (`{kind: "same" | "removed" | "added", text}`); `DraftCard({data: DraftData, onRequest, busy})`; `DraftPanel({fingerprint})` (loads, posts, reloads; renders nothing for non-rewritable findings); `FindingView` gains an optional `draftCard` prop, which `FindingPage` fills with `<DraftPanel>`.

Card behaviour: title "Query rewrite (advisory)" with the latest attempt's state; not set up → "LLM drafting not set up." with the reason and no button; non-manager → "Only the project owner or a manager can draft a rewrite."; manager → "Draft a rewrite" (no attempts) or "Retry", shown as a disabled "Drafting…" while a request is pending or the latest attempt is drafting/verifying. The selected attempt (latest by default) shows its reason, rationale, rewritten SQL with Copy, a word diff against the original, the per-set equivalence/timing table, the rules, the plan change, the advisory note and the adoption outcome; "Attempts" lists every attempt.

- [ ] **Step 1: Write the failing tests**

`app/client/src/lib/diff.test.ts` (new file, complete):

```ts
import { describe, expect, it } from "vitest";
import { wordDiff } from "./diff";

describe("wordDiff", () => {
  it("keeps common words and marks removed and added runs", () => {
    expect(wordDiff("SELECT a FROM t WHERE x = $1", "SELECT a FROM t JOIN u ON u.id = t.id WHERE x = $1")).toEqual([
      { kind: "same", text: "SELECT a FROM t" },
      { kind: "added", text: "JOIN u ON u.id = t.id" },
      { kind: "same", text: "WHERE x = $1" },
    ]);
    expect(wordDiff("a b c", "a c")).toEqual([
      { kind: "same", text: "a" }, { kind: "removed", text: "b" }, { kind: "same", text: "c" },
    ]);
  });

  it("ignores whitespace differences and falls back on very long text", () => {
    expect(wordDiff("SELECT  a\nFROM t", "SELECT a FROM t")).toEqual([{ kind: "same", text: "SELECT a FROM t" }]);
    const long = Array.from({ length: 1600 }, (_, i) => `w${i}`).join(" ");
    expect(wordDiff(long, "x").map((p) => p.kind)).toEqual(["removed", "added"]);
  });
});
```

`app/client/src/views/DraftCard.test.tsx` (new file, complete):

```tsx
// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DraftCard, type DraftData } from "./DraftCard";

const ADVISORY = "Change it in your application code; this tool does not deploy rewrites.";

const ORIGINAL = "SELECT o.id FROM hc_test.orders o WHERE o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2) ORDER BY o.id";
const REWRITE = "WITH a AS (SELECT avg(amount) AS m FROM hc_test.orders) SELECT o.id FROM hc_test.orders o, a WHERE o.amount > a.m ORDER BY o.id";
const report = JSON.stringify({
  sets: [{ set: 1, rows_original: 1803, rows_rewrite: 1803, same_checksum: true, original_ms: 512.5, rewrite_ms: 2.25 }],
  rules: [{ rule: "equivalence", passed: true, detail: "1 of 1 parameter sets: same rows in order" },
          { rule: "speed", passed: true, detail: "median 512.5 -> 2.25 ms, 227.78x faster (needs 1.2x)" },
          { rule: "plan", passed: true, warning: false, detail: "removed SubPlan; added Hash Join" }],
  plan: { removed: ["SubPlan"], added: ["Hash Join"], expected_nodes: ["Hash Join", "SubPlan"], found: ["Hash Join", "SubPlan"] },
});
const verified = { draft_id: "2".repeat(32), attempt: "2", state: "verified", reason: null, requested_by: "owner@x",
                   requested_at: "2026-10-29T12:00:00Z", rationale: "Computes the average once.", sql: REWRITE, report };
const failed = { draft_id: "1".repeat(32), attempt: "1", state: "draft_failed", reason: "invalid_response: not JSON",
                 requested_by: "owner@x", requested_at: "2026-10-29T11:00:00Z", sql: null, report: null };
const base: DraftData = {
  rewritable: true, setUp: { ready: true, reason: null }, canManage: true, canRequest: true, pending: false,
  refused: null, nextAttempt: 1,
  original: { endpoint_id: "ep-a", queryid: "42", text: ORIGINAL, label: ORIGINAL, kind: "full", truncated: false },
  attempts: [],
};

afterEach(cleanup);

function card(o: Partial<DraftData> = {}) {
  const onRequest = vi.fn();
  render(<MemoryRouter><DraftCard data={{ ...base, ...o }} onRequest={onRequest} /></MemoryRouter>);
  return onRequest;
}

describe("DraftCard", () => {
  it("a manager drafts the first rewrite", () => {
    const onRequest = card();
    fireEvent.click(screen.getByRole("button", { name: "Draft a rewrite" }));
    expect(onRequest).toHaveBeenCalledOnce();
  });

  it("role holder sees no button, with the reason", () => {
    card({ canManage: false, canRequest: false });
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    expect(screen.getByText("Only the project owner or a manager can draft a rewrite.")).toBeTruthy();
  });

  it("not set up: no button and the reason", () => {
    card({ setUp: { ready: false, reason: "serving endpoint m is NOT_READY" }, canRequest: false });
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    expect(screen.getByText("LLM drafting not set up.")).toBeTruthy();
    expect(screen.getByText(/NOT_READY/)).toBeTruthy();
  });

  it("a verified rewrite shows its SQL, diff, evidence and the advisory note, with Retry", () => {
    const onRequest = card({ attempts: [verified], nextAttempt: 3 });
    expect(screen.getByText("verified")).toBeTruthy();
    expect(screen.getByText("Computes the average once.")).toBeTruthy();
    expect(screen.getByText(REWRITE).tagName).toBe("PRE");
    expect(screen.getByLabelText("Diff with the original").querySelector("ins")?.textContent).toContain("WITH");
    expect(screen.getByText("1803", { selector: "td:nth-child(2)" })).toBeTruthy();
    expect(screen.getByText("512.5")).toBeTruthy();
    expect(screen.getByText("2.25")).toBeTruthy();
    expect(screen.getByText(/removed SubPlan; added Hash Join\. Expected Hash Join, SubPlan; found/)).toBeTruthy();
    expect(screen.getByText(ADVISORY)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRequest).toHaveBeenCalledOnce();
  });

  it("while a draft runs, Retry is disabled", () => {
    card({ attempts: [{ ...verified, state: "verifying", report: null }], canRequest: false });
    expect(screen.getByRole("button", { name: "Drafting…" }).hasAttribute("disabled")).toBe(true);
    cleanup();
    card({ attempts: [verified], pending: true, canRequest: false });
    expect(screen.getByRole("button", { name: "Drafting…" }).hasAttribute("disabled")).toBe(true);
  });

  it("a failed draft shows why", () => {
    card({ attempts: [failed], nextAttempt: 2 });
    expect(screen.getByText("invalid_response: not JSON")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Retry" })).toBeTruthy();
  });

  it("history lists every attempt and shows the one picked", () => {
    card({ attempts: [verified, failed], nextAttempt: 3 });
    expect(screen.getByRole("button", { name: "Attempt 2" })).toBeTruthy();
    expect(screen.queryByText("invalid_response: not JSON")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Attempt 1" }));
    expect(screen.getByText("invalid_response: not JSON")).toBeTruthy();
  });

  it("shows the adoption outcome and a refused request", () => {
    card({ attempts: [{ ...verified, state: "adopted" }], refused: { reason: "attempt_changed", at: "2026-10-29T13:00:00Z" } });
    expect(screen.getByText("Adopted: the workload now runs the rewritten query.")).toBeTruthy();
    expect(screen.getByText("The last request was not run: attempt_changed.")).toBeTruthy();
  });

  it("renders nothing for a finding without a query", () => {
    const { container } = render(<MemoryRouter><DraftCard data={{ ...base, rewritable: false }} onRequest={vi.fn()} /></MemoryRouter>);
    expect(container.textContent).toBe("");
  });
});
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd app && npx vitest run client/src/lib/diff.test.ts client/src/views/DraftCard.test.tsx`
Expected: FAIL — modules not found.

- [ ] **Step 3: Implement**

`app/client/src/lib/diff.ts` (new file, complete):

```ts
export type DiffPart = { kind: "same" | "removed" | "added"; text: string };

const MAX_TOKENS = 1500; // LCS is O(n·m); longer queries fall back to "everything changed"

/** Word-level diff (LCS over whitespace-separated tokens) of an original query and its rewrite. */
export function wordDiff(before: string, after: string): DiffPart[] {
  const a = before.split(/\s+/).filter(Boolean);
  const b = after.split(/\s+/).filter(Boolean);
  if (a.length > MAX_TOKENS || b.length > MAX_TOKENS) {
    const all: DiffPart[] = [{ kind: "removed", text: a.join(" ") }, { kind: "added", text: b.join(" ") }];
    return all.filter((p) => p.text);
  }
  const lcs: number[][] = Array.from({ length: a.length + 1 }, () => new Array<number>(b.length + 1).fill(0));
  for (let i = a.length - 1; i >= 0; i--)
    for (let j = b.length - 1; j >= 0; j--)
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
  const out: DiffPart[] = [];
  const push = (kind: DiffPart["kind"], text: string) => {
    const last = out[out.length - 1];
    if (last && last.kind === kind) last.text += ` ${text}`;
    else out.push({ kind, text });
  };
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      push("same", a[i]);
      i++;
      j++;
    } else if (lcs[i + 1][j] >= lcs[i][j + 1]) push("removed", a[i++]);
    else push("added", b[j++]);
  }
  while (i < a.length) push("removed", a[i++]);
  while (j < b.length) push("added", b[j++]);
  return out;
}
```

`app/client/src/views/DraftCard.tsx` (new file, complete):

```tsx
import {
  Button,
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@databricks/appkit-ui/react";
import { Copy } from "lucide-react";
import { useState } from "react";
import { ApiMessage } from "../ApiMessage";
import { StatusBadge } from "../components/Severity";
import { CodeBlock, DataTable, InlineLoading, NUM } from "../components/ui";
import { wordDiff } from "../lib/diff";
import type { QueryLabel } from "../lib/findings";
import { fmtTime, str, type Row } from "../lib/format";
import { useApi } from "../lib/useApi";

export type DraftData = {
  rewritable: boolean;
  setUp: { ready: boolean; reason: string | null };
  canManage: boolean;
  canRequest: boolean;
  pending: boolean;
  refused: { reason: string; at: string } | null;
  nextAttempt: number;
  original: QueryLabel | null;
  attempts: Row[];
};

const RUNNING = new Set(["drafting", "verifying"]);
const ADVISORY = "Change it in your application code; this tool does not deploy rewrites.";
const ADOPTION: Record<string, string> = {
  adopted: "Adopted: the workload now runs the rewritten query.",
  not_adopted: "Not adopted: the rewritten query was not seen in the 7 days after verification.",
};

type SetRow = { set: number; rows_original: number | null; rows_rewrite: number | null; same_checksum: boolean;
  original_ms: number | null; rewrite_ms: number | null };
type Rule = { rule: string; passed: boolean; warning: boolean; detail: string };
type Report = { sets: SetRow[]; rules: Rule[]; plan: { removed: string[]; added: string[]; expected_nodes: string[]; found: string[] } | null };

const num = (v: unknown) => (typeof v === "number" ? v : null);
const ms = (v: number | null) => (v === null ? "—" : String(Math.round(v * 100) / 100));
const names = (v: unknown) => (Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : []);

function parseDraftReport(raw: unknown): Report | null {
  if (typeof raw !== "string") return null;
  try {
    const o = JSON.parse(raw) as Record<string, unknown>;
    const list = (v: unknown) =>
      Array.isArray(v) ? v.filter((x): x is Record<string, unknown> => typeof x === "object" && x !== null) : [];
    const p = typeof o.plan === "object" && o.plan !== null ? (o.plan as Record<string, unknown>) : null;
    return {
      sets: list(o.sets).map((s) => ({ set: Number(s.set), rows_original: num(s.rows_original), rows_rewrite: num(s.rows_rewrite),
        same_checksum: s.same_checksum === true, original_ms: num(s.original_ms), rewrite_ms: num(s.rewrite_ms) })),
      rules: list(o.rules).map((r) => ({ rule: str(r.rule), passed: r.passed === true, warning: r.warning === true, detail: str(r.detail) })),
      plan: p ? { removed: names(p.removed), added: names(p.added), expected_nodes: names(p.expected_nodes), found: names(p.found) } : null,
    };
  } catch {
    return null;
  }
}

function Diff({ before, after }: { before: string; after: string }) {
  return (
    <p className="rounded-md border bg-code px-3 py-2 font-mono text-[0.8125rem] leading-relaxed" aria-label="Diff with the original">
      {wordDiff(before, after)
        .map((p, n) => ({ ...p, id: `${n}:${p.kind}` }))
        .map((p) =>
          p.kind === "same" ? <span key={p.id}>{p.text} </span>
            : p.kind === "removed" ? <del key={p.id} className="text-sev-critical-fg">{p.text} </del>
              : <ins key={p.id} className="text-sev-ok-fg no-underline">{p.text} </ins>)}
    </p>
  );
}

function AttemptDetail({ attempt, original }: { attempt: Row; original: QueryLabel | null }) {
  const report = parseDraftReport(attempt.report);
  const sql = attempt.sql === null || attempt.sql === undefined ? null : str(attempt.sql);
  const state = str(attempt.state);
  return (
    <div className="space-y-3">
      {attempt.reason ? <p className="text-sm text-sev-critical-fg">{str(attempt.reason)}</p> : null}
      {attempt.error ? <p className="text-sm text-sev-critical-fg">{str(attempt.error)}</p> : null}
      {attempt.rationale ? <p className="text-sm">{str(attempt.rationale)}</p> : null}
      {sql !== null && (
        <>
          <div className="flex items-center justify-between gap-2">
            <h3 className="text-sm font-semibold">Rewritten query</h3>
            <Button variant="outline" size="sm" onClick={() => void navigator.clipboard?.writeText(sql)}>
              <Copy />
              Copy SQL
            </Button>
          </div>
          <CodeBlock>{sql}</CodeBlock>
          {original?.text ? <Diff before={original.text} after={sql} /> : null}
          <p className="text-sm font-medium">{ADVISORY}</p>
        </>
      )}
      {report && report.sets.length > 0 && (
        <div className="overflow-hidden rounded-md border bg-card">
          <DataTable>
            <TableHeader>
              <TableRow>
                <TableHead>Parameter set</TableHead>
                <TableHead className="text-right">Rows (original)</TableHead>
                <TableHead className="text-right">Rows (rewrite)</TableHead>
                <TableHead>Same results</TableHead>
                <TableHead className="text-right">Original (ms)</TableHead>
                <TableHead className="text-right">Rewrite (ms)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {report.sets.map((s) => (
                <TableRow key={s.set}>
                  <TableCell>set {s.set}</TableCell>
                  <TableCell className={NUM}>{s.rows_original ?? "—"}</TableCell>
                  <TableCell className={NUM}>{s.rows_rewrite ?? "—"}</TableCell>
                  <TableCell><StatusBadge status={s.same_checksum ? "pass" : "fail"} label={s.same_checksum ? "same" : "different"} /></TableCell>
                  <TableCell className={NUM}>{ms(s.original_ms)}</TableCell>
                  <TableCell className={NUM}>{ms(s.rewrite_ms)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </DataTable>
        </div>
      )}
      {report && report.rules.length > 0 && (
        <ul className="space-y-1 text-sm">
          {report.rules.map((r) => (
            <li key={r.rule} className="flex flex-wrap items-center gap-2">
              <StatusBadge status={r.passed ? (r.warning ? "blocked" : "pass") : "fail"} label={r.passed ? (r.warning ? "warning" : "pass") : "fail"} />
              <span className="font-mono text-[0.8125rem]">{r.rule}</span>
              <span>{r.detail}</span>
            </li>
          ))}
        </ul>
      )}
      {report?.plan && (
        <p className="text-sm">
          Plan: removed {report.plan.removed.join(", ") || "nothing"}; added {report.plan.added.join(", ") || "nothing"}.
          Expected {report.plan.expected_nodes.join(", ") || "no named plan node"}
          {report.plan.found.length ? `; found ${report.plan.found.join(", ")}.` : "; not found in the new plan."}
        </p>
      )}
      {ADOPTION[state] ? <p className="text-sm font-medium">{ADOPTION[state]}</p> : null}
    </div>
  );
}

export function DraftCard({ data, onRequest, busy = false }: { data: DraftData; onRequest: () => void; busy?: boolean }) {
  const [selected, setSelected] = useState<string | null>(null);
  if (!data.rewritable) return null;
  const latest = data.attempts[0];
  const shown = data.attempts.find((a) => str(a.draft_id) === selected) ?? latest;
  const running = data.pending || (latest !== undefined && RUNNING.has(str(latest.state)));
  return (
    <Card className="gap-4 border-l-4 border-l-coral py-5 shadow-sm">
      <CardHeader className="px-5">
        <CardTitle className="flex flex-wrap items-center gap-2">
          <h2 className="text-base font-semibold">Query rewrite (advisory)</h2>
          {latest ? <StatusBadge status={latest.state} /> : null}
          {data.pending ? <StatusBadge status="running" label="requested" /> : null}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4 px-5">
        {!data.setUp.ready ? (
          <p className="text-sm">
            <span className="font-medium">LLM drafting not set up.</span> {data.setUp.reason}
          </p>
        ) : !latest ? (
          <p className="text-sm text-muted-foreground">
            A model drafts an equivalent query from the query shape, plan and table definitions (no values), and the
            fixer verifies it on a branch: same results, faster.
          </p>
        ) : null}
        {data.refused ? <p className="text-sm text-muted-foreground">The last request was not run: {data.refused.reason}.</p> : null}
        {shown ? <AttemptDetail attempt={shown} original={data.original} /> : null}
        {data.attempts.length > 1 && (
          <div className="space-y-1">
            <h3 className="text-sm font-semibold">Attempts</h3>
            <ul className="divide-y text-sm">
              {data.attempts.map((a) => (
                <li key={str(a.draft_id)} className="flex flex-wrap items-center gap-2 py-1.5">
                  <Button variant="ghost" size="sm" aria-pressed={a === shown} onClick={() => setSelected(str(a.draft_id))}>
                    Attempt {str(a.attempt)}
                  </Button>
                  <StatusBadge status={a.state} />
                  <span className="text-muted-foreground">{str(a.requested_by)} · {fmtTime(a.requested_at)}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </CardContent>
      {data.setUp.ready && (
        <CardFooter className="flex flex-wrap justify-end gap-2 border-t px-5 pt-4 [.border-t]:pt-4">
          {data.canManage ? (
            <Button disabled={busy || running || !data.canRequest} onClick={onRequest}>
              {running ? "Drafting…" : latest ? "Retry" : "Draft a rewrite"}
            </Button>
          ) : (
            <p className="text-sm text-muted-foreground">Only the project owner or a manager can draft a rewrite.</p>
          )}
        </CardFooter>
      )}
    </Card>
  );
}

/** Loads the rewrite card of a finding and records draft requests; reloads after each one. */
export function DraftPanel({ fingerprint }: { fingerprint: string }) {
  const [version, setVersion] = useState(0);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const r = useApi<DraftData>(`/api/findings/${fingerprint}/drafts?v=${version}`);
  const request = () => {
    setBusy(true);
    setNotice(null);
    fetch(`/api/findings/${fingerprint}/drafts`, { method: "POST" })
      .then(async (res) => {
        const body = (await res.json().catch(() => ({}))) as { error?: string; message?: string };
        setNotice(res.ok ? (body.message ?? "Request recorded; the fixer is drafting.") : (body.error ?? res.statusText));
      })
      .catch((e: unknown) => setNotice(String(e)))
      .finally(() => {
        setBusy(false);
        setVersion((v) => v + 1);
      });
  };
  if (r.error) return <ApiMessage error={r.error} />;
  if (!r.data) return <InlineLoading label="Loading rewrite" />;
  if (!r.data.rewritable) return null;
  return (
    <div className="space-y-2">
      <DraftCard data={r.data} onRequest={request} busy={busy} />
      {notice && (
        <p role="status" className="rounded-md border bg-card px-3 py-2 text-sm">
          {notice}
        </p>
      )}
    </div>
  );
}
```

`app/client/src/views/Finding.tsx` (apply this change):

```diff
--- a/app/client/src/views/Finding.tsx
+++ b/app/client/src/views/Finding.tsx
@@ -15,6 +15,7 @@ import { useApi } from "../lib/useApi";
 import { useQueryLabels } from "../lib/useQueryLabels";
 import { pageRows, useShowAll } from "../lib/useShowAll";
 import { ActionPanel } from "./ActionCard";
+import { DraftPanel } from "./DraftCard";
 
 export type FindingData = { finding: Row; history: Row[]; actions: Row[]; related: Page<GroupRow> };
 
@@ -58,8 +59,11 @@ function Fact({ label, children }: { label: string; children: ReactNode }) {
   );
 }
 
-/** `actionCard` is the live approval card for the finding's latest action (rendered by the page, not in tests). */
-export function FindingView({ data, actionCard }: { data: FindingData; actionCard?: ReactNode }) {
+/**
+ * `actionCard` is the live approval card for the finding's latest action, `draftCard` the query rewrite card (both
+ * rendered by the page, not in tests).
+ */
+export function FindingView({ data, actionCard, draftCard }: { data: FindingData; actionCard?: ReactNode; draftCard?: ReactNode }) {
   const f = data.finding;
   const action = data.actions[0];
   // No action, or one expired (e.g. outside the fixer allowlist): the SQL is for a person to run
@@ -185,6 +189,8 @@ export function FindingView({ data, actionCard }: { data: FindingData; actionCar
 
       {actionCard}
 
+      {draftCard}
+
       {action && (
         <SectionCard title="Shadow mode">
           <p className="text-sm">
@@ -240,5 +246,11 @@ export default function FindingPage() {
   if (r.error) return <ApiMessage error={r.error} />;
   if (!r.data) return <LoadingState label="Loading finding" />;
   const action = r.data.actions[0];
-  return <FindingView data={r.data} actionCard={action ? <ActionPanel actionId={str(action.action_id)} /> : undefined} />;
+  return (
+    <FindingView
+      data={r.data}
+      actionCard={action ? <ActionPanel actionId={str(action.action_id)} /> : undefined}
+      draftCard={<DraftPanel fingerprint={str(r.data.finding.fingerprint)} />}
+    />
+  );
 }
```

- [ ] **Step 4: Run the tests, types and lint**

Run: `cd app && npx vitest run && npx tsc -b tsconfig.client.json && npx eslint client/src`
Expected: all pass; no new lint errors.

- [ ] **Step 5: Commit**

```bash
git add app/client/src/lib/diff.ts app/client/src/lib/diff.test.ts app/client/src/views/DraftCard.tsx app/client/src/views/DraftCard.test.tsx app/client/src/views/Finding.tsx
git commit -m "feat(app): rewrite card with Draft/Retry, diff, verification evidence, attempt history

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 13: Planted query, workload modes and documentation

**Files:**
- Modify: `scripts/workload.py`, `scripts/seed_test_workload.sql`, `engine/src/lakebase_health/checks/telemetry/plan_flip.yaml`, `README.md`, `DESIGN.md`, `HEALTH_CHECKS.md`
- Regenerate: `docs/CHECKS.md` (and the README checks summary), `notebooks/lakebase_health_check.py`

**Interfaces:**
- Produces: `workload.PLANTED_QUERY` (the correlated subquery on `hc_test.orders`, parameters `$1`, `$2` = an id range of 2,000), `run_text(conn, sql, values)` (libpq `exec_params` with untyped text parameters, so the query id matches the branch's `PREPARE`), `--mode all|rewrite`, `--rewrite-file PATH`. In `--mode all` the planted query runs alongside the existing workload; `--mode rewrite` runs only it (or the rewrite file) in 2 sessions, so it dominates database time.

Why this query: it touches only the `orders` primary key (always present), so the fix loop's index changes on `hc_test.orders` do not change its cost; the subquery is re-run for each of ~1,800 outer rows over the same 2,000-id range (~3.6 M row visits, roughly 0.5 s), while computing the per-status average once is equivalent (the outer rows and the subquery read the same range) and needs ~4,000 row visits.

- [ ] **Step 1: Add the planted query and the workload modes**

`scripts/workload.py` (apply this change):

```diff
--- a/scripts/workload.py
+++ b/scripts/workload.py
@@ -1,7 +1,12 @@
 """Generate runtime signals on the seeded hc_test schema so telemetry-based checks have data.
 
 Usage: python3 scripts/workload.py --profile my-profile \
-         --endpoint projects/healthcheck/branches/production/endpoints/primary --minutes 10
+         --endpoint projects/healthcheck/branches/production/endpoints/primary --minutes 10 [--mode all|rewrite]
+         [--rewrite-file rewrite.sql]
+
+--mode rewrite runs only the planted correlated subquery (PLANTED_QUERY), so it dominates database time and gets a
+top_db_time_query finding for the LLM-rewrite acceptance; with --rewrite-file it runs that SQL instead (the rewrite
+copied from the app, with the same $1/$2), so the post-check can mark the rewrite adopted.
 """
 import argparse
 import hashlib
@@ -15,6 +20,35 @@ import time
 import psycopg
 
 
+# Planted for LLM query rewrites: the subquery runs once per outer row over the same id range (about 2,000 rows each),
+# so each call costs ~2,000^2 row visits; aggregating once per status is equivalent and far faster. Only the primary
+# key index is used, so the fix loop's index changes on hc_test.orders do not affect it.
+PLANTED_QUERY = (
+    "SELECT o.id, o.status, o.amount FROM hc_test.orders o "
+    "WHERE o.id BETWEEN $1 AND $2 "
+    "AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
+    "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) "
+    "ORDER BY o.id")
+ORDER_IDS = (1600000001, 1601000000)  # seed_test_workload.sql: setval 1600000000, then 1,000,000 orders
+RANGE = 2000
+
+
+def run_text(c, sql, values):
+    """Sends the text verbatim with $n placeholders and untyped text parameters (as PREPARE infers them), so the
+    statement's query id matches the one the fixer measured on the branch."""
+    res = c.pgconn.exec_params(sql.encode(), [str(v).encode() for v in values])
+    if res.status != psycopg.pq.ExecStatus.TUPLES_OK:
+        raise RuntimeError(res.error_message.decode(errors="replace"))
+
+
+def planted(dsn, stop, sql):
+    with psycopg.connect(dsn, autocommit=True, application_name="hc-rewrite") as c:
+        while not stop.is_set():
+            lo = random.randint(ORDER_IDS[0], ORDER_IDS[1] - RANGE)
+            run_text(c, sql, (lo, lo + RANGE))
+            time.sleep(1)
+
+
 def cli(*args):
     return json.loads(subprocess.check_output(["databricks", *args, "-o", "json"]))
 
@@ -110,11 +144,23 @@ def main():
     ap.add_argument("--profile", required=True)
     ap.add_argument("--endpoint", required=True)
     ap.add_argument("--minutes", type=float, default=10)
+    ap.add_argument("--mode", choices=("all", "rewrite"), default="all")
+    ap.add_argument("--rewrite-file", help="run this SQL (same $1, $2) instead of PLANTED_QUERY in --mode rewrite")
     a = ap.parse_args()
     dsn = conninfo(a.profile, a.endpoint)
     stop = threading.Event()
+    if a.mode == "rewrite":
+        sql = open(a.rewrite_file).read().strip().rstrip(";") if a.rewrite_file else PLANTED_QUERY
+        threads = [threading.Thread(target=planted, args=(dsn, stop, sql)) for _ in range(2)]
+        [t.start() for t in threads]
+        time.sleep(a.minutes * 60)
+        stop.set()
+        [t.join() for t in threads]
+        print("rewrite workload done")
+        return
     workers = [threading.Thread(target=oltp, args=(dsn, stop)) for _ in range(4)]
     workers.append(threading.Thread(target=analytics, args=(dsn, stop)))
+    workers.append(threading.Thread(target=planted, args=(dsn, stop, PLANTED_QUERY)))
     workers += [threading.Thread(target=kv_writer, args=(dsn, stop, i)) for i in range(6)]
     [w.start() for w in workers]
     end = time.time() + a.minutes * 60
```

`scripts/seed_test_workload.sql` (apply this change):

```diff
--- a/scripts/seed_test_workload.sql
+++ b/scripts/seed_test_workload.sql
@@ -55,6 +55,10 @@ ALTER TABLE orders SET (autovacuum_enabled = false);
 UPDATE orders SET status = 'refunded' WHERE id % 3 = 0;
 DELETE FROM orders WHERE id % 10 = 0;
 
+-- LLM query rewrites: scripts/workload.py PLANTED_QUERY is a correlated subquery on orders (average amount per
+-- status over an id range, recomputed for every outer row). It needs only the orders primary key, so nothing is
+-- planted here; run `workload.py --mode rewrite` to make it the top query.
+
 -- PUBLIC CREATE on a schema (check 29)
 GRANT CREATE ON SCHEMA hc_test TO PUBLIC;
```

Run: `python3 -m py_compile scripts/workload.py && python3 scripts/workload.py --help`
Expected: the help lists `--mode {all,rewrite}` and `--rewrite-file`.

- [ ] **Step 2: Drop the pg_hint_plan advice from plan_flip and regenerate the generated docs**

`engine/src/lakebase_health/checks/telemetry/plan_flip.yaml` (apply this change):

```diff
--- a/engine/src/lakebase_health/checks/telemetry/plan_flip.yaml
+++ b/engine/src/lakebase_health/checks/telemetry/plan_flip.yaml
@@ -6,7 +6,8 @@ query_scoped: true   # platform roles filtered out; see exclusions.yaml
 thresholds: {warning: 2, critical: 10}
 explain: >
   Ratio between the slowest and fastest median latency of a query's plan shapes on the same endpoint (7 days,
-  >= 5 executions per shape). Fix statistics first; pin the good plan with pg_hint_plan only as a last resort.
+  >= 5 executions per shape). Fix statistics first; if the bad plan persists, draft a query rewrite on this
+  finding (pg_hint_plan hints cannot be loaded on Lakebase without a support request).
 sql: |
   WITH p AS (
     SELECT project_id, branch_id, endpoint_id, queryid, plan_fingerprint, max(query_text) AS query_text,
@@ -16,4 +17,4 @@ sql: |
   SELECT project_id, branch_id, endpoint_id, CAST(queryid AS STRING) AS object, CAST(queryid AS STRING) AS queryid, max(query_text) AS query_text,
          count(*) AS plan_shapes, max(p50_ms) / NULLIF(min(p50_ms), 0) AS measure
   FROM p GROUP BY project_id, branch_id, endpoint_id, queryid HAVING count(*) > 1
-fix: {kind: manual, template: "ANALYZE the tables of query {object}; if the bad plan persists, pin the good one with a pg_hint_plan hint.", risk: moderate}
+fix: {kind: manual, template: "ANALYZE the tables of query {object}; if the bad plan persists, use Draft a rewrite on this finding.", risk: moderate}
```

Run (from the repo root):
```bash
engine/.venv/bin/python scripts/gen_checks_doc.py
ROLES=$(python3 -c "import re,ast;t=open('notebooks/lakebase_health_check.py').read();print(','.join(ast.literal_eval(re.search(r'^SERVICE_ROLES = (.*)$',t,re.M).group(1))))")
engine/.venv/bin/python scripts/build_notebook.py --service-roles "$ROLES"
engine/.venv/bin/python scripts/gen_checks_doc.py --check && cd engine && .venv/bin/python -m pytest -q tests/unit/test_build_notebook.py
```
Expected: `docs/CHECKS.md` and the notebook change only in the plan_flip text; `--check` exits 0; the notebook tests pass (the roles are read from the committed notebook, so `test_committed_notebook_is_current` keeps matching).

- [ ] **Step 3: README, DESIGN.md and HEALTH_CHECKS.md**

The README gains the "Query rewrites (LLM)" section (after "Fix workflow"), the two variables, the fixer SP's new grants in the identities and grants tables and in step 4's script description, and two troubleshooting rows. DESIGN.md and HEALTH_CHECKS.md replace the claim that pg_hint_plan is available with the 2026-10-29 test result.

`README.md` (apply this change):

````diff
--- a/README.md
+++ b/README.md
@@ -99,7 +99,7 @@ approved.
 | Identity | Runs | Can | Cannot |
 |---|---|---|---|
 | Engine SP (`engine_sp`) | Engine and analysis jobs | Read `system.lakebase`. Create and own `<catalog>.lakebase_health` (tables, the `mask_text` function, column masks). List the Lakebase projects it has `CAN_USE` on. Connect to enrolled branches as a role with only `pg_monitor`, in sessions with `default_transaction_read_only`, `statement_timeout = 30s` and `lock_timeout = 1s`. | Write to any Postgres database (it has no table, schema or database privileges). Wake a suspended compute. Create branches. Approve or apply fixes. |
-| Fixer SP (`fixer_sp`) | Fixer job | `SELECT` on `<catalog>.lakebase_health`; `MODIFY` on `proposed_actions`, `action_runs`, `ledger` and `shadow_log` only. Create and delete `lbh-verify-*` branches on projects where it has `CAN_MANAGE`. Run approved statements as a member of the role that owns the target tables. | Run anything outside its allowlist: `CREATE INDEX CONCURRENTLY` (non-unique), `DROP INDEX CONCURRENTLY`, `ANALYZE <table>`, `ALTER TABLE <table> SET\|RESET (autovacuum_*)`, one statement on one object, no comments or literals. Act without an approval row whose SQL hash matches. Write `findings` or `approvals`. Touch a project where it has no Postgres role. |
+| Fixer SP (`fixer_sp`) | Fixer job | `SELECT` on `<catalog>.lakebase_health`; `MODIFY` on `proposed_actions`, `action_runs`, `ledger`, `shadow_log`, `drafts`, `llm_calls` and `llm_status` only. `CAN_QUERY` on the rewrite model endpoint. Create and delete `lbh-verify-*` branches on projects where it has `CAN_MANAGE`. Run approved statements as a member of the role that owns the target tables. | Run anything outside its allowlist: `CREATE INDEX CONCURRENTLY` (non-unique), `DROP INDEX CONCURRENTLY`, `ANALYZE <table>`, `ALTER TABLE <table> SET\|RESET (autovacuum_*)`, one statement on one object, no comments or literals. Act without an approval row whose SQL hash matches. Write `findings` or `approvals`. Touch a project where it has no Postgres role. |
 | App SP (created by Databricks Apps) | The app | `SELECT` on `<catalog>.lakebase_health`. `MODIFY` on `approvals` (Unity Catalog has no insert-only privilege; every consumed approval is recorded in the hash-chained ledger). `CAN_MANAGE_RUN` on the fixer job. `CAN_USE` on the SQL warehouse. | Connect to Postgres, hold Lakebase project permissions, or write any other table. Per-user checks (project visibility, approver rights, group membership) use the signed-in user's token and GET calls only. |
 
 `CAN_MANAGE` on a project is broader than what the fixer uses (the code only creates and deletes `lbh-verify-*`
@@ -114,7 +114,8 @@ Least-privilege grants, all applied in [Setup](#setup):
 | Engine SP | `CAN_USE` on each Lakebase project to inventory | Project owner |
 | Engine SP | Postgres role with `pg_monitor`, per branch (optional) | Project owner |
 | Engine and fixer SPs | `CAN_READ` on the bundle's workspace folder (to install the wheel) | Deployer (`scripts/fixer_grants.sh`) |
-| Fixer SP | `USE CATALOG`; `USE SCHEMA` and `SELECT` on the schema; `MODIFY` on 4 tables | Catalog owner (`scripts/fixer_grants.sh`) |
+| Fixer SP | `USE CATALOG`; `USE SCHEMA` and `SELECT` on the schema; `MODIFY` on 7 tables | Catalog owner (`scripts/fixer_grants.sh`) |
+| Fixer SP | `CAN_QUERY` on `llm_endpoint` (when the endpoint has a permissions object) | Endpoint manager (`scripts/fixer_grants.sh`) |
 | Fixer SP | `CAN_MANAGE` on the project, and a Postgres role that is a member of the table-owner role, per project (optional) | Project owner |
 | App SP | `USE CATALOG`; `USE SCHEMA` and `SELECT` on the schema | Catalog owner |
 | App SP | `MODIFY` on `approvals`; `CAN_MANAGE_RUN` on the fixer job | Catalog owner (`scripts/fixer_grants.sh`) |
@@ -194,6 +195,8 @@ Variables (from `databricks.yml`):
 | `analysis_schedule` | `0 0/15 * * * ?` | Quartz cron for the analysis-only job |
 | `pii_masking` | `"true"` | `"true"` masks query text, plans and parameters for anyone outside `databricks_pii_access`. Only the exact string `"false"` turns masking off. See [PII masking](#pii-masking). |
 | `engine_enabled` | `"true"` | `"false"` makes every engine and analysis run stop at once with status `disabled` |
+| `llm_endpoint` | `databricks-claude-opus-5-5` | Serving endpoint (`llm/v1/chat`) that drafts query rewrites |
+| `llm_enabled` | `"true"` | `"false"` turns query rewrites off; the app hides **Draft a rewrite** |
 
 Targets:
 
@@ -251,7 +254,8 @@ DATABRICKS_BUNDLE_TARGET=prod PROFILE=<profile> CATALOG=<catalog> WAREHOUSE=<war
 ```
 
 The script grants the fixer `USE CATALOG`, `USE SCHEMA` and `SELECT`, plus `MODIFY` on `proposed_actions`,
-`action_runs`, `ledger` and `shadow_log`; the app SP `MODIFY` on `approvals` and `CAN_MANAGE_RUN` on the fixer
+`action_runs`, `ledger`, `shadow_log`, `drafts`, `llm_calls` and `llm_status`, and `CAN_QUERY` on `LLM_ENDPOINT`
+(default `databricks-claude-opus-5-5`; skipped with `LLM_ENABLED=false`); the app SP `MODIFY` on `approvals` and `CAN_MANAGE_RUN` on the fixer
 job; and both job SPs `CAN_READ` on the bundle folder. It works on the target in `DATABRICKS_BUNDLE_TARGET`
 (default `dev`): the fixer job is the ID `databricks bundle summary` reports for that target, and the folder is the
 target's root path from `databricks bundle validate`. Re-run it if the bundle root path changes.
@@ -452,6 +456,41 @@ rollback SQL on the card, adding `CONCURRENTLY`.
 Who can approve: the project owner, or a user with `CAN_MANAGE` on the Lakebase project. The app checks this with
 the user's own token on every click and records the approver's user name.
 
+### Query rewrites (LLM)
+
+Query findings (`top_db_time_query`, `query_regression`, `plan_flip`, `storage_wait`, `disk_spill`,
+`sort_limit_waste`, and the top query of `row_misestimate`) have a **Query rewrite (advisory)** card. The project
+owner or a manager clicks **Draft a rewrite**; the fixer job then:
+
+1. Reads the query's latest complete text, up to 3 sampled parameter sets and its slowest plan of the last 7 days
+   from the telemetry copy, and the tables' columns, constraints, indexes, row estimates and `pg_stats` summaries
+   (`n_distinct`, `null_frac`, `correlation`) from the target branch in a read-only session.
+2. Sends the model (`var.llm_endpoint`, default `databricks-claude-opus-5-5`) the query with every literal replaced
+   by a numbered parameter, and the plan and definitions with literals replaced by `?`. **No parameter values or
+   literals leave the database.** `llm_calls` keeps only hashes, token counts, latency and the request id.
+3. Checks the reply: one `SELECT`/`WITH`, only the original's tables, the same `$n` parameters, no new literals but
+   `0` and `1`, the same number of output columns, safe functions only, no data-modifying CTE, `SET`, locking
+   clause or comment. A failure ends the attempt `draft_failed` with the check's name.
+4. Verifies on `lbh-verify-<id>` (TTL 4 hours, deleted afterwards): for each parameter set, the same row count and
+   checksum (in order when the query has `ORDER BY`), the median of 3 warm runs at least 1.2x faster, no set more
+   than 10% and 1 ms slower, and the plan node changes compared with what the model expected (a mismatch is only a
+   warning).
+
+The card shows the rationale, the rewritten SQL with a copy button and a diff, the per-set results and timings, the
+plan change, and every attempt. **Retry** asks for a new attempt once the last one has finished. Nothing is
+applied: change the query in your application code. Once the endpoint has run the rewritten query 20 times, the
+next engine run marks it **adopted**; after 7 days without, **not adopted**. The rewrite SQL follows the PII
+masking rule for query text.
+
+Setup: the grants script gives the fixer SP `MODIFY` on `drafts`, `llm_calls` and `llm_status`, and `CAN_QUERY`
+on the endpoint when it has a permissions object (Databricks-hosted foundation model endpoints have none and are
+queryable by workspace users unless an admin restricted them). With `--var llm_enabled=false` the feature is off.
+Until the fixer has checked the endpoint (it does so when a draft is requested, so the first click may be refused
+with "not set up") or while the endpoint is missing, not ready or not permitted, the card says **LLM drafting not
+set up** with the reason, which is stored in `llm_status`. After upgrading, run the engine job once before using
+the app: it adds the `drafts`, `llm_calls` and `llm_status` tables and the `approvals.fingerprint`,
+`proposed_actions.origin` and `draft_id` columns the app writes and reads.
+
 ## Operations
 
 ### Schedules
@@ -545,6 +584,8 @@ test workspace for now. To turn masking back on:
 | No telemetry for a project | Lakebase observability in system tables is off for the workspace, or the endpoint is not exporting. On the endpoint, `SHOW neon_monitor.log_capture_enabled;` and `SHOW databricks.o11y_exporter_enabled;` must both be `on`; if not, restart the endpoint. |
 | A user sees no projects | They do not own a project or hold a Postgres role on one. Grant a role, then use **Refresh access**. |
 | An approve button returns 403 | Only the project owner or `CAN_MANAGE` holders can approve. |
+| The rewrite card says "LLM drafting not set up" | `llm_status.reason` says why: `llm_enabled` is `"false"`, the endpoint is missing or not READY, or the fixer SP lacks `CAN_QUERY`. Fix it, then click again (the fixer re-checks on each request). |
+| A draft ends `draft_failed: no_samples` | The query's text or parameters are `<REDACTED>` (add the fixer SP to `databricks_pii_access`) or no sampled plan in 7 days has parameter values. |
 
 ### Uninstall
````

`DESIGN.md` (apply this change):

```diff
--- a/DESIGN.md
+++ b/DESIGN.md
@@ -9,7 +9,7 @@ Goal: give every Lakebase customer a health check that (1) runs deterministic ch
 | Lakebase is fully managed and serverless. There is no host to install an agent on. | A literal "sidecar running next to Postgres" is not possible. Anything "standalone" runs elsewhere and connects over the network. |
 | `system.lakebase.*` keeps **7 days**, is account‑wide, and needs account admin + metastore admin (or grants). Statement text and plans are masked unless the reader is in `databricks_pii_access`. | Read telemetry with an identity that has those grants. Persist rollups to Delta for trends beyond 7d. Design for masked text (key on `queryid`). |
 | `pg_stat_*` and `pg_stat_statements` **reset on suspend/restart** (scale‑to‑zero). | Postgres‑side checks need a stats‑window guard and confidence labels. Prefer system tables for anything time‑based. |
-| **HypoPG, pg_repack, pg_cron, auto_explain are not available.** pg_hint_plan, pgstattuple, pgrowlocks and pg_prewarm are. | Validate index and rewrite suggestions on a **copy‑on‑write branch**, not hypothetically. Bloat fixes are limited to REINDEX CONCURRENTLY, VACUUM FULL, or swap. |
+| **HypoPG, pg_repack, pg_cron, auto_explain are not available.** pgstattuple, pgrowlocks and pg_prewarm are. pg_hint_plan is installable but its library is not loaded, and `LOAD`, `session_preload_libraries` and `pg_hint_plan.enable_hint_table` are refused for non-superusers (tested 2026-10-29), so hints have no effect without a support request. | Validate index and rewrite suggestions on a **copy‑on‑write branch**, not hypothetically. Bloat fixes are limited to REINDEX CONCURRENTLY, VACUUM FULL, or swap. |
 | **Branches are instant, copy‑on‑write, and have a TTL.** A project allows 10 unarchived branches and 3 root branches. | This is the differentiator: test every proposed fix on real data at no risk to prod, then promote. |
 | Built‑in **Insights** (LLM agent, Beta) and **Genie** already diagnose issues from the same telemetry. | Don't rebuild them. Add what they lack: **deterministic, thresholded, auditable checks**, a **fleet view across projects**, **long‑term trends and forecasts**, and **branch‑verified fixes**. Link to "Fix with Genie" where it fits. |
 | The Postgres API / SDK (`w.postgres`) exposes restore window, protection, HA group, CU range, roles' auth method and pg_settings. | Backup, security and config checks come from the API. No SQL is needed for them. |
@@ -127,7 +127,7 @@ The loop that turns findings into safe changes:
 - Write penalty: extra time on DML replay against the affected table, and added size.
 - Data quality of the evidence: stats window, sample count, and whether parameters were available (`plan_history.parameters` needs PII access; without it, use representative values and lower confidence).
 
-**What it covers:** missing and FK indexes, dropping unused or duplicate indexes (verify no plan regresses), `work_mem` per role for spills, extended statistics, pg_hint_plan pins, query rewrites, and per‑table autovacuum settings (verified over the 24h window rather than on a branch).
+**What it covers:** missing and FK indexes, dropping unused or duplicate indexes (verify no plan regresses), `work_mem` per role for spills, extended statistics, query rewrites (drafted by an LLM, advisory), and per‑table autovacuum settings (verified over the 24h window rather than on a branch).
 
 **Guardrails:**
 - Nothing auto‑applies to prod. Every action carries a stored rollback.
@@ -157,20 +157,20 @@ A container or VM outside Databricks that polls Lakebase and pushes metrics to P
 | Audience | **Self-serve product** | Option A becomes an internal test harness only. Ship B + C + D. |
 | Access | **SP can read `system.lakebase` and join `databricks_pii_access`** | Full query text, plans and parameters. Fix verification can replay real parameters. |
 | Write actions | **May create branches and apply DDL, with approval** | Option D is in scope. Prod apply stays behind an explicit per-change approval. |
-| LLM scope | **May draft query rewrites and pg_hint_plan hints** | Rewrites and hints go through the same branch verification as DDL: same results, faster, plan changed as intended. |
+| LLM scope | **May draft query rewrites** (on demand, advisory) | Rewrites go through branch verification: same results, faster, plan change reported. pg_hint_plan hints are out until Lakebase lets the library load (see §1). |
 
 Build order:
 1. **Ingestion job + check runner + findings tables (Option B core, §2a).** Calibrate thresholds on the `healthcheck` test project and 1–2 design-partner projects.
 2. **Dashboard + alerts** on `findings` and `metrics_daily`, plus forecasts (storage, XID, sequences, connections).
 3. **App (Option C):** fleet view, project drill-down, finding detail with evidence.
-4. **Fix loop (Option D):** a "Test on branch" button runs `verify_on_branch` logic, then an approval, an apply with stored rollback, and a 24h post-apply check. LLM-drafted rewrites and hints enter here as candidates.
+4. **Fix loop (Option D):** a "Test on branch" button runs `verify_on_branch` logic, then an approval, an apply with stored rollback, and a 24h post-apply check. LLM-drafted rewrites are verified the same way but stay advisory (the customer changes application code).
 
 Prototype status: `scripts/verify_on_branch.py` already runs the loop for one index. Tested on the FK finding: 72.9 ms → 0.05 ms, plan changed from Seq Scan to Index Only Scan, branch created in about 8 s, production untouched. Its write-penalty measure was noisy (one cold sample gave −33%), so it needs repeated, warmed probes before the confidence score can be trusted.
 
 ## 6. Remaining open decisions
 
 1. **Positioning with Insights/Genie:** complement them (deterministic checks, fleet view, verified fixes) or wrap them (surface Insights findings in the same console)?
-2. **Model:** which serving endpoint drafts rewrites and hints? Default proposal: Claude on Databricks Foundation Model APIs. Findings are sent as JSON; parameters are included only when the customer opts in.
+2. **Model:** decided: `databricks-claude-opus-5-5` through the serving endpoint in `var.llm_endpoint`. The model gets the normalized query, plan, DDL and statistics summaries, never parameter values or literals.
 3. **Packaging:** DABs bundle in a public repo (e.g. databricks-solutions), a Marketplace app, or internal field tooling?
 4. **Where the derived tables live:** a catalog the customer chooses at install, or a fixed `lakebase_health` schema?
 
@@ -250,7 +250,7 @@ All three ran against the same `healthcheck` project (PG 17, 1 CU), with 15 plan
 | Deterministic, tunable thresholds | No (fixed agent instructions) | Yes | Yes |
 | Validating a fix | No | HypoPG what‑if (**unavailable on Lakebase**, so marked "unverified"), then post‑apply verify and rollback | Real execution on a copy‑on‑write branch before prod |
 | Executing fixes | Via Genie, after confirmation | Mature trust‑ramped executor: shadow mode, approvals, rollback | Planned (approval → apply → 24 h verify) |
-| LLM | Yes (built in) | Optional (any OpenAI‑compatible endpoint) | Planned (rewrites and hints, verified on a branch) |
+| LLM | Yes (built in) | Optional (any OpenAI‑compatible endpoint) | Query rewrites on demand, verified on a branch, advisory |
 | Fleet / history | Per project, 7 days | Fleet mode; history in `sage` schema or a meta DB | All projects; unlimited history in Delta |
 | Effect on Lakebase cost | None | 60 s polling likely stops scale‑to‑zero (not measured) | Ingestion reads system tables, not the database; Postgres catalog checks run on a schedule |
 | Auth on Lakebase | Managed | Static password (OAuth tokens expire after 1 h; no refresh found) | OAuth token per run |
```

`HEALTH_CHECKS.md` (apply this change):

```diff
--- a/HEALTH_CHECKS.md
+++ b/HEALTH_CHECKS.md
@@ -26,7 +26,7 @@ Sources used (as of the docs dated Sep–Oct 2026):
 | Query cost | Tested: `system.lakebase` statements spent **200–3,000 s compiling** and about 1 s executing, even for `LIMIT 1`. Running 16 statements in parallel made compile time worse. Copy rows into Delta first, then run checks on the copies (DESIGN.md §2a). Don't query these tables live from an app. |
 | Planning stats | `pg_stat_statements.track_planning` is off, so `total_plan_time`/`plans` are 0 or NULL. `mean_plan_ms` will be empty. |
 | Two caches | **Postgres shared buffers** (`shared_blks_hit/read`) sit in front of the **Local File Cache (LFC)**, the "compute cache / local SSD cache" in the UI (`lfc_hits/misses`). The LFC sits in front of the remote **pageserver** (`neon_getpage_*`). The LFC hit rate is the one the UI's 99% target refers to. |
-| Extensions | Available: `pg_stat_statements`, `pg_hint_plan`, `pgstattuple`, `pgrowlocks`, `pg_prewarm`. **Not available:** `hypopg`, `pg_repack`, `pg_cron`, `auto_explain` (per‑execution plans come from `system.lakebase.plan_history` instead). |
+| Extensions | Available: `pg_stat_statements`, `pgstattuple`, `pgrowlocks`, `pg_prewarm`. `pg_hint_plan` can be created but its library is not loaded, and `LOAD`, `ALTER ROLE/DATABASE … SET session_preload_libraries` and `pg_hint_plan.enable_hint_table` are refused for non-superusers (tested 2026-10-29): hint comments do not change plans. **Not available:** `hypopg`, `pg_repack`, `pg_cron`, `auto_explain` (per‑execution plans come from `system.lakebase.plan_history` instead). |
 | Connection limits | 0.5 CU=105, 1=218, 2=443, 4=894, 8=1,795, 16=3,597, ≥24 CU=3,993. With autoscaling the limit is the limit for `min(max_CU, 8 × min_CU)`. `SHOW max_connections` reads higher because it includes reserved slots. |
 | Sizing facts | 1 CU ≈ 2 GB RAM. LFC ≤ 75% of RAM. Autoscale 0.5–32 CU, and max − min ≤ 16 CU. Fixed sizes run 36–112 CU. 8 TB logical data per branch. |
 
@@ -468,7 +468,7 @@ GROUP BY ALL ORDER BY p.endpoint_id, p.queryid, first_seen;
 **If regressed:**
 1. Correlate with `ddl_history` (check 31) and with stats changes: a stale `ANALYZE` after a bulk load is a common cause.
 2. Fix with `ANALYZE <table>`, extended statistics (`CREATE STATISTICS`) for correlated columns, or a higher `default_statistics_target` on skewed columns.
-3. As a guarded last resort, pin the good plan with **pg_hint_plan** (check 15).
+3. If the plan stays bad, rewrite the query: the app's **Draft a rewrite** verifies an LLM-drafted rewrite on a branch. (pg_hint_plan pins need its library loaded, which Lakebase refuses without a support request.)
 
 ### 15. Bad plans: disk spills, hash spills, misestimates, missed index scans
 Uses `plan_history.plan_json`, which requires `databricks_pii_access` membership.
@@ -503,14 +503,14 @@ factor). Platform roles such as `databricks_writer_<n>` are excluded.
 
 **Fix playbook (each fix should be tested on a branch, see DESIGN.md):**
 
-| Symptom | Fix | pg_hint_plan directive (guarded fallback) |
+| Symptom | Fix | pg_hint_plan directive (only if Lakebase loads the library) |
 |---|---|---|
 | Disk sort / hash batches > 1 | Raise `work_mem` **per role or session** (`ALTER ROLE app SET work_mem='64MB'`), not globally. Add an index that delivers the sort order. | `/*+ Set(work_mem "64MB") */` |
 | Seq scan on a selective predicate | Create the index (B‑tree, partial, or covering `INCLUDE`). | `/*+ IndexScan(t idx_name) */` |
 | Nested loop with a 100× underestimate | `ANALYZE`, `CREATE STATISTICS (dependencies, ndistinct)` on the correlated columns. | `/*+ HashJoin(a b) */` or `/*+ Rows(a b #10000) */` |
 | Wrong join order | Rewrite the query (CTE `MATERIALIZED`, explicit join), or fix the estimates. | `/*+ Leading((a b) c) */` |
 
-To use pg_hint_plan: `CREATE EXTENSION pg_hint_plan;`, then prefix the query with the hint comment. Alternatively, register hints in `hint_plan.hints` by `query_id` with `pg_hint_plan.enable_hint_table = on`, which pins a plan without changing app code. Treat hints as technical debt: record why each one exists, and re‑validate after major version upgrades.
+pg_hint_plan on Lakebase (tested 2026-10-29): the extension installs, but its library is not loaded and non-superusers may not `LOAD` it, add it to `session_preload_libraries` or set `pg_hint_plan.enable_hint_table`, so the directives above do nothing until a support request loads it per endpoint. Where it is loaded: `CREATE EXTENSION pg_hint_plan;`, then prefix the query with the hint comment. Alternatively, register hints in `hint_plan.hints` by `query_id` with `pg_hint_plan.enable_hint_table = on`, which pins a plan without changing app code. Treat hints as technical debt: record why each one exists, and re‑validate after major version upgrades.
 
 ### 16. Storage (pageserver) wait per query (Lakebase‑specific)
 **What:** how much of each query's time goes to fetching pages from remote storage after an LFC miss.
```

Run: `git grep -n -i "pg_hint_plan" -- README.md DESIGN.md HEALTH_CHECKS.md docs/CHECKS.md engine/src`
Expected: every remaining mention says the library is not loaded on Lakebase (or is the `hint_plan` schema exclusion); none recommends hints as available.

- [ ] **Step 4: Run all tests**

Run: `cd engine && .venv/bin/python -m pytest -q && cd ../app && npx vitest run`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/workload.py scripts/seed_test_workload.sql engine/src/lakebase_health/checks/telemetry/plan_flip.yaml docs/CHECKS.md notebooks/lakebase_health_check.py README.md DESIGN.md HEALTH_CHECKS.md
git commit -m "docs: query rewrites in the README; pg_hint_plan is not loadable on Lakebase; planted rewrite workload

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 14: Deploy and acceptance on healthcheck

**Files:**
- Create: `scripts/llm_acceptance.py`

**Interfaces:**
- Consumes: everything; `scripts/fix_acceptance.py` (`WarehouseSql`, `call`, `sp_token`), `scripts/workload.py` (`--mode rewrite`, `--rewrite-file`, `conninfo`).
- Produces: output `ALL LLM-REWRITE ACCEPTANCE CHECKS PASSED` for the spec's six criteria:
  1. Step 1 — the planted query's draft is `verified`: equivalence and speed (≥ 1.2×) pass, the plan rule has no warning, a rewrite query id is recorded.
  2. Step 2 — the test switch: `unsafe` ends `draft_failed: data_modifying_cte…`, `non_equivalent` ends `verify_failed: equivalence…`.
  3. Step 3 — Retry from the app creates the next attempt; the card lists every attempt.
  4. Step 4 — `llm_calls` has exactly the hash/count columns, `ok` rows carry 64-hex hashes and token counts, and the payload rebuilt with the fixer's code holds none of the sampled values.
  5. Step 5 — no `lbh-verify-*` branch remains, the ledger chains of the drafts verify, the test SP (a role holder, not a manager) gets 403 on POST draft.
  6. Step 6 — after the workload runs the rewritten SQL and the engine job ingests it, the rewrite is `adopted`.

- [ ] **Step 1: Write the script**

`scripts/llm_acceptance.py` (new file, complete):

```python
"""End-to-end acceptance for LLM query rewrites (spec 2026-10-29-lakebase-health-llm-rewrites-design.md, section 8).

Usage: TEST_SP_SECRET=... engine/.venv/bin/python scripts/llm_acceptance.py --profile my-profile \
         --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 \
         --test-sp-id <application id> --test-sp-secret-env TEST_SP_SECRET [--steps 0,1,2,3,4,5,6]
Needs the deployed bundle (fixer SP enrolled on healthcheck, grants script run) and the hc_test schema. Step 0 makes
the planted correlated subquery (scripts/workload.py PLANTED_QUERY) the top query: it runs the workload in
`--mode rewrite` and then the engine job, until a top_db_time_query finding exists. Steps find their finding and
attempts from the app and the tables, so a re-run resumes. Step 2 inserts draft approvals directly and starts the
fixer job itself with the acceptance-only --llm-test-response flag (never part of the bundle).
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fix_acceptance import WarehouseSql, call, sp_token  # noqa: E402
from lakebase_health import ledger  # noqa: E402
from lakebase_health.config import EngineConfig  # noqa: E402
from lakebase_health.fixer.draft import context as dcontext  # noqa: E402
from lakebase_health.fixer.draft import prompt as dprompt  # noqa: E402
from lakebase_health.fixer.draft.sqltext import normalize  # noqa: E402
from lakebase_health.fixer.draft.states import FINAL, draft_token  # noqa: E402
from lakebase_health.fixer.draft.static import shape  # noqa: E402
from lakebase_health.fixer.pg import FIXER_SETUP  # noqa: E402

from databricks.sdk import WorkspaceClient  # noqa: E402

PLANTED_MARK = "o2.status = o.status"
LLM_CALL_COLUMNS = {"call_id", "draft_id", "endpoint", "prompt_hash", "response_hash", "input_tokens", "output_tokens",
                    "latency_ms", "status", "request_id", "called_at"}


class Acceptance:
    def __init__(self, a):
        self.a = a
        self.w = WorkspaceClient(profile=a.profile)
        self.sql = WarehouseSql(self.w, a.warehouse)
        self.cfg = EngineConfig(catalog=a.catalog)
        self.url = self.w.apps.get("lakebase-health").url.rstrip("/")
        self.failures, self.drafts = [], set()
        self.project = next(p for p in self.w.postgres.list_projects() if p.uid == a.project_uid).name
        branch = next(b for b in self.w.postgres.list_branches(parent=self.project)
                      if b.name.endswith("/branches/production"))
        self.ep = f"{branch.name}/endpoints/primary"

    def owner(self):
        return self.w.config.authenticate()["Authorization"].split()[1]

    def check(self, crit, ok, msg):
        print(f"[{'PASS' if ok else 'FAIL'}] {crit}: {msg}", flush=True)
        if not ok:
            self.failures.append(f"{crit}: {msg}")
        return ok

    def job(self, suffix):
        return next(j.job_id for j in self.w.jobs.list() if j.settings.name.endswith(suffix))

    def finding(self):
        rows = self.sql.query(f"""
            SELECT fingerprint, status FROM {self.cfg.table('findings')}
            WHERE project_id = :p AND check_id = 'top_db_time_query' AND status IN ('new', 'open')
              AND query_text LIKE :mark ORDER BY last_seen DESC""", {"p": self.a.project_uid, "mark": f"%{PLANTED_MARK}%"})
        if not rows:
            raise RuntimeError("no open top_db_time_query finding for the planted query; run step 0")
        return rows[0]["fingerprint"]

    def view(self, fp, token=None):
        status, body = call(f"{self.url}/api/findings/{fp}/drafts", token or self.owner())
        if status != 200:
            raise RuntimeError(f"GET drafts -> {status} {body}")
        return body

    def wait_attempt(self, fp, attempt, timeout=45 * 60):
        """Waits until attempt `attempt` exists and is final; returns it (with its parsed report)."""
        t = time.time()
        while time.time() - t < timeout:
            v = self.view(fp)
            mine = [x for x in v["attempts"] if int(x["attempt"]) == attempt]
            if mine and mine[0]["state"] in FINAL and not v["pending"]:
                self.drafts.add(mine[0]["draft_id"])
                return {**mine[0], "report": json.loads(mine[0]["report"] or "{}")}
            if v.get("refused") and not v["pending"] and not mine:
                raise RuntimeError(f"request refused: {v['refused']}")
            time.sleep(30)
        raise RuntimeError(f"attempt {attempt} not final after {timeout} s")

    def request(self, fp, token=None):
        return call(f"{self.url}/api/findings/{fp}/drafts", token or self.owner(), {})

    def _state(self, fp, draft_id):
        return next(x["state"] for x in self.view(fp)["attempts"] if x["draft_id"] == draft_id)

    @staticmethod
    def rule(report, name):
        return next((r for r in report.get("rules", []) if r.get("rule") == name), {})

    def step0(self):
        """Makes the planted query the top query: the rewrite workload, then a full engine run."""
        if self._finding_or_none():
            print("planted finding already open; skipping the workload")
            return
        subprocess.run([sys.executable, str(ROOT / "scripts" / "workload.py"), "--profile", self.a.profile,
                        "--endpoint", self.ep, "--minutes", str(self.a.workload_minutes), "--mode", "rewrite"],
                       check=True)
        run = self.w.jobs.run_now(job_id=self.job("lakebase-health-engine")).result(timeout=timedelta(minutes=90))
        self.check("0", run.state.result_state.value == "SUCCESS", f"engine run {run.run_id}")
        self.check("0", self._finding_or_none() is not None, "planted query has a top_db_time_query finding")

    def _finding_or_none(self):
        try:
            return self.finding()
        except RuntimeError:
            return None

    def step1(self):
        fp = self.finding()
        v = self.view(fp)
        self.check("1", v["setUp"]["ready"], f"drafting set up: {v['setUp']}")
        s, body = self.request(fp)
        self.check("1", s == 202, f"draft request -> {s} {body}")
        d = self.wait_attempt(fp, body["attempt"])
        r = d["report"]
        self.check("1", d["state"] == "verified", f"attempt {d['attempt']} is {d['state']}: {d.get('reason')}")
        self.check("1", self.rule(r, "equivalence").get("passed"), f"equivalence: {self.rule(r, 'equivalence')}")
        self.check("1", self.rule(r, "speed").get("passed"), f"speed (>= 1.2x): {self.rule(r, 'speed')}")
        plan = self.rule(r, "plan")
        self.check("1", plan.get("passed") and not plan.get("warning"), f"plan changed as described: {plan}")
        self.check("1", bool(r.get("rewrite_queryid")), f"rewrite query id recorded: {r.get('rewrite_queryid')}")

    def _switch_run(self, fp, response):
        """A draft approval for the next attempt, then one fixer run with the test switch."""
        job = self.job("lakebase-health-fixer")
        while list(self.w.jobs.list_runs(job_id=job, active_only=True)):
            time.sleep(20)
        while time.gmtime().tm_min % 15 in (14, 0):  # keep clear of the 15-minute schedule
            time.sleep(20)
        nxt = self.view(fp)["nextAttempt"]
        me = self.w.current_user.me().user_name
        self.sql.execute(f"INSERT INTO {self.cfg.table('approvals')} (approval_id, action_id, fingerprint, request, "
                         "sql_hash, approver, requested_at, note) VALUES (:id, NULL, :fp, 'draft', :h, :me, "
                         "current_timestamp(), :note)",
                         {"id": uuid.uuid4().hex, "fp": fp, "h": draft_token(fp, nxt), "me": me,
                          "note": f"acceptance test switch {response}"})
        params = list(self.w.jobs.get(job).settings.tasks[0].python_wheel_task.parameters)
        run = self.w.jobs.run_now(job_id=job, python_params=params + ["--llm-test-response", response])
        run = run.result(timeout=timedelta(minutes=60))
        self.check("2", run.state.result_state.value == "SUCCESS", f"fixer run {run.run_id} with {response}")
        return self.wait_attempt(fp, nxt)

    def step2(self):
        fp = self.finding()
        d = self._switch_run(fp, "unsafe")
        self.check("2", d["state"] == "draft_failed" and str(d["reason"]).startswith("data_modifying_cte"),
                   f"unsafe reply -> {d['state']}: {d['reason']}")
        d = self._switch_run(fp, "non_equivalent")
        self.check("2", d["state"] == "verify_failed" and str(d["reason"]).startswith("equivalence"),
                   f"non-equivalent reply -> {d['state']}: {d['reason']}")

    def step3(self):
        fp = self.finding()
        before = self.view(fp)
        self.check("3", before["canRequest"], "Retry is offered after a final attempt")
        s, body = self.request(fp)
        self.check("3", s == 202 and body["attempt"] == before["nextAttempt"], f"retry -> {s} {body}")
        self.wait_attempt(fp, body["attempt"])
        attempts = [int(x["attempt"]) for x in self.view(fp)["attempts"]]
        self.check("3", len(attempts) >= 2 and attempts == sorted(set(attempts), reverse=True),
                   f"card lists attempts {attempts}")

    def step4(self):
        cols = {r["col_name"] for r in self.sql.query(f"DESCRIBE TABLE {self.cfg.table('llm_calls')}")
                if r["col_name"] and not r["col_name"].startswith("#")}
        self.check("4", cols == LLM_CALL_COLUMNS, f"llm_calls columns {sorted(cols)}")
        calls = self.sql.query(f"SELECT prompt_hash, response_hash, input_tokens, output_tokens, status "
                               f"FROM {self.cfg.table('llm_calls')} ORDER BY called_at DESC LIMIT 50")
        ok = [c for c in calls if c["status"] == "ok"]
        self.check("4", bool(ok) and all(len(c["prompt_hash"]) == 64 and len(c["response_hash"] or "") == 64
                                         and c["input_tokens"] and c["output_tokens"] for c in ok),
                   f"{len(ok)} model calls hold hashes and token counts")
        # The payload the fixer sends, rebuilt with the same code: none of the sampled values are in it.
        fp = self.finding()
        f = self.sql.query(f"SELECT * FROM {self.cfg.table('findings')} WHERE fingerprint = :fp", {"fp": fp})[0]
        hist = dcontext.load_history(self.sql, self.cfg, f)
        norm, _ = normalize(hist["query"])
        original = shape(norm)
        import psycopg
        from workload import conninfo
        with psycopg.connect(conninfo(self.a.profile, self.ep), autocommit=True, prepare_threshold=None) as conn:
            for stmt in FIXER_SETUP("draft_context"):
                conn.execute(stmt)
            tables = dcontext.load_catalog(conn, original.tables)
        payload = json.dumps(dprompt.messages(dcontext.build_context(f, norm, original.ordered, hist["plan"], tables)))
        values = {v for s in hist["param_sets"] for v in s if v}
        self.check("4", values and not any(v in payload for v in values),
                   f"payload holds none of {len(values)} sampled values")

    def step5(self):
        left = [b.name for b in self.w.postgres.list_branches(parent=self.project) if "/branches/lbh-verify-" in b.name]
        self.check("5", not left, f"lbh-verify-* branches left: {left}")
        fp = self.finding()
        ids = {x["draft_id"] for x in self.view(fp)["attempts"]} | self.drafts
        id_list = ",".join(f"'{i}'" for i in sorted(ids))
        rows = self.sql.query(f"SELECT event_id, action_id, event_type, actor, payload, "
                              f"date_format(ts, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\") AS ts, prev_hash, hash "
                              f"FROM {self.cfg.table('ledger')} WHERE action_id IN ({id_list}) ORDER BY action_id, ts")
        from datetime import datetime, timezone
        for r in rows:
            r["ts"] = datetime.fromisoformat(r["ts"]).replace(tzinfo=timezone.utc)
        self.check("5", bool(rows) and ledger.verify(rows), f"ledger chain over {len(rows)} events, {len(ids)} drafts")
        sp = sp_token(self.w.config.host, self.a.test_sp_id, os.environ[self.a.test_sp_secret_env])
        s, body = self.request(fp, sp)
        self.check("5", s == 403, f"non-manager draft request -> {s} {body}")

    def step6(self):
        fp = self.finding()
        verified = [x for x in self.view(fp)["attempts"] if x["state"] in ("verified", "adopted")]
        if not verified:
            raise RuntimeError("no verified rewrite; run step 1")
        d = verified[0]
        if d["state"] != "adopted":
            with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
                f.write(d["sql"])
            subprocess.run([sys.executable, str(ROOT / "scripts" / "workload.py"), "--profile", self.a.profile,
                            "--endpoint", self.ep, "--minutes", str(self.a.workload_minutes), "--mode", "rewrite",
                            "--rewrite-file", f.name], check=True)
            for _ in range(2):  # system.lakebase can lag one engine run behind
                self.w.jobs.run_now(job_id=self.job("lakebase-health-engine")).result(timeout=timedelta(minutes=90))
                if self._state(fp, d["draft_id"]) == "adopted":
                    break
        state = self._state(fp, d["draft_id"])
        self.check("6", state == "adopted", f"rewrite {d['draft_id'][:8]} is {state}")


def main():
    ap = argparse.ArgumentParser()
    for a in ("--profile", "--warehouse", "--catalog", "--project-uid", "--test-sp-id", "--test-sp-secret-env"):
        ap.add_argument(a, required=True)
    ap.add_argument("--steps", default="0,1,2,3,4,5,6")
    ap.add_argument("--workload-minutes", type=float, default=15)
    a = ap.parse_args()
    acc = Acceptance(a)
    for step in a.steps.split(","):
        try:
            getattr(acc, f"step{step.strip()}")()
        except Exception as e:
            acc.check(step, False, f"{type(e).__name__}: {e}")
    print(f"drafts touched: {sorted(acc.drafts)}")
    print("\n".join(["FAILURES:"] + acc.failures) if acc.failures else "ALL LLM-REWRITE ACCEPTANCE CHECKS PASSED")
    sys.exit(1 if acc.failures else 0)


if __name__ == "__main__":
    main()
```

Run: `engine/.venv/bin/python scripts/llm_acceptance.py --help`
Expected: usage text.

- [ ] **Step 2: Deploy, run the engine once, grant**

```bash
VARS=(--var catalog=my_catalog --var engine_sp=00000000-0000-4000-8000-0000000000e1 \
      --var warehouse_id=0123456789abcdef --var fixer_sp=<fixer app id>)
databricks bundle deploy "${VARS[@]}" --profile my-profile
databricks bundle run lakebase_health_engine "${VARS[@]}" --profile my-profile   # adds the tables and columns
PROFILE=my-profile CATALOG=my_catalog ENGINE_SP=00000000-0000-4000-8000-0000000000e1 FIXER_SP=<fixer app id> \
  APP_SP=00000000-0000-4000-8000-0000000000b1 WAREHOUSE=0123456789abcdef bash scripts/fixer_grants.sh
```
Expected: deploy succeeds (the jobs install `sqlglot`; if a job fails to install it, stop and report — see the sqlglot decision); the engine run is `SUCCESS`; the grants script prints the foundation-model note for `databricks-claude-opus-5-5` and `grants done`. Read the fixer SP id from the fix-loop setup (`databricks service-principals list --filter 'displayName eq "lakebase-health-fixer"'`).

- [ ] **Step 3: Run acceptance**

Run: `TEST_SP_SECRET=$(cat /tmp/test_sp_secret) engine/.venv/bin/python scripts/llm_acceptance.py --profile my-profile --warehouse 0123456789abcdef --catalog my_catalog --project-uid 11111111-2222-4333-8444-555555555555 --test-sp-id $(cat /tmp/test_sp_app) --test-sp-secret-env TEST_SP_SECRET`
Expected: `ALL LLM-REWRITE ACCEPTANCE CHECKS PASSED` (about 2 hours: two 15-minute workloads and up to three engine runs). If criterion 1 fails because the model's rewrite was not verified, re-run `--steps 3` (Retry) once and report both attempts; do not loosen a rule.

- [ ] **Step 4: Commit**

```bash
git add scripts/llm_acceptance.py
git commit -m "test(draft): end-to-end LLM rewrite acceptance on healthcheck

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

