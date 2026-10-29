# Lakebase Health — Read-only App (sub-project 3a) Design

**Date:** 2026-10-28  **Status:** Draft for review  **Related:** `2026-10-27-lakebase-health-engine-design.md` (engine, tables), `DESIGN.md` (Option C)

## 1. Goal and scope

A Databricks App that lets each user see the health of the Lakebase projects they have access to: a fleet
overview, a page per project, finding detail with evidence and history, and an incident timeline for one endpoint.
It reads only the engine's tables. It never connects to a customer database and never changes anything.

Build order (changed by the user on 2026-10-28): engine → **this read-only app** → fix loop (adds approval cards
to this app) → LLM layer.

**Out of scope:** approvals, applying fixes, branch creation, LLM calls, alert delivery, editing suppressions.

### Decisions this spec builds on

| Topic | Decision |
|---|---|
| Audience | Per-user scoping: each user sees only the Lakebase projects they have permission on; workspace admins see the fleet |
| Views | Fleet overview, project page, finding detail, incident timeline |
| Stack | AppKit (TypeScript server + React), deployed as an `apps` resource in the existing bundle |
| Data access | App service principal reads `<catalog>.lakebase_health.*` through a SQL warehouse; the server filters every query by the user's allowed projects |
| Rejected | Queries as the user with UC row filters: UC cannot see Lakebase project permissions, so the engine would need `CAN_MANAGE` on every project to mirror ACLs |
| Freshness | Data is as of the last engine run (≤ about 1 hour); every page shows it |

## 2. Architecture

```
Browser ──► Databricks App "lakebase-health" (AppKit: Node server + React)
              │
              ├─ user token (forwarded by Apps, user authorization)
              │     ├─ Lakebase API: list projects the user can see ──► allowed project_ids (cached 5 min per user)
              │     └─ SCIM /Me: groups ──► show query text only if member of databricks_pii_access
              │
              └─ app service principal ──► SQL warehouse ──► <catalog>.lakebase_health.*
                       (SELECT only; every query takes :project_ids and filters on it server-side)
```

| Unit | Responsibility |
|---|---|
| `server/access` | Resolves the user's allowed project IDs and PII membership from the forwarded user token; per-user cache (5 min) |
| `server/queries/*.sql` | One parameterized SELECT/WITH file per view; all take `:project_ids` |
| `server/routes` | Validates parameters, calls `access`, runs the query with the app SP, applies masking, returns JSON |
| `server/mask` | Replaces text fields (`query_text`, plan and statement text in evidence) with `"[masked]"` for non-PII users |
| `client/` | React views below, built from AppKit components and charts |

- Code lives in `app/`; the bundle gains an `apps` resource with the SQL warehouse as an app resource and
  user-authorization scopes for the Lakebase API, identity and SQL.
- The app SP gets `USE CATALOG`, `USE SCHEMA` and `SELECT` on the `lakebase_health` schema only. It needs no
  Lakebase project permissions and no Postgres role.
- Candidate fixes are text with a copy button and a note that one-click approval arrives with the fix loop.

## 3. Views

1. **Fleet overview** (`/`): header with the last engine run, its status and data freshness. Table of visible
   projects with open critical / warning / info counts, newest finding, computes (active vs. suspended) and
   whether Postgres checks are enrolled. Sorted by criticals, then warnings. Filters: category, severity,
   "new since last visit" (stored in the browser).
2. **Project page** (`/projects/:id`): findings grouped by category (severity, title, object, first/last seen,
   occurrences, confidence); resolved findings of the last 7 days in a collapsed list. Computes panel: state, CU
   range, suspend timeout, HA, restore window, branch protection, enrolled or not, with the one-line enrolment
   instructions when not enrolled.
3. **Finding detail** (`/findings/:fingerprint`): explanation; measure-over-time chart from `check_results`;
   evidence as key/value pairs (query text masked per §4); candidate SQL and rollback as copyable text with risk
   and verify-on-branch flags; the shadow-mode verdict (what an L3 policy would have done and why); related
   findings on the same endpoint; for `hot_row_lock`, `commit_quorum_wait`, `statement_timeouts`,
   `gin_cleanup_lock_wait` and `connection_drops`, a link to the incident timeline for the finding's window.
4. **Incident timeline** (`/endpoints/:id/timeline?from&to`): default last 6 hours, maximum 7 days. One shared
   time axis with tracks: active sessions by wait class; lock waits coloured by blocker class (commit wait /
   idle in transaction / active / not sampled, using the `hot_row_lock` join); `SyncRep` commit waits; statement
   timeouts and connection drops as markers; CPU and connections from `compute_gauges`. Clicking a second lists
   the sampled sessions (query ID, wait event, blocker PID, ages; query text masked per §4). All timeline queries
   are bounded by `(project_id, endpoint_id, ts)` to use the clustered copy.

## 4. Access, security, errors

- **Scoping.** The Lakebase API lists and describes every workspace project to every workspace user, and only
  project managers can read project ACLs, so the project list is not a permission signal (found in acceptance on
  2026-10-28). A project is visible to a user when they are a workspace admin (`admins` group), own it
  (`status.owner`), or hold a Lakebase Postgres role on any of its branches as their user/service principal or
  through a group they belong to: the identity that can read its data. This is computed with the forwarded user
  token and cached per user for 5 minutes ("refresh access" clears it). A project whose roles cannot be read is
  hidden; a failed project listing fails the request. Route parameters are checked against the visible list;
  anything else returns 404.
- **Masking.** Text fields are returned only to members of `databricks_pii_access` (checked with the user's
  token); others get `"[masked]"`, applied on the server. The engine's UC column masks remain the second layer.
- **Input validation.** Project and endpoint IDs must be in the allowed list; fingerprints are 32-char hex; times
  are ISO 8601 and clamped to the last 7 days and a 7-day maximum span. SQL uses named parameters only.
- **Read-only.** The app SP has only SELECT; there is no write SQL; a test fails if any SQL file in `app/` does
  not start with SELECT or WITH.

| Failure | Behaviour |
|---|---|
| Warehouse cold start | Loading state; one retry when a query runs past 50 s |
| Lakebase API or SCIM call fails | Error page for that request; never fall back to showing all projects or unmasked text |
| No visible projects | Empty fleet view explaining how access works |
| Engine never ran / tables missing | Setup page pointing to the README install steps |
| Last successful run older than 2 schedule intervals | Banner on every page |

## 5. Testing and acceptance

- **Unit (Vitest, server):** scoping (allowed list always passed; disallowed ID → 404; empty list → empty
  result), masking for PII and non-PII users, parameter validation and clamping, SQL files SELECT/WITH only,
  stale-engine detection. Fakes for the Lakebase API, SCIM and the warehouse.
- **Query tests:** every SQL file runs against `my_catalog.lakebase_health` on `my-profile` with healthcheck
  allowed; checks the columns and that each finishes in < 5 s (timeline over a 7-day window).
- **UI:** component tests for the four views on a recorded healthcheck snapshot, including masked/unmasked and
  empty states.

**Acceptance criteria (deployed with the bundle on `my-profile`):**
1. The project owner (workspace admin) sees the fleet; a test service principal with `CAN_USE` on healthcheck
   only, calling the app's API with its own token, sees only healthcheck, and a request for another project
   returns 404.
2. healthcheck's planted findings appear on its project page; counts match the `findings` table.
3. The incident timeline for the workload window shows the idle-in-transaction lock waits and the statement
   timeout markers.
4. Query text is masked for the test principal and shown to a `databricks_pii_access` member. If the group does
   not exist in the workspace, this is verified with the group lookup faked and recorded as a limitation.
5. The app SP cannot write: an INSERT into `findings` as the app SP fails with a permission error.
6. Fleet, project and finding pages respond in < 3 s with a warm warehouse.

## 6. Open items (not blocking)

- Visual design (layout and theme) follows AppKit defaults; revisit with the fix loop's approval cards.
- Whether "new since last visit" should move server-side when users want it across devices.
