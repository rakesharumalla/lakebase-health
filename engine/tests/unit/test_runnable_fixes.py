"""Final fixes B1, B2, B6: every proposed action can run; rollbacks are allowlisted or a plain note."""
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from lakebase_health.actions import plan_actions
from lakebase_health.checkspec import CheckSpecError, load_checks, parse_check, render_template, template_values
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.fixer.allowlist import NotAllowed, parse, runnable
from lakebase_health.models import Action, Finding

CHECKS = Path(__file__).resolve().parents[2] / "src" / "lakebase_health" / "checks"
NOW = datetime(2026, 10, 29, tzinfo=timezone.utc)
SPECS = {s.id: s for s in load_checks(CHECKS, frozenset(API_CHECKS))}

# pg_get_indexdef output shapes, and identifier-like values for every other placeholder
INDEX_DEFS = ["CREATE INDEX customers_email_idx2 ON hc_test.customers USING btree (email)",
              'CREATE INDEX "Odd Name" ON "Hc"."Cust" USING btree ("Email", region DESC)']
SAMPLE = {"table_ident": "hc_test.t", "index_ident": "hc_test.i", "index_name": "i", "index_schema": "hc_test",
          "child_table": "hc_test.t", "fk_columns": "a, b", "schema_ident": "public", "role_ident": '"app"',
          "fix_sql": "ALTER DATABASE db SET idle_in_transaction_session_timeout = '60s';",
          "fix_rollback": "ALTER DATABASE db RESET idle_in_transaction_session_timeout;"}


def _render(template, index_def=INDEX_DEFS[0]):
    names = set(re.findall(r"\{([a-z_]+)\}", template or ""))
    row = template_values({**SAMPLE, "index_def": index_def})
    return render_template(template, {n: row[n] for n in names})


def test_index_def_concurrently_inserts_concurrently_after_index():
    v = template_values({"index_def": INDEX_DEFS[0]})
    assert v["index_def_concurrently"] == ("CREATE INDEX CONCURRENTLY customers_email_idx2 ON hc_test.customers "
                                           "USING btree (email)")
    u = template_values({"index_def": "CREATE UNIQUE INDEX u ON s.t USING btree (a)"})
    assert u["index_def_concurrently"] == "CREATE UNIQUE INDEX CONCURRENTLY u ON s.t USING btree (a)"
    with pytest.raises(NotAllowed):  # the allowlist never accepts UNIQUE, so such a drop is not proposed
        parse(u["index_def_concurrently"])
    assert "index_def_concurrently" not in template_values({"index_def": None})


@pytest.mark.parametrize("check_id", ["unused_index", "duplicate_index", "redundant_index"])
@pytest.mark.parametrize("index_def", INDEX_DEFS)
def test_drop_index_rollbacks_are_allowlisted_create_index_concurrently(check_id, index_def):
    rb = _render(SPECS[check_id].fix.rollback, index_def)
    st = parse(rb)
    assert st.kind == "create_index" and rb.startswith("CREATE INDEX CONCURRENTLY ")


def test_every_runnable_fix_has_a_runnable_rollback_or_a_note():
    """Checks whose fix the allowlist accepts: the rollback is accepted too, or empty with a rollback_note."""
    runnable_checks = []
    for s in SPECS.values():
        if not (s.fix and s.fix.actionable) or not runnable(_render(s.fix.template)):
            continue
        runnable_checks.append(s.id)
        if s.fix.rollback:
            assert runnable(_render(s.fix.rollback)), s.id
        else:
            assert s.fix.rollback_note, s.id
    assert set(runnable_checks) >= {"missing_fk_index", "unused_index", "duplicate_index", "redundant_index",
                                    "stale_statistics", "autovacuum_disabled", "autovacuum_scale_factor"}


@pytest.mark.parametrize("check_id", ["dead_tuples", "index_bloat", "xid_age_table", "public_schema_create",
                                      "guardrail_settings", "disk_spill"])
def test_hand_applied_checks_keep_their_sql_but_are_not_runnable(check_id):
    fix = SPECS[check_id].fix
    assert fix.template and not runnable(_render(fix.template))


def test_nothing_to_undo_fixes_use_a_note_not_a_comment():
    for check_id in ("dead_tuples", "index_bloat", "xid_age_table", "stale_statistics"):
        fix = SPECS[check_id].fix
        assert fix.rollback is None and fix.rollback_note.startswith("Not needed:"), check_id
    for s in SPECS.values():
        assert not (s.fix and (s.fix.rollback or "").lstrip().startswith(("--", "/*"))), s.id


def _spec(fix):
    return parse_check({"id": "c", "title": "C", "category": "c", "source": "postgres", "explain": "e",
                        "sql": "SELECT 1", "thresholds": {"warning": 0}, "fix": fix}, stem="c")


def test_comment_only_rollback_is_rejected_and_a_note_satisfies_reversible():
    base = {"kind": "ddl", "template": "ANALYZE {table_ident};", "risk": "safe", "reversible": True}
    with pytest.raises(CheckSpecError, match="rollback"):
        _spec({**base, "rollback": "-- nothing to roll back"})
    with pytest.raises(CheckSpecError, match="rollback"):
        _spec(base)
    assert _spec({**base, "rollback_note": "Not needed: ANALYZE changes no data"}).fix.rollback_note


def test_allowlist_runnable():
    assert runnable("ANALYZE s.t;") and not runnable("-- VACUUM changes no data") and not runnable("")
    assert not runnable(None) and not runnable("VACUUM (ANALYZE) s.t;")


# --- plan_actions ---

def _finding(check_id, sql, rollback, note=None, fp="f1", status="open"):
    return Finding(fp, check_id, "T", "c", "p", "b", "e", "o", status, "warning", "high", 1.0, NOW, NOW, NOW, 1, 0,
                   "{}", None, "e", sql, rollback, rollback_note=note)


def test_only_allowlisted_fixes_become_actions():
    vacuum = _finding("dead_tuples", "VACUUM (ANALYZE) hc_test.t;", None, "Not needed: VACUUM changes no data")
    assert plan_actions([vacuum], {}, SPECS, NOW) == ([], [])
    plain_rollback = _finding("unused_index", "DROP INDEX CONCURRENTLY hc_test.i;", "CREATE INDEX i ON hc_test.t (a);")
    assert plan_actions([plain_rollback], {}, SPECS, NOW) == ([], [])
    ok = _finding("stale_statistics", "ANALYZE hc_test.t;", None, "Not needed: ANALYZE changes no data")
    [a], [ev] = plan_actions([ok], {}, SPECS, NOW)
    assert a.rollback_sql is None and a.rollback_note == "Not needed: ANALYZE changes no data"
    assert ev["payload"]["rollback_note"] == a.rollback_note


def test_an_existing_proposed_action_that_cannot_run_expires():
    old = Action("x1", "f1", "dead_tuples", "ddl", "VACUUM (ANALYZE) hc_test.t;", "-- nothing", "safe", True, False,
                 "proposed", NOW, NOW, NOW)
    vacuum = _finding("dead_tuples", "VACUUM (ANALYZE) hc_test.t;", None, "Not needed: VACUUM changes no data")
    [a], [ev] = plan_actions([vacuum], {"x1": old}, SPECS, NOW)
    assert a.state == "expired" and ev["payload"] == {"reason": "not_runnable"}


@pytest.mark.parametrize("state", ["proposed", "verify_failed", "verified", "apply_failed"])
def test_a_stored_rollback_is_refreshed_before_apply(state):
    """Live 6b7a38d0: the stored rollback is a plain CREATE INDEX the allowlist rejects; once the check renders
    CREATE INDEX CONCURRENTLY the action gets it, so an approved rollback can run."""
    drop = "DROP INDEX CONCURRENTLY hc_test.customers_region_idx;"
    old = Action("x1", "f1", "redundant_index", "ddl", drop,
                 "CREATE INDEX customers_region_idx ON hc_test.customers USING btree (region);", "high", True, True,
                 state, NOW, NOW, NOW)
    new_rb = "CREATE INDEX CONCURRENTLY customers_region_idx ON hc_test.customers USING btree (region);"
    f = _finding("redundant_index", drop, new_rb)
    from lakebase_health.actions import _action_id
    old.action_id = _action_id("f1", drop)
    changed, events = plan_actions([f], {old.action_id: old}, SPECS, NOW)
    [a] = changed
    assert a.rollback_sql == new_rb and a.state == state and a.action_id == old.action_id
    assert [e["event_type"] for e in events] == ["rollback_updated"]


@pytest.mark.parametrize("state", ["verifying", "applying", "applied", "regressed", "rolling_back", "blocked"])
def test_a_rollback_is_never_rewritten_in_flight_or_after_apply(state):
    drop = "DROP INDEX CONCURRENTLY hc_test.i;"
    from lakebase_health.actions import _action_id
    old = Action(_action_id("f1", drop), "f1", "unused_index", "ddl", drop, "CREATE INDEX i ON hc_test.t (a);",
                 "high", True, True, state, NOW, NOW, NOW)
    f = _finding("unused_index", drop, "CREATE INDEX CONCURRENTLY i ON hc_test.t (a);")
    assert plan_actions([f], {old.action_id: old}, SPECS, NOW) == ([], [])


@pytest.mark.parametrize("state", ["rolled_back", "rejected", "confirmed"])
def test_a_decided_action_is_never_overwritten_by_a_new_proposal_with_the_same_id(state):
    """Live 6b7a38d0: after the rollback recreated the index the finding reopened, and the next run re-proposed the
    same SQL under the same action id, overwriting rolled_back with proposed."""
    from lakebase_health.actions import _action_id
    drop = "DROP INDEX CONCURRENTLY hc_test.i;"
    rb = "CREATE INDEX CONCURRENTLY i ON hc_test.t (a);"
    old = Action(_action_id("f1", drop), "f1", "unused_index", "ddl", drop, rb, "high", True, True, state,
                 NOW, NOW, NOW)
    assert plan_actions([_finding("unused_index", drop, rb)], {old.action_id: old}, SPECS, NOW) == ([], [])
