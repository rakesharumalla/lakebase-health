from datetime import datetime, timedelta, timezone
from lakebase_health.actions import plan_actions
from lakebase_health.models import Action

NOW = datetime(2026, 10, 28, tzinfo=timezone.utc)


def action(state):
    return Action("aid", "fp", "missing_fk_index", "ddl", "CREATE INDEX CONCURRENTLY i ON t (c);", "DROP INDEX CONCURRENTLY IF EXISTS i;",
                  "moderate", True, True, state, NOW, NOW, NOW + timedelta(days=7))


class F:
    fingerprint, check_id, status, candidate_sql, rollback_sql = "fp", "missing_fk_index", "open", "CREATE INDEX CONCURRENTLY i ON t (c);", None
    rollback_note = None


class Fix:
    kind, risk, reversible, verify_on_branch, actionable = "ddl", "moderate", True, True, True


class Spec:
    fix = Fix()


def test_actions_in_fixer_states_are_never_re_proposed_or_expired():
    for state in ("verifying", "verified", "applying", "applied", "regressed", "rolling_back"):
        changed, events = plan_actions([F()], {"aid": action(state)}, {"missing_fk_index": Spec()}, NOW)
        assert changed == [] and events == [], state
