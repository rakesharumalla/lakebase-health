from datetime import datetime, timedelta, timezone

from lakebase_health.actions import plan_actions, policy_level, shadow_rows
from lakebase_health.checkspec import parse_check
from lakebase_health.ledger import chain, verify
from lakebase_health.models import Finding

NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
FIX = {"kind": "ddl", "template": "CREATE INDEX CONCURRENTLY ON s.t (x);", "rollback": "DROP INDEX CONCURRENTLY IF EXISTS s.x;", "risk": "moderate",
       "reversible": True, "verify_on_branch": True}
SPECS = {
    "a": parse_check({"id": "a", "title": "A", "category": "c", "source": "postgres", "explain": "e",
                      "sql": "SELECT 1", "thresholds": {"warning": 0}, "fix": FIX}, stem="a"),
    "m": parse_check({"id": "m", "title": "M", "category": "c", "source": "postgres", "explain": "e",
                      "sql": "SELECT 1", "thresholds": {"warning": 0},
                      "fix": {"kind": "manual", "template": "do it", "risk": "high"}}, stem="m"),
}


def finding(check="a", status="new", sql="CREATE INDEX CONCURRENTLY ON s.t (x);", fp="f1"):
    return Finding(fp, check, "T", "c", "p", "b", "e", "o", status, "warning", "high", 1.0, NOW, NOW, NOW, 1, 0,
                   "{}", None, "e", sql, "DROP INDEX CONCURRENTLY IF EXISTS s.x;")


def test_policy_levels():
    assert policy_level("safe", True, False) == "L3"
    assert policy_level("safe", True, True) == "L2"
    assert policy_level("moderate", True, True) == "L2"
    assert policy_level("high", True, True) == "L1"
    assert policy_level("safe", False, False) == "L1"


def test_new_actionable_finding_proposes_once():
    actions, events = plan_actions([finding()], {}, SPECS, NOW)
    assert len(actions) == 1 and actions[0].state == "proposed" and events[0]["event_type"] == "proposed"
    again, events2 = plan_actions([finding(status="open")], {actions[0].action_id: actions[0]}, SPECS, NOW)
    assert events2 == [] and again[0].expires_at == NOW + timedelta(days=7)


def test_manual_or_missing_sql_never_proposes():
    assert plan_actions([finding(check="m")], {}, SPECS, NOW) == ([], [])
    assert plan_actions([finding(sql=None)], {}, SPECS, NOW) == ([], [])


def test_resolved_finding_expires_action_and_changed_sql_replaces_it():
    [a], _ = plan_actions([finding()], {}, SPECS, NOW)
    expired, ev = plan_actions([finding(status="resolved")], {a.action_id: a}, SPECS, NOW)
    assert expired[0].state == "expired" and ev[0]["event_type"] == "expired"
    [b], _ = plan_actions([finding()], {}, SPECS, NOW)  # fresh proposed action (a is now expired)
    replaced, ev2 = plan_actions([finding(sql="CREATE INDEX CONCURRENTLY ON s.t (y);")], {b.action_id: b}, SPECS, NOW)
    assert {x.state for x in replaced} == {"expired", "proposed"} and len(ev2) == 2


def test_shadow_rows():
    [a], _ = plan_actions([finding()], {}, SPECS, NOW)
    [row] = shadow_rows([a], "r1", NOW)
    assert row["would_allow_level"] == "L2" and row["would_execute"] is False


def test_ledger_chain_and_tamper_detection():
    rows = chain({}, [{"action_id": "a1", "event_type": "proposed", "payload": {"x": 1}}], NOW)
    rows += chain({"a1": rows[-1]["hash"]}, [{"action_id": "a1", "event_type": "expired", "payload": {}}], NOW)
    assert verify(rows) and rows[1]["prev_hash"] == rows[0]["hash"]
    rows[0]["payload"] = '{"x": 2}'
    assert not verify(rows)
