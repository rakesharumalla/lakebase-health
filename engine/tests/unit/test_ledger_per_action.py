from datetime import datetime, timezone
from lakebase_health.ledger import chain, verify

NOW = datetime(2026, 10, 28, tzinfo=timezone.utc)


def test_events_link_to_the_previous_event_of_the_same_action():
    rows = chain({"a": "h_a", "b": None}, [
        {"action_id": "a", "event_type": "verify_requested", "payload": {}},
        {"action_id": "b", "event_type": "proposed", "payload": {}},
        {"action_id": "a", "event_type": "verified", "payload": {}},
    ], NOW, actor="fixer")
    assert rows[0]["prev_hash"] == "h_a" and rows[1]["prev_hash"] is None
    assert rows[2]["prev_hash"] == rows[0]["hash"]
    assert all(r["actor"] == "fixer" for r in rows)


def test_verify_checks_each_action_chain_and_detects_tampering():
    rows = chain({}, [{"action_id": "a", "event_type": "x", "payload": {"n": 1}},
                      {"action_id": "b", "event_type": "y", "payload": {}},
                      {"action_id": "a", "event_type": "z", "payload": {}}], NOW)
    assert verify(rows)
    rows[2]["payload"] = '{"n": 2}'
    assert not verify(rows)


def test_events_in_one_batch_get_strictly_increasing_ts_and_last_hash_has_greatest_ts():
    rows = chain({}, [{"action_id": "a", "event_type": "verify_started", "payload": {}},
                      {"action_id": "a", "event_type": "verified", "payload": {}}], NOW)
    assert rows[0]["ts"] < rows[1]["ts"]
    assert max(rows, key=lambda r: r["ts"])["hash"] == rows[-1]["hash"]
    assert verify(rows)
