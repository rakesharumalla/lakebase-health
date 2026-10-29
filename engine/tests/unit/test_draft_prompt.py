from lakebase_health.fixer.draft.prompt import PROMPT_VERSION, SYSTEM, context_hash, messages, parse_response, prompt_hash


def test_the_versioned_system_prompt_states_the_rules():
    assert PROMPT_VERSION == "rewrite_v2" and "generic execution plan" in SYSTEM
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
