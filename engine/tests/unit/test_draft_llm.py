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


def test_sdk_query_sends_2000_tokens_and_no_temperature():
    """Live: databricks-claude-opus-5-5 rejects the temperature parameter with 400 BAD_REQUEST."""
    seen = {}
    w = NS(serving_endpoints=NS(query=lambda **kw: seen.update(kw) or reply("{}")))
    sdk_query(w)("ep", MSGS)
    assert seen["name"] == "ep" and "temperature" not in seen and seen["max_tokens"] == 2000
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


def test_reasoning_blocks_are_not_part_of_the_reply():
    """Live: the model answers with a reasoning block (summary, signature) followed by the text block."""
    resp = NS(choices=[NS(message=NS(content=[{"type": "reasoning", "summary": [{"type": "summary_text", "text": "",
                                                                                 "signature": "abc"}]},
                                              {"type": "text", "text": '{"rewrite": null, "reason": "x"}'}]))],
              usage=NS(prompt_tokens=10, completion_tokens=5), id="r")
    rows = []
    out = ModelClient(lambda e, m: resp, "ep").complete(MSGS, "d1", rows.append)
    assert out == '{"rewrite": null, "reason": "x"}' and rows[0]["output_tokens"] == 5
