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
        content = "".join(b.get("text", "") for b in content
                          if isinstance(b, dict) and b.get("type", "text") == "text")  # not reasoning blocks
    usage = resp.usage
    return (content or "", getattr(usage, "prompt_tokens", None), getattr(usage, "completion_tokens", None),
            getattr(resp, "id", None))


def sdk_query(w):
    """The serving call through the SDK. `w` should carry http_timeout_seconds=TIMEOUT_S (see jobs/fix.py)."""
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

    def query(endpoint, msgs):
        # No temperature: databricks-claude-opus-5-5 rejects the parameter (400 BAD_REQUEST).
        return w.serving_endpoints.query(name=endpoint, max_tokens=MAX_TOKENS,
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
