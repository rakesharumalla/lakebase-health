"""The versioned rewrite prompt, the message list sent to the model, and parsing of its reply."""
import hashlib
import json
import re
from importlib.resources import files

PROMPT_VERSION = "rewrite_v2"  # v2: the plan is the generic plan (spec §10)
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
